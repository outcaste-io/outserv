// Portions Copyright 2017-2018 Dgraph Labs, Inc. are available under the Apache License v2.0.
// Portions Copyright 2022 Outcaste LLC are available under the Sustainable License v1.0.

package worker

import (
	"context"

	"github.com/pkg/errors"
	otrace "go.opencensus.io/trace"

	"github.com/outcaste-io/outserv/conn"
	"github.com/outcaste-io/outserv/protos/pb"
	"github.com/outcaste-io/outserv/schema"
	"github.com/outcaste-io/outserv/types"
)

var (
	emptySchemaResult pb.SchemaResult
)

type resultErr struct {
	result *pb.SchemaResult
	err    error
}

// getSchema iterates over all predicates and populates the asked fields, if list of
// predicates is not specified, then all the predicates belonging to the group
// are returned
func getSchema(ctx context.Context, s *pb.SchemaRequest) (*pb.SchemaResult, error) {
	_, span := otrace.StartSpan(ctx, "worker.getSchema")
	defer span.End()

	var result pb.SchemaResult
	var predicates []string
	var fields []string
	if len(s.Predicates) > 0 {
		predicates = s.Predicates
	} else {
		predicates = schema.State().Predicates()
	}
	if len(s.Fields) > 0 {
		fields = s.Fields
	} else {
		// TODO: Perhaps remove upsert as well.
		fields = []string{"type", "index", "tokenizer", "count", "list", "upsert",
			"noconflict"}
	}

	myGid := groups().groupId()
	for _, attr := range predicates {
		// This can happen after a predicate is moved. We don't delete predicate from schema state
		// immediately. So lets ignore this predicate.
		gid, err := groups().BelongsToReadOnly(attr, 0)
		if err != nil {
			return nil, err
		}
		if myGid != gid {
			continue
		}

		if schemaNode := populateSchema(attr, fields); schemaNode != nil {
			result.Schema = append(result.Schema, schemaNode)
		}
	}
	return &result, nil
}

// populateSchema returns the information of asked fields for given attribute
func populateSchema(attr string, fields []string) *pb.SchemaNode {
	var schemaNode pb.SchemaNode
	var typ types.TypeID
	var err error
	if typ, err = schema.State().TypeOf(attr); err != nil {
		// schema is not defined
		return nil
	}
	schemaNode.Predicate = attr
	ctx := context.Background()
	pred, _ := schema.State().Get(ctx, attr)

	for _, field := range fields {
		switch field {
		case "type":
			schemaNode.Type = typ.String()
		case "index":
			schemaNode.Index = len(pred.GetTokenizer()) > 0
		case "tokenizer":
			if len(pred.GetTokenizer()) > 0 {
				schemaNode.Tokenizer = schema.State().TokenizerNames(ctx, attr)
			}
		case "count":
			schemaNode.Count = pred.GetCount()
		case "list":
			schemaNode.List = pred.GetList()
		case "upsert":
			schemaNode.Upsert = pred.GetUpsert()
		default:
			//pass
		}
	}
	return &schemaNode
}

// addToSchemaMap groups the predicates by group id, if list of predicates is
// empty then it adds all known groups
func addToSchemaMap(schemaMap map[uint32]*pb.SchemaRequest, schema *pb.SchemaRequest) error {
	for _, attr := range schema.Predicates {
		gid, err := groups().BelongsToReadOnly(attr, 0)
		if err != nil {
			return err
		}
		if gid == 0 {
			continue
		}

		s := schemaMap[gid]
		if s == nil {
			s = &pb.SchemaRequest{GroupId: gid}
			s.Fields = schema.Fields
			schemaMap[gid] = s
		}
		s.Predicates = append(s.Predicates, attr)
	}
	if len(schema.Predicates) > 0 {
		// If the req had asked for specific predicates, then just return.
		return nil
	}
	// No specific predicates asked for. Fetch all the predicates we know about.
	gids := KnownGroups()
	for _, gid := range gids {
		if gid == 0 {
			continue
		}
		s := schemaMap[gid]
		if s == nil {
			s = &pb.SchemaRequest{GroupId: gid}
			s.Fields = schema.Fields
			schemaMap[gid] = s
		}
	}
	return nil
}

// If the current node serves the group serve the schema or forward
// to relevant node
// TODO: Janardhan - if read fails try other servers serving same group
func getSchemaOverNetwork(ctx context.Context, gid uint32, s *pb.SchemaRequest, ch chan resultErr) {
	if groups().ServesGroup(gid) {
		schema, e := getSchema(ctx, s)
		ch <- resultErr{result: schema, err: e}
		return
	}

	pl := groups().Leader(gid)
	if pl == nil {
		ch <- resultErr{err: conn.ErrNoConnection}
		return
	}
	c := pb.NewWorkerClient(pl.Get())
	schema, e := c.Schema(ctx, s)
	ch <- resultErr{result: schema, err: e}
}

// GetSchemaOverNetwork checks which group should be serving the schema
// according to fingerprint of the predicate and sends it to that instance.
func GetSchemaOverNetwork(ctx context.Context, schema *pb.SchemaRequest) (
	[]*pb.SchemaNode, error) {

	ctx, span := otrace.StartSpan(ctx, "worker.GetSchemaOverNetwork")
	defer span.End()

	// There was a health check here which is not needed. The health check should be done by the
	// receiver of the request, not the sender.

	if len(schema.Predicates) == 0 && len(schema.Types) > 0 {
		return nil, nil
	}

	// Map of groupd id => Predicates for that group.
	schemaMap := make(map[uint32]*pb.SchemaRequest)
	if err := addToSchemaMap(schemaMap, schema); err != nil {
		return nil, err
	}

	results := make(chan resultErr, len(schemaMap))
	var schemaNodes []*pb.SchemaNode

	for gid, s := range schemaMap {
		go getSchemaOverNetwork(ctx, gid, s, results)
	}

	// wait for all the goroutines to reply back.
	// we return if an error was returned or the parent called ctx.Done()
	for i := 0; i < len(schemaMap); i++ {
		select {
		case r := <-results:
			if r.err != nil {
				return nil, r.err
			}
			schemaNodes = append(schemaNodes, r.result.Schema...)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	return schemaNodes, nil
}

// Schema is used to get schema information over the network on other instances.
func (w *grpcWorker) Schema(ctx context.Context, s *pb.SchemaRequest) (*pb.SchemaResult, error) {
	if ctx.Err() != nil {
		return &emptySchemaResult, ctx.Err()
	}

	if !groups().ServesGroup(s.GroupId) {
		return &emptySchemaResult, errors.Errorf("This server doesn't serve group id: %v", s.GroupId)
	}
	return getSchema(ctx, s)
}
