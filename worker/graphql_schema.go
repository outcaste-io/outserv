// Portions Copyright 2020 Dgraph Labs, Inc. are available under the Apache License v2.0.
// Portions Copyright 2022 Outcaste LLC are available under the Smart License v1.0.

package worker

import (
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"sync"
	"time"

	"google.golang.org/grpc/metadata"

	"github.com/golang/glog"

	"github.com/outcaste-io/outserv/conn"
	"github.com/outcaste-io/outserv/lambda"
	"github.com/outcaste-io/outserv/protos/pb"
	"github.com/outcaste-io/outserv/schema"
	"github.com/outcaste-io/outserv/x"
	"github.com/outcaste-io/outserv/zero"
	"github.com/pkg/errors"
)

// TODO(schartey/lambda): Move lambda in another file
const (
	errGraphQLSchemaCommitFailed = "error occurred updating GraphQL schema, please retry"
	ErrGraphQLSchemaAlterFailed  = "succeeded in saving GraphQL schema but failed to alter Dgraph schema - " +
		"GraphQL layer may exhibit unexpected behaviour, reapplying the old GraphQL schema may prevent any issues"

	GqlSchemaPred      = "dgraph.graphql.schema"
	gqlSchemaXidPred   = "dgraph.graphql.xid"
	gqlSchemaXidVal    = "dgraph.graphql.schema"
	LambdaScriptPred   = "dgraph.graphql.lambda.script"
	LambdaHashPred     = "dgraph.graphql.lambda.hash"
	LambdaXidPred      = "dgraph.graphql.lambda.xid"
	lambdaScriptXidVal = "dgraph.graphql.lambda.script"
	lambdaHashXidVal   = "dgraph.graphql.lambda.hash"
)

var (
	schemaLock                                  sync.Mutex
	errUpdatingGraphQLSchemaOnNonGroupOneLeader = errors.New(
		"while updating GraphQL schema: this server isn't group-1 leader, please retry")
	errUpdatingLambdaScriptOnNonGroupOneLeader = errors.New(
		"while updating lambda script: this server isn't group-1 leader, please retry")
	errUpdatingLambdaScriptNoChanges = errors.New(
		"while updating lambda script: no changes to apply")
	ErrMultipleGraphQLSchemaNodes = errors.New("found multiple nodes for GraphQL schema")
	gqlSchemaStore                *GQLSchemaStore
)

type GqlSchema struct {
	ID              string `json:"id,omitempty"`
	Schema          string `json:"schema,omitempty"`
	Version         uint64
	GeneratedSchema string
	Loaded          bool // This indicate whether the schema has been loaded into graphql server
	// or not
}

type GQLSchemaStore struct {
	mux    sync.RWMutex
	schema map[uint64]*GqlSchema
}

func NewGQLSchemaStore() *GQLSchemaStore {
	gqlSchemaStore = &GQLSchemaStore{
		mux:    sync.RWMutex{},
		schema: make(map[uint64]*GqlSchema),
	}
	return gqlSchemaStore
}

func (gs *GQLSchemaStore) Set(ns uint64, sch *GqlSchema) {
	gs.mux.Lock()
	defer gs.mux.Unlock()
	gs.schema[ns] = sch
}

func (gs *GQLSchemaStore) GetCurrent(ns uint64) (*GqlSchema, bool) {
	gs.mux.RLock()
	defer gs.mux.RUnlock()
	sch, ok := gs.schema[ns]
	return sch, ok
}

func (gs *GQLSchemaStore) resetGQLSchema() {
	gs.mux.Lock()
	defer gs.mux.Unlock()

	gs.schema = make(map[uint64]*GqlSchema)
}

func ResetGQLSchemaStore() {
	gqlSchemaStore.resetGQLSchema()
}

// UpdateGQLSchemaOverNetwork sends the request to the group one leader for execution.
func UpdateGQLSchemaOverNetwork(ctx context.Context, req *pb.UpdateGraphQLSchemaRequest) (*pb.
	UpdateGraphQLSchemaResponse, error) {
	if isGroupOneLeader() {
		return (&grpcWorker{}).UpdateGraphQLSchema(ctx, req)
	}

	pl := groups().Leader(1)
	if pl == nil {
		return nil, conn.ErrNoConnection
	}
	con := pl.Get()
	c := pb.NewWorkerClient(con)

	// pass on the incoming metadata to the group-1 leader
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		ctx = metadata.NewOutgoingContext(ctx, md)
	}

	return c.UpdateGraphQLSchema(ctx, req)
}

// UpdateLambdaScriptOverNetwork sends the request to the group one leader for execution.
func UpdateLambdaScriptOverNetwork(ctx context.Context, req *pb.UpdateLambdaScriptRequest) (*pb.
	UpdateLambdaScriptResponse, error) {
	if isGroupOneLeader() {
		return (&grpcWorker{}).UpdateLambdaScript(ctx, req)
	}

	pl := groups().Leader(1)
	if pl == nil {
		return nil, conn.ErrNoConnection
	}
	con := pl.Get()
	c := pb.NewWorkerClient(con)

	// pass on the incoming metadata to the group-1 leader
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		ctx = metadata.NewOutgoingContext(ctx, md)
	}

	return c.UpdateLambdaScript(ctx, req)
}

func ParseAsSchemaAndScript(b []byte) (string, string) {
	var data x.GQL
	if err := json.Unmarshal(b, &data); err != nil {
		glog.Warningf("Cannot unmarshal existing GQL schema into new format. Got err: %+v. "+
			" Assuming old format.", err)
		return string(b), ""
	}
	return data.Schema, data.Script
}

func ParseLambdaScript(b []byte) (*lambda.LambdaScript, error) {
	var script *lambda.LambdaScript
	if err := json.Unmarshal(b, &script); err != nil {
		glog.Warningf("Cannot unmarshal existing LambdaScript into new format. Got err: %+v.", err)
		return nil, err
	}
	return script, nil
}

// TODO(schartey/lambda): Split this from UpdateLambda
// UpdateGraphQLSchema updates the GraphQL schema node with the new GraphQL schema,
// and then alters the dgraph schema. All this is done only on group one leader.
func (w *grpcWorker) UpdateGraphQLSchema(ctx context.Context,
	req *pb.UpdateGraphQLSchemaRequest) (*pb.UpdateGraphQLSchemaResponse, error) {

	if !isGroupOneLeader() {
		return nil, errUpdatingGraphQLSchemaOnNonGroupOneLeader
	}

	ctx = x.AttachJWTNamespace(ctx)
	namespace, err := x.ExtractNamespace(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "While updating gql schema")
	}

	waitStart := time.Now()

	// lock here so that only one request is served at a time by group 1 leader
	schemaLock.Lock()
	defer schemaLock.Unlock()

	waitDuration := time.Since(waitStart)
	if waitDuration > 500*time.Millisecond {
		glog.Warningf("GraphQL schema update for namespace %d waited for %s as another schema"+
			" update was in progress.", namespace, waitDuration.String())
	}

	const schemaNodeUid = uint64(1) // It is always one.

	var gql x.GQL
	// Fetch the current graphql schema and script using the schema node uid.
	res, err := ProcessTaskOverNetwork(ctx, &pb.Query{
		Attr:    x.NamespaceAttr(namespace, GqlSchemaPred),
		UidList: &pb.List{SortedUids: []uint64{schemaNodeUid}},
		ReadTs:  req.StartTs,
	})
	if err != nil {
		return nil, err
	}
	if len(res.GetValueMatrix()) > 0 && len(res.ValueMatrix[0].GetValues()) > 0 {
		gql.Schema, gql.Script = ParseAsSchemaAndScript(res.ValueMatrix[0].Values[0].Val)
	}

	gql.Schema = req.GraphqlSchema
	val, err := json.Marshal(gql)
	if err != nil {
		return nil, err
	}

	// prepare GraphQL schema mutation
	m := &pb.Mutations{
		Edges: []*pb.DirectedEdge{
			{
				Entity:    schemaNodeUid,
				Attr:      x.NamespaceAttr(namespace, GqlSchemaPred),
				Value:     val,
				ValueType: pb.Posting_STRING,
				Op:        pb.DirectedEdge_SET,
			},
			{
				// if this server is no more the Group-1 leader and is mutating the GraphQL
				// schema node, also if concurrently another schema update is requested which is
				// being performed at the actual Group-1 leader, then mutating the xid with the
				// same value will cause one of the mutations to abort, because of the upsert
				// directive on xid. So, this way we make sure that even in this rare case there can
				// only be one server which is able to successfully update the GraphQL schema.
				Entity:    schemaNodeUid,
				Attr:      x.NamespaceAttr(namespace, gqlSchemaXidPred),
				Value:     []byte(gqlSchemaXidVal),
				ValueType: pb.Posting_STRING,
				Op:        pb.DirectedEdge_SET,
			},
			{
				Entity:    schemaNodeUid,
				Attr:      x.NamespaceAttr(namespace, "dgraph.type"),
				Value:     []byte("dgraph.graphql"),
				ValueType: pb.Posting_STRING,
				Op:        pb.DirectedEdge_SET,
			},
		},
	}
	glog.Infof("Sending GraphQL schema updates: %+v\n", m)

	// mutate the GraphQL schema. As it is a reserved predicate, and we are in group 1,
	// so this call is gonna come back to all the group 1 servers only
	_, err = MutateOverNetwork(ctx, m)
	if err != nil {
		return nil, err
	}

	glog.Infof("Now applying Dgraph Schema Updates: %+v\n", req.DgraphPreds)
	// perform dgraph schema alter, if required. As the schema could be empty if it only has custom
	// types/queries/mutations.
	if len(req.DgraphPreds) != 0 {
		if _, err = MutateOverNetwork(ctx, &pb.Mutations{
			Schema: req.DgraphPreds,
		}); err != nil {
			return nil, errors.Wrap(err, ErrGraphQLSchemaAlterFailed)
		}
		// busy waiting for indexing to finish
		if err = WaitForIndexing(ctx, true); err != nil {
			return nil, err
		}
	}

	// return the uid of the GraphQL schema node
	return &pb.UpdateGraphQLSchemaResponse{Uid: schemaNodeUid}, nil
}

func (w *grpcWorker) UpdateLambdaScript(ctx context.Context,
	req *pb.UpdateLambdaScriptRequest) (*pb.UpdateLambdaScriptResponse, error) {

	// TODO(schartey/lambda)
	if !isGroupOneLeader() {
		return nil, errUpdatingLambdaScriptOnNonGroupOneLeader
	}

	ctx = x.AttachJWTNamespace(ctx)
	namespace, err := x.ExtractNamespace(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "While updating gql schema")
	}

	waitStart := time.Now()

	// lock here so that only one request is served at a time by group 1 leader
	schemaLock.Lock()
	defer schemaLock.Unlock()

	waitDuration := time.Since(waitStart)
	if waitDuration > 500*time.Millisecond {
		glog.Warningf("LambdaScript update for namespace %d waited for %s as another lambda"+
			" update was in progress.", namespace, waitDuration.String())
	}

	var hash string
	if req.LambdaScript != nil {
		h := sha1.New()
		h.Write(req.LambdaScript)
		hash = base64.URLEncoding.EncodeToString(h.Sum(nil))
	}

	m := &pb.Mutations{}

	var uid uint64
	lambdaScript, ok := lambda.Instance(namespace).GetCurrentScript()
	if !ok {
		if assignedId, err := zero.AssignUids(ctx, 1); err != nil {
			return nil, err
		} else {
			uid = assignedId.StartId
		}
		lambdaScript = &lambda.LambdaScript{}
		// Create the empty script
		m.Edges = append(m.Edges, &pb.DirectedEdge{
			Entity:    uid,
			Attr:      x.NamespaceAttr(namespace, "dgraph.type"),
			Value:     []byte("dgraph.graphql.lambda"),
			ValueType: pb.Posting_STRING,
			Op:        pb.DirectedEdge_SET,
		})
	} else {
		uid = x.FromHex(lambdaScript.ID)

		if lambdaScript.Hash == hash {
			return nil, errUpdatingLambdaScriptNoChanges
		}
	}

	// prepare GraphQL schema mutation
	m.Edges = append(m.Edges, &pb.DirectedEdge{
		Entity:    uid,
		Attr:      x.NamespaceAttr(namespace, LambdaScriptPred),
		Value:     req.LambdaScript,
		ValueType: pb.Posting_BINARY,
		Op:        pb.DirectedEdge_SET,
	},
		&pb.DirectedEdge{
			Entity:    uid,
			Attr:      x.NamespaceAttr(namespace, LambdaHashPred),
			Value:     []byte(hash),
			ValueType: pb.Posting_STRING,
			Op:        pb.DirectedEdge_SET,
		},
		&pb.DirectedEdge{
			// if this server is no more the Group-1 leader and is mutating the GraphQL
			// schema node, also if concurrently another schema update is requested which is
			// being performed at the actual Group-1 leader, then mutating the xid with the
			// same value will cause one of the mutations to abort, because of the upsert
			// directive on xid. So, this way we make sure that even in this rare case there can
			// only be one server which is able to successfully update the GraphQL schema.
			Entity:    uid,
			Attr:      x.NamespaceAttr(namespace, LambdaXidPred),
			Value:     []byte(lambdaScriptXidVal),
			ValueType: pb.Posting_BINARY,
			Op:        pb.DirectedEdge_SET,
		},
		&pb.DirectedEdge{
			// if this server is no more the Group-1 leader and is mutating the GraphQL
			// schema node, also if concurrently another schema update is requested which is
			// being performed at the actual Group-1 leader, then mutating the xid with the
			// same value will cause one of the mutations to abort, because of the upsert
			// directive on xid. So, this way we make sure that even in this rare case there can
			// only be one server which is able to successfully update the GraphQL schema.
			Entity:    uid,
			Attr:      x.NamespaceAttr(namespace, LambdaXidPred),
			Value:     []byte(lambdaHashXidVal),
			ValueType: pb.Posting_STRING,
			Op:        pb.DirectedEdge_SET,
		})
	// mutate the GraphQL schema. As it is a reserved predicate, and we are in group 1,
	// so this call is gonna come back to all the group 1 servers only
	_, err = MutateOverNetwork(ctx, m)
	if err != nil {
		return nil, err
	}

	// return the uid of the GraphQL schema node
	return &pb.UpdateLambdaScriptResponse{Uid: uid, Hash: hash}, nil
}

// WaitForIndexing does a busy wait for indexing to finish or the context to error out,
// if the input flag shouldWait is true. Otherwise, it just returns nil straight away.
// If the context errors, it returns that error.
func WaitForIndexing(ctx context.Context, shouldWait bool) error {
	for shouldWait {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !schema.State().IndexingInProgress() {
			break
		}
		time.Sleep(time.Second * 2)
	}
	return nil
}

// isGroupOneLeader returns true if the current server is the leader of Group One,
// it returns false otherwise.
func isGroupOneLeader() bool {
	return groups().ServesGroup(1) && groups().Node.AmLeader()
}
