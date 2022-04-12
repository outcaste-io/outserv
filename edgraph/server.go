// Portions Copyright 2017-2018 Dgraph Labs, Inc. are available under the Apache License v2.0.
// Portions Copyright 2022 Outcaste LLC are available under the Smart License v1.0.

package edgraph

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unicode"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/golang/glog"
	"github.com/pkg/errors"
	ostats "go.opencensus.io/stats"
	"go.opencensus.io/tag"
	"go.opencensus.io/trace"
	otrace "go.opencensus.io/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/outcaste-io/outserv/chunker"
	"github.com/outcaste-io/outserv/conn"
	"github.com/outcaste-io/outserv/gql"
	gqlSchema "github.com/outcaste-io/outserv/graphql/schema"
	"github.com/outcaste-io/outserv/posting"
	"github.com/outcaste-io/outserv/protos/pb"
	"github.com/outcaste-io/outserv/query"
	"github.com/outcaste-io/outserv/schema"
	"github.com/outcaste-io/outserv/telemetry"
	"github.com/outcaste-io/outserv/types"
	"github.com/outcaste-io/outserv/worker"
	"github.com/outcaste-io/outserv/x"
	"github.com/outcaste-io/outserv/zero"
)

const (
	methodMutate = "Server.Mutate"
	methodQuery  = "Server.Query"
)

type GraphqlContextKey int

const (
	// IsGraphql is used to validate requests which are allowed to mutate GraphQL reserved
	// predicates, like dgraph.graphql.schema and dgraph.graphql.xid.
	IsGraphql GraphqlContextKey = iota
	// Authorize is used to set if the request requires validation.
	Authorize
)

type AuthMode int

const (
	// NeedAuthorize is used to indicate that the request needs to be authorized.
	NeedAuthorize AuthMode = iota
	// NoAuthorize is used to indicate that authorization needs to be skipped.
	// Used when ACL needs to query information for performing the authorization check.
	NoAuthorize
)

var (
	numGraphQLPM uint64
	numGraphQL   uint64
)

var (
	errIndexingInProgress = errors.New("errIndexingInProgress. Please retry")
)

// graphQLSchemaNode represents the node which contains GraphQL schema
type graphQLSchemaNode struct {
	Uid    string `json:"uid"`
	UidInt uint64
	Schema string `json:"dgraph.graphql.schema"`
}

type existingGQLSchemaQryResp struct {
	ExistingGQLSchema []graphQLSchemaNode `json:"ExistingGQLSchema"`
}

// PeriodicallyPostTelemetry periodically reports telemetry data for alpha.
func PeriodicallyPostTelemetry() {
	glog.V(2).Infof("Starting telemetry data collection for alpha...")

	start := time.Now()
	ticker := time.NewTicker(time.Minute * 10)
	defer ticker.Stop()

	var lastPostedAt time.Time
	for range ticker.C {
		if time.Since(lastPostedAt) < time.Hour {
			continue
		}
		ms := zero.MembershipState()
		t := telemetry.NewAlpha(ms)
		t.NumGraphQLPM = atomic.SwapUint64(&numGraphQLPM, 0)
		t.NumGraphQL = atomic.SwapUint64(&numGraphQL, 0)
		t.SinceHours = int(time.Since(start).Hours())
		glog.V(2).Infof("Posting Telemetry data: %+v", t)

		err := t.Post()
		if err == nil {
			lastPostedAt = time.Now()
		} else {
			atomic.AddUint64(&numGraphQLPM, t.NumGraphQLPM)
			atomic.AddUint64(&numGraphQL, t.NumGraphQL)
			glog.V(2).Infof("Telemetry couldn't be posted. Error: %v", err)
		}
	}
}

func GetLambdaScript(namespace uint64) (script string, err error) {
	gql, err := getGQLSchema(namespace)
	if err != nil {
		return "", err
	}
	return gql.Script, nil
}

func GetGQLSchema(namespace uint64) (graphQLSchema string, err error) {
	gql, err := getGQLSchema(namespace)
	if err != nil {
		return "", err
	}
	return gql.Schema, nil
}

// getGQLSchema queries for the GraphQL schema node, and returns the uid and the GraphQL schema and
// lambda script.
// If multiple schema nodes were found, it returns an error.
func getGQLSchema(namespace uint64) (*x.GQL, error) {
	ctx := context.WithValue(context.Background(), Authorize, false)
	ctx = x.AttachNamespace(ctx, namespace)
	resp, err := Query(ctx,
		&pb.Request{
			Query: fmt.Sprintf(`
			query {
			  ExistingGQLSchema(func: uid(%#x)) {
				dgraph.graphql.schema
			  }
			}`, worker.SchemaNodeUid)})
	if err != nil {
		return nil, err
	}

	var result existingGQLSchemaQryResp
	if err := json.Unmarshal(resp.GetJson(), &result); err != nil {
		return nil, errors.Wrap(err, "Couldn't unmarshal response from Dgraph query")
	}

	data := &x.GQL{}
	res := result.ExistingGQLSchema
	if len(res) == 0 {
		// no schema has been stored yet in Dgraph
		return data, nil
	}
	if len(res) == 1 {
		// we found an existing GraphQL schema
		gqlSchemaNode := res[0]
		data.Schema, data.Script = worker.ParseAsSchemaAndScript([]byte(gqlSchemaNode.Schema))
		return data, nil
	}
	panic(fmt.Sprintf("Not expecting multiple schemas. Found: %s\n", res))
}

// UpdateGQLSchema updates the GraphQL and Dgraph schemas using the given inputs.
// It first validates and parses the dgraphSchema given in input. If that fails,
// it returns an error. All this is done on the alpha on which the update request is received.
// Then it sends an update request to the worker, which is executed only on Group-1 leader.
func UpdateGQLSchema(ctx context.Context, gqlSchema,
	dgraphSchema string) (*pb.UpdateGraphQLSchemaResponse, error) {
	var err error
	parsedDgraphSchema := &schema.ParsedSchema{}

	if !x.WorkerConfig.AclEnabled {
		ctx = x.AttachNamespace(ctx, x.GalaxyNamespace)
	}
	// The schema could be empty if it only has custom types/queries/mutations.
	if dgraphSchema != "" {
		glog.Infof("Dgraph schema is:\n%s\n", dgraphSchema)
		op := &pb.Operation{Schema: dgraphSchema}
		if err = validateAlterOperation(ctx, op); err != nil {
			return nil, err
		}
		if parsedDgraphSchema, err = parseSchemaFromAlterOperation(ctx, op); err != nil {
			return nil, err
		}
	}

	resp, err := worker.UpdateGQLSchemaOverNetwork(ctx, &pb.UpdateGraphQLSchemaRequest{
		// TODO: Understand this better and see what timestamp should be set.
		StartTs:       posting.ReadTimestamp(),
		GraphqlSchema: gqlSchema,
		DgraphPreds:   parsedDgraphSchema.Preds,
		Op:            pb.UpdateGraphQLSchemaRequest_SCHEMA,
	})
	glog.Infof("UpdateGQLSchemaOverNetwork returned with error: %v\n", err)
	return resp, err
}

// UpdateLambdaScript updates the Lambda Script using the given inputs.
// It sends an update request to the worker, which is executed only on Group-1 leader.
func UpdateLambdaScript(
	ctx context.Context, script string) (*pb.UpdateGraphQLSchemaResponse, error) {
	if !x.WorkerConfig.AclEnabled {
		ctx = x.AttachNamespace(ctx, x.GalaxyNamespace)
	}

	return worker.UpdateGQLSchemaOverNetwork(ctx, &pb.UpdateGraphQLSchemaRequest{
		// TODO: Understand this better and see what timestamp should be set.
		StartTs:      posting.ReadTimestamp(),
		LambdaScript: script,
		Op:           pb.UpdateGraphQLSchemaRequest_SCRIPT,
	})
}

// validateAlterOperation validates the given operation for alter.
func validateAlterOperation(ctx context.Context, op *pb.Operation) error {
	// The following code block checks if the operation should run or not.
	if op.Schema == "" && op.DropAttr == "" && !op.DropAll && op.DropOp == pb.Operation_NONE {
		// Must have at least one field set. This helps users if they attempt
		// to set a field but use the wrong name (could be decoded from JSON).
		return errors.Errorf("Operation must have at least one field set")
	}
	if err := x.HealthCheck(); err != nil {
		return err
	}

	if isDropAll(op) && op.DropOp == pb.Operation_DATA {
		return errors.Errorf("Only one of DropAll and DropData can be true")
	}

	if !isMutationAllowed(ctx) {
		return errors.Errorf("No mutations allowed by server.")
	}
	if _, err := hasAdminAuth(ctx, "Alter"); err != nil {
		glog.Warningf("Alter denied with error: %v\n", err)
		return err
	}

	if err := authorizeAlter(ctx, op); err != nil {
		glog.Warningf("Alter denied with error: %v\n", err)
		return err
	}

	return nil
}

// parseSchemaFromAlterOperation parses the string schema given in input operation to a Go
// struct, and performs some checks to make sure that the schema is valid.
func parseSchemaFromAlterOperation(ctx context.Context, op *pb.Operation) (*schema.ParsedSchema,
	error) {
	// If a background task is already running, we should reject all the new alter requests.
	if schema.State().IndexingInProgress() {
		return nil, errIndexingInProgress
	}

	namespace, err := x.ExtractNamespace(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "While parsing schema")
	}

	if x.IsGalaxyOperation(ctx) {
		// Only the guardian of the galaxy can do a galaxy wide query/mutation. This operation is
		// needed by live loader.
		if err := AuthGuardianOfTheGalaxy(ctx); err != nil {
			s := status.Convert(err)
			return nil, status.Error(s.Code(),
				"Non guardian of galaxy user cannot bypass namespaces. "+s.Message())
		}
		var err error
		namespace, err = strconv.ParseUint(x.GetForceNamespace(ctx), 0, 64)
		if err != nil {
			return nil, errors.Wrapf(err, "Valid force namespace not found in metadata")
		}
	}

	result, err := schema.ParseWithNamespace(op.Schema, namespace)
	if err != nil {
		return nil, err
	}

	preds := make(map[string]struct{})

	for _, update := range result.Preds {
		if _, ok := preds[update.Predicate]; ok {
			return nil, errors.Errorf("predicate %s defined multiple times",
				x.ParseAttr(update.Predicate))
		}
		preds[update.Predicate] = struct{}{}

		// Pre-defined predicates cannot be altered but let the update go through
		// if the update is equal to the existing one.
		if schema.IsPreDefPredChanged(update) {
			return nil, errors.Errorf("predicate %s is pre-defined and is not allowed to be"+
				" modified", x.ParseAttr(update.Predicate))
		}

		if err := validatePredName(update.Predicate); err != nil {
			return nil, err
		}
		// Users are not allowed to create a predicate under the reserved `dgraph.` namespace. But,
		// there are pre-defined predicates (subset of reserved predicates), and for them we allow
		// the schema update to go through if the update is equal to the existing one.
		// So, here we check if the predicate is reserved but not pre-defined to block users from
		// creating predicates in reserved namespace.
		if x.IsReservedPredicate(update.Predicate) && !x.IsPreDefinedPredicate(update.Predicate) {
			return nil, errors.Errorf("Can't alter predicate `%s` as it is prefixed with `dgraph.`"+
				" which is reserved as the namespace for dgraph's internal types/predicates.",
				x.ParseAttr(update.Predicate))
		}
	}

	return result, nil
}

// InsertDropRecord is used to insert a helper record when a DROP operation is performed.
// This helper record lets us know during backup that a DROP operation was performed and that we
// need to write this information in backup manifest. So that while restoring from a backup series,
// we can create an exact replica of the system which existed at the time the last backup was taken.
// Note that if the server crashes after the DROP operation & before this helper record is inserted,
// then restoring from the incremental backup of such a DB would restore even the dropped
// data back. This is also used to capture the delete namespace operation during backup.
func InsertDropRecord(ctx context.Context, dropOp string) error {
	_, err := doQuery(context.WithValue(ctx, IsGraphql, true), &Request{
		req: &pb.Request{
			Mutations: []*pb.Mutation{{
				Nquads: []*pb.NQuad{{
					Subject:     "_:r",
					Predicate:   "dgraph.drop.op",
					ObjectValue: &pb.Value{Val: &pb.Value_StrVal{StrVal: dropOp}},
				}},
			}},
			CommitNow: true,
		}, doAuth: NoAuthorize})
	return err
}

// Alter handles requests to change the schema or remove parts or all of the data.
func Alter(ctx context.Context, op *pb.Operation) (*pb.Payload, error) {
	ctx, span := otrace.StartSpan(ctx, "Server.Alter")
	defer span.End()

	ctx = x.AttachJWTNamespace(ctx)
	span.Annotatef(nil, "Alter operation: %+v", op)

	// Always print out Alter operations because they are important and rare.
	glog.Infof("Received ALTER op: %+v", op)

	// check if the operation is valid
	if err := validateAlterOperation(ctx, op); err != nil {
		return nil, err
	}

	defer glog.Infof("ALTER op: %+v done", op)

	empty := &pb.Payload{}
	namespace, err := x.ExtractNamespace(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "While altering")
	}

	m := &pb.Mutations{}
	if isDropAll(op) {
		if x.Config.BlockClusterWideDrop {
			glog.V(2).Info("Blocked drop-all because it is not permitted.")
			return empty, errors.New("Drop all operation is not permitted.")
		}
		if err := AuthGuardianOfTheGalaxy(ctx); err != nil {
			s := status.Convert(err)
			return empty, status.Error(s.Code(),
				"Drop all can only be called by the guardian of the galaxy. "+s.Message())
		}
		if len(op.DropValue) > 0 {
			return empty, errors.Errorf("If DropOp is set to ALL, DropValue must be empty")
		}

		m.DropOp = pb.Mutations_ALL
		_, err := query.ApplyMutations(ctx, m)
		if err != nil {
			return empty, err
		}

		// insert a helper record for backup & restore, indicating that drop_all was done
		err = InsertDropRecord(ctx, "DROP_ALL;")
		if err != nil {
			return empty, err
		}

		// insert empty GraphQL schema, so all alphas get notified to
		// reset their in-memory GraphQL schema
		// NOTE: As lambda script and graphql schema are stored in same predicate, there is no need
		// to send a notification to update in-memory lambda script.
		_, err = UpdateGQLSchema(ctx, "", "")
		// recreate the admin account after a drop all operation
		ResetAcl(nil)
		return empty, err
	}

	if op.DropOp == pb.Operation_DATA {
		if len(op.DropValue) > 0 {
			return empty, errors.Errorf("If DropOp is set to DATA, DropValue must be empty")
		}

		// query the GraphQL schema and keep it in memory, so it can be inserted again
		graphQLSchema, err := GetGQLSchema(namespace)
		if err != nil {
			return empty, err
		}
		lambdaScript, err := GetLambdaScript(namespace)
		if err != nil {
			return empty, err
		}

		m.DropOp = pb.Mutations_DATA
		m.DropValue = fmt.Sprintf("%#x", namespace)
		_, err = query.ApplyMutations(ctx, m)
		if err != nil {
			return empty, err
		}

		// insert a helper record for backup & restore, indicating that drop_data was done
		err = InsertDropRecord(ctx, fmt.Sprintf("DROP_DATA;%#x", namespace))
		if err != nil {
			return empty, err
		}

		// just reinsert the GraphQL schema, no need to alter dgraph schema as this was drop_data
		if _, err := UpdateGQLSchema(ctx, graphQLSchema, ""); err != nil {
			return empty, errors.Wrap(err, "While updating gql schema ")
		}
		if _, err := UpdateLambdaScript(ctx, lambdaScript); err != nil {
			return empty, errors.Wrap(err, "While updating lambda script ")
		}
		// recreate the admin account after a drop data operation
		upsertGuardianAndGroot(nil, namespace)
		return empty, err
	}

	if len(op.DropAttr) > 0 || op.DropOp == pb.Operation_ATTR {
		if op.DropOp == pb.Operation_ATTR && op.DropValue == "" {
			return empty, errors.Errorf("If DropOp is set to ATTR, DropValue must not be empty")
		}

		var attr string
		if len(op.DropAttr) > 0 {
			attr = op.DropAttr
		} else {
			attr = op.DropValue
		}
		attr = x.NamespaceAttr(namespace, attr)
		// Pre-defined predicates cannot be dropped.
		if x.IsPreDefinedPredicate(attr) {
			return empty, errors.Errorf("predicate %s is pre-defined and is not allowed to be"+
				" dropped", x.ParseAttr(attr))
		}

		nq := &pb.NQuad{
			Subject:     x.Star,
			Predicate:   x.ParseAttr(attr),
			ObjectValue: &pb.Value{Val: &pb.Value_StrVal{StrVal: x.Star}},
		}
		wnq := &gql.NQuad{NQuad: nq}
		edge, err := wnq.ToDeletePredEdge()
		if err != nil {
			return empty, err
		}
		edges := []*pb.DirectedEdge{edge}
		m.Edges = edges
		_, err = query.ApplyMutations(ctx, m)
		if err != nil {
			return empty, err
		}

		// insert a helper record for backup & restore, indicating that drop_attr was done
		err = InsertDropRecord(ctx, "DROP_ATTR;"+attr)
		return empty, err
	}

	// it is a schema update
	result, err := parseSchemaFromAlterOperation(ctx, op)
	if err == errIndexingInProgress {
		// Make the client wait a bit.
		time.Sleep(time.Second)
		return nil, err
	} else if err != nil {
		return nil, err
	}
	if err = validateDQLSchemaForGraphQL(ctx, result, namespace); err != nil {
		return nil, err
	}

	glog.Infof("Got schema: %+v\n", result)
	// TODO: Maybe add some checks about the schema.
	m.Schema = result.Preds
	for i := 0; i < 3; i++ {
		_, err = query.ApplyMutations(ctx, m)
		if err != nil && strings.Contains(err.Error(), "Please retry operation") {
			time.Sleep(time.Second)
			continue
		}
		break
	}
	if err != nil {
		return empty, errors.Wrapf(err, "During ApplyMutations")
	}

	// wait for indexing to complete or context to be canceled.
	if err = worker.WaitForIndexing(ctx, !op.RunInBackground); err != nil {
		return empty, err
	}

	return empty, nil
}

func validateDQLSchemaForGraphQL(ctx context.Context,
	dqlSch *schema.ParsedSchema, ns uint64) error {
	// fetch the GraphQL schema for this namespace from disk
	existingGQLSch, err := GetGQLSchema(ns)
	if err != nil || existingGQLSch == "" {
		return err
	}

	// convert the existing GraphQL schema to a DQL schema
	handler, err := gqlSchema.NewHandler(existingGQLSch, false)
	if err != nil {
		return err
	}
	dgSchema := handler.DGSchema()
	if dgSchema == "" {
		return nil
	}
	gqlReservedDgSch, err := parseSchemaFromAlterOperation(ctx, &pb.Operation{Schema: dgSchema})
	if err != nil {
		return err
	}

	// create a mapping for the GraphQL reserved predicates and types
	gqlReservedPreds := make(map[string]*pb.SchemaUpdate)
	for _, pred := range gqlReservedDgSch.Preds {
		gqlReservedPreds[pred.Predicate] = pred
	}

	// now validate the DQL schema to check that it doesn't break the existing GraphQL schema

	// Step-1: validate predicates
	for _, dqlPred := range dqlSch.Preds {
		gqlPred := gqlReservedPreds[dqlPred.Predicate]
		if gqlPred == nil {
			continue // if the predicate isn't used by GraphQL, no need to validate it.
		}

		// type (including list) must match exactly
		if gqlPred.ValueType != dqlPred.ValueType || gqlPred.List != dqlPred.List {
			gqlType := strings.ToLower(gqlPred.ValueType.String())
			dqlType := strings.ToLower(dqlPred.ValueType.String())
			if gqlPred.List {
				gqlType = "[" + gqlType + "]"
			}
			if dqlPred.List {
				dqlType = "[" + dqlType + "]"
			}
			return errors.Errorf("can't alter predicate %s as it is used by the GraphQL API, "+
				"and type definition is incompatible with what is expected by the GraphQL API. "+
				"want: %s, got: %s", x.ParseAttr(gqlPred.Predicate), gqlType, dqlType)
		}
		// if gqlSchema had any indexes, then those must be present in the dqlSchema.
		// dqlSchema may add more indexes than what gqlSchema had initially, but can't remove them.
		if gqlPred.Directive == pb.SchemaUpdate_INDEX {
			if dqlPred.Directive != pb.SchemaUpdate_INDEX {
				return errors.Errorf("can't alter predicate %s as it is used by the GraphQL API, "+
					"and is missing index definition that is expected by the GraphQL API. "+
					"want: @index(%s)", x.ParseAttr(gqlPred.Predicate),
					strings.Join(gqlPred.Tokenizer, ","))
			}
			var missingIndexes []string
			for _, t := range gqlPred.Tokenizer {
				if !x.HasString(dqlPred.Tokenizer, t) {
					missingIndexes = append(missingIndexes, t)
				}
			}
			if len(missingIndexes) > 0 {
				return errors.Errorf("can't alter predicate %s as it is used by the GraphQL API, "+
					"and is missing index definition that is expected by the GraphQL API. "+
					"want: @index(%s, %s), got: @index(%s)", x.ParseAttr(gqlPred.Predicate),
					strings.Join(dqlPred.Tokenizer, ","), strings.Join(missingIndexes, ","),
					strings.Join(dqlPred.Tokenizer, ","))
			}
		}
		// if gqlSchema had @count, then dqlSchema must have it. dqlSchema can't remove @count.
		// if gqlSchema didn't had @count, it is allowed to dqlSchema to add it.
		if gqlPred.Count && !dqlPred.Count {
			return errors.Errorf("can't alter predicate %s as it is used by the GraphQL API, "+
				"and is missing @count that is expected by the GraphQL API.",
				x.ParseAttr(gqlPred.Predicate))
		}
		// if gqlSchema had @upsert, then dqlSchema must have it. dqlSchema can't remove @upsert.
		// if gqlSchema didn't had @upsert, it is allowed to dqlSchema to add it.
		if gqlPred.Upsert && !dqlPred.Upsert {
			return errors.Errorf("can't alter predicate %s as it is used by the GraphQL API, "+
				"and is missing @upsert that is expected by the GraphQL API.",
				x.ParseAttr(gqlPred.Predicate))
		}
	}

	return nil
}

func annotateNamespace(span *otrace.Span, ns uint64) {
	span.AddAttributes(otrace.Int64Attribute("ns", int64(ns)))
}

func annotateStartTs(span *otrace.Span, ts uint64) {
	span.AddAttributes(otrace.Int64Attribute("startTs", int64(ts)))
}

func doMutate(ctx context.Context, qc *queryContext, resp *pb.Response) error {
	if len(qc.gmuList) == 0 {
		return nil
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}

	start := time.Now()
	defer func() {
		qc.latency.Processing += time.Since(start)
	}()

	if !isMutationAllowed(ctx) {
		return errors.Errorf("no mutations allowed")
	}

	// update mutations from the query results before assigning UIDs
	if err := updateMutations(qc); err != nil {
		return err
	}

	mu, err := query.ToMutations(ctx, qc.gmuList)
	if err != nil {
		return err
	}
	_ = mu

	// newUids, err := query.AssignUids(ctx, qc.gmuList)
	// if err != nil {
	// 	return err
	// }
	// glog.Infof("Got a map for newUids: %+v\n", newUids)

	// TODO(mrjn): Add a verify pb.Mutations function, which can double check
	// the UIDs used in the mutation.

	//
	// TODO: We could assign each of the new UIDs a query. If the query returns
	// a valid result, the newUids map would get updated with the result UID.
	// Otherwise, it would use the assigned UID.

	// resp.Uids contains a map of the node name to the uid.
	// 1. For a blank node, like _:foo, the key would be foo.
	// 2. For a uid variable that is part of an upsert query,
	//    like uid(foo), the key would be uid(foo).
	// resp.Uids = query.UidsToHex(query.StripBlankNode(newUids))

	// We could keep directed edges. But, just have Mutations keep a list of
	// NQuads. The objects can be
	// edges, err := query.ToDirectedEdges(qc.gmuList, newUids)
	// if err != nil {
	// 	return err
	// }

	// m := &pb.Mutations{
	// 	Edges: edges,
	// }

	qc.span.Annotatef(nil, "Applying mutations: %+v", mu)
	resp.Txn, err = query.ApplyMutations(ctx, mu)
	qc.span.Annotatef(nil, "Txn Context: %+v. Err=%v", resp.Txn, err)

	// calculateMutationMetrics calculate cost for the mutation.
	// calculateMutationMetrics := func() {
	// 	cost := uint64(len(newUids) + len(edges))
	// 	resp.Metrics.NumUids["mutation_cost"] = cost
	// 	resp.Metrics.NumUids["_total"] = resp.Metrics.NumUids["_total"] + cost
	// }
	// calculateMutationMetrics()
	return err
}

// buildUpsertQuery modifies the query to evaluate the
// @if condition defined in Conditional Upsert.
func buildUpsertQuery(qc *queryContext) string {
	if qc.req.Query == "" || len(qc.gmuList) == 0 {
		return qc.req.Query
	}

	qc.condVars = make([]string, len(qc.req.Mutations))

	var b strings.Builder
	x.Check2(b.WriteString(strings.TrimSuffix(qc.req.Query, "}")))

	for i, gmu := range qc.gmuList {
		isCondUpsert := strings.TrimSpace(gmu.Cond) != ""
		if isCondUpsert {
			qc.condVars[i] = "__dgraph__" + strconv.Itoa(i)
			qc.uidRes[qc.condVars[i]] = nil
			// @if in upsert is same as @filter in the query
			cond := strings.Replace(gmu.Cond, "@if", "@filter", 1)

			// Add dummy query to evaluate the @if directive, ok to use uid(0) because
			// dgraph doesn't check for existence of UIDs until we query for other predicates.
			// Here, we are only querying for uid predicate in the dummy query.
			//
			// For example if - mu.Query = {
			//      me(...) {...}
			//   }
			//
			// Then, upsertQuery = {
			//      me(...) {...}
			//      __dgraph_0__ as var(func: uid(0)) @filter(...)
			//   }
			//
			// The variable __dgraph_0__ will -
			//      * be empty if the condition is true
			//      * have 1 UID (the 0 UID) if the condition is false
			x.Check2(b.WriteString(qc.condVars[i] + ` as var(func: uid(0)) ` + cond + `
			 `))
		}
	}
	x.Check2(b.WriteString(`}`))

	return b.String()
}

// updateMutations updates the mutation and replaces uid(var) and val(var) with
// their values or a blank node, in case of an upsert.
// We use the values stored in qc.uidRes and qc.valRes to update the mutation.
func updateMutations(qc *queryContext) error {
	for i, condVar := range qc.condVars {
		gmu := qc.gmuList[i]
		if condVar != "" {
			uids, ok := qc.uidRes[condVar]
			if !(ok && len(uids) == 1) {
				gmu.Nquads = nil
				continue
			}
		}

		if err := updateUIDInMutations(gmu, qc); err != nil {
			return err
		}
		if err := updateValInMutations(gmu, qc); err != nil {
			return err
		}
	}

	return nil
}

// findMutationVars finds all the variables used in mutation block and stores them
// qc.uidRes and qc.valRes so that we only look for these variables in query results.
func findMutationVars(qc *queryContext) []string {
	updateVars := func(s string) {
		if strings.HasPrefix(s, "uid(") {
			varName := s[4 : len(s)-1]
			qc.uidRes[varName] = nil
		} else if strings.HasPrefix(s, "val(") {
			varName := s[4 : len(s)-1]
			qc.valRes[varName] = nil
		}
	}

	for _, gmu := range qc.gmuList {
		for _, nq := range gmu.Nquads {
			updateVars(nq.Subject)
			updateVars(nq.ObjectId)
		}
	}

	varsList := make([]string, 0, len(qc.uidRes)+len(qc.valRes))
	for v := range qc.uidRes {
		varsList = append(varsList, v)
	}
	for v := range qc.valRes {
		varsList = append(varsList, v)
	}

	return varsList
}

// updateValInNQuads picks the val() from object and replaces it with its value
// Assumption is that Subject can contain UID, whereas Object can contain Val
// If val(variable) exists in a query, but the values are not there for the variable,
// it will ignore the mutation silently.
func updateValInNQuads(nquads []*pb.NQuad, qc *queryContext) []*pb.NQuad {
	getNewVals := func(s string) (map[uint64]types.Val, bool) {
		if strings.HasPrefix(s, "val(") {
			varName := s[4 : len(s)-1]
			if v, ok := qc.valRes[varName]; ok && v != nil {
				return v, true
			}
			return nil, true
		}
		return nil, false
	}

	getValue := func(key uint64, uidToVal map[uint64]types.Val) (types.Val, bool) {
		val, ok := uidToVal[key]
		if ok {
			return val, true
		}

		// Check if the variable is aggregate variable
		// Only 0 key would exist for aggregate variable
		val, ok = uidToVal[0]
		return val, ok
	}

	newNQuads := nquads[:0]
	for _, nq := range nquads {
		// Check if the nquad contains a val() in Object or not.
		// If not then, keep the mutation and continue
		uidToVal, found := getNewVals(nq.ObjectId)
		if !found {
			newNQuads = append(newNQuads, nq)
			continue
		}

		isSet := nq.Op == pb.NQuad_SET
		// uid(u) <amount> val(amt)
		// For each NQuad, we need to convert the val(variable_name)
		// to *pb.Value before applying the mutation. For that, first
		// we convert key to uint64 and get the UID to Value map from
		// the result of the query.
		var key uint64
		var err error
		switch {
		case nq.Subject[0] == '_' && isSet:
			// in case aggregate val(var) is there, that should work with blank node.
			key = 0
		case nq.Subject[0] == '_' && !isSet:
			// UID is of format "_:uid(u)". Ignore the delete silently
			continue
		default:
			key, err = strconv.ParseUint(nq.Subject, 0, 64)
			if err != nil {
				// Key conversion failed, ignoring the nquad. Ideally,
				// it shouldn't happen as this is the result of a query.
				glog.Errorf("Conversion of subject %s failed. Error: %s",
					nq.Subject, err.Error())
				continue
			}
		}

		// Get the value to the corresponding UID(key) from the query result
		nq.ObjectId = ""
		val, ok := getValue(key, uidToVal)
		if !ok {
			continue
		}

		// Convert the value from types.Val to *pb.Value
		nq.ObjectValue, err = types.ObjectValue(val.Tid, val.Value)
		if err != nil {
			// Value conversion failed, ignoring the nquad. Ideally,
			// it shouldn't happen as this is the result of a query.
			glog.Errorf("Conversion of %s failed for %d subject. Error: %s",
				nq.ObjectId, key, err.Error())
			continue
		}

		newNQuads = append(newNQuads, nq)
	}
	qc.nquadsCount += len(newNQuads)
	return newNQuads
}

// updateValInMutations does following transformations:
// 0x123 <amount> val(v) -> 0x123 <amount> 13.0
func updateValInMutations(gmu *pb.Mutation, qc *queryContext) error {
	gmu.Nquads = updateValInNQuads(gmu.Nquads, qc)
	if qc.nquadsCount > x.Config.LimitMutationsNquad {
		return errors.Errorf("NQuad count in the request: %d, is more that threshold: %d",
			qc.nquadsCount, int(x.Config.LimitMutationsNquad))
	}
	return nil
}

// updateUIDInMutations does following transformations:
//   * uid(v) -> 0x123     -- If v is defined in query block
//   * uid(v) -> _:uid(v)  -- Otherwise
func updateUIDInMutations(gmu *pb.Mutation, qc *queryContext) error {
	// usedMutationVars keeps track of variables that are used in mutations.
	getNewVals := func(s string) []string {
		if strings.HasPrefix(s, "uid(") {
			varName := s[4 : len(s)-1]
			if uids, ok := qc.uidRes[varName]; ok && len(uids) != 0 {
				return uids
			}

			return []string{"_:" + s}
		}

		return []string{s}
	}

	getNewNQuad := func(nq *pb.NQuad, s, o string) *pb.NQuad {
		// The following copy is fine because we only modify Subject and ObjectId.
		// The pointer values are not modified across different copies of NQuad.
		n := *nq

		n.Subject = s
		n.ObjectId = o
		return &n
	}

	// Remove the mutations from gmu.Del when no UID was found.
	nquads := make([]*pb.NQuad, 0, len(gmu.Nquads))
	for _, nq := range gmu.Nquads {
		if nq.Op != pb.NQuad_DEL {
			continue
		}
		// if Subject or/and Object are variables, each NQuad can result
		// in multiple NQuads if any variable stores more than one UIDs.
		newSubs := getNewVals(nq.Subject)
		newObs := getNewVals(nq.ObjectId)

		for _, s := range newSubs {
			for _, o := range newObs {
				// Blank node has no meaning in case of deletion.
				if strings.HasPrefix(s, "_:uid(") ||
					strings.HasPrefix(o, "_:uid(") {
					continue
				}

				nquads = append(nquads, getNewNQuad(nq, s, o))
				qc.nquadsCount++
			}
			if qc.nquadsCount > int(x.Config.LimitMutationsNquad) {
				return errors.Errorf("NQuad count in the request: %d, is more that threshold: %d",
					qc.nquadsCount, int(x.Config.LimitMutationsNquad))
			}
		}
	}

	// Update the values in mutation block from the query block.
	for _, nq := range gmu.Nquads {
		if nq.Op != pb.NQuad_SET {
			continue
		}
		newSubs := getNewVals(nq.Subject)
		newObs := getNewVals(nq.ObjectId)

		qc.nquadsCount += len(newSubs) * len(newObs)
		if qc.nquadsCount > int(x.Config.LimitQueryEdge) {
			return errors.Errorf("NQuad count in the request: %d, is more that threshold: %d",
				qc.nquadsCount, int(x.Config.LimitQueryEdge))
		}

		for _, s := range newSubs {
			for _, o := range newObs {
				nquads = append(nquads, getNewNQuad(nq, s, o))
			}
		}
	}
	gmu.Nquads = nquads
	return nil
}

// queryContext is used to pass around all the variables needed
// to process a request for query, mutation or upsert.
type queryContext struct {
	// req is the incoming, not yet parsed request containing
	// a query or more than one mutations or both (in case of upsert)
	req *pb.Request
	// gmuList is the list of mutations after parsing req.Mutations
	gmuList []*pb.Mutation
	// gqlRes contains result of parsing the req.Query
	gqlRes gql.Result
	// condVars are conditional variables used in the (modified) query to figure out
	// whether the condition in Conditional Upsert is true. The string would be empty
	// if the corresponding mutation is not a conditional upsert.
	// Note that, len(condVars) == len(gmuList).
	condVars []string
	// uidRes stores mapping from variable names to UIDs for UID variables.
	// These variables are either dummy variables used for Conditional
	// Upsert or variables used in the mutation block in the incoming request.
	uidRes map[string][]string
	// valRes stores mapping from variable names to values for value
	// variables used in the mutation block of incoming request.
	valRes map[string]map[uint64]types.Val
	// l stores latency numbers
	latency *query.Latency
	// span stores a opencensus span used throughout the query processing
	span *trace.Span
	// graphql indicates whether the given request is from graphql admin or not.
	graphql bool
	// gqlField stores the GraphQL field for which the query is being processed.
	// This would be set only if the request is a query from GraphQL layer,
	// otherwise it would be nil. (Eg. nil cases: in case of a DQL query,
	// a mutation being executed from GraphQL layer).
	gqlField *gqlSchema.Field
	// nquadsCount maintains numbers of nquads which would be inserted as part of this request.
	// In some cases(mostly upserts), numbers of nquads to be inserted can to huge(we have seen upto
	// 1B) and resulting in OOM. We are limiting number of nquads which can be inserted in
	// a single request.
	nquadsCount int
}

// Request represents a query request sent to the doQuery() method on the Server.
// It contains all the metadata required to execute a query.
type Request struct {
	// req is the incoming gRPC request
	req *pb.Request
	// gqlField is the GraphQL field for which the request is being sent
	gqlField *gqlSchema.Field
	// doAuth tells whether this request needs ACL authorization or not
	doAuth AuthMode
}

// Health handles /health and /health?all requests.
func Health(ctx context.Context, all bool) (*pb.Response, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	var healthAll []pb.HealthInfo
	if all {
		if err := AuthorizeGuardians(ctx); err != nil {
			return nil, err
		}
		pool := conn.GetPools().GetAll()
		for _, p := range pool {
			if p.Addr == x.WorkerConfig.MyAddr {
				continue
			}
			healthAll = append(healthAll, p.HealthInfo())
		}
	}

	// Append self.
	healthAll = append(healthAll, pb.HealthInfo{
		Instance: "alpha",
		Address:  x.WorkerConfig.MyAddr,
		Status:   "healthy",
		Group:    strconv.Itoa(int(worker.GroupId())),
		Version:  x.Version(),
		Uptime:   int64(time.Since(x.WorkerConfig.StartTime) / time.Second),
		LastEcho: time.Now().Unix(),
		Ongoing:  worker.GetOngoingTasks(),
		Indexing: schema.GetIndexingPredicates(),
		ReadTs:   posting.ReadTimestamp(),
	})

	var err error
	var jsonOut []byte
	if jsonOut, err = json.Marshal(healthAll); err != nil {
		return nil, errors.Errorf("Unable to Marshal. Err %v", err)
	}
	return &pb.Response{Json: jsonOut}, nil
}

// Filter out the tablets that do not belong to the requestor's namespace.
func filterTablets(ctx context.Context, ms *pb.MembershipState) error {
	if !x.WorkerConfig.AclEnabled {
		return nil
	}
	namespace, err := x.ExtractJWTNamespace(ctx)
	if err != nil {
		return errors.Errorf("Namespace not found in JWT.")
	}
	if namespace == x.GalaxyNamespace {
		// For galaxy namespace, we don't want to filter out the predicates.
		return nil
	}
	tablets := make(map[string]*pb.Tablet)
	for pred, tablet := range ms.GetTablets() {
		if ns, attr := x.ParseNamespaceAttr(pred); namespace == ns {
			tablets[attr] = tablet
			tablets[attr].Predicate = attr
		}
	}
	ms.Tablets = tablets
	return nil
}

// State handles state requests
func State(ctx context.Context) (*pb.Response, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	if err := AuthorizeGuardians(ctx); err != nil {
		return nil, err
	}

	ms := zero.MembershipState()
	if ms == nil {
		return nil, errors.Errorf("No membership state found")
	}

	if err := filterTablets(ctx, ms); err != nil {
		return nil, err
	}

	m := jsonpb.Marshaler{EmitDefaults: true}
	var jsonState bytes.Buffer
	if err := m.Marshal(&jsonState, ms); err != nil {
		return nil, errors.Errorf("Error marshalling state information to JSON")
	}

	return &pb.Response{Json: jsonState.Bytes()}, nil
}

func getAuthMode(ctx context.Context) AuthMode {
	if auth := ctx.Value(Authorize); auth == nil || auth.(bool) {
		return NeedAuthorize
	}
	return NoAuthorize
}

// QueryGraphQL handles only GraphQL queries, neither mutations nor DQL.
func QueryGraphQL(ctx context.Context, req *pb.Request,
	field *gqlSchema.Field) (*pb.Response, error) {
	// Add a timeout for queries which don't have a deadline set. We don't want to
	// apply a timeout if it's a mutation, that's currently handled by flag
	// "txn-abort-after".
	if req.GetMutations() == nil && x.Config.QueryTimeout != 0 {
		if d, _ := ctx.Deadline(); d.IsZero() {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, x.Config.QueryTimeout)
			defer cancel()
		}
	}
	// no need to attach namespace here, it is already done by GraphQL layer
	return doQuery(ctx, &Request{req: req, gqlField: field, doAuth: getAuthMode(ctx)})
}

// Query handles queries or mutations
func Query(ctx context.Context, req *pb.Request) (*pb.Response, error) {
	ctx = x.AttachJWTNamespace(ctx)
	if x.WorkerConfig.AclEnabled && req.GetStartTs() != 0 {
		// A fresh StartTs is assigned if it is 0.
		ns, err := x.ExtractNamespace(ctx)
		if err != nil {
			return nil, err
		}
		if req.GetHash() != getHash(ns, req.GetStartTs()) {
			return nil, x.ErrHashMismatch
		}
	}
	// Add a timeout for queries which don't have a deadline set. We don't want to
	// apply a timeout if it's a mutation, that's currently handled by flag
	// "txn-abort-after".
	if req.GetMutations() == nil && x.Config.QueryTimeout != 0 {
		if d, _ := ctx.Deadline(); d.IsZero() {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, x.Config.QueryTimeout)
			defer cancel()
		}
	}
	return doQuery(ctx, &Request{req: req, doAuth: getAuthMode(ctx)})
}

var pendingQueries int64
var maxPendingQueries int64
var serverOverloadErr = errors.New("429 Too Many Requests. Please throttle your requests")

func Init() {
	maxPendingQueries = x.Config.Limit.GetInt64("max-pending-queries")
}

func StopServingQueries() {
	// Mark the server unhealthy so that no new operations starts and wait for 5 seconds for
	// the pending queries to finish.
	x.UpdateHealthStatus(false)
	for i := 0; i < 10; i++ {
		if atomic.LoadInt64(&pendingQueries) == 0 {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func doQuery(ctx context.Context, req *Request) (resp *pb.Response, rerr error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	defer atomic.AddInt64(&pendingQueries, -1)
	if val := atomic.AddInt64(&pendingQueries, 1); val > maxPendingQueries {
		return nil, serverOverloadErr
	}

	if bool(glog.V(3)) || worker.LogRequestEnabled() {
		glog.Infof("Got a query: %+v", req.req)
	}

	isGraphQL, _ := ctx.Value(IsGraphql).(bool)
	if isGraphQL {
		atomic.AddUint64(&numGraphQL, 1)
	} else {
		atomic.AddUint64(&numGraphQLPM, 1)
	}

	l := &query.Latency{}
	l.Start = time.Now()

	isMutation := len(req.req.Mutations) > 0
	methodRequest := methodQuery
	if isMutation {
		methodRequest = methodMutate
	}

	var measurements []ostats.Measurement
	ctx, span := otrace.StartSpan(ctx, methodRequest)
	if ns, err := x.ExtractNamespace(ctx); err == nil {
		annotateNamespace(span, ns)
	}

	ctx = x.WithMethod(ctx, methodRequest)
	defer func() {
		span.End()
		v := x.TagValueStatusOK
		if rerr != nil {
			v = x.TagValueStatusError
		}
		ctx, _ = tag.New(ctx, tag.Upsert(x.KeyStatus, v))
		timeSpentMs := x.SinceMs(l.Start)
		measurements = append(measurements, x.LatencyMs.M(timeSpentMs))
		ostats.Record(ctx, measurements...)
	}()

	if rerr = x.HealthCheck(); rerr != nil {
		return
	}

	req.req.Query = strings.TrimSpace(req.req.Query)
	isQuery := len(req.req.Query) != 0
	if !isQuery && !isMutation {
		span.Annotate(nil, "empty request")
		return nil, errors.Errorf("empty request")
	}

	span.Annotatef(nil, "Request received: %v", req.req)
	if isQuery {
		ostats.Record(ctx, x.PendingQueries.M(1), x.NumQueries.M(1))
		defer func() {
			measurements = append(measurements, x.PendingQueries.M(-1))
		}()
	}
	if isMutation {
		ostats.Record(ctx, x.NumMutations.M(1))
	}

	if req.doAuth == NeedAuthorize && x.IsGalaxyOperation(ctx) {
		// Only the guardian of the galaxy can do a galaxy wide query/mutation. This operation is
		// needed by live loader.
		if err := AuthGuardianOfTheGalaxy(ctx); err != nil {
			s := status.Convert(err)
			return nil, status.Error(s.Code(),
				"Non guardian of galaxy user cannot bypass namespaces. "+s.Message())
		}
	}

	qc := &queryContext{
		req:      req.req,
		latency:  l,
		span:     span,
		graphql:  isGraphQL,
		gqlField: req.gqlField,
	}
	// parseRequest converts mutation JSON to NQuads.
	if rerr = parseRequest(qc); rerr != nil {
		return
	}

	if req.doAuth == NeedAuthorize {
		if rerr = authorizeRequest(ctx, qc); rerr != nil {
			return
		}
	}

	// We use defer here because for queries, startTs will be
	// assigned in the processQuery function called below.
	defer annotateStartTs(qc.span, qc.req.StartTs)

	if x.WorkerConfig.AclEnabled {
		ns, err := x.ExtractNamespace(ctx)
		if err != nil {
			return nil, err
		}
		defer func() {
			if resp != nil && resp.Txn != nil {
				// attach the hash, user must send this hash when further operating on this startTs.
				resp.Txn.Hash = getHash(ns, resp.Txn.StartTs)
			}
		}()
	}

	var gqlErrs error
	if resp, rerr = processQuery(ctx, qc); rerr != nil {
		// if rerr is just some error from GraphQL encoding, then we need to continue the normal
		// execution ignoring the error as we still need to assign latency info to resp. If we can
		// change the pb.Response proto to have a field to contain GraphQL errors, that would be
		// great. Otherwise, we will have to do such checks a lot and that would make code ugly.
		if qc.gqlField != nil && x.IsGqlErrorList(rerr) {
			gqlErrs = rerr
		} else {
			return
		}
	}
	// if it were a mutation, simple or upsert, in any case gqlErrs would be empty as GraphQL JSON
	// is formed only for queries. So, gqlErrs can have something only in the case of a pure query.
	// So, safe to ignore gqlErrs and not return that here.
	if rerr = doMutate(ctx, qc, resp); rerr != nil {
		return
	}

	// TODO(Ahsan): resp.Txn.Preds contain predicates of form gid-namespace|attr.
	// Remove the namespace from the response.
	// resp.Txn.Preds = x.ParseAttrList(resp.Txn.Preds)

	// TODO(martinmr): Include Transport as part of the latency. Need to do
	// this separately since it involves modifying the API protos.
	resp.Latency = &pb.Latency{
		ParsingNs:    uint64(l.Parsing.Nanoseconds()),
		ProcessingNs: uint64(l.Processing.Nanoseconds()),
		EncodingNs:   uint64(l.Json.Nanoseconds()),
		TotalNs:      uint64((time.Since(l.Start)).Nanoseconds()),
	}
	md := metadata.Pairs(x.DgraphCostHeader, fmt.Sprint(resp.Metrics.NumUids["_total"]))
	grpc.SendHeader(ctx, md)
	return resp, gqlErrs
}

func processQuery(ctx context.Context, qc *queryContext) (*pb.Response, error) {
	resp := &pb.Response{}
	if qc.req.Query == "" {
		// No query, so make the query cost 0.
		resp.Metrics = &pb.Metrics{
			NumUids: map[string]uint64{"_total": 0},
		}
		return resp, nil
	}
	if ctx.Err() != nil {
		return resp, ctx.Err()
	}
	qr := query.Request{
		Latency:  qc.latency,
		GqlQuery: &qc.gqlRes,
	}

	if qc.req.StartTs == 0 {
		qc.req.StartTs = posting.ReadTimestamp()
	}

	qr.ReadTs = qc.req.StartTs
	resp.Txn = &pb.TxnContext{StartTs: qc.req.StartTs}

	// Core processing happens here.
	er, err := qr.Process(ctx)

	if err != nil {
		return resp, errors.Wrap(err, "")
	}

	if len(er.SchemaNode) > 0 {
		if err = authorizeSchemaQuery(ctx, &er); err != nil {
			return resp, err
		}
		sort.Slice(er.SchemaNode, func(i, j int) bool {
			return er.SchemaNode[i].Predicate < er.SchemaNode[j].Predicate
		})

		respMap := make(map[string]interface{})
		if len(er.SchemaNode) > 0 {
			respMap["schema"] = er.SchemaNode
		}
		resp.Json, err = json.Marshal(respMap)
	} else {
		resp.Json, err = query.ToJson(ctx, qc.latency, er.Subgraphs, qc.gqlField)
	}
	// if err is just some error from GraphQL encoding, then we need to continue the normal
	// execution ignoring the error as we still need to assign metrics and latency info to resp.
	if err != nil && (qc.gqlField == nil || !x.IsGqlErrorList(err)) {
		return resp, err
	}
	qc.span.Annotatef(nil, "Response = %s", resp.Json)

	// varToUID contains a map of variable name to the uids corresponding to it.
	// It is used later for constructing set and delete mutations by replacing
	// variables with the actual uids they correspond to.
	// If a variable doesn't have any UID, we generate one ourselves later.
	for name := range qc.uidRes {
		v := qr.Vars[name]

		// If the list of UIDs is empty but the map of values is not,
		// we need to get the UIDs from the keys in the map.
		var uidList []uint64
		if v.OrderedUIDs != nil && len(v.OrderedUIDs.SortedUids) > 0 {
			uidList = v.OrderedUIDs.SortedUids
		} else if !v.UidMap.IsEmpty() {
			uidList = v.UidMap.ToArray()
		} else {
			uidList = make([]uint64, 0, len(v.Vals))
			for uid := range v.Vals {
				uidList = append(uidList, uid)
			}
		}
		if len(uidList) == 0 {
			continue
		}

		// We support maximum 1 million UIDs per variable to ensure that we
		// don't do bad things to alpha and mutation doesn't become too big.
		if len(uidList) > 1e6 {
			return resp, errors.Errorf("var [%v] has over million UIDs", name)
		}

		uids := make([]string, len(uidList))
		for i, u := range uidList {
			// We use base 10 here because the RDF mutations expect the uid to be in base 10.
			uids[i] = strconv.FormatUint(u, 10)
		}
		qc.uidRes[name] = uids
	}

	// look for values for value variables
	for name := range qc.valRes {
		v := qr.Vars[name]
		qc.valRes[name] = v.Vals
	}

	resp.Metrics = &pb.Metrics{
		NumUids: er.Metrics,
	}

	var total uint64
	for _, num := range resp.Metrics.NumUids {
		total += num
	}
	resp.Metrics.NumUids["_total"] = total

	return resp, err
}

// parseRequest parses the incoming request
func parseRequest(qc *queryContext) error {
	start := time.Now()
	defer func() {
		qc.latency.Parsing = time.Since(start)
	}()

	var needVars []string
	upsertQuery := qc.req.Query
	if len(qc.req.Mutations) > 0 {
		// parsing mutations
		qc.gmuList = make([]*pb.Mutation, 0, len(qc.req.Mutations))
		for _, mu := range qc.req.Mutations {
			gmu, err := parseMutationObject(mu, qc)
			if err != nil {
				return err
			}

			qc.gmuList = append(qc.gmuList, gmu)
		}

		qc.uidRes = make(map[string][]string)
		qc.valRes = make(map[string]map[uint64]types.Val)
		upsertQuery = buildUpsertQuery(qc)
		needVars = findMutationVars(qc)
		if upsertQuery == "" {
			if len(needVars) > 0 {
				return errors.Errorf("variables %v not defined", needVars)
			}

			return nil
		}
	}

	// parsing the updated query
	var err error
	qc.gqlRes, err = gql.ParseWithNeedVars(gql.Request{
		Str:       upsertQuery,
		Variables: qc.req.Vars,
	}, needVars)
	if err != nil {
		return err
	}
	return validateQuery(qc.gqlRes.Query)
}

func authorizeRequest(ctx context.Context, qc *queryContext) error {
	if err := authorizeQuery(ctx, &qc.gqlRes, qc.graphql); err != nil {
		return err
	}

	// TODO(Aman): can be optimized to do the authorization in just one func call
	for _, gmu := range qc.gmuList {
		if err := authorizeMutation(ctx, gmu); err != nil {
			return err
		}
	}

	return nil
}

func getHash(ns, startTs uint64) string {
	h := sha256.New()
	h.Write([]byte(fmt.Sprintf("%#x%#x%s", ns, startTs, x.WorkerConfig.HmacSecret)))
	return hex.EncodeToString(h.Sum(nil))
}

func validateNamespace(ctx context.Context, tc *pb.TxnContext) error {
	if !x.WorkerConfig.AclEnabled {
		return nil
	}

	ns, err := x.ExtractJWTNamespace(ctx)
	if err != nil {
		return err
	}
	if tc.Hash != getHash(ns, tc.StartTs) {
		return x.ErrHashMismatch
	}
	return nil
}

func CommitOrAbort(ctx context.Context, tc *pb.TxnContext) (*pb.TxnContext, error) {
	return tc, nil
}

// CheckVersion returns the version of this Dgraph instance.
func CheckVersion(ctx context.Context, c *pb.Check) (v *pb.Version, err error) {
	if err := x.HealthCheck(); err != nil {
		return v, err
	}

	v = new(pb.Version)
	v.Tag = x.Version()
	return v, nil
}

//-------------------------------------------------------------------------------------------------
// HELPER FUNCTIONS
//-------------------------------------------------------------------------------------------------
func isMutationAllowed(ctx context.Context) bool {
	if worker.Config.MutationsMode != worker.DisallowMutations {
		return true
	}
	shareAllowed, ok := ctx.Value("_share_").(bool)
	if !ok || !shareAllowed {
		return false
	}
	return true
}

var errNoAuth = errors.Errorf("No Auth Token found. Token needed for Admin operations.")

func hasAdminAuth(ctx context.Context, tag string) (net.Addr, error) {
	ipAddr, err := x.HasWhitelistedIP(ctx)
	if err != nil {
		return nil, err
	}
	glog.Infof("Got %s request from: %q\n", tag, ipAddr)
	if err = hasPoormansAuth(ctx); err != nil {
		return nil, err
	}
	return ipAddr, nil
}

func hasPoormansAuth(ctx context.Context) error {
	if worker.Config.AuthToken == "" {
		return nil
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return errNoAuth
	}
	tokens := md.Get("auth-token")
	if len(tokens) == 0 {
		return errNoAuth
	}
	if tokens[0] != worker.Config.AuthToken {
		return errors.Errorf("Provided auth token [%s] does not match. Permission denied.", tokens[0])
	}
	return nil
}

// parseMutationObject tries to consolidate fields of the pb.Mutation into the
// corresponding field of the returned gql.Mutation. For example, the 3 fields,
// pb.Mutation#SetJson, pb.Mutation#SetNquads and pb.Mutation#Set are consolidated into the
// gql.Mutation.Set field. Similarly the 3 fields pb.Mutation#DeleteJson, pb.Mutation#DelNquads
// and pb.Mutation#Del are merged into the gql.Mutation#Del field.
func parseMutationObject(mu *pb.Mutation, qc *queryContext) (*pb.Mutation, error) {
	res := &pb.Mutation{Cond: mu.Cond}

	if len(mu.SetJson) > 0 {
		nqs, err := chunker.ParseJSON(mu.SetJson, chunker.SetNquads)
		if err != nil {
			return nil, err
		}
		res.Nquads = append(res.Nquads, nqs...)
	}
	if len(mu.DeleteJson) > 0 {
		// The metadata is not currently needed for delete operations so it can be safely ignored.
		nqs, err := chunker.ParseJSON(mu.DeleteJson, chunker.DeleteNquads)
		if err != nil {
			return nil, err
		}
		for _, nq := range nqs {
			nq.Op = pb.NQuad_DEL
		}
		res.Nquads = append(res.Nquads, nqs...)
	}
	res.Nquads = append(res.Nquads, mu.Nquads...)
	if err := validateNQuads(res.Nquads, qc); err != nil {
		return nil, err
	}
	return res, nil
}

// validateForGraphql validate nquads for graphql
func validateForGraphql(nq *pb.NQuad, isGraphql bool) error {
	// Check whether the incoming predicate is graphql reserved predicate or not.
	if !isGraphql && x.IsGraphqlReservedPredicate(nq.Predicate) {
		return errors.Errorf("Cannot mutate graphql reserved predicate %s", nq.Predicate)
	}
	return nil
}

func validateNQuads(nquads []*pb.NQuad, qc *queryContext) error {
	for _, nq := range nquads {
		if nq.Op != pb.NQuad_SET {
			continue
		}
		if err := validatePredName(nq.Predicate); err != nil {
			return err
		}
		var ostar bool
		if o, ok := nq.ObjectValue.GetVal().(*pb.Value_DefaultVal); ok {
			ostar = o.DefaultVal == x.Star
		}
		if nq.Subject == x.Star || nq.Predicate == x.Star || ostar {
			return errors.Errorf("Cannot use star in set n-quad: %+v", nq)
		}
		if err := validateKeys(nq); err != nil {
			return errors.Wrapf(err, "key error: %+v", nq)
		}
		if err := validateForGraphql(nq, qc.graphql); err != nil {
			return err
		}
	}
	for _, nq := range nquads {
		if nq.Op != pb.NQuad_DEL {
			continue
		}
		if err := validatePredName(nq.Predicate); err != nil {
			return err
		}
		var ostar bool
		if o, ok := nq.ObjectValue.GetVal().(*pb.Value_DefaultVal); ok {
			ostar = o.DefaultVal == x.Star
		}
		if nq.Subject == x.Star || (nq.Predicate == x.Star && !ostar) {
			return errors.Errorf("Only valid wildcard delete patterns are 'S * *' and 'S P *': %v", nq)
		}
		if err := validateForGraphql(nq, qc.graphql); err != nil {
			return err
		}
		// NOTE: we dont validateKeys() with delete to let users fix existing mistakes
		// with bad predicate forms. ex: foo@bar ~something
	}
	return nil
}

func validateKey(key string) error {
	switch {
	case key == "":
		return errors.Errorf("Has zero length")
	case strings.ContainsAny(key, "~@"):
		return errors.Errorf("Has invalid characters")
	case strings.IndexFunc(key, unicode.IsSpace) != -1:
		return errors.Errorf("Must not contain spaces")
	}
	return nil
}

// validateKeys checks predicate and facet keys in N-Quad for syntax errors.
func validateKeys(nq *pb.NQuad) error {
	if err := validateKey(nq.Predicate); err != nil {
		return errors.Wrapf(err, "predicate %q", nq.Predicate)
	}
	return nil
}

// validateQuery verifies that the query does not contain any preds that
// are longer than the limit (2^16).
func validateQuery(queries []*gql.GraphQuery) error {
	for _, q := range queries {
		if err := validatePredName(q.Attr); err != nil {
			return err
		}

		if err := validateQuery(q.Children); err != nil {
			return err
		}
	}

	return nil
}

func validatePredName(name string) error {
	if len(name) > math.MaxUint16 {
		return errors.Errorf("Predicate name length cannot be bigger than 2^16. Predicate: %v",
			name[:80])
	}
	return nil
}

func isDropAll(op *pb.Operation) bool {
	if op.DropAll || op.DropOp == pb.Operation_ALL {
		return true
	}
	return false
}
