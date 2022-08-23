// Portions Copyright 2019 Dgraph Labs, Inc. are available under the Apache License v2.0.
// Portions Copyright 2022 Outcaste LLC are available under the Sustainable License v1.0.

package resolve

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/outcaste-io/outserv/edgraph"
	"github.com/outcaste-io/outserv/protos/pb"
	"github.com/outcaste-io/outserv/x"
	"github.com/pkg/errors"
	"go.opencensus.io/trace"
	otrace "go.opencensus.io/trace"

	"github.com/golang/glog"

	"github.com/outcaste-io/outserv/graphql/dgraph"
	"github.com/outcaste-io/outserv/graphql/schema"
)

type resolveCtxKey string

const (
	methodResolve = "RequestResolver.Resolve"

	resolveStartTime resolveCtxKey = "resolveStartTime"

	resolverFailed    = false
	resolverSucceeded = true

	ErrInternal = "Internal error"
)

// RequestResolver can process GraphQL requests and write GraphQL JSON responses.
// A schema.Request may contain any number of queries or mutations (never both).
// RequestResolver.Resolve() resolves all of them by finding the resolved answers
// of the component queries/mutations and joining into a single schema.Response.
type RequestResolver struct {
	schema    *schema.Schema
	resolvers *ResolverFactory
}

// A ResolverFactory is the main implementation of ResolverFactory.  It stores a
// map of all the resolvers that have been registered and returns a resolver that
// just returns errors if it's asked for a resolver for a field that it doesn't
// know about.
type ResolverFactory struct {
	sync.RWMutex
	queryResolvers    map[string]func(*schema.Field) QueryResolver
	mutationResolvers map[string]func(*schema.Field) MutationResolver

	queryMiddlewareConfig    map[string]QueryMiddlewares
	mutationMiddlewareConfig map[string]MutationMiddlewares

	// returned if the factory gets asked for resolver for a field that it doesn't
	// know about.
	queryError    QueryResolverFunc
	mutationError MutationResolverFunc
}

// ResolverFns is a convenience struct for passing blocks of rewriters and executors.
type ResolverFns struct {
	Qrw *QueryRewriter
	Ex  DgraphExecutor
}

type DgraphEx struct{}

// Execute is the underlying dgraph implementation of Dgraph execution.
// If field is nil, returned response has JSON in DQL form, otherwise it will be in GraphQL form.
func (dg *DgraphEx) Execute(ctx context.Context,
	ereq *edgraph.Request) (*pb.Response, error) {

	span := trace.FromContext(ctx)
	stop := x.SpanTimer(span, "dgraph.Execute")
	defer stop()

	// TODO(mrjn): Ideally, we can set parsed queries directly. But, that part
	// of the code is old and easy to mess up, so it would take time. Also, I
	// doubt I want to continue exposing DQL.
	req := ereq.Req
	if req == nil || (req.Query == "" && len(req.Mutations) == 0) {
		return nil, nil
	}

	if glog.V(3) {
		muts := make([]string, len(req.Mutations))
		for i, m := range req.Mutations {
			muts[i] = m.String()
		}

		glog.Infof("Executing Dgraph request; with\nQuery: \n%s\nMutations:%s",
			req.Query, strings.Join(muts, "\n"))
	}

	ctx = context.WithValue(ctx, edgraph.IsGraphql, true)
	resp, err := edgraph.QueryGraphQL(ctx, ereq)
	if !x.IsGqlErrorList(err) {
		err = schema.GQLWrapf(err, "Dgraph execution failed")
	}

	return resp, err
}

// A Resolved is the result of resolving a single field - generally a query or mutation.
type Resolved struct {
	Data       []byte
	Field      *schema.Field
	Err        error
	Extensions *schema.Extensions
}

// CompletionFunc is an adapter that allows us to compose completions and build a
// ResultCompleter from a function.  Based on the http.HandlerFunc pattern.
type CompletionFunc func(ctx context.Context, resolved *Resolved)

// Complete calls cf(ctx, resolved)
func (cf CompletionFunc) Complete(ctx context.Context, resolved *Resolved) {
	cf(ctx, resolved)
}

// NewDgraphExecutor builds a DgraphExecutor for proxying requests through dgraph.
func NewDgraphExecutor() DgraphExecutor {
	return &DgraphEx{}
}

func (rf *ResolverFactory) WithQueryResolver(
	name string, resolver func(*schema.Field) QueryResolver) *ResolverFactory {
	rf.Lock()
	defer rf.Unlock()
	rf.queryResolvers[name] = resolver
	return rf
}

func (rf *ResolverFactory) WithMutationResolver(
	name string, resolver func(*schema.Field) MutationResolver) *ResolverFactory {
	rf.Lock()
	defer rf.Unlock()
	rf.mutationResolvers[name] = resolver
	return rf
}

func (rf *ResolverFactory) WithSchemaIntrospection() *ResolverFactory {
	return rf.
		WithQueryResolver("__schema",
			func(q *schema.Field) QueryResolver {
				return QueryResolverFunc(resolveIntrospection)
			}).
		WithQueryResolver("__type",
			func(q *schema.Field) QueryResolver {
				return QueryResolverFunc(resolveIntrospection)
			}).
		WithQueryResolver("__typename",
			func(q *schema.Field) QueryResolver {
				return QueryResolverFunc(resolveIntrospection)
			}).
		WithMutationResolver("__typename",
			func(m *schema.Field) MutationResolver {
				return MutationResolverFunc(
					func(ctx context.Context, m *schema.Field) (*Resolved, bool) {
						return DataResult(m, map[string]interface{}{"__typename": "Mutation"}, nil),
							resolverSucceeded
					})
			})
}

func (rf *ResolverFactory) WithConventionResolvers(
	s *schema.Schema, fns *ResolverFns) *ResolverFactory {

	queries := append(s.Queries(schema.GetQuery), s.Queries(schema.FilterQuery)...)
	queries = append(queries, s.Queries(schema.PasswordQuery)...)
	queries = append(queries, s.Queries(schema.AggregateQuery)...)
	for _, q := range queries {
		rf.WithQueryResolver(q, func(q *schema.Field) QueryResolver {
			return NewQueryResolver(fns.Qrw, fns.Ex)
		})
	}

	for _, q := range s.Queries(schema.HTTPQuery) {
		rf.WithQueryResolver(q, func(q *schema.Field) QueryResolver {
			return NewHTTPQueryResolver(nil)
		})
	}

	for _, m := range s.Mutations(schema.AddMutation) {
		rf.WithMutationResolver(m, func(m *schema.Field) MutationResolver {
			return NewDgraphResolver()
		})
	}

	for _, m := range s.Mutations(schema.UpdateMutation) {
		rf.WithMutationResolver(m, func(m *schema.Field) MutationResolver {
			return NewDgraphResolver()
		})
	}

	for _, m := range s.Mutations(schema.DeleteMutation) {
		rf.WithMutationResolver(m, func(m *schema.Field) MutationResolver {
			return NewDgraphResolver()
		})
	}

	for _, m := range s.Mutations(schema.HTTPMutation) {
		rf.WithMutationResolver(m, func(m *schema.Field) MutationResolver {
			return NewHTTPMutationResolver(nil)
		})
	}

	return rf
}

func (rf *ResolverFactory) WithQueryMiddlewareConfig(
	config map[string]QueryMiddlewares) *ResolverFactory {
	if len(config) != 0 {
		rf.queryMiddlewareConfig = config
	}
	return rf
}

func (rf *ResolverFactory) WithMutationMiddlewareConfig(
	config map[string]MutationMiddlewares) *ResolverFactory {
	if len(config) != 0 {
		rf.mutationMiddlewareConfig = config
	}
	return rf
}

// NewResolverFactory returns a ResolverFactory that resolves requests via
// query/mutation rewriting and execution through Dgraph.  If the factory gets asked
// to resolve a query/mutation it doesn't know how to rewrite, it uses
// the queryError/mutationError to build an error result.
func NewResolverFactory(
	queryError QueryResolverFunc, mutationError MutationResolverFunc) *ResolverFactory {

	return &ResolverFactory{
		queryResolvers:    make(map[string]func(*schema.Field) QueryResolver),
		mutationResolvers: make(map[string]func(*schema.Field) MutationResolver),

		queryMiddlewareConfig:    make(map[string]QueryMiddlewares),
		mutationMiddlewareConfig: make(map[string]MutationMiddlewares),

		queryError:    queryError,
		mutationError: mutationError,
	}
}

// noopCompletion just passes back it's result and err arguments
func noopCompletion(ctx context.Context, resolved *Resolved) {}

func (rf *ResolverFactory) queryResolverFor(query *schema.Field) QueryResolver {
	rf.RLock()
	defer rf.RUnlock()
	mws := rf.queryMiddlewareConfig[query.Name()]
	if resolver, ok := rf.queryResolvers[query.Name()]; ok {
		return mws.Then(resolver(query))
	}
	return rf.queryError
}

func (rf *ResolverFactory) mutationResolverFor(mutation *schema.Field) MutationResolver {
	rf.RLock()
	defer rf.RUnlock()
	mws := rf.mutationMiddlewareConfig[mutation.Name()]
	if resolver, ok := rf.mutationResolvers[mutation.Name()]; ok {
		return mws.Then(resolver(mutation))
	}
	return rf.mutationError
}

// New creates a new RequestResolver.
func New(s *schema.Schema, resolverFactory *ResolverFactory) *RequestResolver {
	return &RequestResolver{
		schema:    s,
		resolvers: resolverFactory,
	}
}

// Resolve processes r.GqlReq and returns a GraphQL response.
// r.GqlReq should be set with a request before Resolve is called
// and a schema and backend Dgraph should have been added.
// Resolve records any errors in the response's error field.
func (r *RequestResolver) Resolve(ctx context.Context, gqlReq *schema.Request) (resp *schema.Response) {
	span := otrace.FromContext(ctx)
	stop := x.SpanTimer(span, methodResolve)
	defer stop()

	if r == nil {
		glog.Errorf("Call to Resolve with nil RequestResolver")
		return schema.ErrorResponse(errors.New(ErrInternal))
	}

	if r.schema == nil {
		glog.Errorf("Call to Resolve with no schema")
		return schema.ErrorResponse(errors.New(ErrInternal))
	}

	startTime := time.Now()
	resp = &schema.Response{
		Extensions: &schema.Extensions{},
	}
	// Panic Handler for mutation. This ensures that the mutation which causes panic
	// gets logged in Alpha logs. This panic handler overrides the default Panic Handler
	// used in recoveryHandler in admin/http.go
	defer x.PanicHandler(
		func(err error) {
			resp.Errors = schema.AsGQLErrors(schema.AppendGQLErrs(resp.Errors, err))
		}, gqlReq.Query)

	ctx = context.WithValue(ctx, resolveStartTime, startTime)
	ctx = x.AttachJWTNamespace(ctx)
	op, err := r.schema.Operation(gqlReq)
	if err != nil {
		resp.Errors = schema.AsGQLErrors(err)
		return
	}

	if glog.V(3) {
		// don't log the introspection queries they are sent too frequently
		// by GraphQL dev tools
		if !op.IsQuery() ||
			(op.IsQuery() && !strings.HasPrefix(op.Queries()[0].Name(), "__")) {
			b, err := json.Marshal(gqlReq.Variables)
			if err != nil {
				glog.Infof("Failed to marshal variables for logging : %s", err)
			}
			glog.Infof("Resolving GQL request: \n%s\nWith Variables: \n%s\n",
				gqlReq.Query, string(b))
		}
	}

	// resolveQueries will resolve user's queries.
	resolveQueries := func() {
		// Queries run in parallel and are independent of each other: e.g.
		// an error in one query, doesn't affect the others.

		var wg sync.WaitGroup
		allResolved := make([]*Resolved, len(op.Queries()))

		for i, q := range op.Queries() {
			wg.Add(1)

			go func(q *schema.Field, storeAt int) {
				defer wg.Done()
				defer x.PanicHandler(
					func(err error) {
						allResolved[storeAt] = &Resolved{
							Data:  nil,
							Field: q,
							Err:   err,
						}
					}, gqlReq.Query)
				allResolved[storeAt] = r.resolvers.queryResolverFor(q).Resolve(ctx, q)
			}(q, i)
		}
		wg.Wait()

		// The GraphQL data response needs to be written in the same order as the
		// queries in the request.
		for _, res := range allResolved {
			// Errors and data in the same response is valid.  Both WithError and
			// AddData handle nil cases.
			addResult(resp, res)

		}
	}
	// A single request can contain either queries or mutations - not both.
	// GraphQL validation on the request would have caught that error case
	// before we get here.  At this point, we know it's valid, it's passed
	// GraphQL validation and any additional validation we've added.  So here,
	// we can just execute it.
	switch {
	case op.IsQuery():
		if op.CacheControl() != "" {
			resp.Header = make(map[string][]string)
			resp.Header.Set(schema.CacheControlHeader, op.CacheControl())
			resp.Header.Set("Vary", "Accept-Encoding")
		}
		resolveQueries()
	case op.IsMutation():
		// A mutation operation can contain any number of mutation fields.  Those should be executed
		// serially.
		// (spec https://graphql.github.io/graphql-spec/June2018/#sec-Normal-and-Serial-Execution)
		//
		// The spec is ambiguous about what to do in the case of errors during that serial execution
		// - apparently deliberately so; see this comment from Lee Byron:
		// https://github.com/graphql/graphql-spec/issues/277#issuecomment-385588590
		// and clarification
		// https://github.com/graphql/graphql-spec/pull/438
		//
		// A reasonable interpretation of that is to stop a list of mutations after the first error -
		// which seems like the natural semantics and is what we enforce here.
		allSuccessful := true

		for _, m := range op.Mutations() {
			if !allSuccessful {
				resp.WithError(x.GqlErrorf(
					"Mutation %s was not executed because of a previous error.",
					m.ResponseName()).
					WithLocations(m.Location()).
					WithPath([]interface{}{m.ResponseName()}))

				continue
			}

			var res *Resolved
			res, allSuccessful = r.resolvers.mutationResolverFor(m).Resolve(ctx, m)
			addResult(resp, res)
		}
	case op.IsSubscription():
		resolveQueries()
	}

	return resp
}

// ValidateSubscription will check the given subscription query is valid or not.
func (r *RequestResolver) ValidateSubscription(req *schema.Request) error {
	op, err := r.schema.Operation(req)
	if err != nil {
		return err
	}

	if !op.IsSubscription() {
		return errors.New("given GraphQL operation is not a subscription")
	}

	for _, q := range op.Queries() {
		for _, field := range q.SelectionSet() {
			if err := validateCustomFieldsRecursively(field); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *RequestResolver) Schema() *schema.Schema {
	return r.schema
}

// validateCustomFieldsRecursively will return err if the given field is custom or any of its
// children is type of a custom field.
func validateCustomFieldsRecursively(field *schema.Field) error {
	if field.IsCustomHTTP() {
		return x.GqlErrorf("Custom field `%s` is not supported in graphql subscription",
			field.Name()).WithLocations(field.Location())
	}
	for _, f := range field.SelectionSet() {
		err := validateCustomFieldsRecursively(f)
		if err != nil {
			return err
		}
	}
	return nil
}

func addResult(resp *schema.Response, res *Resolved) {
	// Errors should report the "path" into the result where the error was found.
	//
	// The definition of a path in a GraphQL error is here:
	// https://graphql.github.io/graphql-spec/June2018/#sec-Errors
	// For a query like (assuming field f is of a list type and g is a scalar type):
	// - q { f { g } }
	// a path to the 3rd item in the f list would look like:
	// - [ "q", "f", 2, "g" ]
	if res.Data == nil && !res.Field.Type().Nullable() {
		// According to GraphQL spec, out of all the queries in the request, if any one query
		// returns null but expected return type is non-nullable then we set root data to null.
		resp.SetDataNull()
	} else {
		resp.AddData(res.Data)
	}

	resp.WithError(res.Err)
	resp.MergeExtensions(res.Extensions)
}

// a httpResolver can resolve a single GraphQL field from an HTTP endpoint
type httpResolver struct {
	*http.Client
}

type httpQueryResolver httpResolver
type httpMutationResolver httpResolver

// NewHTTPQueryResolver creates a resolver that can resolve GraphQL query from an HTTP endpoint
func NewHTTPQueryResolver(hc *http.Client) QueryResolver {
	return &httpQueryResolver{hc}
}

// NewHTTPMutationResolver creates a resolver that resolves GraphQL mutation from an HTTP endpoint
func NewHTTPMutationResolver(hc *http.Client) MutationResolver {
	return &httpMutationResolver{hc}
}

func (hr *httpResolver) Resolve(ctx context.Context, field *schema.Field) *Resolved {
	span := otrace.FromContext(ctx)
	stop := x.SpanTimer(span, "resolveHTTP")
	defer stop()

	resolved := hr.rewriteAndExecute(ctx, field)
	return resolved
}

func (hr *httpResolver) rewriteAndExecute(ctx context.Context, field *schema.Field) *Resolved {
	ns, _ := x.ExtractNamespace(ctx)
	hrc, err := field.CustomHTTPConfig(ns)
	if err != nil {
		return EmptyResult(field, err)
	}

	// If this is a lambda field, it will always have a body template.
	// Just convert that into a lambda template.
	if field.HasLambdaDirective() {
		hrc.Template = schema.GetBodyForLambda(ctx, field, nil, hrc.Template)
	}

	fieldData, errs, hardErrs := hrc.MakeAndDecodeHTTPRequest(hr.Client, hrc.URL, hrc.Template,
		field)
	if hardErrs != nil {
		// Not using EmptyResult() here as we don't want to wrap the errors returned from remote
		// endpoints
		return &Resolved{
			Data:  field.NullResponse(),
			Field: field,
			Err:   hardErrs,
		}
	}

	return DataResult(field, map[string]interface{}{field.Name(): fieldData}, errs)
}

func (h *httpQueryResolver) Resolve(ctx context.Context, query *schema.Field) *Resolved {
	return (*httpResolver)(h).Resolve(ctx, query)
}

func (h *httpMutationResolver) Resolve(ctx context.Context, mutation *schema.Field) (*Resolved,
	bool) {
	resolved := (*httpResolver)(h).Resolve(ctx, mutation)
	return resolved, resolved.Err == nil || resolved.Err.Error() == ""
}

func EmptyResult(f *schema.Field, err error) *Resolved {
	return &Resolved{
		Data:  f.NullResponse(),
		Field: f,
		Err:   schema.GQLWrapLocationf(err, f.Location(), "resolving %s failed", f.Name()),
	}
}

func DataResult(f *schema.Field, data map[string]interface{}, err error) *Resolved {
	b, errs := schema.CompleteObject(f.PreAllocatePathSlice(), []*schema.Field{f}, data)

	return &Resolved{
		Data:  b,
		Field: f,
		Err:   schema.AppendGQLErrs(err, errs),
	}
}

var errNotScalar = errors.New("provided value is not a scalar, can't convert it to string")

// A QueryResolver can resolve a single query.
// Implemented by:
// - httpResolver
// - httpQueryResolver
// - QueryResolverFunc
// - queryResolver
// - customDQLQueryResolver
// - getSchemaResolver in admin/schema.go
type QueryResolver interface {
	Resolve(ctx context.Context, query *schema.Field) *Resolved
}

// QueryResolverFunc is an adapter that allows to build a QueryResolver from
// a function.  Based on the http.HandlerFunc pattern.
type QueryResolverFunc func(ctx context.Context, query *schema.Field) *Resolved

// Resolve calls qr(ctx, query)
func (qr QueryResolverFunc) Resolve(ctx context.Context, query *schema.Field) *Resolved {
	return qr(ctx, query)
}

// NewQueryResolver creates a new query resolver.  The resolver runs the pipeline:
// 1) rewrite the query using qr (return error if failed)
// 2) execute the rewritten query with ex (return error if failed)
// 3) process the result with rc
func NewQueryResolver(qr *QueryRewriter, ex DgraphExecutor) QueryResolver {
	return &queryResolver{queryRewriter: qr, executor: ex, resultCompleter: CompletionFunc(noopCompletion)}
}

// a queryResolver can resolve a single GraphQL query field.
type queryResolver struct {
	queryRewriter   *QueryRewriter
	executor        DgraphExecutor
	resultCompleter CompletionFunc
}

func (qr *queryResolver) Resolve(ctx context.Context, query *schema.Field) *Resolved {
	span := otrace.FromContext(ctx)
	stop := x.SpanTimer(span, "resolveQuery")
	defer stop()

	resolved := qr.rewriteAndExecute(ctx, query)
	qr.resultCompleter.Complete(ctx, resolved)
	return resolved
}

func (qr *queryResolver) rewriteAndExecute(ctx context.Context, query *schema.Field) *Resolved {
	ext := &schema.Extensions{}

	emptyResult := func(err error) *Resolved {
		return &Resolved{
			// all the auto-generated queries are nullable, but users may define queries with
			// @custom(dql: ...) which may be non-nullable. So, we need to set the Data field
			// only if the query was nullable and keep it nil if it was non-nullable.
			// query.NullResponse() method handles that.
			Data:       query.NullResponse(),
			Field:      query,
			Err:        schema.SetPathIfEmpty(err, query.ResponseName()),
			Extensions: ext,
		}
	}

	dgQuery, err := qr.queryRewriter.Rewrite(ctx, query)
	if err != nil {
		return emptyResult(schema.GQLWrapf(err, "couldn't rewrite query %s",
			query.ResponseName()))
	}
	qry := dgraph.AsString(dgQuery)
	glog.V(2).Infof("DQL Query: %s\n", qry)

	req := &edgraph.Request{
		Req:      &pb.Request{Query: qry, ReadOnly: true},
		GqlField: query,
	}
	resp, err := qr.executor.Execute(ctx, req)

	if err != nil && !x.IsGqlErrorList(err) {
		err = schema.GQLWrapf(err, "Dgraph query failed")
		glog.Infof("Dgraph query execution failed : %s", err)
		return emptyResult(err)
	}

	ext.TouchedUids = resp.GetMetrics().GetNumUids()[touchedUidsKey]
	resolved := &Resolved{
		Data:       resp.GetJson(),
		Field:      query,
		Err:        schema.SetPathIfEmpty(err, query.ResponseName()),
		Extensions: ext,
	}

	return resolved
}

func resolveIntrospection(ctx context.Context, q *schema.Field) *Resolved {
	data, err := schema.Introspect(q)
	return &Resolved{
		Data:  data,
		Field: q,
		Err:   err,
	}
}

// converts scalar values received from GraphQL arguments to go string
// If it is a scalar only possible cases are: string, bool, int64, float64 and nil.
func convertScalarToString(val interface{}) (string, error) {
	var str string
	switch v := val.(type) {
	case string:
		str = v
	case bool:
		str = strconv.FormatBool(v)
	case int64:
		str = strconv.FormatInt(v, 10)
	case float64:
		str = strconv.FormatFloat(v, 'f', -1, 64)
	case json.Number:
		str = v.String()
	case nil:
		str = ""
	default:
		return "", errNotScalar
	}
	return str, nil
}
