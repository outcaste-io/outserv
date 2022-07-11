// Portions Copyright 2019 Dgraph Labs, Inc. are available under the Apache License v2.0.
// Portions Copyright 2022 Outcaste LLC are available under the Sustainable License v1.0.

package admin

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/glog"
	"github.com/pkg/errors"

	"github.com/outcaste-io/outserv/edgraph"
	"github.com/outcaste-io/outserv/graphql/resolve"
	"github.com/outcaste-io/outserv/graphql/schema"
	"github.com/outcaste-io/outserv/worker"
	"github.com/outcaste-io/outserv/x"
)

const (
	errMsgServerNotReady = "Unavailable: Server not ready."

	errNoGraphQLSchema = "Not resolving %s. There's no GraphQL schema in Dgraph. " +
		"Use the /admin API to add a GraphQL schema"
	errResolverNotFound = "%s was not executed because no suitable resolver could be found - " +
		"this indicates a resolver or validation bug. Please let us know by filing an issue."
)

var (
	// gogQryMWs are the middlewares which should be applied to queries served by
	// admin server for guardian of galaxy unless some exceptional behaviour is required
	gogQryMWs = resolve.QueryMiddlewares{
		resolve.IpWhitelistingMW4Query,
		resolve.GuardianOfTheGalaxyAuthMW4Query,
		resolve.LoggingMWQuery,
	}
	// gogMutMWs are the middlewares which should be applied to mutations
	// served by admin server for guardian of galaxy unless some exceptional behaviour is required
	gogMutMWs = resolve.MutationMiddlewares{
		resolve.IpWhitelistingMW4Mutation,
		resolve.GuardianOfTheGalaxyAuthMW4Mutation,
		resolve.LoggingMWMutation,
	}
	// gogAclMutMWs are the middlewares which should be applied to mutations
	// served by the admin server for guardian of galaxy with ACL enabled.
	gogAclMutMWs = resolve.MutationMiddlewares{
		resolve.IpWhitelistingMW4Mutation,
		resolve.AclOnlyMW4Mutation,
		resolve.GuardianOfTheGalaxyAuthMW4Mutation,
		resolve.LoggingMWMutation,
	}
	// stdAdminQryMWs are the middlewares which should be applied to queries served by admin
	// server unless some exceptional behaviour is required
	stdAdminQryMWs = resolve.QueryMiddlewares{
		resolve.IpWhitelistingMW4Query, // good to apply ip whitelisting before Guardian auth
		resolve.GuardianAuthMW4Query,
		resolve.LoggingMWQuery,
	}
	// stdAdminMutMWs are the middlewares which should be applied to mutations served by
	// admin server unless some exceptional behaviour is required
	stdAdminMutMWs = resolve.MutationMiddlewares{
		resolve.IpWhitelistingMW4Mutation, // good to apply ip whitelisting before Guardian auth
		resolve.GuardianAuthMW4Mutation,
		resolve.LoggingMWMutation,
	}
	// minimalAdminQryMWs is the minimal set of middlewares that should be applied to any query
	// served by the admin server
	minimalAdminQryMWs = resolve.QueryMiddlewares{
		resolve.IpWhitelistingMW4Query,
		resolve.LoggingMWQuery,
	}
	// minimalAdminMutMWs is the minimal set of middlewares that should be applied to any mutation
	// served by the admin server
	minimalAdminMutMWs = resolve.MutationMiddlewares{
		resolve.IpWhitelistingMW4Mutation,
		resolve.LoggingMWMutation,
	}
	adminQueryMWConfig = map[string]resolve.QueryMiddlewares{
		"health":          minimalAdminQryMWs, // dgraph checks Guardian auth for health
		"state":           minimalAdminQryMWs, // dgraph checks Guardian auth for state
		"config":          gogQryMWs,
		"getGQLSchema":    stdAdminQryMWs,
		"getLambdaScript": stdAdminQryMWs,
		// for queries and mutations related to User/Group, dgraph handles Guardian auth,
		// so no need to apply GuardianAuth Middleware
		"queryUser":      minimalAdminQryMWs,
		"queryGroup":     minimalAdminQryMWs,
		"getUser":        minimalAdminQryMWs,
		"getCurrentUser": minimalAdminQryMWs,
		"getGroup":       minimalAdminQryMWs,
	}
	adminMutationMWConfig = map[string]resolve.MutationMiddlewares{
		"config":             gogMutMWs,
		"draining":           gogMutMWs,
		"export":             stdAdminMutMWs, // dgraph handles the export by GoG internally
		"login":              minimalAdminMutMWs,
		"shutdown":           gogMutMWs,
		"removeNode":         gogMutMWs,
		"moveTablet":         gogMutMWs,
		"assign":             gogMutMWs,
		"enterpriseLicense":  gogMutMWs,
		"updateGQLSchema":    stdAdminMutMWs,
		"updateLambdaScript": stdAdminMutMWs,
		"addNamespace":       gogAclMutMWs,
		"deleteNamespace":    gogAclMutMWs,
		"resetPassword":      gogAclMutMWs,
		// for queries and mutations related to User/Group, dgraph handles Guardian auth,
		// so no need to apply GuardianAuth Middleware
		"addUser":     minimalAdminMutMWs,
		"addGroup":    minimalAdminMutMWs,
		"updateUser":  minimalAdminMutMWs,
		"updateGroup": minimalAdminMutMWs,
		"deleteUser":  minimalAdminMutMWs,
		"deleteGroup": minimalAdminMutMWs,
	}
	// mainHealthStore stores the health of the main GraphQL server.
	mainHealthStore = &GraphQLHealthStore{}
	// adminServerVar stores a pointer to the adminServer. It is used for lazy loading schema.
	adminServerVar *adminServer
)

func SchemaValidate(sch string) error {
	schHandler, err := schema.NewHandler(sch, false)
	if err != nil {
		return err
	}

	_, err = schema.FromString(schHandler.GQLSchema(), x.GalaxyNamespace)
	return err
}

// GraphQLHealth is used to report the health status of a GraphQL server.
// It is required for kubernetes probing.
type GraphQLHealth struct {
	Healthy   bool
	StatusMsg string
}

// GraphQLHealthStore stores GraphQLHealth in a thread-safe way.
type GraphQLHealthStore struct {
	v atomic.Value
}

func (g *GraphQLHealthStore) GetHealth() GraphQLHealth {
	v := g.v.Load()
	if v == nil {
		return GraphQLHealth{Healthy: false, StatusMsg: "init"}
	}
	return v.(GraphQLHealth)
}

func (g *GraphQLHealthStore) up() {
	g.v.Store(GraphQLHealth{Healthy: true, StatusMsg: "up"})
}

func (g *GraphQLHealthStore) updatingSchema() {
	g.v.Store(GraphQLHealth{Healthy: true, StatusMsg: "updating schema"})
}

type adminServer struct {
	rf       *resolve.ResolverFactory
	resolver *resolve.RequestResolver

	// The mutex that locks schema update operations
	mux sync.RWMutex

	// The GraphQL server that's being admin'd
	gqlServer *GqlHandler

	gqlSchemas *worker.GQLSchemaStore
	// When the schema changes, we use these to create a new RequestResolver for
	// the main graphql endpoint (gqlServer) and thus refresh the API.
	fns               *resolve.ResolverFns
	withIntrospection bool
	globalEpoch       map[uint64]*uint64
}

// NewServers initializes the GraphQL servers.  It sets up an empty server for the
// main /graphql endpoint and an admin server.  The result is mainServer, adminServer.
func NewServers(withIntrospection bool, globalEpoch map[uint64]*uint64) (
	*GqlHandler, *GqlHandler, *GraphQLHealthStore) {

	gqlSchema, err := schema.FromString("", x.GalaxyNamespace)
	if err != nil {
		x.Panic(err)
	}

	resolvers := resolve.New(gqlSchema, resolverFactoryWithErrorMsg(errNoGraphQLSchema))
	e := globalEpoch[x.GalaxyNamespace]
	mainServer := NewServer()
	mainServer.Set(x.GalaxyNamespace, e, resolvers)

	fns := &resolve.ResolverFns{
		Qrw: resolve.NewQueryRewriter(),
		Ex:  resolve.NewDgraphExecutor(),
	}

	// newAdminResolver loads up GraphQL schema.
	adminResolvers := newAdminResolver(mainServer, fns, withIntrospection, globalEpoch)
	e = globalEpoch[x.GalaxyNamespace]
	adminServer := NewServer()
	adminServer.Set(x.GalaxyNamespace, e, adminResolvers)

	return mainServer, adminServer, mainHealthStore
}

// newAdminResolver creates a GraphQL request resolver for the /admin endpoint.
func newAdminResolver(
	defaultGqlServer *GqlHandler,
	fns *resolve.ResolverFns,
	withIntrospection bool,
	epoch map[uint64]*uint64) *resolve.RequestResolver {

	adminSchema, err := schema.FromString(graphqlAdminSchema, x.GalaxyNamespace)
	if err != nil {
		x.Panic(err)
	}

	rf := newAdminResolverFactory()

	server := &adminServer{
		rf:                rf,
		resolver:          resolve.New(adminSchema, rf),
		fns:               fns,
		withIntrospection: withIntrospection,
		globalEpoch:       epoch,
		gqlSchemas:        worker.NewGQLSchemaStore(),
		gqlServer:         defaultGqlServer,
	}
	adminServerVar = server // store the admin server in package variable

	// The subscribe for updates code is really ugly. Removing it for now.
	server.initServer()

	return server.resolver
}

func newAdminResolverFactory() *resolve.ResolverFactory {
	adminMutationResolvers := map[string]resolve.MutationResolverFunc{
		"addNamespace":       resolveAddNamespace,
		"config":             resolveUpdateConfig,
		"deleteNamespace":    resolveDeleteNamespace,
		"draining":           resolveDraining,
		"export":             resolveExport,
		"login":              resolveLogin,
		"resetPassword":      resolveResetPassword,
		"shutdown":           resolveShutdown,
		"updateLambdaScript": resolveUpdateLambda,

		"removeNode": resolveRemoveNode,
		"moveTablet": resolveMoveTablet,
		"assign":     resolveAssign,
	}

	rf := resolverFactoryWithErrorMsg(errResolverNotFound).
		WithQueryMiddlewareConfig(adminQueryMWConfig).
		WithMutationMiddlewareConfig(adminMutationMWConfig).
		WithQueryResolver("health", func(q *schema.Field) resolve.QueryResolver {
			return resolve.QueryResolverFunc(resolveHealth)
		}).
		WithQueryResolver("state", func(q *schema.Field) resolve.QueryResolver {
			return resolve.QueryResolverFunc(resolveState)
		}).
		WithQueryResolver("config", func(q *schema.Field) resolve.QueryResolver {
			return resolve.QueryResolverFunc(resolveGetConfig)
		}).
		WithQueryResolver("task", func(q *schema.Field) resolve.QueryResolver {
			return resolve.QueryResolverFunc(resolveTask)
		}).
		WithQueryResolver("getLambdaScript", func(q *schema.Field) resolve.QueryResolver {
			return resolve.QueryResolverFunc(resolveGetLambda)
		}).
		WithQueryResolver("getGQLSchema", func(q *schema.Field) resolve.QueryResolver {
			return resolve.QueryResolverFunc(
				func(ctx context.Context, query *schema.Field) *resolve.Resolved {
					return &resolve.Resolved{Err: errors.Errorf(errMsgServerNotReady), Field: q}
				})
		}).
		WithMutationResolver("updateGQLSchema", func(m *schema.Field) resolve.MutationResolver {
			return resolve.MutationResolverFunc(
				func(ctx context.Context, m *schema.Field) (*resolve.Resolved, bool) {
					return &resolve.Resolved{Err: errors.Errorf(errMsgServerNotReady), Field: m},
						false
				})
		})
	for gqlMut, resolver := range adminMutationResolvers {
		// gotta force go to evaluate the right function at each loop iteration
		// otherwise you get variable capture issues
		func(f resolve.MutationResolver) {
			rf.WithMutationResolver(gqlMut, func(m *schema.Field) resolve.MutationResolver {
				return f
			})
		}(resolver)
	}

	return rf.WithSchemaIntrospection()
}

func getCurrentGraphQLSchema(namespace uint64) (*worker.GqlSchema, error) {
	graphQLSchema, err := edgraph.GetGQLSchema(namespace)
	if err != nil {
		return nil, err
	}

	return &worker.GqlSchema{Schema: graphQLSchema}, nil
}

func generateGQLSchema(sch *worker.GqlSchema, ns uint64) (*schema.Schema, error) {
	schHandler, err := schema.NewHandler(sch.Schema, false)
	if err != nil {
		return nil, err
	}
	sch.GeneratedSchema = schHandler.GQLSchema()
	generatedSchema, err := schema.FromString(sch.GeneratedSchema, ns)
	if err != nil {
		return nil, err
	}
	generatedSchema.SetMeta(schHandler.MetaInfo())

	return generatedSchema, nil
}

func (as *adminServer) initServer() {
	x.BlockUntilHealthy()
	defer mainHealthStore.up()

	// Nothing else should be able to lock before here.  The admin resolvers aren't yet
	// set up (they all just error), so we will obtain the lock here without contention.
	// We then setup the admin resolvers and they must wait until we are done before the
	// first admin calls will go through.
	as.mux.Lock()
	defer as.mux.Unlock()

	for {
		sch, err := getCurrentGraphQLSchema(x.GalaxyNamespace)
		if err != nil {
			glog.Errorf("namespace: %d. Error reading GraphQL schema: %s.", x.GalaxyNamespace, err)
			time.Sleep(time.Second)
			continue
		}
		as.gqlSchemas.Set(x.GalaxyNamespace, sch)
		// adding the actual resolvers for updateGQLSchema and getGQLSchema only after server has
		// current GraphQL schema, if there was any.
		as.addConnectedAdminResolvers()

		if sch.Schema == "" {
			glog.Infof("namespace: %d. No GraphQL schema in Dgraph; serving empty GraphQL API",
				x.GalaxyNamespace)
			return
		}

		generatedSchema, err := generateGQLSchema(sch, x.GalaxyNamespace)
		if err != nil {
			glog.Errorf("namespace: %d. Error processing GraphQL schema: %s.",
				x.GalaxyNamespace, err)
			return
		}
		as.incrementSchemaUpdateCounter(x.GalaxyNamespace)
		as.resetSchema(x.GalaxyNamespace, generatedSchema)

		glog.Infof("namespace: %d. Successfully loaded GraphQL schema: \n---\n%s\n---\n",
			x.GalaxyNamespace, sch.Schema)
		return
	}
}

// addConnectedAdminResolvers sets up the real resolvers
func (as *adminServer) addConnectedAdminResolvers() {
	as.rf.WithMutationResolver("updateGQLSchema",
		func(m *schema.Field) resolve.MutationResolver {
			return &updateSchemaResolver{admin: as}
		}).
		WithQueryResolver("getGQLSchema",
			func(q *schema.Field) resolve.QueryResolver {
				return &getSchemaResolver{admin: as}
			})
}

func resolverFactoryWithErrorMsg(msg string) *resolve.ResolverFactory {
	errFunc := func(name string) error { return errors.Errorf(msg, name) }
	qErr :=
		resolve.QueryResolverFunc(func(ctx context.Context, query *schema.Field) *resolve.Resolved {
			return &resolve.Resolved{Err: errFunc(query.ResponseName()), Field: query}
		})

	mErr := resolve.MutationResolverFunc(
		func(ctx context.Context, mutation *schema.Field) (*resolve.Resolved, bool) {
			return &resolve.Resolved{Err: errFunc(mutation.ResponseName()), Field: mutation}, false
		})

	return resolve.NewResolverFactory(qErr, mErr)
}

func (as *adminServer) getGlobalEpoch(ns uint64) *uint64 {
	e := as.globalEpoch[ns]
	if e == nil {
		e = new(uint64)
		as.globalEpoch[ns] = e
	}
	return e
}

func (as *adminServer) incrementSchemaUpdateCounter(ns uint64) {
	// Increment the Epoch when you get a new schema. So, that subscription's local epoch
	// will match against global epoch to terminate the current subscriptions.
	atomic.AddUint64(as.getGlobalEpoch(ns), 1)
}

func (as *adminServer) resetSchema(ns uint64, gqlSchema *schema.Schema) {
	// set status as updating schema
	mainHealthStore.updatingSchema()
	defer mainHealthStore.up()

	var resolverFactory *resolve.ResolverFactory
	// gqlSchema can be nil in following cases:
	// * after DROP_ALL
	// * if the schema hasn't yet been set even once for a non-Galaxy namespace
	// If schema is nil then do not attach Resolver for
	// introspection operations, and set GQL schema to empty.
	if gqlSchema == nil {
		resolverFactory = resolverFactoryWithErrorMsg(errNoGraphQLSchema)
		gqlSchema, _ = schema.FromString("", ns)
	} else {
		resolverFactory = resolverFactoryWithErrorMsg(errResolverNotFound).
			WithConventionResolvers(gqlSchema, as.fns)
		if as.withIntrospection {
			resolverFactory.WithSchemaIntrospection()
		}
		// Removing the Apollo federation code. The GraphQL codebase is complex
		// and confusing as it is.
	}

	resolvers := resolve.New(gqlSchema, resolverFactory)
	as.gqlServer.Set(ns, as.getGlobalEpoch(ns), resolvers)
}

func (as *adminServer) loadSchema(namespace uint64) error {
	glog.Infof("LoadSchema: Fetching schema from disk for ns: %#x\n", namespace)
	cur, err := getCurrentGraphQLSchema(namespace)
	if err != nil {
		glog.Errorf("namespace: %d. Error reading GraphQL schema: %s.", namespace, err)
		return errors.Wrap(err, "failed to lazy-load GraphQL schema")
	}

	var generatedSchema *schema.Schema
	if cur.Schema == "" {
		// if there was no schema stored in Dgraph, we still need to attach resolvers to the main
		// graphql server which should just return errors for any incoming request.
		// generatedSchema will be nil in this case
		glog.Infof("namespace: %d. No GraphQL schema in Dgraph; serving empty GraphQL API",
			namespace)
	} else {
		generatedSchema, err = generateGQLSchema(cur, namespace)
		if err != nil {
			glog.Errorf("namespace: %d. Error processing GraphQL schema: %s.", namespace, err)
			return errors.Wrap(err, "failed to lazy-load GraphQL schema")
		}
	}

	as.mux.Lock()
	defer as.mux.Unlock()
	as.gqlSchemas.Set(namespace, cur)
	as.resetSchema(namespace, generatedSchema)

	glog.Infof("LoadSchema: Successfully loaded GraphQL schema for ns: %#x", namespace)
	return nil
}

func LoadSchema(namespace uint64) error {
	if err := adminServerVar.loadSchema(namespace); err != nil {
		return err
	}

	// Even if the script is already in memory, we should fetch from disk
	// because we have updated it.
	glog.Infof("LoadSchema: Fetching lambda script from disk for ns: %#x", namespace)
	// Otherwise, fetch it from disk.
	script, err := edgraph.GetLambdaScript(namespace)
	if err != nil {
		glog.Errorf("namespace: %d. Error reading Lambda Script: %s.", namespace, err)
		return errors.Wrap(err, "failed to lazy-load Lambda Script")
	}
	worker.Lambda().Set(namespace, &worker.LambdaScript{
		Script: script,
	})
	return nil
}

func inputArgError(err error) error {
	return schema.GQLWrapf(err, "couldn't parse input argument")
}

func response(code, msg string) map[string]interface{} {
	return map[string]interface{}{
		"response": map[string]interface{}{"code": code, "message": msg}}
}

// DestinationFields is used by export to specify destination
type DestinationFields struct {
	Destination  string
	AccessKey    string
	SecretKey    string
	SessionToken string
	Anonymous    bool
}
