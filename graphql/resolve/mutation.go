// Portions Copyright 2019 Dgraph Labs, Inc. are available under the Apache License v2.0.
// Portions Copyright 2022 Outcaste LLC are available under the Smart License v1.0.

package resolve

import (
	"context"

	"github.com/outcaste-io/outserv/gql"
	"github.com/outcaste-io/outserv/graphql/schema"
	"github.com/outcaste-io/outserv/protos/pb"
)

const touchedUidsKey = "_total"

// Mutations come in like this with variables:
//
// mutation themutation($post: PostInput!) {
//   addPost(input: $post) { ... some query ...}
// }
// - with variable payload
// { "post":
//   { "title": "My Post",
//     "author": { authorID: 0x123 },
//     ...
//   }
// }
//
//
// Or, like this with the payload in the mutation arguments
//
// mutation themutation {
//   addPost(input: { title: ... }) { ... some query ...}
// }
//
//
// Either way we build up a Dgraph json mutation to add the object
//
// For now, all mutations are only 1 level deep (cause of how we build the
// input objects) and only create a single node (again cause of inputs)

// A MutationResolver can resolve a single mutation.
type MutationResolver interface {
	Resolve(ctx context.Context, mutation *schema.Field) (*Resolved, bool)
}

// A MutationRewriter can transform a GraphQL mutation into a Dgraph mutation and
// can build a Dgraph gql.GraphQuery to follow a GraphQL mutation.
//
// Mutations come in like:
//
// mutation addAuthor($auth: AuthorInput!) {
//   addAuthor(input: $auth) {
// 	   author {
// 	     id
// 	     name
// 	   }
//   }
// }
//
// Where `addAuthor(input: $auth)` implies a mutation that must get run - written
// to a Dgraph mutation by Rewrite.  The GraphQL following `addAuthor(...)`implies
// a query to run and return the newly created author, so the
// mutation query rewriting is dependent on the context set up by the result of
// the mutation.
type MutationRewriter interface {
	// RewriteQueries generates and rewrites GraphQL mutation m into DQL queries which
	// check if any referenced node by XID or ID exist or not.
	// Instead of filtering on dgraph.type like @filter(type(Parrot)), we query `dgraph.type` and
	// filter it on GraphQL side. @filter(type(Parrot)) is costly in terms of memory and cpu.
	// Example existence queries:
	// 1. Parrot1(func: uid(0x127)) {
	//      uid
	//      dgraph.type
	//    }
	// 2.  Computer2(func: eq(Computer.name, "computer1")) {
	//       uid
	//       dgraph.type
	//     }
	// These query will be created in case of Add or Update Mutation which references node
	// 0x127 or Computer of name "computer1"
	RewriteQueries(ctx context.Context, m *schema.Field) ([]*gql.GraphQuery, []string, error)
	// Rewrite rewrites GraphQL mutation m into a Dgraph mutation - that could
	// be as simple as a single DelNquads, or could be a Dgraph upsert mutation
	// with a query and multiple mutations guarded by conditions.
	Rewrite(ctx context.Context, m *schema.Field, idExistence map[string]string) ([]*UpsertMutation, error)
	// FromMutationResult takes a GraphQL mutation and the results of a Dgraph
	// mutation and constructs a Dgraph query.  It's used to find the return
	// value from a GraphQL mutation - i.e. we've run the mutation indicated by m
	// now we need to query Dgraph to satisfy all the result fields in m.
	FromMutationResult(
		ctx context.Context,
		m *schema.Field,
		assigned map[string]string,
		result map[string]interface{}) ([]*gql.GraphQuery, error)
	// MutatedRootUIDs returns a list of Root UIDs that were mutated as part of the mutation.
	MutatedRootUIDs(
		mutation *schema.Field,
		assigned map[string]string,
		result map[string]interface{}) []string
}

// A DgraphExecutor can execute a query/mutation and returns the request response and any errors.
type DgraphExecutor interface {
	// Execute performs the actual query/mutation and returns a Dgraph response. If an error
	// occurs, that indicates that the execution failed in some way significant enough
	// way as to not continue processing this query/mutation or others in the same request.
	Execute(ctx context.Context, req *pb.Request, field *schema.Field) (*pb.Response, error)
}

// An UpsertMutation is the query and mutations needed for a Dgraph upsert.
// The node types is a blank node name -> Type mapping of nodes that could
// be created by the upsert.
type UpsertMutation struct {
	Query     []*gql.GraphQuery
	Mutations []*pb.Mutation
	NewNodes  map[string]*schema.Type
}

// MutationResolverFunc is an adapter that allows to build a MutationResolver from
// a function.  Based on the http.HandlerFunc pattern.
type MutationResolverFunc func(ctx context.Context, m *schema.Field) (*Resolved, bool)

// Resolve calls mr(ctx, mutation)
func (mr MutationResolverFunc) Resolve(ctx context.Context, m *schema.Field) (*Resolved, bool) {
	return mr(ctx, m)
}

// NewDgraphResolver creates a new mutation resolver.  The resolver runs the pipeline:
// 1) rewrite the mutation using mr (return error if failed)
// 2) execute the mutation with me (return error if failed)
// 3) write a query for the mutation with mr (return error if failed)
// 4) execute the query with qe (return error if failed)
func NewDgraphResolver() MutationResolver {
	return &dgraphResolver{}
}

// mutationResolver can resolve a single GraphQL mutation field
type dgraphResolver struct{}
