// Portions Copyright 2020 Dgraph Labs, Inc. are available under the Apache License v2.0.
// Portions Copyright 2022 Outcaste LLC are available under the Smart License v1.0.

package admin

import (
	"context"

	"github.com/outcaste-io/outserv/gql"
	"github.com/outcaste-io/outserv/graphql/resolve"
	"github.com/outcaste-io/outserv/graphql/schema"
	"github.com/outcaste-io/outserv/x"
)

type currentUserResolver struct {
	baseRewriter resolve.QueryRewriter
}

func extractName(ctx context.Context) (string, error) {
	accessJwt, err := x.ExtractJwt(ctx)
	if err != nil {
		return "", err
	}

	return x.ExtractUserName(accessJwt)
}

func (gsr *currentUserResolver) Rewrite(ctx context.Context,
	gqlQuery *schema.Field) ([]*gql.GraphQuery, error) {

	name, err := extractName(ctx)
	if err != nil {
		return nil, err
	}

	gqlQuery.Rename("getUser")
	gqlQuery.SetArgTo("name", name)

	return gsr.baseRewriter.Rewrite(ctx, gqlQuery)
}
