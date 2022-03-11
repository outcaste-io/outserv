// Portions Copyright 2019 Dgraph Labs, Inc. are available under the Apache License v2.0.
// Portions Copyright 2022 Outcaste LLC are available under the Smart License v1.0.

package dgraph

import (
	"context"
	"strings"

	"github.com/golang/glog"
	"go.opencensus.io/trace"

	"github.com/outcaste-io/outserv/edgraph"
	"github.com/outcaste-io/outserv/graphql/schema"
	"github.com/outcaste-io/outserv/protos/pb"
	"github.com/outcaste-io/outserv/x"
)

type DgraphEx struct{}

// Execute is the underlying dgraph implementation of Dgraph execution.
// If field is nil, returned response has JSON in DQL form, otherwise it will be in GraphQL form.
func (dg *DgraphEx) Execute(ctx context.Context, req *pb.Request,
	field schema.Field) (*pb.Response, error) {

	span := trace.FromContext(ctx)
	stop := x.SpanTimer(span, "dgraph.Execute")
	defer stop()

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
	resp, err := (&edgraph.Server{}).QueryGraphQL(ctx, req, field)
	if !x.IsGqlErrorList(err) {
		err = schema.GQLWrapf(err, "Dgraph execution failed")
	}

	return resp, err
}
