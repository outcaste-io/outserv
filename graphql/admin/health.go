// Portions Copyright 2019 Dgraph Labs, Inc. are available under the Apache License v2.0.
// Portions Copyright 2022 Outcaste LLC are available under the Smart License v1.0.

package admin

import (
	"context"

	"github.com/golang/glog"
	"github.com/outcaste-io/outserv/edgraph"
	"github.com/outcaste-io/outserv/graphql/resolve"
	"github.com/outcaste-io/outserv/graphql/schema"
	"github.com/outcaste-io/outserv/x"
	"github.com/pkg/errors"
)

func resolveHealth(ctx context.Context, q *schema.Field) *resolve.Resolved {
	glog.Info("Got health request")

	resp, err := edgraph.Health(ctx, true)
	if err != nil {
		return resolve.EmptyResult(q, errors.Errorf("%s: %s", x.Error, err.Error()))
	}

	var health []map[string]interface{}
	err = schema.Unmarshal(resp.GetJson(), &health)

	return resolve.DataResult(
		q,
		map[string]interface{}{q.Name(): health},
		err,
	)
}
