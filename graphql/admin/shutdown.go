// Portions Copyright 2020 Dgraph Labs, Inc. are available under the Apache License v2.0.
// Portions Copyright 2022 Outcaste LLC are available under the Sustainable License v1.0.

package admin

import (
	"context"

	"github.com/golang/glog"
	"github.com/outcaste-io/outserv/graphql/resolve"
	"github.com/outcaste-io/outserv/graphql/schema"
	"github.com/outcaste-io/outserv/x"
)

func resolveShutdown(ctx context.Context, m *schema.Field) (*resolve.Resolved, bool) {
	glog.Info("Got shutdown request through GraphQL admin API")

	x.ServerCloser.Signal()

	return resolve.DataResult(
		m,
		map[string]interface{}{m.Name(): response("Success", "Server is shutting down")},
		nil,
	), true
}
