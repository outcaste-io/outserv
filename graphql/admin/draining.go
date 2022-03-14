// Portions Copyright 2020 Dgraph Labs, Inc. are available under the Apache License v2.0.
// Portions Copyright 2022 Outcaste LLC are available under the Smart License v1.0.

package admin

import (
	"context"
	"fmt"

	"github.com/golang/glog"
	"github.com/outcaste-io/outserv/graphql/resolve"
	"github.com/outcaste-io/outserv/graphql/schema"
	"github.com/outcaste-io/outserv/x"
)

func resolveDraining(ctx context.Context, m *schema.Field) (*resolve.Resolved, bool) {
	glog.Info("Got draining request through GraphQL admin API")

	enable := getDrainingInput(m)
	x.UpdateDrainingMode(enable)

	return resolve.DataResult(
		m,
		map[string]interface{}{
			m.Name(): response("Success", fmt.Sprintf("draining mode has been set to %v", enable)),
		},
		nil,
	), true
}

func getDrainingInput(m *schema.Field) bool {
	enable, _ := m.ArgValue("enable").(bool)
	return enable
}
