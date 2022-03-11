/*
 * Copyright 2020 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
