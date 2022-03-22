// Portions Copyright 2020 Dgraph Labs, Inc. are available under the Apache License v2.0.
// Portions Copyright 2022 Outcaste LLC are available under the Smart License v1.0.

package admin

import (
	"context"
	"encoding/json"
	"strconv"

	"github.com/golang/glog"
	"github.com/outcaste-io/outserv/edgraph"
	"github.com/outcaste-io/outserv/graphql/resolve"
	"github.com/outcaste-io/outserv/graphql/schema"
)

func resolveResetPassword(ctx context.Context, m *schema.Field) (*resolve.Resolved, bool) {
	inp, err := getPasswordInput(m)
	if err != nil {
		glog.Error("Failed to parse the reset password input")
	}
	if err = edgraph.ResetPassword(ctx, inp); err != nil {
		return resolve.EmptyResult(m, err), false
	}

	return resolve.DataResult(
		m,
		map[string]interface{}{
			m.Name(): map[string]interface{}{
				"userId":    inp.UserID,
				"message":   "Reset password is successful",
				"namespace": json.Number(strconv.Itoa(int(inp.Namespace))),
			},
		},
		nil,
	), true

}

func getPasswordInput(m *schema.Field) (*edgraph.ResetPasswordInput, error) {
	var input edgraph.ResetPasswordInput

	inputArg := m.ArgValue(schema.InputArgName)
	inputByts, err := json.Marshal(inputArg)

	if err != nil {
		return nil, schema.GQLWrapf(err, "couldn't get input argument")
	}

	if err := json.Unmarshal(inputByts, &input); err != nil {
		return nil, schema.GQLWrapf(err, "couldn't get input argument")
	}

	return &input, nil
}
