// Portions Copyright 2021 Dgraph Labs, Inc. are available under the Apache License v2.0.
// Portions Copyright 2022 Outcaste LLC are available under the Smart License v1.0.

package admin

import (
	"context"
	"encoding/json"

	"github.com/golang/glog"
	"github.com/outcaste-io/outserv/edgraph"
	"github.com/outcaste-io/outserv/graphql/resolve"
	"github.com/outcaste-io/outserv/graphql/schema"
	"github.com/outcaste-io/outserv/query"
	"github.com/outcaste-io/outserv/worker"
	"github.com/outcaste-io/outserv/x"
)

type updateLambdaInput struct {
	Set worker.LambdaScript `json:"set,omitempty"`
}

func resolveUpdateLambda(ctx context.Context, m *schema.Field) (*resolve.Resolved, bool) {
	glog.Info("Got updateLambdaScript request")

	input, err := getLambdaInput(m)
	if err != nil {
		return resolve.EmptyResult(m, err), false
	}

	resp, err := edgraph.UpdateLambdaScript(ctx, input.Set.Script)
	if err != nil {
		return resolve.EmptyResult(m, err), false
	}

	return resolve.DataResult(
		m,
		map[string]interface{}{
			m.Name(): map[string]interface{}{
				"lambdaScript": map[string]interface{}{
					"id":     query.UidToHex(resp.Uid),
					"script": input.Set.Script,
				}}},
		nil), true
}

func resolveGetLambda(ctx context.Context, q *schema.Field) *resolve.Resolved {
	var data map[string]interface{}

	ns, err := x.ExtractNamespace(ctx)
	if err != nil {
		return resolve.EmptyResult(q, err)
	}

	cs, _ := worker.Lambda().GetCurrent(ns)
	if cs == nil || cs.ID == "" {
		data = map[string]interface{}{q.Name(): nil}
	} else {
		data = map[string]interface{}{
			q.Name(): map[string]interface{}{
				"id":     cs.ID,
				"script": cs.Script,
			}}
	}

	return resolve.DataResult(q, data, nil)
}

func getLambdaInput(m *schema.Field) (*updateLambdaInput, error) {
	inputArg := m.ArgValue(schema.InputArgName)
	inputByts, err := json.Marshal(inputArg)
	if err != nil {
		return nil, schema.GQLWrapf(err, "couldn't get input argument")
	}

	var input updateLambdaInput
	err = json.Unmarshal(inputByts, &input)
	return &input, schema.GQLWrapf(err, "couldn't get input argument")
}
