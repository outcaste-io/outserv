// Portions Copyright 2021 Dgraph Labs, Inc. are available under the Apache License v2.0.
// Portions Copyright 2022 Outcaste LLC are available under the Smart License v1.0.

package admin

import (
	"context"

	"github.com/golang/glog"
	"github.com/outcaste-io/outserv/edgraph"
	"github.com/outcaste-io/outserv/graphql/resolve"
	"github.com/outcaste-io/outserv/graphql/schema"
	"github.com/outcaste-io/outserv/lambda"
	"github.com/outcaste-io/outserv/query"
	"github.com/outcaste-io/outserv/worker"
	"github.com/outcaste-io/outserv/x"
	"github.com/pkg/errors"
)

func resolveUpdateLambda(ctx context.Context, m *schema.Field) (*resolve.Resolved, bool) {
	glog.Info("Got updateLambdaScript request")

	input, err := getLambdaInput(m)
	if err != nil {
		return resolve.EmptyResult(m, err), false
	}

	resp, err := edgraph.UpdateLambdaScript(ctx, input.Script)
	if err != nil {
		return resolve.EmptyResult(m, err), false
	}

	return resolve.DataResult(
		m,
		map[string]interface{}{
			m.Name(): map[string]interface{}{
				"lambdaScript": map[string]interface{}{
					"id":     query.UidToHex(resp.Uid),
					"script": input.Script,
					"hash":   resp.Hash,
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

func getLambdaInput(m *schema.Field) (*lambda.LambdaScript, error) {
	inputArg := m.ArgValue(schema.InputArgName)

	// Not using json, because we need the raw binaries, not base64 encoded
	i, ok := inputArg.(map[string]interface{})
	if !ok {
		return nil, errors.New("invalid input")
	}
	v, ok := i["set"].(map[string]interface{})
	if !ok {
		return nil, errors.New("invalid input for set")
	}
	b, ok := v["script"].([][]byte)
	if !ok {
		return nil, errors.New("invalid input for script")
	}
	if len(b) == 0 {
		return nil, errors.New("invalid input for script data")
	}

	lambdaScript := &lambda.LambdaScript{
		Script: b[0],
	}
	return lambdaScript, nil
}
