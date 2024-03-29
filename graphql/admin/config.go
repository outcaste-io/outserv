// Portions Copyright 2020 Dgraph Labs, Inc. are available under the Apache License v2.0.
// Portions Copyright 2022 Outcaste LLC are available under the Sustainable License v1.0.

package admin

import (
	"context"
	"encoding/json"
	"strconv"

	"github.com/golang/glog"
	"github.com/outcaste-io/outserv/graphql/resolve"
	"github.com/outcaste-io/outserv/graphql/schema"
	"github.com/outcaste-io/outserv/worker"
)

type configInput struct {
	CacheMb *float64
	// LogRequest is used to update WorkerOptions.LogRequest. true value of LogRequest enables
	// logging of all requests coming to alphas. LogRequest type has been kept as *bool instead of
	// bool to avoid updating WorkerOptions.LogRequest when it has default value of false.
	LogRequest *bool
}

func resolveUpdateConfig(ctx context.Context, m *schema.Field) (*resolve.Resolved, bool) {
	glog.Info("Got config update through GraphQL admin API")

	input, err := getConfigInput(m)
	if err != nil {
		return resolve.EmptyResult(m, err), false
	}

	// update cacheMB only when it is specified by user
	if input.CacheMb != nil {
		if err = worker.UpdateCacheMb(int64(*input.CacheMb)); err != nil {
			return resolve.EmptyResult(m, err), false
		}
	}

	// input.LogRequest will be nil, when it is not specified explicitly in config request.
	if input.LogRequest != nil {
		worker.UpdateLogRequest(*input.LogRequest)
	}

	return resolve.DataResult(
		m,
		map[string]interface{}{m.Name(): response("Success", "Config updated successfully")},
		nil,
	), true
}

func resolveGetConfig(ctx context.Context, q *schema.Field) *resolve.Resolved {
	glog.Info("Got config query through GraphQL admin API")

	return resolve.DataResult(
		q,
		map[string]interface{}{q.Name(): map[string]interface{}{
			"cacheMb": json.Number(strconv.FormatInt(worker.Config.CacheMb, 10)),
		}},
		nil,
	)

}

func getConfigInput(m *schema.Field) (*configInput, error) {
	inputArg := m.ArgValue(schema.InputArgName)
	inputByts, err := json.Marshal(inputArg)
	if err != nil {
		return nil, schema.GQLWrapf(err, "couldn't get input argument")
	}

	var input configInput
	err = json.Unmarshal(inputByts, &input)
	return &input, schema.GQLWrapf(err, "couldn't get input argument")
}
