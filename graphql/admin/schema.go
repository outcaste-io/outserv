// Portions Copyright 2019 Dgraph Labs, Inc. are available under the Apache License v2.0.
// Portions Copyright 2022 Outcaste LLC are available under the Sustainable License v1.0.

package admin

import (
	"context"
	"encoding/json"

	"github.com/outcaste-io/outserv/worker"

	"github.com/golang/glog"
	"github.com/outcaste-io/outserv/edgraph"
	"github.com/outcaste-io/outserv/graphql/resolve"
	"github.com/outcaste-io/outserv/graphql/schema"
	"github.com/outcaste-io/outserv/query"
	"github.com/outcaste-io/outserv/x"
)

type getSchemaResolver struct {
	admin *adminServer
}

type updateGQLSchemaInput struct {
	Set worker.GqlSchema `json:"set,omitempty"`
}

type updateSchemaResolver struct {
	admin *adminServer
}

func (usr *updateSchemaResolver) Resolve(ctx context.Context,
	m *schema.Field) (*resolve.Resolved, bool) {

	glog.Info("Got updateGQLSchema request")
	input, err := getSchemaInput(m)
	if err != nil {
		return resolve.EmptyResult(m, err), false
	}

	// We just need to validate the schema. Schema is later set in `resetSchema()` when the schema
	// is returned from badger.
	schHandler, err := schema.NewHandler(input.Set.Schema)
	if err != nil {
		return resolve.EmptyResult(m, err), false
	}

	namespace, err := x.ExtractNamespace(ctx)
	if err != nil {
		return resolve.EmptyResult(m, err), false
	}
	// We don't need the correct namespace for validation, so Galaxy
	// namespace can be passed as well.
	sch, err := schema.FromString(schHandler.GQLSchema(), namespace)
	if err != nil {
		return resolve.EmptyResult(m, err), false
	}

	resp, err := edgraph.UpdateGQLSchema(ctx, input.Set.Schema, schHandler.DGSchema())
	if err != nil {
		return resolve.EmptyResult(m, err), false
	}

	adminServerVar.resetSchema(namespace, sch)
	return resolve.DataResult(
		m,
		map[string]interface{}{
			m.Name(): map[string]interface{}{
				"gqlSchema": map[string]interface{}{
					"id":              query.UidToHex(resp.Uid),
					"schema":          input.Set.Schema,
					"generatedSchema": schHandler.GQLSchema(),
				}}},
		nil), true
}

func (gsr *getSchemaResolver) Resolve(ctx context.Context, q *schema.Field) *resolve.Resolved {
	var data map[string]interface{}

	gsr.admin.mux.RLock()
	defer gsr.admin.mux.RUnlock()

	ns, err := x.ExtractNamespace(ctx)
	if err != nil {
		return resolve.EmptyResult(q, err)
	}

	cs, _ := gsr.admin.gqlSchemas.GetCurrent(ns)
	if cs == nil || cs.ID == "" {
		data = map[string]interface{}{q.Name(): nil}
	} else {
		data = map[string]interface{}{
			q.Name(): map[string]interface{}{
				"id":              cs.ID,
				"schema":          cs.Schema,
				"generatedSchema": cs.GeneratedSchema,
			}}
	}

	return resolve.DataResult(q, data, nil)
}

func getSchemaInput(m *schema.Field) (*updateGQLSchemaInput, error) {
	inputArg := m.ArgValue(schema.InputArgName)
	inputByts, err := json.Marshal(inputArg)
	if err != nil {
		return nil, schema.GQLWrapf(err, "couldn't get input argument")
	}

	var input updateGQLSchemaInput
	err = json.Unmarshal(inputByts, &input)
	return &input, schema.GQLWrapf(err, "couldn't get input argument")
}
