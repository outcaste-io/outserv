// Portions Copyright 2020 Dgraph Labs, Inc. are available under the Apache License v2.0.
// Portions Copyright 2022 Outcaste LLC are available under the Smart License v1.0.

package admin

import (
	"context"

	"github.com/outcaste-io/outserv/x"

	"github.com/pkg/errors"

	"github.com/outcaste-io/outserv/graphql/resolve"
	"github.com/outcaste-io/outserv/graphql/schema"
)

type moveTabletInput struct {
	Namespace uint64
	Tablet    string
	GroupId   uint32
}

func resolveMoveTablet(ctx context.Context, m *schema.Field) (*resolve.Resolved, bool) {
	panic("TODO: Implement resolveMoveTablet")
	// input, err := getMoveTabletInput(m)
	// if err != nil {
	// 	return resolve.EmptyResult(m, err), false
	// }

	// gRPC call returns a nil status if the error is non-nil
	// status, err := worker.MoveTabletOverNetwork(ctx, &pb.MoveTabletRequest{
	// 	Namespace: input.Namespace,
	// 	Tablet:    input.Tablet,
	// 	DstGroup:  input.GroupId,
	// })
	// if err != nil {
	// 	return resolve.EmptyResult(m, err), false
	// }

	// return resolve.DataResult(m,
	// 	map[string]interface{}{m.Name(): response("Success", status.GetMsg())},
	// 	nil,
	// ), true
}

func getMoveTabletInput(m *schema.Field) (*moveTabletInput, error) {
	inputArg, ok := m.ArgValue(schema.InputArgName).(map[string]interface{})
	if !ok {
		return nil, inputArgError(errors.Errorf("can't convert input to map"))
	}

	inputRef := &moveTabletInput{}
	// namespace is an optional parameter
	if _, ok = inputArg["namespace"]; !ok {
		inputRef.Namespace = x.GalaxyNamespace
	} else {
		ns, err := parseAsUint64(inputArg["namespace"])
		if err != nil {
			return nil, inputArgError(schema.GQLWrapf(err,
				"can't convert input.namespace to uint64"))
		}
		inputRef.Namespace = ns
	}

	inputRef.Tablet, ok = inputArg["tablet"].(string)
	if !ok {
		return nil, inputArgError(errors.Errorf("can't convert input.tablet to string"))
	}

	gId, err := parseAsUint32(inputArg["groupId"])
	if err != nil {
		return nil, inputArgError(schema.GQLWrapf(err, "can't convert input.groupId to uint32"))
	}
	inputRef.GroupId = gId

	return inputRef, nil
}
