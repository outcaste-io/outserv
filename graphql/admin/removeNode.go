// Portions Copyright 2020 Dgraph Labs, Inc. are available under the Apache License v2.0.
// Portions Copyright 2022 Outcaste LLC are available under the Smart License v1.0.

package admin

import (
	"context"
	"encoding/json"
	"strconv"

	"github.com/pkg/errors"

	"github.com/outcaste-io/outserv/graphql/resolve"
	"github.com/outcaste-io/outserv/graphql/schema"
)

type removeNodeInput struct {
	NodeId  uint64
	GroupId uint32
}

func resolveRemoveNode(ctx context.Context, m *schema.Field) (*resolve.Resolved, bool) {
	panic("TODO: Implement resolveRemoveNode")
	// input, err := getRemoveNodeInput(m)
	// if err != nil {
	// 	return resolve.EmptyResult(m, err), false
	// }

	// if _, err = worker.RemoveNodeOverNetwork(ctx, &pb.RemoveNodeRequest{NodeId: input.NodeId,
	// 	GroupId: input.GroupId}); err != nil {
	// 	return resolve.EmptyResult(m, err), false
	// }

	// return resolve.DataResult(m,
	// 	map[string]interface{}{m.Name(): response("Success",
	// 		fmt.Sprintf("Removed node with group: %v, idx: %v", input.GroupId, input.NodeId))},
	// 	nil,
	// ), true
}

func getRemoveNodeInput(m *schema.Field) (*removeNodeInput, error) {
	inputArg, ok := m.ArgValue(schema.InputArgName).(map[string]interface{})
	if !ok {
		return nil, inputArgError(errors.Errorf("can't convert input to map"))
	}

	inputRef := &removeNodeInput{}
	nodeId, err := parseAsUint64(inputArg["nodeId"])
	if err != nil {
		return nil, inputArgError(schema.GQLWrapf(err, "can't convert input.nodeId to uint64"))
	}
	inputRef.NodeId = nodeId

	gId, err := parseAsUint32(inputArg["groupId"])
	if err != nil {
		return nil, inputArgError(schema.GQLWrapf(err, "can't convert input.groupId to uint32"))
	}
	inputRef.GroupId = gId

	return inputRef, nil
}

func parseAsUint64(val interface{}) (uint64, error) {
	return parseAsUint(val, 64)
}

func parseAsUint32(val interface{}) (uint32, error) {
	ret, err := parseAsUint(val, 32)
	return uint32(ret), err
}

func parseAsUint(val interface{}, bitSize int) (uint64, error) {
	ret := uint64(0)
	var err error

	switch v := val.(type) {
	case string:
		ret, err = strconv.ParseUint(v, 10, bitSize)
	case json.Number:
		ret, err = strconv.ParseUint(v.String(), 10, bitSize)
	default:
		err = errors.Errorf("got unexpected value type")
	}

	return ret, err
}
