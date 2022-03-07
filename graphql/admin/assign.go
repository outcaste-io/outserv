// Portions Copyright 2020 Dgraph Labs, Inc. are available under the Apache 2.0 license.
// Portions Copyright 2022 Outcaste, Inc. are available under the Smart License.

package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/pkg/errors"

	"github.com/outcaste-io/outserv/graphql/resolve"
	"github.com/outcaste-io/outserv/graphql/schema"
	"github.com/outcaste-io/outserv/protos/pb"
	"github.com/outcaste-io/outserv/zero"
)

const (
	uid         = "UID"
	namespaceId = "NAMESPACE_ID"
)

type assignInput struct {
	What string
	Num  uint32
}

func resolveAssign(ctx context.Context, m schema.Mutation) (*resolve.Resolved, bool) {
	input, err := getAssignInput(m)
	if err != nil {
		return resolve.EmptyResult(m, err), false
	}

	var resp *pb.AssignedIds
	switch input.What {
	case uid:
		resp, err = zero.AssignUids(ctx, input.Num)
	case namespaceId:
		resp, err = zero.AssignNsids(ctx, input.Num)
	default:
		err = fmt.Errorf("Invalid request for resolveAssign")
	}
	if err != nil {
		return resolve.EmptyResult(m, err), false
	}

	var startId, endId, readOnly interface{}
	// if it was readonly TIMESTAMP request, then let other output fields be `null`,
	// otherwise, let readOnly field remain `null`.
	startId = json.Number(strconv.FormatUint(resp.GetStartId(), 10))
	endId = json.Number(strconv.FormatUint(resp.GetEndId(), 10))

	return resolve.DataResult(m,
		map[string]interface{}{m.Name(): map[string]interface{}{
			"response": map[string]interface{}{
				"startId":  startId,
				"endId":    endId,
				"readOnly": readOnly,
			},
		}},
		nil,
	), true
}

func getAssignInput(m schema.Mutation) (*assignInput, error) {
	inputArg, ok := m.ArgValue(schema.InputArgName).(map[string]interface{})
	if !ok {
		return nil, inputArgError(errors.Errorf("can't convert input to map"))
	}

	inputRef := &assignInput{}
	inputRef.What, ok = inputArg["what"].(string)
	if !ok {
		return nil, inputArgError(errors.Errorf("can't convert input.what to string"))
	}

	num, err := parseAsUint32(inputArg["num"])
	if err != nil {
		return nil, inputArgError(schema.GQLWrapf(err, "can't convert input.num to uint64"))
	}
	inputRef.Num = num

	return inputRef, nil
}
