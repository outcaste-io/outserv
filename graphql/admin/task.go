// Portions Copyright 2021 Dgraph Labs, Inc. are available under the Apache License v2.0.
// Portions Copyright 2022 Outcaste LLC are available under the Sustainable License v1.0.

package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/outcaste-io/outserv/graphql/resolve"
	"github.com/outcaste-io/outserv/graphql/schema"
	"github.com/outcaste-io/outserv/protos/pb"
	"github.com/outcaste-io/outserv/worker"
	"github.com/pkg/errors"
)

type taskInput struct {
	Id string
}

func resolveTask(ctx context.Context, q *schema.Field) *resolve.Resolved {
	// Get Task ID.
	input, err := getTaskInput(q)
	if err != nil {
		return resolve.EmptyResult(q, err)
	}
	if input.Id == "" {
		return resolve.EmptyResult(q, fmt.Errorf("task ID is missing"))
	}
	taskId, err := strconv.ParseUint(input.Id, 0, 64)
	if err != nil {
		err = errors.Wrapf(err, "invalid task ID: %s", input.Id)
		return resolve.EmptyResult(q, err)
	}

	// Get TaskMeta from network.
	req := &pb.TaskStatusRequest{TaskId: taskId}
	resp, err := worker.TaskStatusOverNetwork(context.Background(), req)
	if err != nil {
		return resolve.EmptyResult(q, err)
	}
	meta := worker.TaskMeta(resp.GetTaskMeta())
	return resolve.DataResult(
		q,
		map[string]interface{}{q.Name(): map[string]interface{}{
			"kind":        meta.Kind().String(),
			"status":      meta.Status().String(),
			"lastUpdated": meta.Timestamp().Format(time.RFC3339),
		}},
		nil,
	)
}

func getTaskInput(q *schema.Field) (*taskInput, error) {
	inputArg := q.ArgValue(schema.InputArgName)
	inputBytes, err := json.Marshal(inputArg)
	if err != nil {
		return nil, schema.GQLWrapf(err, "couldn't get input argument")
	}

	var input taskInput
	if err := json.Unmarshal(inputBytes, &input); err != nil {
		return nil, schema.GQLWrapf(err, "couldn't get input argument")
	}
	return &input, nil
}
