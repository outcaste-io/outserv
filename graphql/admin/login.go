// Portions Copyright 2020 Dgraph Labs, Inc. are available under the Apache License v2.0.
// Portions Copyright 2022 Outcaste LLC are available under the Smart License v1.0.

package admin

import (
	"context"
	"encoding/json"

	"github.com/golang/glog"
	"github.com/outcaste-io/outserv/edgraph"
	"github.com/outcaste-io/outserv/graphql/resolve"
	"github.com/outcaste-io/outserv/graphql/schema"
	"github.com/outcaste-io/outserv/protos/pb"
)

type loginInput struct {
	UserId       string
	Password     string
	Namespace    uint64
	RefreshToken string
}

func resolveLogin(ctx context.Context, m schema.Mutation) (*resolve.Resolved, bool) {
	glog.Info("Got login request")

	input := getLoginInput(m)
	resp, err := (&edgraph.Server{}).Login(ctx, &pb.LoginRequest{
		Userid:       input.UserId,
		Password:     input.Password,
		Namespace:    input.Namespace,
		RefreshToken: input.RefreshToken,
	})
	if err != nil {
		return resolve.EmptyResult(m, err), false
	}

	jwt := &pb.Jwt{}
	if err := jwt.Unmarshal(resp.GetJson()); err != nil {
		return resolve.EmptyResult(m, err), false
	}

	return resolve.DataResult(
		m,
		map[string]interface{}{
			m.Name(): map[string]interface{}{
				"response": map[string]interface{}{
					"accessJWT":  jwt.AccessJwt,
					"refreshJWT": jwt.RefreshJwt}}},
		nil,
	), true

}

func getLoginInput(m schema.Mutation) *loginInput {
	// We should be able to convert these to string as GraphQL schema validation should ensure this.
	// If the input wasn't specified, then the arg value would be nil and the string value empty.

	var input loginInput

	input.UserId, _ = m.ArgValue("userId").(string)
	input.Password, _ = m.ArgValue("password").(string)
	input.RefreshToken, _ = m.ArgValue("refreshToken").(string)

	b, err := json.Marshal(m.ArgValue("namespace"))
	if err != nil {
		return nil
	}

	err = json.Unmarshal(b, &input.Namespace)
	if err != nil {
		return nil
	}
	return &input
}
