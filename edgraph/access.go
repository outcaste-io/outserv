/*
 * Copyright 2018 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edgraph

import (
	"context"

	"github.com/golang/glog"
	"github.com/outcaste-io/outserv/gql"
	"github.com/outcaste-io/outserv/protos/pb"
	"github.com/outcaste-io/outserv/query"
	"github.com/outcaste-io/outserv/x"
	"github.com/outcaste-io/ristretto/z"
)

// Login handles login requests from clients. This version rejects all requests
// since ACL is only supported in the enterprise version.
func (s *Server) Login(ctx context.Context,
	request *pb.LoginRequest) (*pb.Response, error) {
	if err := x.HealthCheck(); err != nil {
		return nil, err
	}

	glog.Warningf("Login failed: %s", x.ErrNotSupported)
	return &pb.Response{}, x.ErrNotSupported
}

// ResetAcl is an empty method since ACL is only supported in the enterprise version.
func ResetAcl(closer *z.Closer) {
	// do nothing
}

func upsertGuardianAndGroot(closer *z.Closer, ns uint64) {
	// do nothing
}

// ResetAcls is an empty method since ACL is only supported in the enterprise version.
func RefreshAcls(closer *z.Closer) {
	// do nothing
	<-closer.HasBeenClosed()
	closer.Done()
}

func authorizeAlter(ctx context.Context, op *pb.Operation) error {
	return nil
}

func authorizeMutation(ctx context.Context, gmu *gql.Mutation) error {
	return nil
}

func authorizeQuery(ctx context.Context, parsedReq *gql.Result, graphql bool) error {
	// always allow access
	return nil
}

func authorizeSchemaQuery(ctx context.Context, er *query.ExecutionResult) error {
	// always allow schema access
	return nil
}

func AuthorizeGuardians(ctx context.Context) error {
	// always allow access
	return nil
}

func AuthGuardianOfTheGalaxy(ctx context.Context) error {
	// always allow access
	return nil
}

func validateToken(jwtStr string) ([]string, error) {
	return nil, nil
}

func upsertGuardian(ctx context.Context) error {
	return nil
}

func upsertGroot(ctx context.Context) error {
	return nil
}
