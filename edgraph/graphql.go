// Portions Copyright 2017-2020 Dgraph Labs, Inc. are available under the Apache License v2.0.
// Portions Copyright 2022 Outcaste LLC are available under the Sustainable License v1.0.

package edgraph

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/golang/glog"
	"github.com/outcaste-io/outserv/graphql/schema"
	"github.com/outcaste-io/outserv/protos/pb"
	"github.com/outcaste-io/outserv/types"
	"github.com/outcaste-io/outserv/x"
	"github.com/pkg/errors"
)

// ProcessPersistedQuery stores and retrieves persisted queries by following waterfall logic:
// 1. If sha256Hash is not provided process queries without persisting
// 2. If sha256Hash is provided try retrieving persisted queries
//		2a. Persisted Query not found
//		    i) If query is not provided then throw "PersistedQueryNotFound"
//			ii) If query is provided then store query in dgraph only if sha256 of the query is correct
//				otherwise throw "provided sha does not match query"
//      2b. Persisted Query found
//		    i)  If query is not provided then update gqlRes with the found query and proceed
//			ii) If query is provided then match query retrieved, if identical do nothing else
//				throw "query does not match persisted query"
func ProcessPersistedQuery(ctx context.Context, gqlReq *schema.Request) error {
	query := gqlReq.Query
	sha256Hash := gqlReq.Extensions.PersistedQuery.Sha256Hash

	if sha256Hash == "" {
		return nil
	}

	if x.WorkerConfig.AclEnabled {
		accessJwt, err := x.ExtractJwt(ctx)
		if err != nil {
			return err
		}
		if _, err := validateToken(accessJwt); err != nil {
			return err
		}
	}

	join := sha256Hash + query

	queryForSHA := `query Me($join: string){
						me(func: eq(dgraph.graphql.p_query, $join)){
							dgraph.graphql.p_query
						}
					}`
	variables := map[string]string{
		"$join": join,
	}
	req := &Request{
		Req: &pb.Request{
			Query:    queryForSHA,
			Vars:     variables,
			ReadOnly: true,
		},
		doAuth: NoAuthorize,
	}
	storedQuery, err := doQuery(ctx, req)

	if err != nil {
		glog.Errorf("Error while querying sha %s", sha256Hash)
		return err
	}

	type shaQueryResponse struct {
		Me []struct {
			PersistedQuery string `json:"dgraph.graphql.p_query"`
		} `json:"me"`
	}

	shaQueryRes := &shaQueryResponse{}
	if len(storedQuery.Json) > 0 {
		if err := json.Unmarshal(storedQuery.Json, shaQueryRes); err != nil {
			return err
		}
	}

	if len(shaQueryRes.Me) == 0 {
		if query == "" {
			return errors.New("PersistedQueryNotFound")
		}
		if match, err := hashMatches(query, sha256Hash); err != nil {
			return err
		} else if !match {
			return errors.New("provided sha does not match query")
		}

		req = &Request{
			Req: &pb.Request{
				Mutations: []*pb.Mutation{
					{
						Edges: []*pb.Edge{
							{
								Subject:     "_:a",
								Predicate:   "dgraph.graphql.p_query",
								ObjectValue: types.StringToBinary(join),
							},
							{
								Subject:   "_:a",
								Predicate: "dgraph.type",
								ObjectValue: types.StringToBinary(
									"dgraph.graphql.persisted_query"),
							},
						},
					},
				},
				CommitNow: true,
			},
			doAuth: NoAuthorize,
		}

		ctx := context.WithValue(ctx, IsGraphql, true)
		_, err := doQuery(ctx, req)
		return err
	}

	if len(shaQueryRes.Me) != 1 {
		return fmt.Errorf("same sha returned %d queries", len(shaQueryRes.Me))
	}

	gotQuery := ""
	if len(shaQueryRes.Me[0].PersistedQuery) >= 64 {
		gotQuery = shaQueryRes.Me[0].PersistedQuery[64:]
	}

	if len(query) > 0 && gotQuery != query {
		return errors.New("query does not match persisted query")
	}

	gqlReq.Query = gotQuery
	return nil

}

func hashMatches(query, sha256Hash string) (bool, error) {
	hasher := sha256.New()
	_, err := hasher.Write([]byte(query))
	if err != nil {
		return false, err
	}
	hashGenerated := hex.EncodeToString(hasher.Sum(nil))
	return hashGenerated == sha256Hash, nil
}
