package admin

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/outcaste-io/outserv/edgraph"
	"github.com/outcaste-io/outserv/graphql/resolve"
	"github.com/outcaste-io/outserv/graphql/schema"
	"github.com/outcaste-io/outserv/protos/pb"
	"github.com/outcaste-io/outserv/x"
	"github.com/pkg/errors"
)

type membershipState struct {
	Tablets    []*pb.Tablet `json:"groups,omitempty"`
	Members    []*pb.Member `json:"zeros,omitempty"`
	MaxUID     uint64       `json:"maxUID,omitempty"`
	MaxNsID    uint64       `json:"maxNsID,omitempty"`
	Removed    []*pb.Member `json:"removed,omitempty"`
	Cid        string       `json:"cid,omitempty"`
	Namespaces []uint64     `json:"namespaces,omitempty"`
}

type clusterGroup struct {
	Id         uint32       `json:"id,omitempty"`
	Tablets    []*pb.Tablet `json:"tablets,omitempty"`
	SnapshotTs uint64       `json:"snapshotTs,omitempty"`
	Checksum   uint64       `json:"checksum,omitempty"`
}

func resolveState(ctx context.Context, q schema.Query) *resolve.Resolved {
	resp, err := (&edgraph.Server{}).State(ctx)
	if err != nil {
		return resolve.EmptyResult(q, errors.Errorf("%s: %s", x.Error, err.Error()))
	}

	// unmarshal it back to MembershipState proto in order to map to graphql response
	u := jsonpb.Unmarshaler{}
	var ms pb.MembershipState
	err = u.Unmarshal(bytes.NewReader(resp.GetJson()), &ms)
	if err != nil {
		return resolve.EmptyResult(q, err)
	}

	ns, _ := x.ExtractNamespace(ctx)
	// map to graphql response structure. Only guardian of galaxy can list the namespaces.
	state := convertToGraphQLResp(ms, ns == x.GalaxyNamespace)
	b, err := json.Marshal(state)
	if err != nil {
		return resolve.EmptyResult(q, err)
	}
	var resultState map[string]interface{}
	err = schema.Unmarshal(b, &resultState)
	if err != nil {
		return resolve.EmptyResult(q, err)
	}

	return resolve.DataResult(
		q,
		map[string]interface{}{q.Name(): resultState},
		nil,
	)
}

// convertToGraphQLResp converts MembershipState proto to GraphQL layer response
// MembershipState proto contains some fields which are of type map, and as GraphQL
// does not have a map type, we convert those maps to lists by using just the map
// values and not the keys. For pb.MembershipState.Group, the keys are the group IDs
// and pb.Group didn't contain this ID, so we are creating a custom clusterGroup type,
// which is same as pb.Group and also contains the ID for the group.
func convertToGraphQLResp(ms pb.MembershipState, listNs bool) membershipState {
	var state membershipState

	// namespaces stores set of namespaces
	namespaces := make(map[uint64]struct{})

	for name, v1 := range ms.Tablets {
		state.Tablets = append(state.Tablets, v1)
		if listNs {
			namespaces[x.ParseNamespace(name)] = struct{}{}
		}
	}
	for _, v := range ms.Members {
		state.Members = append(state.Members, v)
	}
	state.MaxUID = ms.MaxUID
	state.MaxNsID = ms.MaxNsID
	state.Removed = ms.Removed
	state.Cid = ms.Cid

	state.Namespaces = []uint64{}
	for ns := range namespaces {
		state.Namespaces = append(state.Namespaces, ns)
	}

	return state
}
