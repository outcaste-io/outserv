// Portions Copyright 2017-2018 Dgraph Labs, Inc. are available under the Apache License v2.0.
// Portions Copyright 2022 Outcaste LLC are available under the Sustainable License v1.0.

package query

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	otrace "go.opencensus.io/trace"

	"github.com/golang/glog"
	"github.com/outcaste-io/outserv/graphql/schema"
	"github.com/outcaste-io/outserv/protos/pb"
	"github.com/outcaste-io/outserv/worker"
	"github.com/outcaste-io/outserv/x"
	"github.com/outcaste-io/outserv/zero"
	"github.com/pkg/errors"
)

// ApplyMutations performs the required edge expansions and forwards the results to the
// worker to perform the mutations.
func ApplyMutations(ctx context.Context, m *pb.Mutations) (*pb.TxnContext, error) {
	ns, err := x.ExtractNamespace(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "while extracting namespace")
	}
	for _, obj := range m.NewObjects {
		for _, edge := range obj.Edges {
			edge.Predicate = x.NamespaceAttr(ns, edge.Predicate)
		}
	}
	for _, edge := range m.Edges {
		edge.Predicate = x.NamespaceAttr(ns, edge.Predicate)
	}

	err = checkIfDeletingAclOperation(ctx, m.Edges)
	if err != nil {
		return nil, err
	}
	tctx, err := worker.MutateOverNetwork(ctx, m)
	if err != nil {
		if span := otrace.FromContext(ctx); span != nil {
			span.Annotatef(nil, "MutateOverNetwork Error: %v. Mutation: %v.", err, m)
		}
	}
	return tctx, err
}

func verifyUid(ctx context.Context, uid uint64) error {
	if uid <= worker.MaxLeaseId() {
		return nil
	}
	deadline := time.Now().Add(3 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			lease := worker.MaxLeaseId()
			if uid <= lease {
				return nil
			}
			if time.Now().After(deadline) {
				err := errors.Errorf("Uid: [%d] cannot be greater than lease: [%d]", uid, lease)
				glog.V(2).Infof("verifyUid returned error: %v", err)
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func ToMutations(ctx context.Context, gmuList []*pb.Mutation,
	schema *schema.Schema) (*pb.Mutations, error) {

	var edges []*pb.Edge
	for _, gmu := range gmuList {
		edges = append(edges, gmu.Edges...)
	}

	sort.Slice(edges, func(i, j int) bool {
		left, right := edges[i], edges[j]
		if left.Op != right.Op {
			return left.Op < right.Op
		}
		// We don't consider Namespace here, because it should be the same for
		// all Edges. We check that later.
		if left.Subject != right.Subject {
			return left.Subject < right.Subject
		}
		if left.Predicate != right.Predicate {
			return left.Predicate < right.Predicate
		}
		return left.ObjectId < right.ObjectId
	})

	mu := &pb.Mutations{}
	var obj *pb.Object

	types := make(map[string]bool)
	xids := make(map[string]bool)
	ns := uint64(math.MaxUint64)
	for _, edge := range edges {
		if ns < math.MaxUint64 {
			if edge.Namespace != ns {
				return nil, fmt.Errorf("Multiple namespaces found in the same mutation")
			}
		} else {
			ns = edge.Namespace
		}

		if edge.Op == pb.Edge_DEL {
			mu.Edges = append(mu.Edges, edge)
			obj = nil
			continue
		}
		attr := edge.Predicate
		if strings.HasPrefix(attr, "dgraph.") {
			mu.Edges = append(mu.Edges, edge)
			continue
		}
		gqlType := strings.Split(attr, ".")[0]
		if _, done := types[gqlType]; !done {
			typ := schema.Type(gqlType)
			for _, xid := range typ.XIDFields() {
				xids[xid.DgraphAlias()] = true
			}
		}

		// We only consider Objects which have an XID. This would not work if a
		// type doesn't have any XID. If we want to include objects without any
		// XID, this is where a fix needs to be made. Later in the process
		// UidsForObject in worker/draft.go would check all the edges within
		// pb.Object for a matching UID.
		//
		// If we intend to expand this logic to include non-XID objects, then we
		// could just allocate a UID here and be done. Anyways, for now, only
		// considering objects with an XID.

		_, isXid := xids[attr]
		if !isXid {
			mu.Edges = append(mu.Edges, edge)
			continue
		}
		if obj != nil && obj.Var == edge.Subject {
			// Subject matches an existing object, append to it.
			obj.Edges = append(obj.Edges, edge)
			continue
		}
		if strings.HasPrefix(edge.Subject, "_:") {
			// We're creating an object only with the XIDs.
			obj = &pb.Object{Var: edge.Subject}
			mu.NewObjects = append(mu.NewObjects, obj)
			obj.Edges = append(obj.Edges, edge)
			continue
		}
		mu.Edges = append(mu.Edges, edge)
		obj = nil
	}

	objMap := make(map[string]*pb.Object)
	objs := mu.NewObjects
	mu.NewObjects = objs[:0]
	for _, cur := range objs {
		var key string
		for _, edge := range cur.Edges {
			key += fmt.Sprintf("%q|%q|", edge.Predicate, edge.ObjectValue)
		}
		if prev, has := objMap[key]; !has {
			mu.NewObjects = append(mu.NewObjects, cur)
			objMap[key] = cur
		} else {
			// cur == prev. We should replace all the edges to point to the prev
			// object's var and not include cur in the list of new objects.
			x.ReplaceUidsIn(mu.Edges, map[string]string{cur.Var: prev.Var})
		}
	}

	if num := len(mu.NewObjects); num > 0 {
		var res *pb.AssignedIds
		var err error
		// TODO: Optimize later by prefetching. Also consolidate all the UID requests into a single
		// pending request from this server to zero.
		if res, err = zero.AssignUids(ctx, uint32(num)); err != nil {
			return nil, errors.Wrapf(err, "while assigning UIDs")
		}
		curId := res.StartId
		// assign generated ones now
		for _, obj := range mu.NewObjects {
			x.AssertTruef(curId != 0 && curId <= res.EndId,
				"not enough uids generated: res: %+v . curId: %d", res, curId)
			obj.Uid = curId
			curId++
		}
	}
	glog.V(2).Infof("Found %d objects. Mu: %+v\n", len(mu.NewObjects), mu)

	// TODO(mrjn): Now run verifications.
	return mu, nil
}

// AssignUids tries to assign unique ids to each identity in the subjects and objects in the
// format of _:xxx. An identity, e.g. _:a, will only be assigned one uid regardless how many times
// it shows up in the subjects or objects
func AssignUids(ctx context.Context, gmuList []*pb.Mutation) (map[string]uint64, error) {
	newUids := make(map[string]uint64)
	var err error
	for _, gmu := range gmuList {
		for _, nq := range gmu.Edges {
			if nq.Op != pb.Edge_SET {
				continue
			}
			// We dont want to assign uids to these.
			if nq.Subject == x.Star && x.IsStarAll(nq.ObjectValue) {
				return newUids, errors.New("predicate deletion should be called via alter")
			}

			if len(nq.Subject) == 0 {
				return nil, errors.Errorf("subject must not be empty for nquad: %+v", nq)
			}
			var uid uint64
			if strings.HasPrefix(nq.Subject, "_:") {
				newUids[nq.Subject] = 0
			} else if uid, err = x.ParseUid(nq.Subject); err != nil {
				return newUids, err
			}
			if err = verifyUid(ctx, uid); err != nil {
				return newUids, err
			}

			if len(nq.ObjectId) > 0 {
				var uid uint64
				if strings.HasPrefix(nq.ObjectId, "_:") {
					newUids[nq.ObjectId] = 0
				} else if uid, err = x.ParseUid(nq.ObjectId); err != nil {
					return newUids, err
				}
				if err = verifyUid(ctx, uid); err != nil {
					return newUids, err
				}
			}
		}
	}

	if num := len(newUids); num > 0 {
		var res *pb.AssignedIds
		// TODO: Optimize later by prefetching. Also consolidate all the UID requests into a single
		// pending request from this server to zero.
		if res, err = zero.AssignUids(ctx, uint32(num)); err != nil {
			return newUids, err
		}
		curId := res.StartId
		// assign generated ones now
		for k := range newUids {
			x.AssertTruef(curId != 0 && curId <= res.EndId,
				"not enough uids generated: res: %+v . curId: %d", res, curId)
			newUids[k] = curId
			curId++
		}
	}
	return newUids, nil
}

func checkIfDeletingAclOperation(ctx context.Context, edges []*pb.Edge) error {
	// Don't need to make any checks if ACL is not enabled
	if !x.WorkerConfig.AclEnabled {
		return nil
	}
	namespace, err := x.ExtractNamespace(ctx)
	if err != nil {
		return errors.Wrapf(err, "While checking ACL delete operation")
	}

	// If the guardian or groot node is not present, then the request cannot be a delete operation
	// on guardian or groot node.
	guardianUid, ok := x.GuardiansUid.Load(namespace)
	if !ok {
		return nil
	}
	grootsUid, ok := x.GrootUid.Load(namespace)
	if !ok {
		return nil
	}

	isDeleteAclOperation := false
	for _, edge := range edges {
		// Disallow deleting of guardians group
		if edge.Subject == guardianUid && edge.Op == pb.Edge_DEL {
			isDeleteAclOperation = true
			break
		}
		// Disallow deleting of groot user
		if edge.Subject == grootsUid && edge.Op == pb.Edge_DEL {
			isDeleteAclOperation = true
			break
		}
	}
	if isDeleteAclOperation {
		return errors.Errorf("Properties of guardians group and groot user cannot be deleted.")
	}
	return nil
}
