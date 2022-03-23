package resolve

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/golang/glog"
	"github.com/outcaste-io/outserv/codec"
	"github.com/outcaste-io/outserv/edgraph"
	"github.com/outcaste-io/outserv/gql"
	"github.com/outcaste-io/outserv/graphql/dgraph"
	"github.com/outcaste-io/outserv/graphql/schema"
	"github.com/outcaste-io/outserv/posting"
	"github.com/outcaste-io/outserv/protos/pb"
	"github.com/outcaste-io/outserv/worker"
	"github.com/outcaste-io/outserv/x"
	"github.com/outcaste-io/sroar"
	"github.com/pkg/errors"
	otrace "go.opencensus.io/trace"
)

func (mr *dgraphResolver) Resolve(ctx context.Context, m *schema.Field) (*Resolved, bool) {
	span := otrace.FromContext(ctx)
	stop := x.SpanTimer(span, "resolveMutation")
	defer stop()
	if span != nil {
		span.Annotatef(nil, "mutation alias: [%s] type: [%s]", m.Alias(), m.MutationType())
	}

	resolved, success := mr.rewriteAndExecute(ctx, m)
	return resolved, success
}

func extractVal(xidVal interface{}, xid *schema.FieldDefinition) (string, error) {
	typeName := xid.Type().Name()

	switch typeName {
	case "Int", "Int64":
		switch xVal := xidVal.(type) {
		case json.Number:
			val, err := xVal.Int64()
			if err != nil {
				return "", err
			}
			return strconv.FormatInt(val, 10), nil
		case int64:
			return strconv.FormatInt(xVal, 10), nil
		default:
			return "", fmt.Errorf("encountered an XID %s with %s that isn't "+
				"a Int but data type in schema is Int", xid.Name(), typeName)
		}
		// "ID" is given as input for the @extended type mutation.
	case "String", "ID":
		xidString, ok := xidVal.(string)
		if !ok {
			return "", fmt.Errorf("encountered an XID %s with %s that isn't "+
				"a String", xid.Name(), typeName)
		}
		return xidString, nil
	default:
		return "", fmt.Errorf("encountered an XID %s with %s that isn't"+
			"allowed as Xid", xid.Name(), typeName)
	}
}

func runUpsert(ctx context.Context, m *schema.Field) ([]uint64, error) {
	// Parsing input
	val, ok := m.ArgValue(schema.InputArgName).([]interface{})
	x.AssertTrue(ok)

	upsert := false
	upsertVal := m.ArgValue(schema.UpsertArgName)
	if upsertVal != nil {
		upsert = upsertVal.(bool)
	}

	// Just consider the first one for now.
	var resultUids []uint64
	for _, i := range val {
		// TODO: Refactor this so we deal with each val in a nested func.
		obj := i.(map[string]interface{})
		typ := m.MutatedType()
		glog.Infof("mutatedType: %+v obj: %+v\n", typ, obj)
		fields := typ.Fields()

		id := typ.IDField()
		if id != nil {
			glog.Infof("ID Name: %s full: %+v\n", id.Name(), id)
			if idVal := obj[id.Name()]; idVal != nil {
				glog.Infof("Found ID field: %s %+v\n", id.Name(), idVal)
				// If we found the ID field, then we can just send the mutations.
				//
				// TODO: Add logic to check something, not sure what.
			}
		}

		xids := typ.XIDFields()
		for _, x := range xids {
			glog.Infof("Found XID field: %s %+v\n", x.Name(), x)
		}
		// I think all the XID fields should be present for us to ensure that
		// we can find the right ID for this object.

		// var queries []*gql.GraphQuery
		var bms []*sroar.Bitmap
		for _, xid := range xids {
			xidVal := obj[xid.Name()]
			if xidVal == nil {
				return nil, fmt.Errorf("XID %s can't be nil for obj: %+v\n", xid.Name(), obj)
			}
			xidString, err := extractVal(xidVal, xid)
			if err != nil {
				return nil, errors.Wrapf(err, "while extractVal")
			}

			glog.Infof("Converted xid value to string: %q -> %q   %q %q\n",
				xid.Name(), xidString, xid.DgraphPredicate(), xid.DgraphAlias())

			// TODO: Check if we can pass UIDs to this to filter quickly.
			bm, err := UidsForXid(ctx, xid.DgraphAlias(), xidString)
			if err != nil {
				// TODO: Gotta wrap up errors to ensure GraphQL compliance.
				return nil, err
			}
			glog.Infof("Cardinality of xid %s is %d\n", xid.Name(), bm.GetCardinality())
			bms = append(bms, bm)
			if bm.GetCardinality() == 0 {
				// No need to proceed, if we couldn't find any
				break
			}
		}
		bm := sroar.FastAnd(bms...)
		if num := bm.GetCardinality(); num > 1 {
			return nil, fmt.Errorf("Found %d UIDs for %v", num, xids)
		}

		uids := bm.ToArray()
		// If we found an entry and upsert flag is set to false, we should not
		// update the entry.
		if len(uids) == 1 {
			obj["uid"] = "0x" + strconv.FormatUint(uids[0], 16)
			glog.Infof("Rewrote to %+v\n", obj)
			if !upsert {
				// We won't add a new object here, because an object already
				// exists. And we should not be updating this object because
				// upsert is false. Just return the list of UIDS found.
				resultUids = append(resultUids, uids[0])
				continue
			}
		} else {
			obj["uid"] = "_:test"
			// TODO: We should overhaul this type system later.
			obj["dgraph.type"] = typ.DgraphName()
		}

		for i, f := range fields {
			if val, has := obj[f.Name()]; has {
				obj[f.DgraphAlias()] = val
				delete(obj, f.Name())
				glog.Infof("Replacing %d %q -> %q\n", i, f.Name(), f.DgraphAlias())
			}
		}
		// No uids found. Add.
		glog.Infof("Setting object: %+v\n", obj)

		data, err := json.Marshal(obj)
		x.Check(err)
		mu := &pb.Mutation{SetJson: data}

		// TODO: Batch all this up.
		resp, err := edgraph.Query(ctx, &pb.Request{Mutations: []*pb.Mutation{mu}})
		glog.Infof("Got error from query: %v resp: %+v\n", err, resp)
		if err != nil {
			return nil, err
		}
		var uid uint64
		if len(uids) == 1 {
			uid = uids[0]
		} else {
			uid = x.FromHex(resp.Uids["test"])
		}
		glog.Infof("Got UID: %#x\n", uid)
		resultUids = append(resultUids, uid)
	}
	return resultUids, nil
}

func runDelete(ctx context.Context, m *schema.Field) ([]uint64, error) {
	typ := m.MutatedType()
	glog.Infof("m.Name: %q type: %q\n", m.Name(), typ.Name())

	dgQuery := []*gql.GraphQuery{{
		Attr: m.Name(),
	}}
	dgQuery[0].Children = append(dgQuery[0].Children, &gql.GraphQuery{
		Attr: "uid",
	})

	filter := extractMutationFilter(m)
	ids := idFilter(filter, typ.IDField())
	if ids != nil {
		addUIDFunc(dgQuery[0], ids)
	} else {
		addTypeFunc(dgQuery[0], m.MutatedType().DgraphName())
	}

	_ = addFilter(dgQuery[0], m.MutatedType(), filter)

	q := dgraph.AsString(dgQuery)
	glog.Infof("Got Query: %s\n", q)

	resp, err := edgraph.Query(ctx, &pb.Request{Query: q})
	glog.Infof("Got error: %v. Resp: %s\n", err, resp.GetJson())
	if err != nil {
		return nil, errors.Wrapf(err, "while querying")
	}

	type U struct {
		Uid string `json:"uid"`
	}

	data := make(map[string][]U)
	if err := json.Unmarshal(resp.GetJson(), &data); err != nil {
		// Unable to find any UIDs.
		return nil, nil
	}
	glog.Infof("Parsed data: %+v\n", data)
	glog.Infof("Found uids: %+v\n", data[m.Name()])

	// TODO(mrjn): Deal with reverse edges.

	var uids []uint64
	mu := &pb.Mutation{}
	for _, u := range data[m.Name()] {
		uid := u.Uid
		uids = append(uids, x.FromHex(uid))
		for _, field := range m.MutatedType().Fields() {
			mu.Del = append(mu.Del, &pb.NQuad{
				Subject:     uid,
				Predicate:   field.DgraphAlias(),
				ObjectValue: &pb.Value{&pb.Value_DefaultVal{x.Star}},
			})
		}
		mu.Del = append(mu.Del, &pb.NQuad{
			Subject:     uid,
			Predicate:   "dgraph.type",
			ObjectValue: &pb.Value{&pb.Value_StrVal{typ.DgraphName()}},
		})
	}
	glog.Infof("got mutation: %+v\n", mu)
	req := &pb.Request{}
	req.Mutations = append(req.Mutations, mu)

	resp, err = edgraph.Query(ctx, req)
	if err != nil {
		return nil, errors.Wrapf(err, "while executing deletions")
	}
	glog.Infof("Got response: %+v\n", resp)
	return uids, nil
}

func runUpdate(ctx context.Context, m *schema.Field) ([]uint64, error) {
	return nil, fmt.Errorf("Update is not handled yet")
}

func rewriteQueries(ctx context.Context, m *schema.Field) ([]uint64, error) {
	glog.Infof("mutatedType: %s Mutation Type: %s\n", m.MutatedType(), m.MutationType())

	switch m.MutationType() {
	case schema.AddMutation:
		return runUpsert(ctx, m)
	case schema.DeleteMutation:
		return runDelete(ctx, m)
	case schema.UpdateMutation:
		return runUpdate(ctx, m)
	default:
		return nil, fmt.Errorf("Invalid mutation type: %s\n", m.MutationType())
	}
}

func UidsForXid(ctx context.Context, pred, value string) (*sroar.Bitmap, error) {
	q := &pb.Query{
		ReadTs: posting.ReadTimestamp(),
		Attr:   x.NamespaceAttr(0, pred),
		SrcFunc: &pb.SrcFunction{
			Name: "eq",
			Args: []string{value},
		},
		First: 3,
	}
	result, err := worker.ProcessTaskOverNetwork(ctx, q)
	if err != nil {
		return nil, errors.Wrapf(err, "while calling ProcessTaskOverNetwork")
	}
	glog.Infof("Result uidmatrix length: %d", len(result.UidMatrix))
	if len(result.UidMatrix) == 0 {
		// No result found
		return sroar.NewBitmap(), nil
	}
	return codec.FromList(result.UidMatrix[0]), nil
}

func (mr *dgraphResolver) rewriteAndExecute(
	ctx context.Context,
	mutation *schema.Field) (*Resolved, bool) {

	ext := &schema.Extensions{}
	emptyResult := func(err error) *Resolved {
		return &Resolved{
			// all the standard mutations are nullable objects, so Data should pretty-much be
			// {"mutAlias":null} everytime.
			Data:  mutation.NullResponse(),
			Field: mutation,
			// there is no completion down the pipeline, so error's path should be prepended with
			// mutation's alias before returning the response.
			Err:        schema.PrependPath(err, mutation.ResponseName()),
			Extensions: ext,
		}
	}
	_ = emptyResult(nil)

	uids, err := rewriteQueries(ctx, mutation)
	if err != nil {
		glog.Errorf("Error from rewriteQueries: %v", err)
		return nil, false
	}
	glog.Infof("Got uids: %+v\n", uids)

	glog.Infof("Query field: %+v\n", mutation.QueryField())

	field := mutation.QueryField()
	dgQuery := []*gql.GraphQuery{{
		Attr: field.DgraphAlias(),
	}}
	dgQuery[0].Func = &gql.Function{
		Name: "uid",
		UID:  uids,
	}
	addArgumentsToField(dgQuery[0], field)
	glog.Infof("Got dgQuery[0]: %+v\n", dgQuery)
	dgQuery[0].DebugPrint("mu  ")

	dgQuery = append(dgQuery, addSelectionSetFrom(dgQuery[0], field, nil)...)

	q := dgraph.AsString(dgQuery)
	glog.Infof("Query: %s\n", q)

	resp, err := (&DgraphEx{}).Execute(ctx, &pb.Request{Query: q}, field)
	glog.Infof("Got error: %+v\n", err)
	glog.Infof("Response: %+v\n", resp)

	res := &Resolved{
		Data:  completeMutationResult(mutation, resp.Json, len(uids)),
		Field: mutation,
		Err:   schema.PrependPath(nil, mutation.ResponseName()),
	}
	return res, resolverSucceeded
}
