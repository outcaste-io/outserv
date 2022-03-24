package resolve

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"

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

func UidsFromManyXids(ctx context.Context, obj map[string]interface{},
	typ *schema.Type, useDgraphNames bool) ([]uint64, error) {

	var bms []*sroar.Bitmap
	for _, xid := range typ.XIDFields() {
		var xidVal interface{}
		if useDgraphNames {
			xidVal = obj[xid.DgraphAlias()]
		} else {
			xidVal = obj[xid.Name()]
		}
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
			// No need to proceed, if we couldn't find any.
			return nil, nil
		}
	}
	bm := sroar.FastAnd(bms...)
	return bm.ToArray(), nil
}

type Object map[string]interface{}

var objCounter uint64

func gatherObjects(ctx context.Context, src Object, typ *schema.Type, upsert bool) ([]Object, error) {
	// TODO: Refactor this so we deal with each val in a nested func.
	glog.Infof("mutatedType: %+v obj: %+v\n", typ, src)
	fields := typ.Fields()

	var res []Object
	var dst Object
	for i, f := range fields {
		val, has := src[f.Name()]
		if !has {
			continue
		}

		dst[f.DgraphAlias()] = val
		glog.Infof("Replacing %d %q -> %q\n", i, f.Name(), f.DgraphAlias())
		if f.Type().IsInbuiltOrEnumType() {
			continue
		}

		glog.Infof("--> got a field which is not inbuilt type: %s %s %+v\n", f.Name(), f.Type().Name(), val)
		switch val.(type) {
		case []Object:
			v := val.([]map[string]interface{})
			for _, elem := range v {
				objs, err := gatherObjects(ctx, elem, f.Type(), upsert)
				if err != nil {
					return nil, errors.Wrapf(err, "while nesting into %s", f.Name())
				}
				res = append(res, objs...)
			}
		case Object:
			v := val.(map[string]interface{})
			objs, err := gatherObjects(ctx, v, f.Type(), upsert)
			if err != nil {
				return nil, errors.Wrapf(err, "while nesting into %s", f.Name())
			}
			res = append(res, objs...)
		default:
			panic(fmt.Sprintf("Unhandled type of val: %+v", val))
		}
	}

	// TODO: Why should an ID be present for an upsert? ID only gets
	// assigned via Dgraph UID, doesn't it?
	var idVal uint64
	if id := typ.IDField(); id != nil {
		glog.Infof("ID Name: %s\n", id.Name())
		if val := src[id.Name()]; val != nil {
			glog.Infof("Found ID field: %s %+v\n", id.Name(), val)
			valStr, err := extractVal(val, id)
			if err != nil {
				return nil, errors.Wrapf(err, "while converting ID to string for %s", id.Name())
			}
			idVal = x.FromHex(valStr)
		}
	}

	xids := typ.XIDFields()
	for _, x := range xids {
		glog.Infof("Found XID field: %s %+v\n", x.Name(), x)
	}
	// I think all the XID fields should be present for us to ensure that
	// we can find the right ID for this object.

	uids, err := UidsFromManyXids(ctx, src, typ, false)
	if err != nil {
		return nil, errors.Wrapf(err, "UidsFromManyXids")
	}

	updateObject := true
	switch {
	case len(uids) > 1:
		return nil, fmt.Errorf("Found %d UIDs for %v", len(uids), xids)
	case len(uids) == 0:
		// No object with the given XIDs exists. This is an insert.
		if idVal > 0 {
			// use the idVal that we parsed before.
			dst["uid"] = x.ToHexString(idVal)
		} else {
			// We need a counter with this variable to allow multiple such
			// objects.
			dst["uid"] = fmt.Sprintf("_:%s-%d", typ.Name(), atomic.AddUint64(&objCounter, 1))
		}
		// TODO: We should overhaul this type system later.
		dst["dgraph.type"] = typ.DgraphName()
	default:
		// len(uids) == 1
		if idVal > 0 && idVal != uids[0] {
			// We found an idVal, but it doesn't match the UID found via
			// XIDs. This is strange.
			return nil, errors.Wrapf(err,
				"ID provided: %#x doesn't match ID found: %#x", idVal, uids[0])
		}
		// idVal if present matches with uids[0]
		dst["uid"] = x.ToHexString(uids[0])
		if !upsert {
			// We won't add a new object here, because an object already
			// exists. And we should not be updating this object because
			// upsert is false. Just return the list of UIDS found.
			updateObject = false
		}
	}

	glog.Infof("Rewrote to %+v\n", dst)
	if updateObject {
		res = append(res, dst)
	}
	return res, nil
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

	typ := m.MutatedType()
	// Just consider the first one for now.
	var res []Object
	for _, i := range val {
		obj := i.(map[string]interface{})
		objs, err := gatherObjects(ctx, obj, typ, upsert)
		if err != nil {
			return nil, errors.Wrapf(err, "while gathering objects")
		}
		res = append(res, objs...)
	}

	var resultUids []uint64
	for _, obj := range res {
		uid := obj["uid"]
		x.AssertTrue(uid != nil)
		uidStr := uid.(string)
		if strings.HasPrefix(uidStr, "_:") {
			continue
		}
		resultUids = append(resultUids, x.FromHex(uidStr))
	}
	data, err := json.Marshal(res)
	x.Check(err)

	glog.Infof("Data to be mutated: %s\n", data)
	mu := &pb.Mutation{SetJson: data}

	// TODO: Batch all this up.
	resp, err := edgraph.Query(ctx, &pb.Request{Mutations: []*pb.Mutation{mu}})
	glog.Infof("Got error from query: %v resp: %+v\n", err, resp)
	if err != nil {
		return nil, err
	}

	for key, uid := range resp.Uids {
		if strings.HasPrefix(key, typ.Name()+"-") {
			resultUids = append(resultUids, x.FromHex(uid))
		}
	}
	glog.Infof("Got resultUids: %+v\n", resultUids)
	return resultUids, nil
}

func getUidsFromFilter(ctx context.Context, m *schema.Field) ([]uint64, error) {
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

	var uids []uint64
	for _, u := range data[m.Name()] {
		uid := u.Uid
		uids = append(uids, x.FromHex(uid))
	}
	return uids, nil
}

func runDelete(ctx context.Context, m *schema.Field) ([]uint64, error) {
	uids, err := getUidsFromFilter(ctx, m)
	if err != nil {
		return nil, errors.Wrapf(err, "getUidsFromFilter")
	}

	// TODO(mrjn): Deal with reverse edges.

	mu := &pb.Mutation{}
	for _, uid := range uids {
		uidHex := x.ToHexString(uid)
		for _, field := range m.MutatedType().Fields() {
			mu.Del = append(mu.Del, &pb.NQuad{
				Subject:     uidHex,
				Predicate:   field.DgraphAlias(),
				ObjectValue: &pb.Value{&pb.Value_DefaultVal{x.Star}},
			})
		}
		mu.Del = append(mu.Del, &pb.NQuad{
			Subject:     uidHex,
			Predicate:   "dgraph.type",
			ObjectValue: &pb.Value{&pb.Value_StrVal{m.MutatedType().DgraphName()}},
		})
	}
	glog.Infof("got mutation: %+v\n", mu)
	req := &pb.Request{}
	req.Mutations = append(req.Mutations, mu)

	resp, err := edgraph.Query(ctx, req)
	if err != nil {
		return nil, errors.Wrapf(err, "while executing deletions")
	}
	glog.Infof("Got response: %+v\n", resp)
	return uids, nil
}

func getObject(ctx context.Context, uid string, fields ...string) (map[string]interface{}, error) {
	q := fmt.Sprintf(`{q(func: uid(%s)) { %s }}`, uid, strings.Join(fields[:], ", "))
	resp, err := edgraph.Query(ctx, &pb.Request{Query: q})
	if err != nil {
		return nil, errors.Wrapf(err, "while requesting for object")
	}

	glog.Infof("Got response: %s\n", resp.Json)
	var out map[string][]map[string]interface{}
	if err := json.Unmarshal(resp.Json, &out); err != nil {
		// This means we couldn't find the object. Ignore.
		return nil, nil
	}
	list, has := out["q"]
	if !has || len(list) == 0 {
		return nil, nil
	}
	x.AssertTrue(len(list) == 1) // Can't be more than 1.
	return list[0], nil
}

func runUpdate(ctx context.Context, m *schema.Field) ([]uint64, error) {
	uids, err := getUidsFromFilter(ctx, m)
	if err != nil {
		return nil, errors.Wrapf(err, "getUidsFromFilter")
	}

	inp := m.ArgValue(schema.InputArgName).(map[string]interface{})
	setObj, hasSet := inp["set"].(map[string]interface{})
	delObj, hasDel := inp["remove"].(map[string]interface{})

	glog.Infof("Got uids: %+v set: %+v del: %+v\n", uids, setObj, delObj)

	// TODO(mrjn): Ensure that the update does not violate the @id uniqueness
	// constraint.
	typ := m.MutatedType()

	dgName := func(field string) string {
		return typ.Name() + "." + field
	}

	var xidList []string
	xidMap := make(map[string]*schema.FieldDefinition)
	for _, xidField := range typ.XIDFields() {
		xidMap[dgName(xidField.Name())] = xidField
		xidList = append(xidList, dgName(xidField.Name()))
	}

	checkIfDuplicateExists := func(dst map[string]interface{}) error {
		u, has := dst["uid"]
		x.AssertTrue(has)
		uid := u.(string)

		var needToCheck bool
		for key := range dst {
			if _, has := xidMap[key]; has {
				// We're updating an XID for this object. So, we need to check
				// for duplicates.
				needToCheck = true
				glog.Infof("Found that we're updating an XID: %s for %s\n", key, uid)
			}
		}
		if !needToCheck {
			glog.Infof("No need to check for duplicate for %s\n", uid)
			return nil
		}
		src, err := getObject(ctx, uid, xidList...)
		if err != nil {
			return errors.Wrapf(err, "while getting object %s", uid)
		}
		x.AssertTrue(src != nil)
		for key, val := range dst {
			src[key] = val
		}
		glog.Infof("Updated object after applying patch: %+v\n", src)
		uids, err := UidsFromManyXids(ctx, src, typ, true)
		if err != nil {
			return errors.Wrapf(err, "UidsFromManyXids")
		}
		glog.Infof("Got UIDs: %+v\n", uids)
		if len(uids) == 0 {
			// No duplicates found.
			return nil
		}
		if uid == x.ToHexString(uids[0]) {
			// This UID shouldn't be the one with the given updated XIDs if they
			// were changed. Irrespective, no duplicate entries exist, so we're
			// good.
			return nil
		}

		var xids []string
		for _, x := range typ.XIDFields() {
			xids = append(xids, x.Name())
		}
		return fmt.Errorf("Duplicate entries exist for these unique ids: %v", xids)
	}

	// We need to ensure that we're not modifying an object which would violate
	// the XID uniqueness constraints.
	//
	// Step 1. Get the object XIDs.
	// Step 2. Apply the set patch to the object.
	// Step 3. Check if there is another UID for this set of XIDs.
	// Step 4. If so, reject the update for this UID.
	var setJson, delJson []map[string]interface{}
	if hasSet {
		for _, uid := range uids {
			dst := make(map[string]interface{})
			dst["uid"] = x.ToHexString(uid)
			for key, val := range setObj {
				dst[dgName(key)] = val
			}
			if err := checkIfDuplicateExists(dst); err != nil {
				// TODO(mrjn): We should just skip this one. And continue onto
				// the next UID -- the GraphQL way.
				return uids, err
			}
			setJson = append(setJson, dst)
		}
	}
	if hasDel {
		for _, uid := range uids {
			dst := make(map[string]interface{})
			dst["uid"] = x.ToHexString(uid)
			for key, val := range delObj {
				dst[dgName(key)] = val
			}
			delJson = append(delJson, dst)
		}
	}
	mu := &pb.Mutation{}
	if len(setJson) > 0 {
		mu.SetJson, err = json.Marshal(setJson)
		x.Check(err)
	}
	if len(delJson) > 0 {
		mu.DeleteJson, err = json.Marshal(delJson)
		x.Check(err)
	}

	glog.Infof("Got mutation: set: %q del: %q\n", mu.SetJson, mu.DeleteJson)

	resp, err := edgraph.Query(ctx, &pb.Request{Mutations: []*pb.Mutation{mu}})
	if err != nil {
		return nil, errors.Wrapf(err, "while executing updates")
	}
	glog.Infof("Got response: %+v\n", resp)
	return uids, nil
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

	calculateResponse := func(uids []uint64) (*pb.Response, error) {
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

		dgQuery = append(dgQuery, addSelectionSetFrom(dgQuery[0], field)...)

		q := dgraph.AsString(dgQuery)
		glog.Infof("Query: %s\n", q)

		resp, err := (&DgraphEx{}).Execute(ctx, &pb.Request{Query: q}, field)
		glog.Infof("Got error: %+v\n", err)
		glog.Infof("Response: %+v\n", resp)
		return resp, err
	}

	uids, err := rewriteQueries(ctx, mutation)
	var resp *pb.Response
	var err2 error
	if len(uids) > 0 {
		resp, err2 = calculateResponse(uids)
	}
	res := &Resolved{Field: mutation}
	if resp != nil && len(resp.Json) > 0 {
		res.Data = completeMutationResult(mutation, resp.Json, len(uids))
	} else {
		res.Data = mutation.NullResponse()
	}
	if err == nil && err2 != nil {
		res.Err = schema.PrependPath(err2, mutation.ResponseName())
	} else {
		res.Err = schema.PrependPath(err, mutation.ResponseName())
	}

	success := err == nil && err2 == nil
	return res, success
}
