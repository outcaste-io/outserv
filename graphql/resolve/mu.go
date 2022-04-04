// Portions Copyright 2019 Dgraph Labs, Inc. are available under the Apache License v2.0.
// Portions Copyright 2022 Outcaste LLC are available under the Smart License v1.0.

package resolve

import (
	"bytes"
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
			return nil, fmt.Errorf("XID %q can't be nil for obj: %+v\n", xid.Name(), obj)
		}
		xidString, err := extractVal(xidVal, xid)
		if err != nil {
			return nil, errors.Wrapf(err, "while extractVal")
		}

		// TODO: Check if we can pass UIDs to this to filter quickly.
		bm, err := UidsForXid(ctx, xid.DgraphAlias(), xidString)
		if err != nil {
			// TODO(mrjn): Wrap up errors to ensure GraphQL compliance.
			return nil, err
		}
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
var upsertFlag int = 0x1

func gatherObjects(ctx context.Context, src Object, typ *schema.Type,
	flags int) ([]Object, error) {

	var idVal uint64
	if id := typ.IDField(); id != nil {
		if val := src[id.Name()]; val != nil {
			valStr, err := extractVal(val, id)
			if err != nil {
				return nil, errors.Wrapf(err, "while converting ID to string for %s", id.Name())
			}
			idVal = x.FromHex(valStr)
		}
	}

	// TODO(mrjn): Optimization for later. We should query all of them in a
	// single call to make this more efficient. Or, run gatherObjects via
	// goroutines.
	uids, err := UidsFromManyXids(ctx, src, typ, false)
	if err != nil {
		return nil, errors.Wrapf(err, "UidsFromManyXids")
	}

	dst := make(map[string]interface{})
	updateObject := true

	switch {
	case len(uids) > 1:
		return nil, fmt.Errorf("Found %d UIDs", len(uids))
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
		// TODO(mrjn): We should overhaul this type system later.
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
		if flags&upsertFlag == 0 {
			// We won't add a new object here, because an object already
			// exists. And we should not be updating this object because
			// upsert is false. Just return the list of UIDS found.
			updateObject = false
		}
	}

	// Now parse the fields.
	var res []Object
	fields := typ.Fields()

	for _, f := range fields {
		val, has := src[f.Name()]
		if !has {
			continue
		}
		if f.Type().IsInbuiltOrEnumType() {
			dst[f.DgraphAlias()] = val
			continue
		}

		var children []Object
		if vlist, ok := val.([]interface{}); ok {
			for _, elem := range vlist {
				e := elem.(map[string]interface{})
				objs, err := gatherObjects(ctx, e, f.Type(), flags)
				if err != nil {
					return nil, errors.Wrapf(err, "while nesting into %s", f.Name())
				}
				children = append(children, objs...)
			}

		} else if vmap, ok := val.(map[string]interface{}); ok {
			objs, err := gatherObjects(ctx, vmap, f.Type(), flags)
			if err != nil {
				return nil, errors.Wrapf(err, "while nesting into %s", f.Name())
			}
			children = append(children, objs...)

		} else {
			panic(fmt.Sprintf("Unhandled type of val: %+v type: %T", val, val))
		}

		if inv := f.Inverse(); inv != nil {
			uid := dst["uid"]
			lt := inv.Type().ListType()
			// Add reverse edge from child -> parent.
			for _, child := range children {
				if lt != nil {
					child[inv.DgraphAlias()] = []Object{{"uid": uid}}
				} else {
					child[inv.DgraphAlias()] = Object{"uid": uid}
				}
			}
		}

		if list := f.Type().ListType(); list != nil {
			dst[f.DgraphAlias()] = children
		} else if len(children) == 1 {
			dst[f.DgraphAlias()] = children[0]
		} else if len(children) > 1 {
			return nil, fmt.Errorf("Found multiple children for non-list field: %s",
				f.DgraphAlias())
		}
	}

	if updateObject {
		res = append(res, dst)
	}
	return res, nil
}
func runAdd(ctx context.Context, m *schema.Field) ([]uint64, error) {
	// Parsing input
	val, ok := m.ArgValue(schema.InputArgName).([]interface{})
	x.AssertTrue(ok)

	var flags int
	if upsertVal := m.ArgValue(schema.UpsertArgName); upsertVal != nil {
		upsert := upsertVal.(bool)
		if upsert {
			flags |= upsertFlag
		}
	}

	typ := m.MutatedType()
	// Just consider the first one for now.
	var res []Object
	for _, i := range val {
		obj := i.(map[string]interface{})
		objs, err := gatherObjects(ctx, obj, typ, flags)
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
	if len(res) == 0 {
		return resultUids, nil
	}
	data, err := json.Marshal(res)
	x.Check(err)

	if glog.V(2) {
		data2, _ := json.MarshalIndent(res, " ", " ")
		glog.Infof("Mutation Req JSON data: %s\n", data2)
	}
	mu := &pb.Mutation{SetJson: data}
	resp, err := edgraph.Query(ctx, &pb.Request{Mutations: []*pb.Mutation{mu}})
	if err != nil {
		return nil, err
	}
	glog.V(2).Infof("Got response: %s\n", resp.Json)

	for key, uid := range resp.Uids {
		if strings.HasPrefix(key, typ.Name()+"-") {
			resultUids = append(resultUids, x.FromHex(uid))
		}
	}
	return resultUids, nil
}

func extractMutationFilter(m *schema.Field) map[string]interface{} {
	var filter map[string]interface{}
	mutationType := m.MutationType()
	if mutationType == schema.UpdateMutation {
		input, ok := m.ArgValue("input").(map[string]interface{})
		if ok {
			filter, _ = input["filter"].(map[string]interface{})
		}
	} else if mutationType == schema.DeleteMutation {
		filter, _ = m.ArgValue("filter").(map[string]interface{})
	}
	return filter
}

func getUidsFromFilter(ctx context.Context, m *schema.Field) ([]uint64, error) {
	dgQuery := []*gql.GraphQuery{{
		Attr: m.Name(),
	}}
	dgQuery[0].Children = append(dgQuery[0].Children, &gql.GraphQuery{
		Attr: "uid",
	})

	filter := extractMutationFilter(m)
	ids := idFilter(filter, m.MutatedType().IDField())
	if ids != nil {
		addUIDFunc(dgQuery[0], ids)
	} else {
		addTypeFunc(dgQuery[0], m.MutatedType().DgraphName())
	}

	_ = addFilter(dgQuery[0], m.MutatedType(), filter)

	q := dgraph.AsString(dgQuery)
	resp, err := edgraph.Query(ctx, &pb.Request{Query: q})
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

	var uids []uint64
	for _, u := range data[m.Name()] {
		uid := u.Uid
		uids = append(uids, x.FromHex(uid))
	}
	return uids, nil
}

func getChildrenUids(ctx context.Context, uid, pred string) ([]string, error) {
	// We need to get the UID for the object. So, the field in
	// getObject is really a query.
	obj, err := getObject(ctx, uid, fmt.Sprintf("%s {uid}", pred))
	if err != nil {
		return nil, fmt.Errorf("While getting %s: %+v", pred, err)
	}
	childObj := obj[pred]
	if childObj == nil {
		return nil, nil
	}

	// Delete in the reverse direction.
	var children []string
	if co, has := childObj.(map[string]interface{}); has {
		childUid := co["uid"].(string)
		children = append(children, childUid)
	} else if clist, has := childObj.([]interface{}); has {
		for _, co := range clist {
			cm := co.(map[string]interface{})
			childUid := cm["uid"].(string)
			children = append(children, childUid)
		}
	} else {
		return nil, fmt.Errorf("getChildrenUids is unable to parse: %+v", childObj)
	}
	return children, nil
}

func runDelete(ctx context.Context, m *schema.Field) ([]uint64, error) {
	uids, err := getUidsFromFilter(ctx, m)
	if err != nil {
		return nil, errors.Wrapf(err, "getUidsFromFilter")
	}

	mu := &pb.Mutation{}
	accountForInverse := func(uidHex string, f *schema.FieldDefinition) {
		inv := f.Inverse()
		if inv == nil {
			return
		}

		// Find all the children and send deletion markers, so they no longer
		// point to the parent.
		cuids, err := getChildrenUids(ctx, uidHex, f.DgraphAlias())
		if err != nil {
			glog.Errorf("While getting %s.%s: %+v", f.Type().Name(), f.Name(), err)
			return
		}
		for _, childUid := range cuids {
			mu.Nquads = append(mu.Nquads, &pb.NQuad{
				Subject:   childUid,
				Predicate: inv.DgraphAlias(),
				ObjectId:  uidHex,
				Op:        pb.NQuad_DEL,
			})
		}
	}

	for _, uid := range uids {
		uidHex := x.ToHexString(uid)
		for _, f := range m.MutatedType().Fields() {
			accountForInverse(uidHex, f)
			mu.Nquads = append(mu.Nquads, &pb.NQuad{
				Subject:     uidHex,
				Predicate:   f.DgraphAlias(),
				ObjectValue: &pb.Value{&pb.Value_DefaultVal{x.Star}},
				Op:          pb.NQuad_DEL,
			})
		}
		mu.Nquads = append(mu.Nquads, &pb.NQuad{
			Subject:     uidHex,
			Predicate:   "dgraph.type",
			ObjectValue: &pb.Value{&pb.Value_StrVal{m.MutatedType().DgraphName()}},
			Op:          pb.NQuad_DEL,
		})
	}

	req := &pb.Request{}
	req.Mutations = append(req.Mutations, mu)

	resp, err := edgraph.Query(ctx, req)
	if err != nil {
		return nil, errors.Wrapf(err, "while executing deletions")
	}
	glog.V(2).Infof("Mutations: %+v\nGot response: %s\n", req.Mutations, resp.Json)
	return uids, nil
}

func getObject(ctx context.Context, uid string, fields ...string) (map[string]interface{}, error) {
	q := fmt.Sprintf(`{q(func: uid(%s)) { %s }}`, uid, strings.Join(fields[:], ", "))
	resp, err := edgraph.Query(ctx, &pb.Request{Query: q})
	if err != nil {
		return nil, errors.Wrapf(err, "while requesting for object")
	}

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

// checkIfDuplicateExists ensures that there's no other object like dst. That
// dst's XIDs are unique when put together (invidually they can still have
// multiple results).
func checkIfDuplicateExists(ctx context.Context,
	typ *schema.Type, dst map[string]interface{}) error {

	u, has := dst["uid"]
	x.AssertTrue(has)
	uid := u.(string)

	var needToCheck bool
	var xidList []string
	for _, xidField := range typ.XIDFields() {
		if _, has := dst[xidField.DgraphAlias()]; has {
			needToCheck = true
		}
		xidList = append(xidList, xidField.DgraphAlias())
	}
	if !needToCheck {
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
	uids, err := UidsFromManyXids(ctx, src, typ, true)
	if err != nil {
		return errors.Wrapf(err, "UidsFromManyXids")
	}
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

func runUpdate(ctx context.Context, m *schema.Field) ([]uint64, error) {
	uids, err := getUidsFromFilter(ctx, m)
	if err != nil {
		return nil, errors.Wrapf(err, "getUidsFromFilter")
	}
	if len(uids) == 0 {
		return nil, nil
	}

	typ := m.MutatedType()
	defs := make(map[string]*schema.FieldDefinition)
	for _, f := range typ.Fields() {
		defs[f.DgraphAlias()] = f
	}

	mu := &pb.Mutation{}
	parseObjects := func(src map[string]interface{}, forAdd bool) error {
		var dstObjs []Object
		var nquads []*pb.NQuad

		obj := make(Object)
		for _, f := range typ.Fields() {
			val, ok := src[f.Name()]
			if !ok {
				continue
			}
			if f.Type().IsInbuiltOrEnumType() {
				obj[f.DgraphAlias()] = val
				continue
			}
			objs, err := gatherObjects(ctx, val.(map[string]interface{}), f.Type(), upsertFlag)
			if err != nil {
				return errors.Wrapf(err, "while gathering object for %q", f.Name())
			}
			if list := f.Type().ListType(); list != nil {
				var l []Object
				for _, obj := range objs {
					l = append(l, Object{"uid": obj["uid"]})
				}
				obj[f.DgraphAlias()] = l
			} else if len(objs) == 1 {
				obj[f.DgraphAlias()] = Object{"uid": objs[0]["uid"]}
			} else if len(objs) > 1 {
				return fmt.Errorf("Found multiple objects when expecting one: %+v", objs)
			}
			if forAdd {
				// If this is for delete, we shouldn't delete the children.
				// Deletes on those should be run separately.
				dstObjs = append(dstObjs, objs...)
			}
		}

		// TODO: We should probably make this concurrent if the number of uids
		// warrant it.
		for _, uid := range uids {
			dst := make(Object)
			uidStr := x.ToHexString(uid)
			for key, val := range obj {
				dst[key] = val

				f := defs[key]
				inv := f.Inverse()
				if inv == nil {
					continue
				}
				vo := val.(Object)
				nq := &pb.NQuad{
					Subject:   vo["uid"].(string),
					Predicate: inv.DgraphAlias(),
					ObjectId:  uidStr,
					Op:        pb.NQuad_SET,
				}

				include := true
				if list := f.Type().ListType(); forAdd && list == nil {
					// This can only have one UID it points to. And that UID has
					// an inverse. So, we need to delete the inverse edge.
					cuids, err := getChildrenUids(ctx, uidStr, f.DgraphAlias())
					if err != nil {
						return errors.Wrapf(err,
							"while getting %s for %s", f.DgraphAlias(), uidStr)
					}
					if len(cuids) > 1 {
						return fmt.Errorf("Found multiple child UIDs %v for %s",
							cuids, f.DgraphAlias())
					}
					if len(cuids) == 1 {
						cuid := cuids[0]
						if cuid == nq.Subject {
							include = false
						} else {
							// This is "forAdd". So, we can safely just directly
							// set this in mu.Del. This won't be called for
							// delete.
							mu.Nquads = append(mu.Nquads,
								&pb.NQuad{
									Subject:   cuids[0],
									Predicate: inv.DgraphAlias(),
									ObjectId:  uidStr,
									Op:        pb.NQuad_DEL,
								})
						}
					}
				}
				if include {
					nquads = append(nquads, nq)
				}
			}
			dst["uid"] = x.ToHexString(uid)
			if forAdd {
				if err := checkIfDuplicateExists(ctx, typ, dst); err != nil {
					return err
				}
			}
			dstObjs = append(dstObjs, dst)
		}
		data, err := json.Marshal(dstObjs)
		x.Check(err)
		if forAdd {
			mu.SetJson = data
		} else {
			mu.DeleteJson = data
		}
		mu.Nquads = append(mu.Nquads, nquads...)
		return nil
	}

	inp := m.ArgValue(schema.InputArgName).(map[string]interface{})
	if set, hasSet := inp["set"].(map[string]interface{}); hasSet {
		if err := parseObjects(set, true); err != nil {
			return nil, errors.Wrapf(err, "while parseObjAndChildren: %v", err)
		}
	}

	if del, hasDel := inp["remove"].(map[string]interface{}); hasDel {
		if err := parseObjects(del, false); err != nil {
			return nil, errors.Wrapf(err, "while parseObjAndChildren: %v", err)
		}
	}

	// We need to ensure that we're not modifying an object which would violate
	// the XID uniqueness constraints.
	//
	// Step 1. Get the object XIDs.
	// Step 2. Apply the set patch to the object.
	// Step 3. Check if there is another UID for this set of XIDs.
	// Step 4. If so, reject the update for this UID.

	glog.V(2).Infof("Mutation: %+v\n", mu)
	resp, err := edgraph.Query(ctx, &pb.Request{Mutations: []*pb.Mutation{mu}})
	if err != nil {
		return nil, errors.Wrapf(err, "while executing updates")
	}
	glog.V(2).Infof("Got response: %s\n", resp.Json)
	return uids, nil
}

func rewriteQueries(ctx context.Context, m *schema.Field) ([]uint64, error) {
	switch m.MutationType() {
	case schema.AddMutation:
		return runAdd(ctx, m)
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
	if len(result.UidMatrix) == 0 {
		// No result found
		return sroar.NewBitmap(), nil
	}
	return codec.FromList(result.UidMatrix[0]), nil
}

// completeMutationResult takes in the result returned for the query field of mutation and builds
// the JSON required for data field in GraphQL response.
// The input qryResult can either be nil or of the form:
//  {"qryFieldAlias":...}
// and the output will look like:
//  {"addAuthor":{"qryFieldAlias":...,"numUids":2,"msg":"Deleted"}}
func completeMutationResult(mutation *schema.Field, qryResult []byte, numUids int) []byte {
	comma := ""
	var buf bytes.Buffer
	x.Check2(buf.WriteRune('{'))
	mutation.CompleteAlias(&buf)
	x.Check2(buf.WriteRune('{'))

	// Our standard MutationPayloads consist of only the following fields:
	//  * queryField
	//  * numUids
	//  * msg (only for DeleteMutationPayload)
	// And __typename can be present anywhere. So, build data accordingly.
	// Note that all these fields are nullable, so no need to raise non-null errors.
	for _, f := range mutation.SelectionSet() {
		x.Check2(buf.WriteString(comma))
		f.CompleteAlias(&buf)

		switch f.Name() {
		case schema.Typename:
			x.Check2(buf.WriteString(`"` + f.TypeName(nil) + `"`))
		case schema.Msg:
			if numUids == 0 {
				x.Check2(buf.WriteString(`"No nodes were deleted"`))
			} else {
				x.Check2(buf.WriteString(`"Deleted"`))
			}
		case schema.NumUid:
			// Although theoretically it is possible that numUids can be out of the int32 range but
			// we don't need to apply coercion rules here as per Int type because carrying out a
			// mutation which mutates more than 2 billion uids doesn't seem a practical case.
			// So, we are skipping coercion here.
			x.Check2(buf.WriteString(strconv.Itoa(numUids)))
		default: // this has to be queryField
			if len(qryResult) == 0 {
				// don't write null, instead write [] as query field is always a nullable list
				x.Check2(buf.Write(schema.JsonEmptyList))
			} else {
				// need to write only the value returned for query field, so need to remove the JSON
				// key till colon (:) and also the ending brace }.
				// 4 = {"":
				x.Check2(buf.Write(qryResult[4+len(f.ResponseName()) : len(qryResult)-1]))
			}
		}
		comma = ","
	}
	x.Check2(buf.WriteString("}}"))

	return buf.Bytes()
}

func (mr *dgraphResolver) Resolve(ctx context.Context, m *schema.Field) (*Resolved, bool) {
	span := otrace.FromContext(ctx)
	stop := x.SpanTimer(span, "resolveMutation")
	defer stop()
	if span != nil {
		span.Annotatef(nil, "mutation alias: [%s] type: [%s]", m.Alias(), m.MutationType())
	}

	field := m.QueryField()
	calculateResponse := func(uids []uint64) (*pb.Response, error) {
		dgQuery := []*gql.GraphQuery{{
			Attr: field.DgraphAlias(),
		}}
		dgQuery[0].Func = &gql.Function{
			Name: "uid",
			UID:  uids,
		}
		addArgumentsToField(dgQuery[0], field)
		dgQuery = append(dgQuery, addSelectionSetFrom(dgQuery[0], field)...)

		q := dgraph.AsString(dgQuery)
		resp, err := (&DgraphEx{}).Execute(ctx, &pb.Request{Query: q}, field)
		return resp, err
	}

	uids, err := rewriteQueries(ctx, m)
	var resp *pb.Response
	var err2 error
	if len(uids) > 0 {
		resp, err2 = calculateResponse(uids)
	}
	res := &Resolved{Field: m}
	if resp != nil && len(resp.Json) > 0 {
		res.Data = completeMutationResult(m, resp.Json, len(uids))
	} else {
		res.Data = m.NullResponse()
	}
	if err == nil && err2 != nil {
		res.Err = schema.PrependPath(err2, m.ResponseName())
	} else {
		res.Err = schema.PrependPath(err, m.ResponseName())
	}

	success := err == nil && err2 == nil
	return res, success
}
