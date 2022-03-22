package resolve

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/golang/glog"
	"github.com/outcaste-io/outserv/edgraph"
	"github.com/outcaste-io/outserv/gql"
	"github.com/outcaste-io/outserv/graphql/dgraph"
	"github.com/outcaste-io/outserv/graphql/schema"
	"github.com/outcaste-io/outserv/protos/pb"
	"github.com/outcaste-io/outserv/query"
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

func rewriteQueries(ctx context.Context, m *schema.Field) ([]uint64, error) {
	glog.Infof("mutatedType: %s\n", m.MutatedType())

	// Parsing input
	val, _ := m.ArgValue(schema.InputArgName).([]interface{})

	// Just consider the first one for now.
	var resultUids []string
	for _, i := range val {
		obj := i.(map[string]interface{})
		typ := m.MutatedType()
		glog.Infof("mutatedType: %+v obj: %+v\n", typ, obj)
		fields := typ.Fields()
		for i, f := range fields {
			glog.Infof("field %d: %q %+v", i, f.Name(), f)
		}

		id := typ.IDField()
		glog.Infof("ID Name: %s full: %+v\n", id.Name(), id)

		if idVal := obj[id.Name()]; idVal != nil {
			glog.Infof("Found ID field: %s %+v\n", id.Name(), idVal)
			// If we found the ID field, then we can just send the mutations.
			//
			// TODO: Add logic to check something, not sure what.
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

			glog.Infof("Converted xid value to string: %q -> %q   %q %q\n", xid.Name(), xidString, xid.DgraphPredicate(), xid.DgraphAlias())
			bm := query.UidsForXid(context.TODO(), xid.DgraphAlias(), xidString)
			bms = append(bms, bm)

			// query := &gql.GraphQuery{
			// 	Attr: "result1",
			// 	Func: &gql.Function{
			// 		Name: "eq",
			// 		Args: []gql.Arg{
			// 			{Value: typ.DgraphPredicate(xid.DgraphPredicate()},
			// 			{Value: schema.MaybeQuoteArg("eq", xidString)},
			// 		},
			// 	},
			// }
		}
		bm := sroar.FastAnd(bms...)
		uids := bm.ToArray()
		if len(uids) > 1 {
			return nil, fmt.Errorf("Found %d UIDs for %s", len(uids), xids)
		}
		if len(uids) == 1 {
			obj["uid"] = uids[0]
			glog.Infof("Rewrote to %+v\n", obj)
		}

		for i, f := range fields {
			if val, has := obj[f.Name()]; has {
				obj[f.DgraphAlias()] = val
				delete(obj, f.Name())
				glog.Infof("Replacing %d %q -> %q\n", i, f.Name(), f.DgraphAlias())
			}
		}
		// No uids found. Add.
		obj["uid"] = "_:test"
		glog.Infof("Adding object: %+v\n", obj)

		data, err := json.Marshal(obj)
		x.Check(err)
		mu := &pb.Mutation{SetJson: data}

		resp, err := edgraph.Query(ctx, &pb.Request{Mutations: []*pb.Mutation{mu}})
		glog.Infof("Got error from query: %v resp: %+v\n", err, resp)
		if err != nil {
			return nil, err
		}
		uid := resp.Uids["test"]
		glog.Infof("Got UID: %s\n", uid)
		resultUids = append(resultUids, uid)
	}
	return convertIDsWithErr(resultUids)
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
