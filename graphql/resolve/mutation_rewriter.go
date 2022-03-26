// Portions Copyright 2019 Dgraph Labs, Inc. are available under the Apache License v2.0.
// Portions Copyright 2022 Outcaste LLC are available under the Smart License v1.0.

package resolve

import (
	"fmt"
	"strings"

	"github.com/outcaste-io/outserv/gql"
	"github.com/outcaste-io/outserv/graphql/schema"
)

// A VariableGenerator generates unique variable names.
// TODO(mrjn): Do we need this variable generator? Maybe remove after rewriting
// query modules.
type VariableGenerator struct {
	counter       int
	xidVarNameMap map[string]string
}

func NewVariableGenerator() *VariableGenerator {
	return &VariableGenerator{
		counter:       0,
		xidVarNameMap: make(map[string]string),
	}
}

// Next gets the Next variable name for the given type and xid.
// So, if two objects of the same type have same value for xid field,
// then they will get same variable name.
func (v *VariableGenerator) Next(typ *schema.Type, xidName, xidVal string, auth bool) string {
	// return previously allocated variable name for repeating xidVal
	var key string
	flagAndXidName := xidName
	// isInterfaceVariable is true if Next function is being used to generate variable for an
	// interface. This is used for XIDs which are part of interface with interface=true flag.
	isIntefaceVariable := false

	// We pass the xidName as "Int.xidName" to generate variable for existence query
	// of interface type when id filed is inherited from interface and have interface Argument set
	// Here we handle that case
	if strings.Contains(flagAndXidName, ".") {
		xidName = strings.Split(flagAndXidName, ".")[1]
		isIntefaceVariable = true
	}

	if xidName == "" || xidVal == "" {
		key = typ.Name()
	} else {
		// here we are using the assertion that field name or type name can't have "." in them
		// We add "." between values while generating key to removes duplicate xidError from below type of cases
		// mutation {
		//  addABC(input: [{ ab: "cd", abc: "d" }]) {
		//    aBC {
		//      ab
		//      abc
		//    }
		//   }
		// }
		// The two generated keys for this case will be
		// ABC.ab.cd and ABC.abc.d
		// It also ensures that xids from different types gets different variable names
		// here we are using the assertion that field name or type name can't have "." in them
		xidType, _ := typ.FieldOriginatedFrom(xidName)
		key = xidType.Name() + "." + flagAndXidName + "." + xidVal
		if !isIntefaceVariable {
			// This is done to ensure that two implementing types get a different variable
			// assigned in case they are not inheriting the same XID with interface=true flag.
			key = key + typ.Name()
		}
	}

	if varName, ok := v.xidVarNameMap[key]; ok {
		return varName
	}

	// create new variable name
	v.counter++
	var varName string
	if auth {
		varName = fmt.Sprintf("%s_Auth%v", typ.Name(), v.counter)
	} else {
		varName = fmt.Sprintf("%s_%v", typ.Name(), v.counter)
	}

	// save it, if it was created for xidVal
	if xidName != "" && xidVal != "" {
		v.xidVarNameMap[key] = varName
	}

	return varName
}

// removeNodeReference removes any reference we know about (via @hasInverse) into a node.
// TODO: Remove this later once we are handling reverse nodes.
func removeNodeReference(m *schema.Field, authRw *authRewriter,
	qry *gql.GraphQuery) []interface{} {
	var deletes []interface{}
	for _, fld := range m.MutatedType().Fields() {
		invField := fld.Inverse()
		if invField == nil {
			// This field be a reverse edge, in that case we need to delete the incoming connections
			// to this node via its forward edges.
			invField = fld.ForwardEdge()
			if invField == nil {
				continue
			}
		}
		varName := authRw.varGen.Next(fld.Type(), "", "", false)

		qry.Children = append(qry.Children,
			&gql.GraphQuery{
				Var:  varName,
				Attr: invField.Type().DgraphPredicate(fld.Name()),
			})

		delFldName := fld.Type().DgraphPredicate(invField.Name())
		del := map[string]interface{}{"uid": "uid(x)"}
		if invField.Type().ListType() == nil {
			deletes = append(deletes, map[string]interface{}{
				"uid":      fmt.Sprintf("uid(%s)", varName),
				delFldName: del})
		} else {
			deletes = append(deletes, map[string]interface{}{
				"uid":      fmt.Sprintf("uid(%s)", varName),
				delFldName: []interface{}{del}})
		}
	}
	return deletes
}

// TODO: Keep this until we handle the Geo data structures.
// rewriteGeoObject rewrites the given value correctly based on the underlying Geo type.
// Currently, it supports Point, Polygon and MultiPolygon.
func rewriteGeoObject(val map[string]interface{}, typ *schema.Type) []interface{} {
	switch typ.Name() {
	case schema.Point:
		return rewritePoint(val)
	case schema.Polygon:
		return rewritePolygon(val)
	case schema.MultiPolygon:
		return rewriteMultiPolygon(val)
	}
	return nil
}

// rewritePoint constructs coordinates for Point type.
// For Point type, the mutation json is as follows:
// { "type": "Point", "coordinates": [11.11, 22.22] }
func rewritePoint(point map[string]interface{}) []interface{} {
	return []interface{}{point[schema.Longitude], point[schema.Latitude]}
}

// rewritePolygon constructs coordinates for Polygon type.
// For Polygon type, the mutation json is as follows:
//	{
//		"type": "Polygon",
//		"coordinates": [[[22.22,11.11],[16.16,15.15],[21.21,20.2]],[[22.28,11.18],[16.18,15.18],[21.28,20.28]]]
//	}
func rewritePolygon(val map[string]interface{}) []interface{} {
	// type casting this is safe, because of strict GraphQL schema
	coordinates := val[schema.Coordinates].([]interface{})
	resPoly := make([]interface{}, 0, len(coordinates))
	for _, pointList := range coordinates {
		// type casting this is safe, because of strict GraphQL schema
		points := pointList.(map[string]interface{})[schema.Points].([]interface{})
		resPointList := make([]interface{}, 0, len(points))
		for _, point := range points {
			resPointList = append(resPointList, rewritePoint(point.(map[string]interface{})))
		}
		resPoly = append(resPoly, resPointList)
	}
	return resPoly
}

// rewriteMultiPolygon constructs coordinates for MultiPolygon type.
// For MultiPolygon type, the mutation json is as follows:
//	{
//		"type": "MultiPolygon",
//		"coordinates": [[[[22.22,11.11],[16.16,15.15],[21.21,20.2]],[[22.28,11.18],[16.18,15.18],[21.28,20.28]]],[[[92.22,91.11],[16.16,15.15],[21.21,20.2]],[[22.28,11.18],[16.18,15.18],[21.28,20.28]]]]
//	}
func rewriteMultiPolygon(val map[string]interface{}) []interface{} {
	// type casting this is safe, because of strict GraphQL schema
	polygons := val[schema.Polygons].([]interface{})
	res := make([]interface{}, 0, len(polygons))
	for _, polygon := range polygons {
		res = append(res, rewritePolygon(polygon.(map[string]interface{})))
	}
	return res
}
