// Portions Copyright 2019 Dgraph Labs, Inc. are available under the Apache License v2.0.
// Portions Copyright 2022 Outcaste LLC are available under the Sustainable License v1.0.

package resolve

import (
	"github.com/outcaste-io/outserv/graphql/schema"
)

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
