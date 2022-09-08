// Portions Copyright 2018 Dgraph Labs, Inc. are available under the Apache License v2.0.
// Portions Copyright 2022 Outcaste LLC are available under the Sustainable License v1.0.

package chunker

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync/atomic"
	"unicode"

	simdjson "github.com/dgraph-io/simdjson-go"
	"github.com/outcaste-io/outserv/badger/y"
	"github.com/outcaste-io/outserv/graphql/schema"
	gqlSchema "github.com/outcaste-io/outserv/graphql/schema"
	"github.com/outcaste-io/outserv/protos/pb"
	"github.com/outcaste-io/outserv/types"
	"github.com/outcaste-io/outserv/x"
	"github.com/pkg/errors"
	geom "github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/geojson"
)

func stripSpaces(str string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsSpace(r) {
			return -1
		}

		return r
	}, str)
}

// This is the response for a map[string]interface{} i.e. a struct.
type mapResponse struct {
	uid       string // uid retrieved or allocated for the node.
	namespace uint64 // namespace to which the node belongs.
}

var starVal types.Sval

func init() {
	var err error
	starVal, err = types.ToBinary(types.TypeString, x.Star)
	x.Check(err)
}

func handleBasicType(k string, v interface{}, op int, nq *pb.Edge) error {
	switch v := v.(type) {
	case json.Number:
		if strings.ContainsAny(v.String(), ".Ee") {
			f, err := v.Float64()
			if err != nil {
				return err
			}
			nq.ObjectValue, err = types.ToBinary(types.TypeFloat, f)
			if err != nil {
				return err
			}
			return nil
		}
		i, err := v.Int64()
		if err != nil {
			return err
		}
		nq.ObjectValue, err = types.ToBinary(types.TypeInt64, i)
		if err != nil {
			return err
		}

	// this int64 case is needed for FastParseJSON, which doesn't use json.Number
	case int64:
		// TODO(mrjn): Do we need to do the same thing here as others, i.e. use
		// starVal in case of deletion?
		var err error
		nq.ObjectValue, err = types.ToBinary(types.TypeInt64, v)
		return err

	case string:
		var err error
		// Default value is considered as S P * deletion.
		if v == "" && op == DeleteNquads {
			nq.ObjectValue = y.Copy(starVal)
			return nil
		}

		// Handle the uid function in upsert block
		s := stripSpaces(v)
		if strings.HasPrefix(s, "uid(") || strings.HasPrefix(s, "val(") {
			if !strings.HasSuffix(s, ")") {
				return errors.Errorf("While processing '%s', brackets are not closed properly", s)
			}
			nq.ObjectId = s
			return nil
		}

		// In RDF, we assume everything is default (types.DefaultID), but in JSON we assume string
		// (StringID). But this value will be checked against the schema so we don't overshadow a
		// password value (types.PasswordID) - Issue#2623
		nq.ObjectValue, err = types.ToBinary(types.TypeString, v)
		if err != nil {
			return err
		}

	case float64:
		var err error
		if v == 0 && op == DeleteNquads {
			nq.ObjectValue = y.Copy(starVal)
			return nil
		}
		nq.ObjectValue, err = types.ToBinary(types.TypeFloat, v)
		return err

	case bool:
		var err error
		if !v && op == DeleteNquads {
			nq.ObjectValue = y.Copy(starVal)
			return nil
		}
		nq.ObjectValue, err = types.ToBinary(types.TypeBool, v)
		return err

	default:
		return errors.Errorf("Unexpected type for val for attr: %s while converting to Edge", k)
	}
	return nil

}

func (buf *NQuadBuffer) checkForDeletion(mr mapResponse, m map[string]interface{}, op int) {
	// Since uid is the only key, this must be S * * deletion.
	if op == DeleteNquads && len(mr.uid) > 0 && len(m) == 1 {
		val, err := types.ToBinary(types.TypeString, x.Star)
		x.Check(err)
		buf.Push(&pb.Edge{
			Subject:     mr.uid,
			Predicate:   x.Star,
			Namespace:   mr.namespace,
			ObjectValue: val,
		})
	}
}

func handleGeoType(val map[string]interface{}, nq *pb.Edge) (bool, error) {
	_, hasType := val["type"]
	_, hasCoordinates := val["coordinates"]
	if len(val) == 2 && hasType && hasCoordinates {
		b, err := json.Marshal(val)
		if err != nil {
			return false, errors.Errorf("Error while trying to parse value: %+v as geo val", val)
		}
		ok, err := tryParseAsGeo(b, nq)
		if err != nil && ok {
			return true, err
		}
		if ok {
			return true, nil
		}
	}
	return false, nil
}

func tryParseAsGeo(b []byte, nq *pb.Edge) (bool, error) {
	var g geom.T
	err := geojson.Unmarshal(b, &g)
	if err != nil {
		return false, nil
	}

	geo, err := types.ToBinary(types.TypeGeo, g)
	if err != nil {
		return false, errors.Errorf("Couldn't convert value: %s to geo type", string(b))
	}

	nq.ObjectValue = geo
	return true, nil
}

// NQuadBuffer batches up batchSize NQuads per push to channel, accessible via Ch(). If batchSize is
// negative, it only does one push to Ch() during Flush.
type NQuadBuffer struct {
	batchSize int
	nquads    []*pb.Edge
	nqCh      chan []*pb.Edge
}

// NewNQuadBuffer returns a new NQuadBuffer instance with the specified batch size.
func NewNQuadBuffer(batchSize int) *NQuadBuffer {
	buf := &NQuadBuffer{
		batchSize: batchSize,
		nqCh:      make(chan []*pb.Edge, 10),
	}
	if buf.batchSize > 0 {
		buf.nquads = make([]*pb.Edge, 0, batchSize)
	}
	return buf
}

// Ch returns a channel containing slices of NQuads which can be consumed by the caller.
func (buf *NQuadBuffer) Ch() <-chan []*pb.Edge {
	return buf.nqCh
}

// Push can be passed one or more NQuad pointers, which get pushed to the buffer.
func (buf *NQuadBuffer) Push(nqs ...*pb.Edge) {
	for _, nq := range nqs {
		buf.nquads = append(buf.nquads, nq)
		if buf.batchSize > 0 && len(buf.nquads) >= buf.batchSize {
			buf.nqCh <- buf.nquads
			buf.nquads = make([]*pb.Edge, 0, buf.batchSize)
		}
	}
}

// Flush must be called at the end to push out all the buffered NQuads to the channel. Once Flush is
// called, this instance of NQuadBuffer should no longer be used.
func (buf *NQuadBuffer) Flush() {
	if len(buf.nquads) > 0 {
		buf.nqCh <- buf.nquads
		buf.nquads = nil
	}
	close(buf.nqCh)
}

// nextIdx is the index that is used to generate blank node ids for a json map object
// when the map object does not have a "uid" field.
// It should only be accessed through the atomic APIs.
var nextIdx uint64

// randomID will be used to generate blank node ids.
// We use a random number to avoid collision with user specified uids.
var randomID uint32

func init() {
	randomID = rand.Uint32()
}

func getNextBlank() string {
	id := atomic.AddUint64(&nextIdx, 1)
	return fmt.Sprintf("_:dg.%d.%d", randomID, id)
}

func (buf *NQuadBuffer) mapToNquads(m map[string]interface{}, typ *gqlSchema.Type, op int) (
	mapResponse, error) {
	x.AssertTrue(typ != nil)
	var mr mapResponse

	// Find the type of the object, and the corresponding ID field. And use that
	// for determining the UID.
	//
	// Check field in map.
	if uidVal, ok := m["uid"]; ok {
		var uid uint64

		switch uidVal := uidVal.(type) {
		case json.Number:
			ui, err := uidVal.Int64()
			if err != nil {
				return mr, err
			}
			uid = uint64(ui)

		// this int64 case is needed for FastParseJSON, which doesn't use json.Number
		case int64:
			uid = uint64(uidVal)

		case string:
			s := stripSpaces(uidVal)
			if uidVal == "" {
				uid = 0
			} else if ok := strings.HasPrefix(uidVal, "_:"); ok {
				mr.uid = uidVal
			} else if ok := strings.HasPrefix(s, "uid("); ok {
				mr.uid = s
			} else if u, err := strconv.ParseUint(uidVal, 0, 64); err == nil {
				uid = u
			} else {
				return mr, err
			}
		}
		if uid > 0 {
			mr.uid = x.ToHexString(uid)
		}
	}
	if mr.uid == "" {
		// Try to fill using the ID field.
		// fmt.Printf("no uid found for type: %s\n", typ.Name())
		// for _, fd := range typ.Fields() {
		// 	fmt.Printf("type: %s Field: %s\n", typ.Name(), fd.DgraphAlias())
		// }
		var comp []string
		for _, fd := range typ.XIDFields() {
			val, ok := m[fd.Name()]
			if ok {
				comp = append(comp, val.(string))
			}
		}
		mr.uid = fmt.Sprintf("_:%s.%s", typ.Name(), strings.Join(comp, "|"))
		// fmt.Printf("Assigned UID to %s -> %s\n", typ.Name(), mr.uid)
	}

	if mr.uid == "" {
		if op == DeleteNquads {
			// Delete operations with a non-nil value must have a uid specified.
			return mr, errors.Errorf("UID must be present and non-zero while deleting edges.")
		}
		mr.uid = getNextBlank()
	}

	namespace := x.GalaxyNamespace
	if ns, ok := m["namespace"]; ok {
		switch nsVal := ns.(type) {
		case json.Number:
			nsi, err := nsVal.Int64()
			if err != nil {
				return mr, err
			}
			namespace = uint64(nsi)

		// this int64 case is needed for FastParseJSON, which doesn't use json.Number
		case int64:
			namespace = uint64(nsVal)
		case string:
			s := stripSpaces(nsVal)
			if s == "" {
				namespace = 0
			} else if n, err := strconv.ParseUint(s, 0, 64); err == nil {
				namespace = n
			} else {
				return mr, err
			}
		}
	}
	mr.namespace = namespace

	for pred, v := range m {
		// We have already extracted the uid above so we skip that edge.
		// v can be nil if user didn't set a value and if omitEmpty was not supplied as JSON
		// option.
		// We also skip facets here because we parse them with the corresponding predicate.
		if pred == "uid" || pred == "namespace" {
			continue
		}

		if v == nil {
			if op == DeleteNquads {
				// This corresponds to edge deletion.
				val, err := types.ToBinary(types.TypeString, x.Star)
				x.Check(err)
				nq := &pb.Edge{
					Subject:     mr.uid,
					Predicate:   pred,
					Namespace:   namespace,
					ObjectValue: val,
				}
				buf.Push(nq)
				continue
			}

			// If op is SetNquads, ignore this triplet and continue.
			continue
		}

		// TODO: We should perhaps create the type.predicate here.
		nq := pb.Edge{
			Subject:   mr.uid,
			Predicate: pred,
			Namespace: namespace,
		}

		fd := typ.Field(pred)

		switch v := v.(type) {
		// these int64/float64 cases are needed for FastParseJSON, which doesn't use json.Number
		case int64, float64:
			if err := handleBasicType(pred, v, op, &nq); err != nil {
				return mr, err
			}
			buf.Push(&nq)
		case string, json.Number, bool:
			if err := handleBasicType(pred, v, op, &nq); err != nil {
				return mr, err
			}
			buf.Push(&nq)
		case map[string]interface{}:
			if len(v) == 0 {
				continue
			}

			ok, err := handleGeoType(v, &nq)
			if err != nil {
				return mr, err
			}
			if ok {
				buf.Push(&nq)
				continue
			}
			cr, err := buf.mapToNquads(v, fd.Type(), op)
			if err != nil {
				return mr, err
			}

			// Add the connecting edge beteween the entities.
			nq.ObjectId = cr.uid
			buf.Push(&nq)
		case []interface{}:
			var vals []types.Sval
			for _, item := range v {
				nq := pb.Edge{
					Subject:   mr.uid,
					Predicate: pred,
					Namespace: namespace,
				}
				childType := typ.Field(pred).Type()

				switch iv := item.(type) {
				case string, float64, json.Number, int64:
					if err := handleBasicType(pred, iv, op, &nq); err != nil {
						return mr, err
					}
					if bytes.Equal(nq.ObjectValue, starVal) {
						buf.Push(&nq)
						// We found a deletion. Only process that.
						vals = vals[:0]
						break
					}
					// Don't push anything yet. We'd push it all as one value at
					// the end.
					vals = append(vals, nq.ObjectValue)
				case map[string]interface{}:
					// map[string]interface{} can mean geojson or a connecting entity.
					ok, err := handleGeoType(item.(map[string]interface{}), &nq)
					if err != nil {
						return mr, err
					}
					if ok {
						buf.Push(&nq)
						continue
					}

					cr, err := buf.mapToNquads(iv, childType, op)
					if err != nil {
						return mr, err
					}
					nq.ObjectId = cr.uid
					buf.Push(&nq)
				default:
					return mr,
						errors.Errorf("Got unsupported type for list: %s", pred)
				}
			}
			if len(vals) > 0 {
				nq.ObjectValue = types.ToList(vals)
				buf.Push(&nq)
			}
		default:
			return mr, errors.Errorf("Unexpected type for val for attr: %s while converting to nquad", pred)
		}
	}

	return mr, nil
}

const (
	// SetNquads is the constant used to indicate that the parsed NQuads are meant to be added.
	SetNquads = iota
	// DeleteNquads is the constant used to indicate that the parsed NQuads are meant to be
	// deleted.
	DeleteNquads
)

// FastParseJSON currently parses NQuads about 30% faster than ParseJSON.
//
// This function is very similar to buf.ParseJSON, but we just replace encoding/json with
// simdjson-go.
func (buf *NQuadBuffer) FastParseJSON(b []byte, gqlType *schema.Type, op int) error {
	if !simdjson.SupportedCPU() {
		// default to slower / old parser
		return buf.ParseJSON(b, gqlType, op)
	}
	// parse the json into tape format
	tape, err := simdjson.Parse(b, nil)
	if err != nil {
		return err
	}

	// we only need the iter to get the first element, either an array or object
	iter := tape.Iter()

	tmp := &simdjson.Iter{}

	// if root object, this will be filled
	obj := &simdjson.Object{}
	// if root array, this will be filled
	arr := &simdjson.Array{}

	// grab the first element
	typ := iter.Advance()
	switch typ {
	case simdjson.TypeRoot:
		if typ, tmp, err = iter.Root(tmp); err != nil {
			return err
		}
		if typ == simdjson.TypeObject {
			// the root element is an object, so parse the object
			if obj, err = tmp.Object(obj); err != nil {
				return err
			}
			// attempt to convert to map[string]interface{}
			m, err := obj.Map(nil)
			if err != nil {
				return err
			}
			// pass to next parsing stage
			mr, err := buf.mapToNquads(m, gqlType, op)
			if err != nil {
				return err
			}
			buf.checkForDeletion(mr, m, op)
		} else if typ == simdjson.TypeArray {
			// the root element is an array, so parse the array
			if arr, err = tmp.Array(arr); err != nil {
				return err
			}
			// attempt to convert to []interface{}
			a, err := arr.Interface()
			if err != nil {
				return err
			}
			if len(a) > 0 {
				// attempt to convert each array element to a
				// map[string]interface{} for further parsing
				var o interface{}
				for _, o = range a {
					if _, ok := o.(map[string]interface{}); !ok {
						return errors.Errorf("only array of map allowed at root")
					}
					// pass to next parsing stage
					mr, err := buf.mapToNquads(o.(map[string]interface{}), gqlType, op)
					if err != nil {
						return err
					}
					buf.checkForDeletion(mr, o.(map[string]interface{}), op)
				}
			}
		}
	default:
		return errors.Errorf("initial element not found in json")
	}
	return nil
}

// ParseJSON parses the given byte slice and pushes the parsed NQuads into the buffer.
func (buf *NQuadBuffer) ParseJSON(b []byte, typ *gqlSchema.Type, op int) error {
	buffer := bytes.NewBuffer(b)
	dec := json.NewDecoder(buffer)
	dec.UseNumber()
	ms := make(map[string]interface{})
	var list []interface{}
	if err := dec.Decode(&ms); err != nil {
		// Couldn't parse as map, lets try to parse it as a list.
		buffer.Reset() // The previous contents are used. Reset here.
		// Rewrite b into buffer, so it can be consumed.
		if _, err := buffer.Write(b); err != nil {
			return err
		}
		if err = dec.Decode(&list); err != nil {
			return err
		}
	}
	if len(list) == 0 && len(ms) == 0 {
		return nil
	}
	if len(list) > 0 {
		for _, obj := range list {
			if _, ok := obj.(map[string]interface{}); !ok {
				return errors.Errorf("Only array of map allowed at root.")
			}
			mr, err := buf.mapToNquads(obj.(map[string]interface{}), nil, op)
			if err != nil {
				return err
			}
			buf.checkForDeletion(mr, obj.(map[string]interface{}), op)
		}
		return nil
	}
	mr, err := buf.mapToNquads(ms, typ, op)
	if err != nil {
		return err
	}
	buf.checkForDeletion(mr, ms, op)
	return nil
}

// ParseJSON is a convenience wrapper function to get all NQuads in one call. This can however, lead
// to high memory usage. So be careful using this.
func ParseJSON(b []byte, typ *gqlSchema.Type, op int) ([]*pb.Edge, error) {
	buf := NewNQuadBuffer(-1)
	err := buf.FastParseJSON(b, typ, op)
	if err != nil {
		return nil, err
	}
	buf.Flush()
	nqs := <-buf.Ch()
	return nqs, nil
}
