// Portions Copyright 2016-2018 Dgraph Labs, Inc. are available under the Apache License v2.0.
// Portions Copyright 2022 Outcaste LLC are available under the Sustainable License v1.0.

package types

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"time"
	"unsafe"

	"github.com/outcaste-io/outserv/x"
	"github.com/pkg/errors"
	geom "github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/geojson"
	"github.com/twpayne/go-geom/encoding/wkb"
)

// Convert takes a binary version of value, and converts it to given scalar type.
func Convert(from Sval, toID TypeID) (Val, error) {
	to := Val{Tid: toID}
	res := &to.Value

	if len(from) == 0 {
		return to, fmt.Errorf("Data of length zero")
	}
	fromID := TypeID(from[0])
	data := from[1:]

	// Convert from-type to to-type and store in the result interface.
	switch fromID {
	case TypeBinary:
		{
			// Unmarshal from Binary to type interfaces.
			switch toID {
			case TypeBinary:
				*res = data
			case TypeString, TypeDefault:
				// We never modify from Val, so this should be safe.
				*res = *(*string)(unsafe.Pointer(&data))
			case TypeInt64:
				if len(data) < 8 {
					return to, errors.Errorf("Invalid data for int64 %v", data)
				}
				*res = int64(binary.LittleEndian.Uint64(data))
			case TypeFloat:
				if len(data) < 8 {
					return to, errors.Errorf("Invalid data for float %v", data)
				}
				i := binary.LittleEndian.Uint64(data)
				*res = math.Float64frombits(i)
			case TypeBool:
				if len(data) == 0 || data[0] == 0 {
					*res = false
					return to, nil
				} else if data[0] == 1 {
					*res = true
					return to, nil
				}
				return to, errors.Errorf("Invalid value for bool %v", data[0])
			case TypeDatetime:
				var t time.Time
				if err := t.UnmarshalBinary(data); err != nil {
					return to, err
				}
				*res = t
			case TypeGeo:
				w, err := wkb.Unmarshal(data)
				if err != nil {
					return to, err
				}
				*res = w
			case TypePassword:
				*res = string(data)
			case TypeBigInt:
				b := &big.Int{}
				err := b.UnmarshalText(data)
				if err != nil {
					return to, errors.Errorf("Marshalling failed for bigint %v", data)
				}
				*res = *b
			default:
				return to, cantConvert(fromID, toID)
			}
		}
	case TypeString, TypeDefault:
		{
			vc := string(data)
			switch toID {
			case TypeBinary:
				*res = []byte(vc)
			case TypeInt64:
				val, err := strconv.ParseInt(vc, 10, 64)
				if err != nil {
					return to, err
				}
				*res = val
			case TypeBigInt:
				val := &big.Int{}
				val, ok := val.SetString(vc, 10)
				if !ok {
					return to, errors.New("Non-numeric string")
				}
				*res = *val
			case TypeFloat:
				val, err := strconv.ParseFloat(vc, 64)
				if err != nil {
					return to, err
				}
				if math.IsNaN(val) {
					return to, errors.Errorf("Got invalid value: NaN")
				}
				*res = val
			case TypeString, TypeDefault:
				*res = vc
			case TypeBool:
				val, err := strconv.ParseBool(vc)
				if err != nil {
					return to, err
				}
				*res = val
			case TypeDatetime:
				t, err := ParseTime(vc)
				if err != nil {
					return to, err
				}
				*res = t
			case TypeGeo:
				var g geom.T
				text := bytes.Replace([]byte(vc), []byte("'"), []byte("\""), -1)
				if err := geojson.Unmarshal(text, &g); err != nil {
					return to,
						errors.Wrapf(err, "Error while unmarshalling: [%s] as geojson", vc)
				}
				*res = g
			case TypePassword:
				p, err := Encrypt(vc)
				if err != nil {
					return to, err
				}
				*res = p
			default:
				return to, cantConvert(fromID, toID)
			}
		}
	case TypeInt64:
		{
			if len(data) < 8 {
				return to, errors.Errorf("Invalid data for int64 %v", data)
			}
			vc := int64(binary.LittleEndian.Uint64(data))
			switch toID {
			case TypeInt64:
				*res = vc
			case TypeBinary:
				var bs [8]byte
				binary.LittleEndian.PutUint64(bs[:], uint64(vc))
				*res = bs[:]
			case TypeBigInt:
				i := &big.Int{}
				i.SetInt64(vc)
				*res = *i
			case TypeFloat:
				*res = float64(vc)
			case TypeBool:
				*res = vc != 0
			case TypeString, TypeDefault:
				*res = strconv.FormatInt(vc, 10)
			case TypeDatetime:
				*res = time.Unix(vc, 0).UTC()
			default:
				return to, cantConvert(fromID, toID)
			}
		}
	case TypeBigInt:
		{
			vc := &big.Int{}
			vc.UnmarshalText(data)
			switch toID {
			case TypeBigInt, TypeBinary:
				*res = *vc
			case TypeInt64:
				// We are ignoring here, whether the value will fit into a int64
				*res = vc.Int64()
			case TypeFloat:
				f := &big.Float{}
				f.SetInt(vc)
				// We are ignoring here, whether the value will fit into a float64
				*res, _ = f.Float64()
			case TypeBool:
				*res = vc.Int64() != 0
			case TypeString, TypeDefault:
				*res = vc.String()
			case TypeDatetime:
				*res = time.Unix(vc.Int64(), 0).UTC()
			default:
				return to, cantConvert(fromID, toID)
			}
		}
	case TypeFloat:
		{
			if len(data) < 8 {
				return to, errors.Errorf("Invalid data for float %v", data)
			}
			i := binary.LittleEndian.Uint64(data)
			vc := math.Float64frombits(i)
			switch toID {
			case TypeFloat:
				*res = vc
			case TypeBinary:
				var bs [8]byte
				u := math.Float64bits(vc)
				binary.LittleEndian.PutUint64(bs[:], u)
				*res = bs[:]
			case TypeInt64:
				if vc > math.MaxInt64 || vc < math.MinInt64 || math.IsNaN(vc) {
					return to, errors.Errorf("Float out of int64 range")
				}
				*res = int64(vc)
			case TypeBigInt:
				f := &big.Float{}
				f = f.SetFloat64(vc)
				i, _ := f.Int(nil)
				*res = i
			case TypeBool:
				*res = vc != 0
			case TypeString, TypeDefault:
				*res = strconv.FormatFloat(vc, 'G', -1, 64)
			case TypeDatetime:
				secs := int64(vc)
				fracSecs := vc - float64(secs)
				nsecs := int64(fracSecs * nanoSecondsInSec)
				*res = time.Unix(secs, nsecs).UTC()
			default:
				return to, cantConvert(fromID, toID)
			}
		}
	case TypeBool:
		{
			var vc bool
			if len(data) == 0 || data[0] > 1 {
				return to, errors.Errorf("Invalid value for bool %v", data)
			}
			vc = data[0] == 1

			switch toID {
			case TypeBool:
				*res = vc
			case TypeBinary:
				*res = []byte{0}
				if vc {
					*res = []byte{1}
				}
			case TypeInt64:
				*res = int64(0)
				if vc {
					*res = int64(1)
				}
			case TypeFloat:
				*res = float64(0)
				if vc {
					*res = float64(1)
				}
			case TypeBigInt:
				*res = big.NewInt(0)
				if vc {
					*res = big.NewInt(1)
				}
			case TypeString, TypeDefault:
				*res = strconv.FormatBool(vc)
			default:
				return to, cantConvert(fromID, toID)
			}
		}
	case TypeDatetime:
		{
			var t time.Time
			if err := t.UnmarshalBinary(data); err != nil {
				return to, err
			}
			switch toID {
			case TypeDatetime:
				*res = t
			case TypeBinary:
				r, err := t.MarshalBinary()
				if err != nil {
					return to, err
				}
				*res = r
			case TypeString, TypeDefault:
				val, err := t.MarshalText()
				if err != nil {
					return to, err
				}
				*res = string(val)
			case TypeInt64:
				*res = t.Unix()
			case TypeFloat:
				*res = float64(t.UnixNano()) / float64(nanoSecondsInSec)
			case TypeBigInt:
				*res = big.NewInt(t.Unix())
			default:
				return to, cantConvert(fromID, toID)
			}
		}
	case TypeGeo:
		{
			vc, err := wkb.Unmarshal(data)
			if err != nil {
				return to, err
			}
			switch toID {
			case TypeGeo:
				*res = vc
			case TypeBinary:
				r, err := wkb.Marshal(vc, binary.LittleEndian)
				if err != nil {
					return to, err
				}
				*res = r
			case TypeString, TypeDefault:
				val, err := geojson.Marshal(vc)
				if err != nil {
					return to, nil
				}
				*res = string(bytes.Replace(val, []byte("\""), []byte("'"), -1))
			default:
				return to, cantConvert(fromID, toID)
			}
		}
	case TypePassword:
		{
			vc := string(data)
			switch toID {
			case TypeBinary:
				*res = []byte(vc)
			case TypeString, TypePassword:
				*res = vc
			default:
				return to, cantConvert(fromID, toID)
			}
		}
	default:
		return to, cantConvert(fromID, toID)
	}
	return to, nil
}

// Marshal assumes that value is in its native form, and marshals it into one of
// string or binary.
func Marshal(from Val, toID TypeID) (Val, error) {
	if toID != TypeString && toID != TypeBinary {
		return Val{}, errors.Errorf("Invalid conversion to %s", toID)
	}

	fromID := from.Tid
	val := from.Value

	to := Val{Tid: toID}
	res := &to.Value

	// This is a default value from sg.fillVars, don't convert it's empty.
	// Fixes issue #2980.
	if val == nil {
		to = ValueForType(toID)
		return to, nil
	}

	switch fromID {
	case TypeBinary:
		vc := val.([]byte)
		switch toID {
		case TypeString:
			*res = string(vc)
		case TypeBinary, TypeBigInt:
			*res = vc
		default:
			return to, cantConvert(fromID, toID)
		}
	case TypeString, TypeDefault:
		vc := val.(string)
		switch toID {
		case TypeString:
			*res = vc
		case TypeBinary:
			*res = []byte(vc)
		default:
			return to, cantConvert(fromID, toID)
		}
	case TypeInt64:
		vc := val.(int64)
		switch toID {
		case TypeString:
			*res = strconv.FormatInt(vc, 10)
		case TypeBinary:
			var bs [8]byte
			binary.LittleEndian.PutUint64(bs[:], uint64(vc))
			*res = bs[:]
		default:
			return to, cantConvert(fromID, toID)
		}
	case TypeBigInt:
		vc := val.(big.Int)
		switch toID {
		case TypeBinary:
			i, err := vc.MarshalText()
			if err != nil {
				return to, cantConvert(fromID, toID)
			}
			*res = i
		case TypeString:
			*res = vc.String()
		}
	case TypeFloat:
		vc := val.(float64)
		switch toID {
		case TypeString:
			*res = strconv.FormatFloat(vc, 'G', -1, 64)
		case TypeBinary:
			var bs [8]byte
			u := math.Float64bits(vc)
			binary.LittleEndian.PutUint64(bs[:], u)
			*res = bs[:]
		default:
			return to, cantConvert(fromID, toID)
		}
	case TypeBool:
		vc := val.(bool)
		switch toID {
		case TypeString:
			*res = strconv.FormatBool(vc)
		case TypeBinary:
			*res = []byte{0}
			if vc {
				*res = []byte{1}
			}
		default:
			return to, cantConvert(fromID, toID)
		}
	case TypeDatetime:
		vc := val.(time.Time)
		switch toID {
		case TypeString:
			val, err := vc.MarshalText()
			if err != nil {
				return to, err
			}
			*res = string(val)
		case TypeBinary:
			r, err := vc.MarshalBinary()
			if err != nil {
				return to, err
			}
			*res = r
		default:
			return to, cantConvert(fromID, toID)
		}
	case TypeGeo:
		vc, ok := val.(geom.T)
		if !ok {
			return to, errors.Errorf("Expected a Geo type")
		}
		switch toID {
		case TypeString:
			val, err := geojson.Marshal(vc)
			if err != nil {
				return to, err
			}
			*res = string(bytes.Replace(val, []byte("\""), []byte("'"), -1))
		case TypeBinary:
			r, err := wkb.Marshal(vc, binary.LittleEndian)
			if err != nil {
				return to, err
			}
			*res = r
		default:
			return to, cantConvert(fromID, toID)
		}
	case TypePassword:
		vc := val.(string)
		switch toID {
		case TypeString:
			*res = vc
		case TypeBinary:
			*res = []byte(vc)
		default:
			return to, cantConvert(fromID, toID)
		}
	default:
		return to, cantConvert(fromID, toID)
	}
	return to, nil
}

func ToBinary(id TypeID, b interface{}) ([]byte, error) {
	to, err := Marshal(Val{id, b}, TypeBinary)
	if err != nil {
		return nil, err
	}
	out := []byte{byte(id)}
	out = append(out, to.Value.([]byte)...)
	return out, nil
}

func FromBinary(data []byte) (Val, error) {
	if len(data) == 0 {
		return Val{}, nil
	}
	id := TypeID(data[0])
	return Convert(Sval(data), id)
}

func StringToBinary(src string) []byte {
	dst, err := ToBinary(TypeString, src)
	x.Check(err)
	return dst
}

func cantConvert(from TypeID, to TypeID) error {
	return errors.Errorf("Cannot convert %s to type %s", from, to)
}

// MarshalJSON makes Val satisfy the json.Marshaler interface.
func (v Val) MarshalJSON() ([]byte, error) {
	switch v.Tid {
	case TypeInt64:
		return json.Marshal(v.Value.(int64))
	case TypeBool:
		return json.Marshal(v.Value.(bool))
	case TypeFloat:
		return json.Marshal(v.Value.(float64))
	case TypeDatetime:
		return json.Marshal(v.Value.(time.Time))
	case TypeGeo:
		return geojson.Marshal(v.Value.(geom.T))
	case TypeString, TypeDefault:
		return json.Marshal(v.Safe().(string))
	case TypePassword:
		return json.Marshal(v.Value.(string))
	case TypeBigInt:
		i := v.Value.(big.Int)
		return i.MarshalJSON()
	}
	return nil, errors.Errorf("Invalid type for MarshalJSON: %v", v.Tid)
}
