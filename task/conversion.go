/*
 * Copyright 2017-2018 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package task

import (
	"encoding/binary"
	"math"

	"github.com/outcaste-io/outserv/protos/pb"
	"github.com/outcaste-io/outserv/types"
	"github.com/outcaste-io/outserv/x"
)

var (
	// TrueVal is the pb.TaskValue value equivalent to the "true" boolean.
	TrueVal = FromBool(true)
	// FalseVal is the pb.TaskValue value equivalent to the "false" boolean.
	FalseVal = FromBool(false)
)

// FromInt converts the given int value into a pb.TaskValue object.
func FromInt(val int) *pb.TaskValue {
	bs := make([]byte, 9)
	binary.LittleEndian.PutUint64(bs[1:], uint64(val))
	bs[0] = byte(types.TypeInt64)
	return &pb.TaskValue{Val: []byte(bs)}
}

// ToInt converts the given pb.TaskValue object into an integer.
// Note, this panics if there are not enough bytes in val.Val
func ToInt(val *pb.TaskValue) int64 {
	if len(val.Val) == 0 {
		return 0
	}
	x.AssertTrue(val.Val[0] == byte(types.TypeInt64))
	result := binary.LittleEndian.Uint64(val.Val[1:])
	return int64(result)
}

// FromBool converts the given boolean in to a pb.TaskValue object.
func FromBool(val bool) *pb.TaskValue {
	if val {
		return FromInt(1)
	}
	return FromInt(0)
}

// ToBool converts the given pb.TaskValue object into a boolean.
func ToBool(val *pb.TaskValue) bool {
	if len(val.Val) == 0 {
		return false
	}
	result := ToInt(val)
	return result != 0
}

// FromString converts the given string in to a pb.TaskValue object.
func FromString(val string) *pb.TaskValue {
	return &pb.TaskValue{
		Val: append([]byte{byte(types.TypeString)}, []byte(val)...),
	}
}

// ToString converts the given pb.TaskValue object into a string.
func ToString(val *pb.TaskValue) string {
	x.AssertTrue(val.Val[0] == byte(types.TypeString))
	return string(val.Val[1:])
}

// FromFloat converts the given float64 value into a pb.TaskValue object.
func FromFloat(val float64) *pb.TaskValue {
	bs := make([]byte, 9)
	binary.LittleEndian.PutUint64(bs[1:], math.Float64bits(val))
	bs[0] = byte(types.TypeFloat)
	return &pb.TaskValue{Val: []byte(bs)}
}

// ToFloat converts the given pb.TaskValue object into an integer.
// Note, this panics if there are not enough bytes in val.Val
func ToFloat(val *pb.TaskValue) float64 {
	x.AssertTrue(val.Val[0] == byte(types.TypeFloat))
	return math.Float64frombits(binary.LittleEndian.Uint64(val.Val[1:]))
}
