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

package worker

import "github.com/outcaste-io/outserv/types"

func couldApplyAggregatorOn(agrtr string, typ types.TypeID) bool {
	if !typ.IsScalar() {
		return false
	}
	switch agrtr {
	case "min", "max":
		return (typ == types.TypeInt64 ||
			typ == types.TypeFloat ||
			typ == types.TypeDatetime ||
			typ == types.TypeString ||
			typ == types.TypeDefault)
	case "sum", "avg":
		return (typ == types.TypeInt64 ||
			typ == types.TypeFloat)
	default:
		return false
	}
}
