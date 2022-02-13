/*
 * Copyright 2018 Dgraph Labs, Inc. and Contributors
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

package tok

import (
	"github.com/blevesearch/bleve/analysis"
	_ "github.com/blevesearch/bleve/analysis/lang/en"
	"github.com/golang/glog"
)

// filterStopwords filters stop words using an existing filter, imported here.
// If the lang filter is found, the we will forward requests to it.
// Returns filtered tokens if filter is found, otherwise returns tokens unmodified.
func filterStopwords(input analysis.TokenStream) analysis.TokenStream {
	if len(input) == 0 {
		return input
	}
	// get filter from concurrent cache so we dont recreate.
	filter, err := bleveCache.TokenFilterNamed("stop_en")
	if err != nil {
		glog.Errorf("Error while filtering stop words: %s", err)
		return input
	}
	if filter != nil {
		return filter.Filter(input)
	}
	return input
}
