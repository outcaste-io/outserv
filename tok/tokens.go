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

package tok

import (
	"github.com/pkg/errors"
)

// GetTokens returns the tokens for the given tokenizer ID and value.
// funcArgs should only have one element which is the value that needs to be tokenized.
func GetTokens(id byte, funcArgs ...string) ([]string, error) {
	if l := len(funcArgs); l != 1 {
		return nil, errors.Errorf("Function requires 1 arguments, but got %d", l)
	}
	tokenizer, ok := GetTokenizerByID(id)
	if !ok {
		return nil, errors.Errorf("No tokenizer was found with id %v", id)
	}
	return BuildTokens(funcArgs[0], tokenizer)
}

// GetTermTokens returns the term tokens for the given value.
func GetTermTokens(funcArgs []string) ([]string, error) {
	return GetTokens(IdentTerm, funcArgs...)
}

// GetFullTextTokens returns the full-text tokens for the given value.
func GetFullTextTokens(funcArgs []string) ([]string, error) {
	if l := len(funcArgs); l != 1 {
		return nil, errors.Errorf("Function requires 1 arguments, but got %d", l)
	}
	return BuildTokens(funcArgs[0], FullTextTokenizer{})
}
