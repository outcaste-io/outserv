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

import (
	"path/filepath"
	"time"

	"github.com/outcaste-io/outserv/x"
)

const (
	magicVersion = 1

	// AllowMutations is the mode allowing all mutations.
	AllowMutations int = iota
	// DisallowMutations is the mode that disallows all mutations.
	DisallowMutations
	// StrictMutations is the mode that allows mutations if and only if they contain known preds.
	StrictMutations
)

// Options contains options for the Dgraph server.
type Options struct {
	// MutationsMode is the mode used to handle mutation requests.
	MutationsMode int
	// AuthToken is the token to be passed for Alter HTTP requests.
	AuthToken string

	// HmacSecret stores the secret used to sign JSON Web Tokens (JWT).
	HmacSecret x.Sensitive
	// AccessJwtTtl is the TTL for the access JWT.
	AccessJwtTtl time.Duration
	// RefreshJwtTtl is the TTL of the refresh JWT.
	RefreshJwtTtl time.Duration

	// CachePercentage is the comma-separated list of cache percentages
	// used to split the total cache size among the multiple caches.
	CachePercentage string
	// CacheMb is the total memory allocated between all the caches.
	CacheMb int64

	Audit *x.LoggerConf

	// Define different ChangeDataCapture configurations
	ChangeDataConf string
}

// Config holds an instance of the server options..
var Config Options

// SetConfiguration sets the server configuration to the given config.
func SetConfiguration(newConfig *Options) {
	if newConfig == nil {
		return
	}
	newConfig.validate()
	Config = *newConfig
}

// AvailableMemory is the total size of the memory we were able to identify.
var AvailableMemory int64

func (opt *Options) validate() {
	if opt.Audit != nil {
		_, err := filepath.Abs(opt.Audit.Output)
		x.Check(err)
	}
}
