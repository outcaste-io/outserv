// Portions Copyright 2017-2018 Dgraph Labs, Inc. are available under the Apache License v2.0.
// Portions Copyright 2022 Outcaste LLC are available under the Sustainable License v1.0.

package worker

import (
	"path/filepath"
	"time"

	"github.com/outcaste-io/outserv/x"
)

const (
	magicVersion = 1

	// StrictMutations is the mode that allows mutations if and only if they contain known preds.
	StrictMutations int = iota
	// DisallowMutations is the mode that disallows all mutations.
	DisallowMutations
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
