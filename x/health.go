// Portions Copyright 2017-2018 Dgraph Labs, Inc. are available under the Apache License v2.0.
// Portions Copyright 2022 Outcaste LLC are available under the Smart License v1.0.

package x

import (
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
)

var (
	// the drainingMode variable should be accessed through the atomic.Store and atomic.Load
	// functions. The value 0 means the draining-mode is disabled, and the value 1 means the
	// mode is enabled
	drainingMode uint32

	healthCheck     uint32
	errHealth       = errors.New("Please retry again, server is not ready to accept requests")
	errDrainingMode = errors.New("the server is in draining mode " +
		"and client requests will only be allowed after exiting the mode " +
		" by sending a GraphQL draining(enable: false) mutation to /admin")
)

// UpdateHealthStatus updates the server's health status so it can start accepting requests.
func UpdateHealthStatus(ok bool) {
	setStatus(&healthCheck, ok)
}

// UpdateDrainingMode updates the server's draining mode
func UpdateDrainingMode(enable bool) {
	setStatus(&drainingMode, enable)
}

// HealthCheck returns whether the server is ready to accept requests or not
// Load balancer would add the node to the endpoint once health check starts
// returning true
func HealthCheck() error {
	if atomic.LoadUint32(&healthCheck) == 0 {
		return errHealth
	}
	if atomic.LoadUint32(&drainingMode) == 1 {
		return errDrainingMode
	}
	return nil
}

func BlockUntilHealthy() {
	for {
		if err := HealthCheck(); err == nil {
			return
		}
		time.Sleep(time.Second)
	}
}

func setStatus(v *uint32, ok bool) {
	if ok {
		atomic.StoreUint32(v, 1)
	} else {
		atomic.StoreUint32(v, 0)
	}
}
