//go:build dragonfly || freebsd || windows || plan9
// +build dragonfly freebsd windows plan9

// Portions Copyright 2020 Dgraph Labs, Inc. are available under the Apache License v2.0.
// Portions Copyright 2022 Outcaste LLC are available under the Apache License v2.0.

package y

import "syscall"

func init() {
	datasyncFileFlag = syscall.O_SYNC
}
