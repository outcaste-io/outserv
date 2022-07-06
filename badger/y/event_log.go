// Portions Copyright 2020 Dgraph Labs, Inc. are available under the Apache License v2.0.
// Portions Copyright 2022 Outcaste LLC are available under the Apache License v2.0.

package y

import "golang.org/x/net/trace"

var (
	NoEventLog trace.EventLog = nilEventLog{}
)

type nilEventLog struct{}

func (nel nilEventLog) Printf(format string, a ...interface{}) {}

func (nel nilEventLog) Errorf(format string, a ...interface{}) {}

func (nel nilEventLog) Finish() {}
