// Portions Copyright 2020 Dgraph Labs, Inc. are available under the Apache License v2.0.
// Portions Copyright 2022 Outcaste LLC are available under the Apache License v2.0.

package badger

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

type mockLogger struct {
	output string
}

func (l *mockLogger) Errorf(f string, v ...interface{}) {
	l.output = fmt.Sprintf("ERROR: "+f, v...)
}

func (l *mockLogger) Infof(f string, v ...interface{}) {
	l.output = fmt.Sprintf("INFO: "+f, v...)
}

func (l *mockLogger) Warningf(f string, v ...interface{}) {
	l.output = fmt.Sprintf("WARNING: "+f, v...)
}

func (l *mockLogger) Debugf(f string, v ...interface{}) {
	l.output = fmt.Sprintf("DEBUG: "+f, v...)
}

// Test that the DB-specific log is used instead of the global log.
func TestDbLog(t *testing.T) {
	l := &mockLogger{}
	opt := Options{Logger: l}

	opt.Errorf("test")
	require.Equal(t, "ERROR: test", l.output)
	opt.Infof("test")
	require.Equal(t, "INFO: test", l.output)
	opt.Warningf("test")
	require.Equal(t, "WARNING: test", l.output)
}

// Test that the global logger is used when no logger is specified in Options.
func TestNoDbLog(t *testing.T) {
	l := &mockLogger{}
	opt := Options{}
	opt.Logger = l

	opt.Errorf("test")
	require.Equal(t, "ERROR: test", l.output)
	opt.Infof("test")
	require.Equal(t, "INFO: test", l.output)
	opt.Warningf("test")
	require.Equal(t, "WARNING: test", l.output)
}
