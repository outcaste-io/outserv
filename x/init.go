// Portions Copyright 2016-2018 Dgraph Labs, Inc. are available under the Apache License v2.0.
// Portions Copyright 2022 Outcaste LLC are available under the Sustainable License v1.0.

package x

import (
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"regexp"
	"runtime"

	"github.com/golang/glog"
	"github.com/outcaste-io/ristretto/z"
)

var (
	initFunc []func()
	isTest   bool

	// These variables are set using -ldflags
	outservVersion  string
	outservCodename string
	gitBranch       string
	lastCommitSHA   string
	lastCommitTime  string
)

// SetTestRun sets a variable to indicate that the current execution is a test.
func SetTestRun() {
	isTest = true
}

// IsTestRun indicates whether a test is being executed. Useful to handle special
// conditions during tests that differ from normal execution.
func IsTestRun() bool {
	return isTest
}

// AddInit adds a function to be run in x.Init, which should be called at the
// beginning of all mains.
func AddInit(f func()) {
	initFunc = append(initFunc, f)
}

// Init initializes flags and run all functions in initFunc.
func Init() {
	// Default value, would be overwritten by flag.
	//
	// TODO: why is this here?
	// Config.QueryEdgeLimit = 1e6

	// Next, run all the init functions that have been added.
	for _, f := range initFunc {
		f()
	}
}

// BuildDetails returns a string containing details about the Outserv binary.
func BuildDetails() string {
	buf := z.CallocNoRef(1, "X.BuildDetails")
	jem := len(buf) > 0
	z.Free(buf)

	return fmt.Sprintf(`
==> Outserv: Blockchain Search with GraphQL <==
==> Licensed under Sustainable License v1.0 <==
==> Copyright 2022 Outcaste LLC <==

Outserv version   : %v
Outserv codename  : %v
Outserv SHA-256   : %x
Commit SHA-1      : %v
Commit timestamp  : %v
Branch            : %v
Go version        : %v
jemalloc enabled  : %v

`,
		outservVersion, outservCodename, ExecutableChecksum(), lastCommitSHA, lastCommitTime, gitBranch,
		runtime.Version(), jem)
}

// PrintVersion prints version and other helpful information if --version.
func PrintVersion() {
	glog.Infof("\n%s\n", BuildDetails())
}

// Version returns a string containing the outservVersion.
func Version() string {
	return outservVersion
}

// pattern for  dev version = min. 7 hex digits of commit-hash.
var versionRe *regexp.Regexp = regexp.MustCompile(`-g[[:xdigit:]]{7,}`)

// DevVersion returns true if the version string contains the above pattern
// e.g.
//  1. v2.0.0-rc1-127-gd20a768b3 => dev version
//  2. v2.0.0 => prod version
func DevVersion() (matched bool) {
	return (versionRe.MatchString(outservVersion))
}

// ExecutableChecksum returns a byte slice containing the SHA256 checksum of the executable.
// It returns a nil slice if there's an error trying to calculate the checksum.
func ExecutableChecksum() []byte {
	execPath, err := os.Executable()
	if err != nil {
		return nil
	}
	execFile, err := os.Open(execPath)
	if err != nil {
		return nil
	}
	defer execFile.Close()

	h := sha256.New()
	if _, err := io.Copy(h, execFile); err != nil {
		return nil
	}

	return h.Sum(nil)
}
