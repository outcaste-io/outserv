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

package x

import (
	"crypto/tls"
	"fmt"
	"net"
	"path/filepath"
	"time"

	"github.com/golang/glog"
	"github.com/outcaste-io/badger/v3"
	"github.com/outcaste-io/ristretto/z"
	"github.com/spf13/viper"
)

// Options stores the options for this package.
type Options struct {
	// PortOffset will be used to determine the ports to use (port = default port + offset).
	PortOffset int
	// Limit options:
	//
	// query-edge uint64 - maximum number of edges that can be returned in a query
	// normalize-node int - maximum number of nodes that can be returned in a query that uses the
	//                      normalize directive
	// mutations-nquad int - maximum number of nquads that can be inserted in a mutation request
	// BlockDropAll bool - if set to true, the drop all operation will be rejected by the server.
	// query-timeout duration - Maximum time after which a query execution will fail.
	// max-retries int64 - maximum number of retries made by dgraph to commit a transaction to disk.
	// shared-instance bool - if set to true, ACLs will be disabled for non-galaxy users.
	Limit                *z.SuperFlag
	LimitMutationsNquad  int
	LimitQueryEdge       uint64
	BlockClusterWideDrop bool
	LimitNormalizeNode   int
	QueryTimeout         time.Duration
	MaxRetries           int64
	SharedInstance       bool

	// GraphQL options:
	//
	// extensions bool - Will be set to see extensions in GraphQL results
	// debug bool - Will enable debug mode in GraphQL.
	// poll-interval duration - The polling interval for graphql subscription.
	GraphQL GraphQLOptions

	// Lambda options:
	// url string - Stores the URL of lambda functions for custom GraphQL resolvers
	// 			The configured url can have a parameter `$ns`,
	//			which should be replaced with the correct namespace value at runtime.
	// 	===========================================================================================
	// 	|                url                | $ns |           namespacedLambdaUrl          |
	// 	|==========================================|=====|========================================|
	// 	| http://localhost:8686/graphql-worker/$ns |  1  | http://localhost:8686/graphql-worker/1 |
	// 	| http://localhost:8686/graphql-worker     |  1  | http://localhost:8686/graphql-worker   |
	// 	|=========================================================================================|
	//
	// Update(Aug 2021): Now, alpha spins up lambda servers based on cnt and port sub-flags.
	// Also, no special handling of namespace is needed from lambda as we send the script
	// along with request body to lambda server. If url is set, these two flags are ignored.
	Lambda LambdaOptions
}

type GraphQLOptions struct {
	Introspection bool
	Debug         bool
	Extensions    bool
	PollInterval  time.Duration
}

type LambdaOptions struct {
	Url          string
	Num          uint32
	Port         uint32
	RestartAfter time.Duration
}

// Config stores the global instance of this package's options.
var Config Options

// IPRange represents an IP range.
type IPRange struct {
	Lower, Upper net.IP
}

var DataDefaults = `dir=data; p=; w=; t=; zw=; export=;`

type Dir struct {
	Posting     string
	RaftWal     string
	Tmp         string
	ZeroRaftWal string
	Export      string
}

// WorkerOptions stores the options for the worker package. It's declared here
// since it's used by multiple packages.
type WorkerOptions struct {
	// Data stores the list of directories to use.
	Dir Dir
	// Trace options:
	//
	// ratio float64 - the ratio of queries to trace (must be between 0 and 1)
	// jaeger string - URL of Jaeger to send OpenCensus traces
	// datadog string - URL of Datadog to to send OpenCensus traces
	Trace *z.SuperFlag
	// MyAddr stores the address and port for this alpha.
	MyAddr string
	// PeerAddr stores the list of address:port for the other instances in the
	// cluster. You just need one active address to work.
	PeerAddr []string
	// TLS client config which will be used to connect with zero and alpha internally
	TLSClientConfig *tls.Config
	// TLS server config which will be used to initiate server internal port
	TLSServerConfig *tls.Config
	// Raft stores options related to Raft.
	Raft *z.SuperFlag
	// Badger stores the badger options.
	Badger badger.Options
	// WhiteListedIPRanges is a list of IP ranges from which requests will be allowed.
	WhiteListedIPRanges []IPRange
	// StrictMutations will cause mutations to unknown predicates to fail if set to true.
	StrictMutations bool
	// AclEnabled indicates whether the enterprise ACL feature is turned on.
	AclEnabled bool
	// HmacSecret stores the secret used to sign JSON Web Tokens (JWT).
	HmacSecret Sensitive
	// AbortOlderThan tells Dgraph to discard transactions that are older than this duration.
	AbortOlderThan time.Duration
	// StartTime is the start time of the alpha
	StartTime time.Time
	// Security options:
	//
	// whitelist string - comma separated IP addresses
	// token string - if set, all Admin requests to Dgraph will have this token.
	Security *z.SuperFlag
	// EncryptionKey is the key used for encryption at rest, backups, exports. Enterprise only feature.
	EncryptionKey Sensitive
	// LogRequest indicates whether alpha should log all query/mutation requests coming to it.
	// Ideally LogRequest should be a bool value. But we are reading it using atomics across
	// queries hence it has been kept as int32. LogRequest value 1 enables logging of requests
	// coming to alphas and 0 disables it.
	LogRequest int32
	// If true, we should call msync or fsync after every write to survive hard reboots.
	HardSync bool
	// Audit contains the audit flags that enables the audit.
	Audit bool
}

// WorkerConfig stores the global instance of the worker package's options.
var WorkerConfig WorkerOptions

func (w *WorkerOptions) Parse(conf *viper.Viper) {
	w.MyAddr = conf.GetString("my")
	if w.MyAddr == "" {
		w.MyAddr = fmt.Sprintf("localhost:%d", Config.PortOffset+PortInternal)

	} else {
		// check if address is valid or not
		Check(ValidateAddress(w.MyAddr))
		bindall := conf.GetBool("bindall")
		if !bindall {
			glog.Errorln("--my flag is provided without bindall, Did you forget to specify bindall?")
		}
	}
	w.Trace = z.NewSuperFlag(conf.GetString("trace")).MergeAndCheckDefault(TraceDefaults)

	survive := conf.GetString("survive")
	AssertTruef(survive == "process" || survive == "filesystem",
		"Invalid survival mode: %s", survive)
	w.HardSync = survive == "filesystem"

	AssertTruef(len(w.PeerAddr) > 0, "Provide at least one peer node address")
	for _, addr := range w.PeerAddr {
		AssertTruef(addr != w.MyAddr,
			"Peer address %s and my address (IP:Port) %s can't be the same.",
			addr, w.MyAddr)
	}

	data := z.NewSuperFlag(conf.GetString("data")).MergeAndCheckDefault(DataDefaults)
	dir := data.GetPath("dir")

	paths := make(map[string]int)
	getAbsPath := func(v string) string {
		var res string
		if d := data.GetPath(v); len(d) > 0 {
			res = d
		} else {
			res = filepath.Join(dir, v)
		}
		paths[res]++
		return res
	}
	w.Dir.Posting = getAbsPath("p")
	w.Dir.RaftWal = getAbsPath("w")
	w.Dir.Tmp = getAbsPath("t")
	w.Dir.ZeroRaftWal = getAbsPath("zw")
	w.Dir.Export = getAbsPath("export")

	for path, count := range paths {
		AssertTruef(count == 1,
			"All data directories have to be unique. Found repetition with '%s'.", path)
	}
}

func WorkerPort() int {
	return Config.PortOffset + PortInternal
}
