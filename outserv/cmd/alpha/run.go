// Portions Copyright 2017-2021 Dgraph Labs, Inc. are available under the Apache License v2.0.
// Portions Copyright 2022 Outcaste LLC are available under the Smart License v1.0.

package alpha

import (
	"bytes"
	"context"
	"embed"
	"fmt"
	"log"
	"math"
	"net"
	"net/http"
	_ "net/http/pprof" // http profile
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/outcaste-io/badger/v3"
	"github.com/outcaste-io/outserv/billing"
	"github.com/outcaste-io/outserv/conn"
	"github.com/outcaste-io/outserv/ee"
	"github.com/outcaste-io/outserv/ee/audit"
	"github.com/outcaste-io/outserv/protos/pb"
	"github.com/outcaste-io/outserv/zero"

	"github.com/golang/glog"
	"github.com/outcaste-io/outserv/edgraph"
	"github.com/outcaste-io/outserv/graphql/admin"
	"github.com/outcaste-io/outserv/posting"
	"github.com/outcaste-io/outserv/schema"
	"github.com/outcaste-io/outserv/tok"
	"github.com/outcaste-io/outserv/worker"
	"github.com/outcaste-io/outserv/x"
	"github.com/outcaste-io/ristretto/z"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"go.opencensus.io/plugin/ocgrpc"
	otrace "go.opencensus.io/trace"
	"go.opencensus.io/zpages"
	"golang.org/x/net/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	_ "google.golang.org/grpc/encoding/gzip" // grpc compression

	_ "github.com/outcaste-io/gqlparser/v2/validator/rules" // make gql validator init() all rules
)

var (
	bindall bool

	// used for computing uptime
	startTime = time.Now()

	// Alpha is the sub-command invoked when running "dgraph alpha".
	Alpha x.SubCommand

	// need this here to refer it in admin_backup.go
	adminServer *admin.GqlHandler
	initDone    uint32
)

// Embed the Javascript Lambda Server's code to launch lambda server later on.
//go:embed dist/*
var jsLambda embed.FS

func init() {
	Alpha.Cmd = &cobra.Command{
		Use:   "graphql",
		Short: "Run Outserv GraphQL server",
		Run: func(cmd *cobra.Command, args []string) {
			defer x.StartProfile(Alpha.Conf).Stop()
			run()
		},
		Annotations: map[string]string{"group": "core"},
	}
	Alpha.EnvPrefix = "OUTSERV_GRAPHQL"
	Alpha.Cmd.SetHelpTemplate(x.NonRootTemplate)

	// If you change any of the flags below, you must also update run() to call Alpha.Conf.Get
	// with the flag name so that the values are picked up by Cobra/Viper's various config inputs
	// (e.g, config file, env vars, cli flags, etc.)
	flag := Alpha.Cmd.Flags()

	// common
	x.FillCommonFlags(flag)
	// --tls SuperFlag
	x.RegisterServerTLSFlags(flag)
	// --encryption and --vault Superflag
	ee.RegisterAclAndEncFlags(flag)

	flag.String("data", x.DataDefaults, z.NewSuperFlagHelp(x.DataDefaults).
		Head("Directories to store data in").
		Flag("dir", "If provided, all data goes into this directory.").
		Flag("p", "Directory to store state").
		Flag("w", "Directory to store Alpha Raft write-ahead logs").
		Flag("zw", "Directory to store Zero Raft write-ahead logs").
		Flag("t", "Directory to store temporary files and buffers").
		Flag("export", "Directory to store exports").
		String())

	flag.String("peer", "",
		"Comma separated list of Outserv Peer addresses of the form IP_ADDRESS:PORT.")

	// Useful for running multiple servers on the same machine.
	flag.IntP("port_offset", "o", 0,
		"Value added to all listening port numbers. [Internal=7080, HTTP=8080]")

	// Custom plugins.
	flag.String("custom_tokenizers", "",
		"Comma separated list of tokenizer plugins for custom indices.")

	// By default Go GRPC traces all requests.
	grpc.EnableTracing = false

	flag.String("badger", worker.BadgerDefaults, z.NewSuperFlagHelp(worker.BadgerDefaults).
		Head("Badger options (Refer to badger documentation for all possible options)").
		Flag("compression",
			`[none, zstd:level, snappy] Specifies the compression algorithm and
			compression level (if applicable) for the postings directory."none" would disable
			compression, while "zstd:1" would set zstd compression at level 1.`).
		Flag("numgoroutines",
			"The number of goroutines to use in badger.Stream.").
		String())

	// Cache flags.
	flag.String("cache", worker.CacheDefaults, z.NewSuperFlagHelp(worker.CacheDefaults).
		Head("Cache options").
		Flag("size-mb",
			"Total size of cache (in MB) to be used in Outserv.").
		Flag("percentage",
			"Cache percentages summing up to 100 for various caches (FORMAT: PostingListCache,"+
				"PstoreBlockCache,PstoreIndexCache)").
		String())

	flag.String("raft", worker.RaftDefaults, z.NewSuperFlagHelp(worker.RaftDefaults).
		Head("Raft options").
		Flag("idx",
			"Provides an optional Raft ID that this Alpha would use to join Raft groups.").
		Flag("group",
			"Provides an optional Raft Group ID that this Alpha would indicate to Zero to join.").
		Flag("learner",
			`Make this Alpha a "learner" node. In learner mode, this Alpha will not participate `+
				"in Raft elections. This can be used to achieve a read-only replica.").
		Flag("snapshot-after-entries",
			"Create a new Raft snapshot after N number of Raft entries. The lower this number, "+
				"the more frequent snapshot creation will be. Snapshots are created only if both "+
				"snapshot-after-duration and snapshot-after-entries threshold are crossed.").
		Flag("snapshot-after-duration",
			"Frequency at which we should create a new raft snapshots. Set "+
				"to 0 to disable duration based snapshot.").
		Flag("pending-proposals",
			"Number of pending mutation proposals. Useful for rate limiting.").
		String())

	flag.String("security", worker.SecurityDefaults, z.NewSuperFlagHelp(worker.SecurityDefaults).
		Head("Security options").
		Flag("token",
			"If set, all Admin requests to Outserv will need to have this token. The token can be "+
				"passed as follows: for HTTP requests, in the X-Dgraph-AuthToken header. For Grpc, "+
				"in auth-token key in the context.").
		Flag("whitelist",
			"A comma separated list of IP addresses, IP ranges, CIDR blocks, or hostnames you wish "+
				"to whitelist for performing admin actions (i.e., --security "+
				`"whitelist=144.142.126.254,127.0.0.1:127.0.0.3,192.168.0.0/16,host.docker.`+
				`internal").`).
		String())

	flag.String("limit", worker.LimitDefaults, z.NewSuperFlagHelp(worker.LimitDefaults).
		Head("Limit options").
		Flag("query-edge",
			"The maximum number of edges that can be returned in a query. This applies to shortest "+
				"path and recursive queries.").
		Flag("normalize-node",
			"The maximum number of nodes that can be returned in a query that uses the normalize "+
				"directive.").
		Flag("disallow-mutations",
			"Set disallow-mutations to true to block mutations.").
		Flag("mutations-nquad",
			"The maximum number of nquads that can be inserted in a mutation request.").
		Flag("disallow-drop",
			"Set disallow-drop to true to block drop-all and drop-data operation. It still"+
				" allows dropping attributes and types.").
		Flag("query-timeout",
			"Maximum time after which a query execution will fail. If set to"+
				" 0, the timeout is infinite.").
		Flag("max-pending-queries",
			"Number of maximum pending queries before we reject them as too many requests.").
		Flag("max-retries",
			"Commits to disk will give up after these number of retries to prevent locking the "+
				"worker in a failed state. Use -1 to retry infinitely.").
		Flag("shared-instance", "When set to true, it disables ACLs for non-galaxy users. "+
			"It expects the access JWT to be constructed outside dgraph for those users as even "+
			"login is denied to them. Additionally, this disables access to environment variables"+
			"for minio, aws, etc.").
		Flag("max-splits", "How many splits can a single key have, before it is forbidden. "+
			"Also known as Jupiter key.").
		Flag("max-upload-size-mb", "What is the maximum upload size that can be uploaded with "+
			"a multi-part file upload.").
		String())

	flag.String("graphql", worker.GraphQLDefaults, z.NewSuperFlagHelp(worker.GraphQLDefaults).
		Head("GraphQL options").
		Flag("introspection",
			"Enables GraphQL schema introspection.").
		Flag("debug",
			"Enables debug mode in GraphQL. This returns auth errors to clients, and we do not "+
				"recommend turning it on for production.").
		Flag("extensions",
			"Enables extensions in GraphQL response body.").
		Flag("poll-interval",
			"The polling interval for GraphQL subscription.").
		String())

	flag.String("lambda", worker.LambdaDefaults, z.NewSuperFlagHelp(worker.LambdaDefaults).
		Head("Lambda options").
		Flag("url",
			"The URL of a lambda server that implements custom GraphQL Javascript resolvers."+
				" This should be used only when using custom lambda server."+
				" Use num subflag to launch official lambda server."+
				" This flag if set, overrides the other lambda flags.").
		Flag("num",
			"Number of JS lambda servers to be launched by alpha.").
		Flag("port",
			"The starting port at which the lambda server listens.").
		Flag("restart-after",
			"Restarts the lambda server after given duration of unresponsiveness").
		String())

	flag.String("cdc", worker.CDCDefaults, z.NewSuperFlagHelp(worker.CDCDefaults).
		Head("Change Data Capture options").
		Flag("file",
			"The path where audit logs will be stored.").
		Flag("kafka",
			"A comma separated list of Kafka hosts.").
		Flag("sasl-user",
			"The SASL username for Kafka.").
		Flag("sasl-password",
			"The SASL password for Kafka.").
		Flag("sasl-mechanism",
			"The SASL mechanism for Kafka (PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512)").
		Flag("ca-cert",
			"The path to CA cert file for TLS encryption.").
		Flag("client-cert",
			"The path to client cert file for TLS encryption.").
		Flag("client-key",
			"The path to client key file for TLS encryption.").
		String())

	flag.String("audit", worker.AuditDefaults, z.NewSuperFlagHelp(worker.AuditDefaults).
		Head("Audit options").
		Flag("output",
			`[stdout, /path/to/dir] This specifies where audit logs should be output to.
			"stdout" is for standard output. You can also specify the directory where audit logs
			will be saved. When stdout is specified as output other fields will be ignored.`).
		Flag("compress",
			"Enables the compression of old audit logs.").
		Flag("encrypt-file",
			"The path to the key file to be used for audit log encryption.").
		Flag("days",
			"The number of days audit logs will be preserved.").
		Flag("size",
			"The audit log max size in MB after which it will be rolled over.").
		String())

	flag.String("wallet", billing.WalletDefaults, z.NewSuperFlagHelp(billing.WalletDefaults).
		Head("Wallet options").
		Flag("keystore", "Path of the ethereum wallet keystore.").
		Flag("password", "Password used to encrypt the keystore.").
		String())
}

func setupCustomTokenizers() {
	customTokenizers := Alpha.Conf.GetString("custom_tokenizers")
	if customTokenizers == "" {
		return
	}
	for _, soFile := range strings.Split(customTokenizers, ",") {
		tok.LoadCustomTokenizer(soFile)
	}
}

// Parses a comma-delimited list of IP addresses, IP ranges, CIDR blocks, or hostnames
// and returns a slice of []IPRange.
//
// e.g. "144.142.126.222:144.142.126.244,144.142.126.254,192.168.0.0/16,host.docker.internal"
func getIPsFromString(str string) ([]x.IPRange, error) {
	if str == "" {
		return []x.IPRange{}, nil
	}

	var ipRanges []x.IPRange
	rangeStrings := strings.Split(str, ",")

	for _, s := range rangeStrings {
		isIPv6 := strings.Contains(s, "::")
		tuple := strings.Split(s, ":")
		switch {
		case isIPv6 || len(tuple) == 1:
			if !strings.Contains(s, "/") {
				// string is hostname like host.docker.internal,
				// or IPv4 address like 144.124.126.254,
				// or IPv6 address like fd03:b188:0f3c:9ec4::babe:face
				ipAddr := net.ParseIP(s)
				if ipAddr != nil {
					ipRanges = append(ipRanges, x.IPRange{Lower: ipAddr, Upper: ipAddr})
				} else {
					ipAddrs, err := net.LookupIP(s)
					if err != nil {
						return nil, errors.Errorf("invalid IP address or hostname: %s", s)
					}

					for _, addr := range ipAddrs {
						ipRanges = append(ipRanges, x.IPRange{Lower: addr, Upper: addr})
					}
				}
			} else {
				// string is CIDR block like 192.168.0.0/16 or fd03:b188:0f3c:9ec4::/64
				rangeLo, network, err := net.ParseCIDR(s)
				if err != nil {
					return nil, errors.Errorf("invalid CIDR block: %s", s)
				}

				addrLen, maskLen := len(rangeLo), len(network.Mask)
				rangeHi := make(net.IP, len(rangeLo))
				copy(rangeHi, rangeLo)
				for i := 1; i <= maskLen; i++ {
					rangeHi[addrLen-i] |= ^network.Mask[maskLen-i]
				}

				ipRanges = append(ipRanges, x.IPRange{Lower: rangeLo, Upper: rangeHi})
			}
		case len(tuple) == 2:
			// string is range like a.b.c.d:w.x.y.z
			rangeLo := net.ParseIP(tuple[0])
			rangeHi := net.ParseIP(tuple[1])
			switch {
			case rangeLo == nil:
				return nil, errors.Errorf("invalid IP address: %s", tuple[0])
			case rangeHi == nil:
				return nil, errors.Errorf("invalid IP address: %s", tuple[1])
			case bytes.Compare(rangeLo, rangeHi) > 0:
				return nil, errors.Errorf("inverted IP address range: %s", s)
			}
			ipRanges = append(ipRanges, x.IPRange{Lower: rangeLo, Upper: rangeHi})
		default:
			return nil, errors.Errorf("invalid IP address range: %s", s)
		}
	}

	return ipRanges, nil
}

func httpPort() int {
	return x.Config.PortOffset + x.PortHTTP
}

func healthCheck(w http.ResponseWriter, r *http.Request) {
	x.AddCorsHeaders(w)
	var err error

	if _, ok := r.URL.Query()["all"]; ok {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)

		ctx := x.AttachAccessJwt(context.Background(), r)
		var resp *pb.Response
		if resp, err = edgraph.Health(ctx, true); err != nil {
			x.SetStatus(w, x.Error, err.Error())
			return
		}
		if resp == nil {
			x.SetStatus(w, x.ErrorNoData, "No health information available.")
			return
		}
		_, _ = w.Write(resp.Json)
		return
	}

	_, ok := r.URL.Query()["live"]
	if !ok {
		if err := x.HealthCheck(); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, err = w.Write([]byte(err.Error()))
			if err != nil {
				glog.V(2).Infof("Error while writing health check response: %v", err)
			}
			return
		}
	}

	var resp *pb.Response
	if resp, err = edgraph.Health(context.Background(), false); err != nil {
		x.SetStatus(w, x.Error, err.Error())
		return
	}
	if resp == nil {
		x.SetStatus(w, x.ErrorNoData, "No health information available.")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(resp.Json)
}

func stateHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	x.AddCorsHeaders(w)
	w.Header().Set("Content-Type", "application/json")

	ctx := context.Background()
	ctx = x.AttachAccessJwt(ctx, r)

	var aResp *pb.Response
	if aResp, err = edgraph.State(ctx); err != nil {
		x.SetStatus(w, x.Error, err.Error())
		return
	}
	if aResp == nil {
		x.SetStatus(w, x.ErrorNoData, "No state information available.")
		return
	}

	if _, err = w.Write(aResp.Json); err != nil {
		x.SetStatus(w, x.Error, err.Error())
		return
	}
}

func setupListener(addr string, port int) (net.Listener, error) {
	return net.Listen("tcp", fmt.Sprintf("%s:%d", addr, port))
}

func setupLambdaServer(closer *z.Closer) {
	// If --lambda url is set, then don't launch the lambda servers from dgraph.
	if len(x.Config.Lambda.Url) > 0 {
		return
	}

	num := int(x.Config.Lambda.Num)
	port := int(x.Config.Lambda.Port)
	if num == 0 {
		return
	}

	tmpDir := x.WorkerConfig.Dir.Tmp

	// Copy over all the embedded files to actual files.
	dir := "dist"
	files, err := jsLambda.ReadDir(dir)
	x.Check(err)
	for _, file := range files {
		// The separator for embedded files is forward-slash even on Windows.
		data, err := jsLambda.ReadFile(dir + "/" + file.Name())
		x.Check(err)
		filename := filepath.Join(tmpDir, file.Name())
		file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
		x.Check(err)
		_, err = file.Write(data)
		x.Check(err)
		x.Check(file.Close())
	}

	type lambda struct {
		sync.Mutex
		cmd        *exec.Cmd
		active     bool
		lastActive int64

		health string
		port   int
	}

	lambdas := make([]*lambda, 0, num)
	for i := 0; i < num; i++ {
		lambdas = append(lambdas, &lambda{
			port:   port + i,
			health: fmt.Sprintf("http://127.0.0.1:%d/health", port+i),
		})
	}

	// Entry point of the script is index.js.
	filename := filepath.Join(tmpDir, "index.js")
	dgraphUrl := fmt.Sprintf("http://127.0.0.1:%d", httpPort())

	// TODO: If node is not installed, we should not run any lambda servers.
	glog.Infoln("Setting up lambda servers")
	for i := range lambdas {
		go func(i int) {
			for {
				select {
				case <-closer.HasBeenClosed():
					return
				default:
					time.Sleep(2 * time.Second)
					cmd := exec.CommandContext(closer.Ctx(), "node", filename)
					cmd.SysProcAttr = childProcessConfig()
					cmd.Env = append(cmd.Env, fmt.Sprintf("PORT=%d", lambdas[i].port))
					cmd.Env = append(cmd.Env, fmt.Sprintf("DGRAPH_URL="+dgraphUrl))
					cmd.Stdout = os.Stdout
					cmd.Stderr = os.Stderr

					lambdas[i].Lock()
					lambdas[i].cmd = cmd
					lambdas[i].lastActive = time.Now().UnixNano()
					glog.Infof("Running node command: %+v", cmd)
					if err := cmd.Start(); err != nil {
						glog.Errorf("Failed to start lambda server at port: %d. Got err: %+v",
							lambdas[i].port, err)
						lambdas[i].Unlock()
						continue
					}
					lambdas[i].active = true
					lambdas[i].Unlock()
					if err := cmd.Wait(); err != nil {
						glog.Errorf("Lambda server at port: %d stopped with error: %v",
							lambdas[i].port, err)
					}
				}
			}
		}(i)
	}

	client := http.Client{Timeout: 1 * time.Second}
	healthCheck := func(l *lambda) {
		l.Lock()
		defer l.Unlock()

		if !l.active {
			return
		}

		timestamp := time.Now().UnixNano()
		resp, err := client.Get(l.health)
		if err != nil || resp.StatusCode != 200 {
			if time.Duration(timestamp-l.lastActive) > x.Config.Lambda.RestartAfter {
				glog.Warningf("Lambda Server at port: %d not responding."+
					" Killed it with err: %v", l.port, l.cmd.Process.Kill())
				l.active = false
			}
			return
		}

		resp.Body.Close()
		l.lastActive = timestamp
	}

	// Monitor the lambda servers. If the server is unresponsive for more than restart-after time,
	// restart it.
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-closer.HasBeenClosed():
				return
			case <-ticker.C:
				for _, l := range lambdas {
					healthCheck(l)
				}
			}
		}
	}()
}

func setupServer() {
	laddr := "localhost"
	if bindall {
		laddr = "0.0.0.0"
	}

	tlsCfg, err := x.LoadServerTLSConfig(Alpha.Conf)
	if err != nil {
		log.Fatalf("Failed to setup TLS: %v\n", err)
	}

	glog.Infof("Setting up HTTP listener")
	httpListener, err := setupListener(laddr, httpPort())
	if err != nil {
		log.Fatal(err)
	}
	glog.Infof("Setting up HTTP listener: DONE")

	baseMux := http.NewServeMux()
	http.Handle("/", audit.AuditRequestHttp(baseMux))

	baseMux.HandleFunc("/query", queryHandler)
	baseMux.HandleFunc("/query/", queryHandler)
	baseMux.HandleFunc("/mutate", mutationHandler)
	baseMux.HandleFunc("/mutate/", mutationHandler)
	baseMux.HandleFunc("/alter", alterHandler)
	baseMux.HandleFunc("/health", healthCheck)
	baseMux.HandleFunc("/state", stateHandler)
	baseMux.HandleFunc("/debug/jemalloc", x.JemallocHandler)
	baseMux.HandleFunc("/debug/store", worker.StoreHandler)
	zpages.Handle(baseMux, "/debug/z")

	// Global Epoch is a lockless synchronization mechanism for graphql service.
	// It's is just an atomic counter used by the graphql subscription to update its state.
	// It's is used to detect the schema changes and server exit.
	// It is also reported by /probe/graphql endpoint as the schemaUpdateCounter.

	// Implementation for schema change:
	// The global epoch is incremented when there is a schema change.
	// Polling goroutine acquires the current epoch count as a local epoch.
	// The local epoch count is checked against the global epoch,
	// If there is change then we terminate the subscription.

	// Implementation for server exit:
	// The global epoch is set to maxUint64 while exiting the server.
	// By using this information polling goroutine terminates the subscription.
	globalEpoch := make(map[uint64]*uint64)
	e := new(uint64)
	atomic.StoreUint64(e, 0)
	globalEpoch[x.GalaxyNamespace] = e

	glog.Infof("Setting up GraphQL servers")
	// Do not use := notation here because adminServer is a global variable.
	// admin.NewServers loads up GraphQL schema.
	introspection := x.Config.GraphQL.Introspection
	var mainServer *admin.GqlHandler
	var gqlHealthStore *admin.GraphQLHealthStore
	mainServer, adminServer, gqlHealthStore = admin.NewServers(introspection, globalEpoch)

	baseMux.HandleFunc("/graphql", func(w http.ResponseWriter, r *http.Request) {
		namespace := x.ExtractNamespaceHTTP(r)
		r.Header.Set("resolver", strconv.FormatUint(namespace, 10))
		if err := admin.LazyLoadSchema(namespace); err != nil {
			admin.WriteErrorResponse(w, r, err)
			return
		}
		mainServer.HTTPHandler().ServeHTTP(w, r)
	})
	baseMux.Handle("/probe/graphql", graphqlProbeHandler(gqlHealthStore, globalEpoch))

	baseMux.HandleFunc("/admin", func(w http.ResponseWriter, r *http.Request) {
		r.Header.Set("resolver", "0")
		// We don't need to load the schema for all the admin operations.
		// Only a few like getUser, queryGroup require this. So, this can be optimized.
		if err := admin.LazyLoadSchema(x.ExtractNamespaceHTTP(r)); err != nil {
			admin.WriteErrorResponse(w, r, err)
			return
		}
		allowedMethodsHandler(allowedMethods{
			http.MethodGet:     true,
			http.MethodPost:    true,
			http.MethodOptions: true,
		}, adminAuthHandler(adminServer.HTTPHandler())).ServeHTTP(w, r)
	})
	baseMux.Handle("/admin/", getAdminMux())

	addr := fmt.Sprintf("%s:%d", laddr, httpPort())
	glog.Infof("Bringing up GraphQL HTTP API at %s/graphql", addr)
	glog.Infof("Bringing up GraphQL HTTP admin API at %s/admin", addr)

	baseMux.Handle("/", http.HandlerFunc(homeHandler))
	baseMux.Handle("/ui/keywords", http.HandlerFunc(keywordHandler))

	// Initialize the lambda server
	setupLambdaServer(x.ServerCloser)
	// Initialize the servers.
	x.RegisterExporters(Alpha.Conf, "outserv.graphql")
	x.ServerCloser.AddRunning(2)
	go x.StartListenHttpAndHttps(httpListener, tlsCfg, x.ServerCloser)
	go func() {
		defer x.ServerCloser.Done()

		<-x.ServerCloser.HasBeenClosed()
		// TODO - Verify why do we do this and does it have to be done for all namespaces.
		e = globalEpoch[x.GalaxyNamespace]
		atomic.StoreUint64(e, math.MaxUint64)

		// Stops http servers; Already accepted connections are not closed.
		err := httpListener.Close()
		glog.Infof("HTTP listener closed with error: %v", err)
	}()
	glog.Infoln("HTTP server started.  Listening on port", httpPort())

	// Audit needs groupId and nodeId to initialize audit files
	// Therefore we wait for the cluster initialization to be done.
	// Audit is enterprise feature.
	x.Check(audit.InitAuditorIfNecessary(worker.Config.Audit, nil))
}

func run() {
	var err error

	telemetry := z.NewSuperFlag(Alpha.Conf.GetString("telemetry")).MergeAndCheckDefault(
		x.TelemetryDefaults)

	bindall = Alpha.Conf.GetBool("bindall")
	cache := z.NewSuperFlag(Alpha.Conf.GetString("cache")).MergeAndCheckDefault(
		worker.CacheDefaults)
	totalCache := cache.GetInt64("size-mb")
	x.AssertTruef(totalCache >= 0, "ERROR: Cache size must be non-negative")

	cachePercentage := cache.GetString("percentage")
	cachePercent, err := x.GetCachePercentages(cachePercentage, 3)
	x.Check(err)
	postingListCacheSize := (cachePercent[0] * (totalCache << 20)) / 100
	pstoreBlockCacheSize := (cachePercent[1] * (totalCache << 20)) / 100
	pstoreIndexCacheSize := (cachePercent[2] * (totalCache << 20)) / 100

	cacheOpts := fmt.Sprintf("blockcachesize=%d; indexcachesize=%d; ",
		pstoreBlockCacheSize, pstoreIndexCacheSize)
	bopts := badger.DefaultOptions("").FromSuperFlag(worker.BadgerDefaults + cacheOpts).
		FromSuperFlag(Alpha.Conf.GetString("badger"))
	security := z.NewSuperFlag(Alpha.Conf.GetString("security")).MergeAndCheckDefault(
		worker.SecurityDefaults)
	conf := audit.GetAuditConf(Alpha.Conf.GetString("audit"))

	opts := worker.Options{
		CacheMb:         totalCache,
		CachePercentage: cachePercentage,

		MutationsMode:  worker.StrictMutations,
		AuthToken:      security.GetString("token"),
		Audit:          conf,
		ChangeDataConf: Alpha.Conf.GetString("cdc"),
	}

	keys, err := ee.GetKeys(Alpha.Conf)
	x.Check(err)

	if keys.AclKey != nil {
		opts.HmacSecret = keys.AclKey
		opts.AccessJwtTtl = keys.AclAccessTtl
		opts.RefreshJwtTtl = keys.AclRefreshTtl
		glog.Info("ACL secret key loaded successfully.")
	}

	x.Config.Limit = z.NewSuperFlag(Alpha.Conf.GetString("limit")).MergeAndCheckDefault(
		worker.LimitDefaults)
	if x.Config.Limit.GetBool("disallow-mutations") {
		opts.MutationsMode = worker.DisallowMutations
	}

	walletFlag := z.NewSuperFlag(Alpha.Conf.GetString("wallet")).MergeAndCheckDefault(
		billing.WalletDefaults)

	billing.EthKeyStorePath = walletFlag.GetPath("dir")
	billing.EthKeyStorePassword = walletFlag.GetString("password")

	worker.SetConfiguration(&opts)

	ips, err := getIPsFromString(security.GetString("whitelist"))
	x.Check(err)

	tlsClientConf, err := x.LoadClientTLSConfigForInternalPort(Alpha.Conf)
	x.Check(err)
	tlsServerConf, err := x.LoadServerTLSConfigForInternalPort(Alpha.Conf)
	x.Check(err)

	raft := z.NewSuperFlag(Alpha.Conf.GetString("raft")).MergeAndCheckDefault(worker.RaftDefaults)
	x.WorkerConfig = x.WorkerOptions{
		PeerAddr:            strings.Split(Alpha.Conf.GetString("peer"), ","),
		Raft:                raft,
		WhiteListedIPRanges: ips,
		StrictMutations:     opts.MutationsMode == worker.StrictMutations,
		AclEnabled:          keys.AclKey != nil,
		StartTime:           startTime,
		Security:            security,
		TLSClientConfig:     tlsClientConf,
		TLSServerConfig:     tlsServerConf,
		HmacSecret:          opts.HmacSecret,
		Audit:               opts.Audit != nil,
		Badger:              bopts,
	}
	x.WorkerConfig.Parse(Alpha.Conf)

	if telemetry.GetBool("reports") {
		go edgraph.PeriodicallyPostTelemetry()
	}

	// Set the directory for temporary buffers.
	z.SetTmpDir(x.WorkerConfig.Dir.Tmp)

	x.WorkerConfig.EncryptionKey = keys.EncKey

	setupCustomTokenizers()
	x.Init()
	x.Config.PortOffset = Alpha.Conf.GetInt("port_offset")
	x.Config.LimitMutationsNquad = int(x.Config.Limit.GetInt64("mutations-nquad"))
	x.Config.LimitQueryEdge = x.Config.Limit.GetUint64("query-edge")
	x.Config.BlockClusterWideDrop = x.Config.Limit.GetBool("disallow-drop")
	x.Config.LimitNormalizeNode = int(x.Config.Limit.GetInt64("normalize-node"))
	x.Config.QueryTimeout = x.Config.Limit.GetDuration("query-timeout")
	x.Config.MaxRetries = x.Config.Limit.GetInt64("max-retries")
	x.Config.SharedInstance = x.Config.Limit.GetBool("shared-instance")
	x.Config.MaxUploadSizeMb = x.Config.Limit.GetInt64("max-upload-size-mb")

	graphql := z.NewSuperFlag(Alpha.Conf.GetString("graphql")).MergeAndCheckDefault(
		worker.GraphQLDefaults)
	x.Config.GraphQL = x.GraphQLOptions{
		Introspection: graphql.GetBool("introspection"),
		Debug:         graphql.GetBool("debug"),
		Extensions:    graphql.GetBool("extensions"),
		PollInterval:  graphql.GetDuration("poll-interval"),
	}
	lambda := z.NewSuperFlag(Alpha.Conf.GetString("lambda")).MergeAndCheckDefault(
		worker.LambdaDefaults)
	x.Config.Lambda = x.LambdaOptions{
		Url:          lambda.GetString("url"),
		Num:          lambda.GetUint32("num"),
		Port:         lambda.GetUint32("port"),
		RestartAfter: lambda.GetDuration("restart-after"),
	}
	if x.Config.Lambda.Url != "" {
		graphqlLambdaUrl, err := url.Parse(x.Config.Lambda.Url)
		if err != nil {
			glog.Errorf("unable to parse --lambda url: %v", err)
			return
		}
		if !graphqlLambdaUrl.IsAbs() {
			glog.Errorf("expecting --lambda url to be an absolute URL, got: %s",
				graphqlLambdaUrl.String())
			return
		}
	}
	edgraph.Init()

	x.PrintVersion()
	glog.Infof("x.Config: %+v", x.Config)
	glog.Infof("x.WorkerConfig: %+v", x.WorkerConfig)
	glog.Infof("worker.Config: %+v", worker.Config)

	worker.InitServerState()
	worker.InitTasks()

	if Alpha.Conf.GetBool("expose_trace") {
		// TODO: Remove this once we get rid of event logs.
		trace.AuthRequest = func(req *http.Request) (any, sensitive bool) {
			return true, true
		}
	}
	otrace.ApplyConfig(otrace.Config{
		DefaultSampler:             otrace.ProbabilitySampler(x.WorkerConfig.Trace.GetFloat64("ratio")),
		MaxAnnotationEventsPerSpan: 256,
	})

	// Posting will initialize index which requires schema. Hence, initialize
	// schema before calling posting.Init().
	schema.Init(worker.State.Pstore)
	posting.Init(worker.State.Pstore, postingListCacheSize)
	defer posting.Cleanup()
	worker.Init(worker.State.Pstore)

	// setup shutdown os signal handler
	sdCh := make(chan os.Signal, 3)

	defer func() {
		signal.Stop(sdCh)
		close(sdCh)
	}()
	// sigint : Ctrl-C, sigterm : kill command.
	signal.Notify(sdCh, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		var numShutDownSig int
		for range sdCh {
			closer := x.ServerCloser
			select {
			case <-closer.HasBeenClosed():
			default:
				closer.Signal()
			}
			numShutDownSig++
			glog.Infoln("Caught Ctrl-C. Terminating now (this may take a few seconds)...")

			switch {
			case atomic.LoadUint32(&initDone) < 2:
				// Forcefully kill alpha if we haven't finish server initialization.
				glog.Infoln("Stopped before initialization completed")
				os.Exit(1)
			case numShutDownSig == 3:
				glog.Infoln("Signaled thrice. Aborting!")
				os.Exit(1)
			}
		}
	}()

	grpcOpts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(x.GrpcMaxSize),
		grpc.MaxSendMsgSize(x.GrpcMaxSize),
		grpc.MaxConcurrentStreams(math.MaxInt32),
		grpc.StatsHandler(&ocgrpc.ServerHandler{}),
	}
	if x.WorkerConfig.TLSServerConfig != nil {
		grpcOpts = append(grpcOpts, grpc.Creds(credentials.NewTLS(x.WorkerConfig.TLSServerConfig)))
	}
	grpcServer := grpc.NewServer(grpcOpts...)
	laddr := "localhost"
	if bindall {
		laddr = "0.0.0.0"
	}
	ln, err := net.Listen("tcp", fmt.Sprintf("%s:%d", laddr, x.WorkerPort()))
	if err != nil {
		log.Fatalf("While running server: %v", err)
	}
	glog.Infof("Worker listening at address: %v", ln.Addr())

	// Register the internal Grpc services.
	conn.Register(grpcServer)
	worker.Register(grpcServer)

	grpcServerCloser := z.NewCloser(1)
	go func() {
		defer grpcServerCloser.Done()

		go func() {
			<-grpcServerCloser.HasBeenClosed()
			grpcServer.Stop()
		}()

		glog.Infof("Starting to serve Grpc connetions")
		err := grpcServer.Serve(ln)
		glog.Errorf("Grpc serve returned with error: %+v", err)
	}()

	updaters := z.NewCloser(2)
	billing.Run(updaters)
	zero.Run(updaters, bindall)

	// StartRaftNodes loads up DQL schema from disk. This is important step
	// that's needed before GraphQL schema can be loaded in setupServer.
	worker.StartRaftNodes(worker.State.WALstore, bindall)
	atomic.AddUint32(&initDone, 1)

	// initialization of the admin account can only be done after raft nodes are running
	// and health check passes
	// edgraph.ResetAcl(updaters)
	// edgraph.RefreshAcls(updaters)

	// This must be called here, because setupServer does DQL queries, which
	// would return error if health check fails.
	x.UpdateHealthStatus(true)
	setupServer()
	atomic.AddUint32(&initDone, 1)
	glog.Infof("Server is ready: OK")

	// The following would block until Ctrl+C.
	x.ServerCloser.Wait()
	glog.Infoln("HTTP stopped.")

	// This might not close until group is given the signal to close. So, only signal here,
	// wait for it after group is closed.
	updaters.Signal()

	edgraph.StopServingQueries()
	worker.BlockingStop()
	glog.Infoln("Worker stopped.")

	audit.Close()

	worker.State.Dispose()
	glog.Info("worker.State disposed.")

	updaters.Wait()
	glog.Infoln("updaters closed.")

	grpcServerCloser.SignalAndWait()
	glog.Infof("Internal Grpc server closed.")

	conn.GetPools().Close()
	glog.Infof("Shutdown all connections.")

	glog.Infoln("Server shutdown. Bye!")
}
