// Portions Copyright 2016-2018 Dgraph Labs, Inc. are available under the Apache License v2.0.
// Portions Copyright 2022 Outcaste LLC are available under the Sustainable License v1.0.

// Package worker contains code for pb.worker communication to perform
// queries and mutations.
package worker

import (
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/outcaste-io/outserv/badger"
	badgerpb "github.com/outcaste-io/outserv/badger/pb"
	"github.com/outcaste-io/outserv/badger/y"
	"github.com/outcaste-io/outserv/posting"
	"github.com/outcaste-io/outserv/protos/pb"
	"github.com/outcaste-io/outserv/x"
	"github.com/pkg/errors"

	"github.com/golang/glog"
	"google.golang.org/grpc"
)

var (
	pstore *badger.DB

	// In case of flaky network connectivity we would try to keep upto maxPendingEntries in wal
	// so that the nodes which have lagged behind leader can just replay entries instead of
	// fetching snapshot if network disconnectivity is greater than the interval at which snapshots
	// are taken
)

// Init initializes this package.
func Init(ps *badger.DB) {
	pstore = ps
	// needs to be initialized after group config
	limiter = rateLimiter{c: sync.NewCond(&sync.Mutex{}), max: int(x.WorkerConfig.Raft.GetInt64("pending-proposals"))}
	go limiter.bleed()
}

// grpcWorker struct implements the gRPC server interface.
type grpcWorker struct {
	sync.Mutex
}

func (w *grpcWorker) Subscribe(
	req *pb.SubscriptionRequest, stream pb.Worker_SubscribeServer) error {
	// Subscribe on given prefixes.
	var matches []badgerpb.Match
	for _, p := range req.GetPrefixes() {
		matches = append(matches, badgerpb.Match{
			Prefix: p,
		})
	}
	for _, m := range req.GetMatches() {
		matches = append(matches, *m)
	}
	return pstore.Subscribe(stream.Context(), func(kvs *badgerpb.KVList) error {
		return stream.Send(kvs)
	}, matches)
}

// RunServer initializes a tcp server on port which listens to requests from
// other workers for pb.communication.
func Register(server *grpc.Server) {
	pb.RegisterWorkerServer(server, &grpcWorker{})
}

// StoreStats returns stats for data store.
func StoreStats() string {
	return "Currently no stats for badger"
}

// BlockingStop stops all the nodes, server between other workers and syncs all marks.
func BlockingStop() {
	glog.Infof("Stopping group...")
	groups().closer.SignalAndWait()

	// Update checkpoint so that proposals are not replayed after the server restarts.
	glog.Infof("Updating RAFT state before shutting down...")
	if err := groups().Node.updateRaftProgress(); err != nil {
		glog.Warningf("Error while updating RAFT progress before shutdown: %v", err)
	}

	glog.Infof("Stopping node...")
	groups().Node.closer.SignalAndWait()

	groups().Node.cdcTracker.Close()
}

// UpdateCacheMb updates the value of cache_mb and updates the corresponding cache sizes.
func UpdateCacheMb(memoryMB int64) error {
	glog.Infof("Updating cacheMb to %d", memoryMB)
	if memoryMB < 0 {
		return errors.Errorf("cache_mb must be non-negative")
	}

	cachePercent, err := x.GetCachePercentages(Config.CachePercentage, 3)
	if err != nil {
		return err
	}
	plCacheSize := (cachePercent[0] * (memoryMB << 20)) / 100
	blockCacheSize := (cachePercent[1] * (memoryMB << 20)) / 100
	indexCacheSize := (cachePercent[2] * (memoryMB << 20)) / 100

	posting.UpdateMaxCost(plCacheSize)
	if _, err := pstore.CacheMaxCost(badger.BlockCache, blockCacheSize); err != nil {
		return errors.Wrapf(err, "cannot update block cache size")
	}
	if _, err := pstore.CacheMaxCost(badger.IndexCache, indexCacheSize); err != nil {
		return errors.Wrapf(err, "cannot update index cache size")
	}

	Config.CacheMb = memoryMB
	return nil
}

// UpdateLogRequest updates value of x.WorkerConfig.LogRequest.
func UpdateLogRequest(val bool) {
	if val {
		atomic.StoreInt32(&x.WorkerConfig.LogRequest, 1)
		return
	}

	atomic.StoreInt32(&x.WorkerConfig.LogRequest, 0)
}

// LogRequestEnabled returns true if logging of requests is enabled otherwise false.
func LogRequestEnabled() bool {
	return atomic.LoadInt32(&x.WorkerConfig.LogRequest) > 0
}

func StoreHandler(w http.ResponseWriter, r *http.Request) {
	x.AddCorsHeaders(w)

	fmt.Fprintln(w, pstore.LevelsToString())
	fmt.Fprintln(w, "---")
	tables := pstore.Tables()
	for i, src := range tables {
		left, right := src.Left, src.Right
		src.Left, src.Right = []byte{}, []byte{}
		numTables := 0
		for _, dst := range tables[i+1:] {
			if dst.Level-src.Level > 1 {
				continue
			}
			if y.CompareKeys(left, dst.Right) > 0 {
				// no overlap
			} else if y.CompareKeys(right, dst.Left) < 0 {
				// no overlap
			} else {
				numTables++
				fmt.Fprintf(w, "%d.%d overlaps with: %d.%d\n",
					src.Level, src.ID, dst.Level, dst.ID)
			}
		}
		fmt.Fprintf(w, "Table: %+v overlaps with %d tables\n", src, numTables)
	}
	fmt.Fprintln(w, "DONE")
}
