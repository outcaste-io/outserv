// Portions Copyright 2016-2018 Dgraph Labs, Inc. are available under the Apache License v2.0.
// Portions Copyright 2022 Outcaste LLC are available under the Smart License v1.0.

package worker

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/golang/glog"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
	"golang.org/x/net/trace"

	ostats "go.opencensus.io/stats"
	"go.opencensus.io/tag"
	otrace "go.opencensus.io/trace"

	"github.com/outcaste-io/badger/v3"
	bpb "github.com/outcaste-io/badger/v3/pb"
	"github.com/outcaste-io/outserv/codec"
	"github.com/outcaste-io/outserv/conn"
	"github.com/outcaste-io/outserv/posting"
	"github.com/outcaste-io/outserv/protos/pb"
	"github.com/outcaste-io/outserv/raftwal"
	"github.com/outcaste-io/outserv/schema"
	"github.com/outcaste-io/outserv/types"
	"github.com/outcaste-io/outserv/x"
	"github.com/outcaste-io/outserv/zero"
	"github.com/outcaste-io/ristretto/z"
	"github.com/outcaste-io/sroar"
)

const (
	sensitiveString = "******"
)

type operation struct {
	*z.Closer
	ts uint64
}

type node struct {
	// This needs to be 64 bit aligned for atomics to work on 32 bit machine.
	pendingSize int64

	// embedded struct
	*conn.Node

	// Fields which are never changed after init.
	applyCh      chan []*pb.Proposal
	concApplyCh  chan *pb.Proposal
	drainApplyCh chan struct{}
	ctx          context.Context
	gid          uint32
	closer       *z.Closer

	checkpointTs uint64 // Timestamp corresponding to checkpoint.
	streaming    int32  // Used to avoid calculating snapshot

	// Used to track the ops going on in the system.
	ops         map[op]operation
	opsLock     sync.Mutex
	cdcTracker  *CDC
	canCampaign bool
	elog        trace.EventLog

	keysWritten *keysWritten
}

type op int

func (id op) String() string {
	switch id {
	case opRollup:
		return "opRollup"
	case opSnapshot:
		return "opSnapshot"
	case opIndexing:
		return "opIndexing"
	case opPredMove:
		return "opPredMove"
	default:
		return "opUnknown"
	}
}

const (
	opRollup op = iota + 1
	opSnapshot
	opIndexing
	opPredMove
)

// startTask is used for the tasks that do not require tracking of timestamp.
// Currently, only the timestamps for backup and indexing needs to be tracked because they can
// run concurrently.
func (n *node) startTask(id op) (*z.Closer, error) {
	return n.startTaskAtTs(id, 0)
}

// startTaskAtTs is used to check whether an op is already running. If a rollup is running,
// it is canceled and startTask will wait until it completes before returning.
// If the same task is already running, this method returns an errror.
// You should only call Done() on the returned closer. Calling other functions (such as
// SignalAndWait) for closer could result in panics. For more details, see GitHub issue #5034.
func (n *node) startTaskAtTs(id op, ts uint64) (*z.Closer, error) {
	n.opsLock.Lock()
	defer n.opsLock.Unlock()

	stopTask := func(id op) {
		n.opsLock.Lock()
		delete(n.ops, id)
		n.opsLock.Unlock()
		glog.Infof("Operation completed with id: %s", id)

		// Resume rollups if another operation is being stopped.
		if id != opRollup {
			time.Sleep(10 * time.Second) // Wait for 10s to start rollup operation.
			// If any other operation is running, this would error out. This error can
			// be safely ignored because rollups will resume once that other task is done.
			_, _ = n.startTask(opRollup)
		}
	}

	closer := z.NewCloser(1)
	switch id {
	case opRollup:
		if len(n.ops) > 0 {
			return nil, errors.Errorf("another operation is already running")
		}
		go posting.IncrRollup.Process(closer)
	case opIndexing:
		for otherId, otherOp := range n.ops {
			switch otherId {
			case opRollup:
				// Remove from map and signal the closer to cancel the operation.
				delete(n.ops, otherId)
				otherOp.SignalAndWait()
			default:
				return nil, errors.Errorf("operation %s is already running", otherId)
			}
		}
	case opSnapshot, opPredMove:
		for otherId, otherOp := range n.ops {
			if otherId == opRollup {
				// Remove from map and signal the closer to cancel the operation.
				delete(n.ops, otherId)
				otherOp.SignalAndWait()
			} else {
				return nil, errors.Errorf("operation %s is already running", otherId)
			}
		}
	default:
		glog.Errorf("Got an unhandled operation %s. Ignoring...", id)
		return nil, nil
	}

	n.ops[id] = operation{Closer: closer, ts: ts}
	glog.Infof("Operation started with id: %s", id)
	go func(id op, closer *z.Closer) {
		closer.Wait()
		stopTask(id)
	}(id, closer)
	return closer, nil
}

func (n *node) stopTask(id op) {
	n.opsLock.Lock()
	closer, ok := n.ops[id]
	n.opsLock.Unlock()
	if !ok {
		return
	}
	closer.SignalAndWait()
}

func (n *node) waitForTask(id op) {
	n.opsLock.Lock()
	closer, ok := n.ops[id]
	n.opsLock.Unlock()
	if !ok {
		return
	}
	closer.Wait()
}

func (n *node) isRunningTask(id op) bool {
	n.opsLock.Lock()
	_, ok := n.ops[id]
	n.opsLock.Unlock()
	return ok
}

func (n *node) stopAllTasks() {
	defer n.closer.Done() // CLOSER:1
	<-n.closer.HasBeenClosed()

	glog.Infof("Stopping all ongoing registered tasks...")
	n.opsLock.Lock()
	defer n.opsLock.Unlock()
	for op, closer := range n.ops {
		glog.Infof("Stopping op: %s...\n", op)
		closer.SignalAndWait()
	}
	glog.Infof("Stopped all ongoing registered tasks.")
}

// GetOngoingTasks returns the list of ongoing tasks.
func GetOngoingTasks() []string {
	n := groups().Node
	if n == nil {
		return []string{}
	}

	n.opsLock.Lock()
	defer n.opsLock.Unlock()
	var tasks []string
	for id := range n.ops {
		tasks = append(tasks, id.String())
	}
	return tasks
}

func (n *node) Ctx(key uint64) context.Context {
	if pctx := n.Proposals.Get(key); pctx != nil {
		return pctx.Ctx
	}
	return context.Background()
}

func (n *node) applyConfChange(e raftpb.Entry) {
	var cc raftpb.ConfChange
	if err := cc.Unmarshal(e.Data); err != nil {
		glog.Errorf("While unmarshalling confchange: %+v", err)
	}

	if cc.Type == raftpb.ConfChangeRemoveNode {
		n.DeletePeer(cc.NodeID)
	} else if len(cc.Context) > 0 {
		var rc pb.RaftContext
		x.Check(rc.Unmarshal(cc.Context))
		n.Connect(rc.Id, rc.Addr)
	}

	cs := n.Raft().ApplyConfChange(cc)
	n.SetConfState(cs)
	n.DoneConfChange(cc.ID, nil)
}

var errHasPendingTxns = errors.New("Pending transactions found. Please retry operation")

// We must not wait here. Previously, we used to block until we have aborted the
// transactions. We're now applying all updates serially, so blocking for one
// operation is not an option.
func detectPendingTxns(attr string) error {
	return nil
	// TODO: Check if we still need this?

	tctxs := posting.Oracle().IterateTxns(func(key []byte) bool {
		pk, err := x.Parse(key)
		if err != nil {
			glog.Errorf("error %v while parsing key %v", err, hex.EncodeToString(key))
			return false
		}
		return pk.Attr == attr
	})
	if len(tctxs) == 0 {
		return nil
	}
	return errHasPendingTxns
}

func emptyMutation(mu *pb.Mutations) bool {
	if len(mu.GetEdges()) > 0 {
		return false
	}
	if len(mu.GetNewObjects()) > 0 {
		return false
	}
	return true
}

func (n *node) mutationWorker(workerId int) {
	handleEntry := func(p *pb.Proposal) {
		x.AssertTrue(p.Key != 0)
		x.AssertTrue(!emptyMutation(p.Mutations))

		ctx := n.Ctx(p.Key)
		x.AssertTrue(ctx != nil)
		span := otrace.FromContext(ctx)
		span.Annotatef(nil, "Executing mutation from worker id: %d", workerId)

		txn := posting.GetTxn(p.CommitTs)
		x.AssertTruef(txn != nil, "Unable to find txn with commit ts: %d", p.CommitTs)
		txn.ErrCh <- n.concMutations(ctx, p.Mutations, txn)
		close(txn.ErrCh)
	}

	for {
		select {
		case mut, ok := <-n.concApplyCh:
			if !ok {
				return
			}
			handleEntry(mut)
		case <-n.closer.HasBeenClosed():
			return
		}
	}
}

func UidsForObject(ctx context.Context, obj *pb.Object) (*sroar.Bitmap, error) {
	var res *sroar.Bitmap
	for _, nq := range obj.Edges {
		if len(nq.ObjectValue) == 0 {
			return nil, fmt.Errorf("Unexpected nil object value for %+v", nq)
		}

		val, err := types.Convert(nq.ObjectValue, types.TypeString)
		if err != nil {
			return nil, errors.Wrapf(err, "encountered an XID %s with invalid val", nq.Predicate)
		}
		valStr := val.Value.(string)
		q := &pb.Query{
			ReadTs: posting.ReadTimestamp(), // What timestamp should we use?
			Attr:   nq.Predicate,
			SrcFunc: &pb.SrcFunction{
				Name: "eq",
				Args: []string{valStr},
			},
			// First: 3, // We can't just ask for the first 3, because we might have
			// to intersect this result with others.
		}
		glog.Infof("running query: %+v\n", q)
		result, err := ProcessTaskOverNetwork(ctx, q)
		if err != nil {
			return nil, errors.Wrapf(err, "while calling ProcessTaskOverNetwork")
		}
		if len(result.UidMatrix) == 0 {
			// No result found
			return sroar.NewBitmap(), nil
		}
		bm := codec.FromList(result.UidMatrix[0])
		if res == nil {
			res = bm
		} else {
			res.And(bm)
		}
		if res.GetCardinality() == 0 {
			return res, nil
		}
	}
	glog.V(2).Infof("Uids for %+v are: %+v\n", obj, res.ToArray())
	return res, nil
}

func (n *node) concMutations(ctx context.Context, m *pb.Mutations, txn *posting.Txn) error {
	// TODO(mrjn): If this concMutation is a retry, should we upgrade the read
	// timestamp then? It would seem logical to do so.
	//
	// Deal with objects first.
	resolved := make(map[string]string)
	for _, obj := range m.NewObjects {
		glog.Infof("Got object: %+v\n", obj)

		// Does the read timestamp need to be passed to UidsForObject?
		bm, err := UidsForObject(ctx, obj)
		if err != nil {
			return errors.Wrapf(err, "when calling UidsForObject")
		}

		card := bm.GetCardinality()
		if card > 1 {
			return fmt.Errorf("Found XID uniqueness violation while processing"+
				" object with var: %q", obj.Var)
		}
		if card == 1 {
			resolved[obj.Var] = x.ToHexString(bm.ToArray()[0])
			// No need to write the attributes corresponding to the object. The
			// object already have these attributes set.
		} else {
			resolved[obj.Var] = x.ToHexString(obj.Uid)
			// Let's put these edges back into the main list to be executed.
			m.Edges = append(m.Edges, obj.Edges...)
		}
	}

	// Replace all the resolved UIDs.
	x.ReplaceUidsIn(m.Edges, resolved)

	span := otrace.FromContext(ctx)
	// Discard the posting lists from cache to release memory at the end.
	defer func() {
		txn.Update(ctx, resolved)
		span.Annotate(nil, "update done")
	}()

	// Update the applied index that we are seeing.
	atomic.CompareAndSwapUint64(&txn.AppliedIndexSeen, 0, n.Applied.DoneUntil())

	// This txn's Zero assigned start ts could be in the future, because we're
	// trying to greedily run mutations concurrently as soon as we see them.
	// In this case, MaxAssignedSeen could be < txn.StartTs. We'd
	// opportunistically do the processing of this mutation anyway. And later,
	// check if everything that we read is still valid, or was it changed. If
	// it was indeed changed, we can re-do the work.

	process := func(edges []*pb.Edge) error {
		var retries int
		for _, edge := range edges {
			for {
				err := runMutation(ctx, edge, txn)
				if err == nil {
					break
				}
				if err != posting.ErrRetry {
					return err
				}
				retries++
			}
		}
		if retries > 0 {
			span.Annotatef(nil, "retries=true num=%d", retries)
		}
		return nil
	}
	numGo, width := x.DivideAndRule(len(m.Edges))
	span.Annotatef(nil, "To apply: %d edges. NumGo: %d. Width: %d", len(m.Edges), numGo, width)

	if numGo == 1 {
		span.Annotate(nil, "Process mutations done.")
		return process(m.Edges)
	}
	errCh := make(chan error, numGo)
	for i := 0; i < numGo; i++ {
		start := i * width
		end := start + width
		if end > len(m.Edges) {
			end = len(m.Edges)
		}
		go func(start, end int) {
			errCh <- process(m.Edges[start:end])
		}(start, end)
	}
	var rerr error
	for i := 0; i < numGo; i++ {
		if err := <-errCh; err != nil && rerr == nil {
			rerr = err
		}
	}
	span.Annotate(nil, "Process mutations done.")
	return rerr
}

// We don't support schema mutations across nodes in a transaction.
// Wait for all transactions to either abort or complete and all write transactions
// involving the predicate are aborted until schema mutations are done.
func (n *node) applyMutations(ctx context.Context, prop *pb.Proposal) (rerr error) {
	span := otrace.FromContext(ctx)

	if prop.Mutations.DropOp == pb.Mutations_DATA {
		ns, err := strconv.ParseUint(prop.Mutations.DropValue, 0, 64)
		if err != nil {
			return err
		}
		// Ensures nothing get written to disk due to commit proposals.
		n.keysWritten.rejectBeforeIndex = prop.Index

		// Stop rollups, otherwise we might end up overwriting some new data.
		n.stopTask(opRollup)
		defer n.startTask(opRollup)

		posting.Oracle().ResetTxnsForNs(ns)
		if err := posting.DeleteData(ns); err != nil {
			return err
		}

		// TODO: Revisit this when we work on posting cache. Clear entire cache.
		// We don't want to drop entire cache, just due to one namespace.
		// posting.ResetCache()
		return nil
	}

	if prop.Mutations.DropOp == pb.Mutations_ALL {
		// Ensures nothing get written to disk due to commit proposals.
		n.keysWritten.rejectBeforeIndex = prop.Index

		// Stop rollups, otherwise we might end up overwriting some new data.
		n.stopTask(opRollup)
		defer n.startTask(opRollup)

		posting.Oracle().ResetTxns()
		schema.State().DeleteAll()

		if err := posting.DeleteAll(); err != nil {
			return err
		}

		// Clear entire cache.
		posting.ResetCache()

		// It should be okay to set the schema at timestamp 1 after drop all operation.
		if groups().groupId() == 1 {
			initialSchema := schema.InitialSchema(x.GalaxyNamespace)
			for _, s := range initialSchema {
				if err := applySchema(s, 1); err != nil {
					return err
				}
			}
		}
		return nil
	}

	if prop.ReadTs == 0 || prop.CommitTs == 0 {
		return errors.New("ReadTs and CommitTs must be provided")
	}

	if len(prop.Mutations.Schema) > 0 {
		n.keysWritten.rejectBeforeIndex = prop.Index

		span.Annotatef(nil, "Applying schema")
		for _, supdate := range prop.Mutations.Schema {
			// We should not need to check for predicate move here.
			if err := detectPendingTxns(supdate.Predicate); err != nil {
				return err
			}
		}

		if err := runSchemaMutation(ctx, prop.Mutations.Schema, prop.CommitTs); err != nil {
			return err
		}

		// Clear the entire cache if there is a schema update because the index rebuild
		// will invalidate the state.
		if len(prop.Mutations.Schema) > 0 {
			posting.ResetCache()
		}
		return nil
	}

	// Scheduler tracks tasks at subject, predicate level, so doing
	// schema stuff here simplies the design and we needn't worry about
	// serializing the mutations per predicate or schema mutations
	// We derive the schema here if it's not present
	// Since raft committed logs are serialized, we can derive
	// schema here without any locking

	// Stores a map of predicate and type of first mutation for each predicate.
	schemaMap := make(map[string]types.TypeID)
	for _, edge := range prop.Mutations.Edges {
		if isDeletePredicateEdge(edge) {
			// We should only drop the predicate if there is no pending
			// transaction.
			if err := detectPendingTxns(edge.Predicate); err != nil {
				span.Annotatef(nil, "Found pending transactions. Retry later.")
				return err
			}
			span.Annotatef(nil, "Deleting predicate: %s", edge.Predicate)
			n.keysWritten.rejectBeforeIndex = prop.Index
			return posting.DeletePredicate(ctx, edge.Predicate, prop.CommitTs)
		}
		// Don't derive schema when doing deletion.
		if edge.Op == pb.Edge_DEL {
			continue
		}
		if _, ok := schemaMap[edge.Predicate]; !ok {
			schemaMap[edge.Predicate] = posting.TypeID(edge)
		}
	}

	total := len(prop.Mutations.Edges)

	// TODO: Active mutations values can go up or down but with
	// OpenCensus stats bucket boundaries start from 0, hence
	// recording negative and positive values skews up values.
	ostats.Record(ctx, x.ActiveMutations.M(int64(total)))
	defer func() {
		ostats.Record(ctx, x.ActiveMutations.M(int64(-total)))
	}()

	// Go through all the predicates and their first observed schema type. If we are unable to find
	// these predicates in the current schema state, add them to the schema state. Note that the
	// schema deduction is done by JSON chunker.
	for attr, storageType := range schemaMap {
		if _, err := schema.State().TypeOf(attr); err != nil {
			if err := createSchema(attr, storageType, prop.CommitTs); err != nil {
				return err
			}
		}
	}

	m := prop.Mutations
	txn := posting.GetTxn(prop.CommitTs)
	x.AssertTruef(txn != nil, "Unable to find txn with commit ts: %d", prop.CommitTs)

	// If we didn't have it in Oracle, then mutation workers won't be processing it either. So,
	// don't block on txn.ErrCh.
	err, ok := <-txn.ErrCh
	x.AssertTrue(ok)
	if err == nil && n.keysWritten.StillValid(txn) {
		span.Annotate(nil, "Mutation is still valid.")
		return nil
	}
	// If mutation is invalid or we got an error, reset the txn, so we can run again.
	txn = posting.Oracle().ResetTxn(prop.CommitTs)

	// If we have an error, re-run this.
	span.Annotatef(nil, "Re-running mutation from applyCh.")
	return n.concMutations(ctx, m, txn)
}

func (n *node) applyCommitted(proposal *pb.Proposal) error {
	key := proposal.Key
	ctx := n.Ctx(key)
	span := otrace.FromContext(ctx)
	span.Annotatef(nil, "node.applyCommitted Node id: %d. Group id: %d. Got proposal key: %d",
		n.Id, n.gid, key)
	if x.Debug {
		glog.Infof("applyCommitted: Proposal: %+v\n", proposal)
	}

	if proposal.Mutations != nil {
		if proposal.ReadTs == 0 {
			glog.Infof("Got a mutation with read ts = 0: %+v\n", proposal)
		}
		proposal.ReadTs = posting.ReadTimestamp()
		x.AssertTrue(proposal.ReadTs > 0)
		// syncmarks for this shouldn't be marked done until it's committed.
		span.Annotate(nil, "Applying mutations")
		if x.Debug {
			glog.Infof("applyCommitted: Proposal: %+v\n", proposal)
		}
		if err := n.applyMutations(ctx, proposal); err != nil {
			span.Annotatef(nil, "While applying mutations: %v", err)
			posting.DeleteTxnWithCommitTs(proposal.CommitTs)
			return err
		}
		if x.Debug {
			glog.Infof("Proposal.CommitTs: %#x\n", proposal.CommitTs)
		}
		if txn := posting.GetTxn(proposal.CommitTs); txn != nil {
			n.commit(txn)
		}

		span.Annotate(nil, "Done")
		return nil
	}

	switch {
	case len(proposal.Kv) > 0:
		return populateKeyValues(ctx, proposal.Kv)

	case len(proposal.CleanPredicate) > 0:
		n.elog.Printf("Cleaning predicate: %s", proposal.CleanPredicate)
		end := time.Now().Add(10 * time.Second)
		for proposal.ExpectedChecksum > 0 && time.Now().Before(end) {
			cur := atomic.LoadUint64(&groups().membershipChecksum)
			if proposal.ExpectedChecksum == cur {
				break
			}
			time.Sleep(100 * time.Millisecond)
			glog.Infof("Waiting for checksums to match. Expected: %d. Current: %d\n",
				proposal.ExpectedChecksum, cur)
		}
		if time.Now().After(end) {
			glog.Warningf(
				"Giving up on predicate deletion: %q due to timeout. Wanted checksum: %d.",
				proposal.CleanPredicate, proposal.ExpectedChecksum)
			return nil
		}
		return posting.DeletePredicate(ctx, proposal.CleanPredicate, proposal.CommitTs)

	case proposal.Snapshot != nil:
		existing, err := n.Store.Snapshot()
		if err != nil {
			return err
		}
		snap := proposal.Snapshot
		if existing.Metadata.Index >= snap.Index {
			log := fmt.Sprintf("Skipping snapshot at %d, because found one at %d",
				snap.Index, existing.Metadata.Index)
			n.elog.Printf(log)
			glog.Info(log)
			return nil
		}
		n.elog.Printf("Creating snapshot: %+v", snap)
		glog.Infof("Creating snapshot at Index: %d, BaseTs: %d\n", snap.Index, snap.BaseTs)

		data, err := snap.Marshal()
		x.Check(err)
		for {
			// We should never let CreateSnapshot have an error.
			err := n.Store.CreateSnapshot(snap.Index, n.ConfState(), data)
			if err == nil {
				break
			}
			glog.Warningf("Error while calling CreateSnapshot: %v. Retrying...", err)
		}
		atomic.StoreInt64(&lastSnapshotTime, time.Now().Unix())
		// We can now discard all invalid versions of keys below this ts.
		pstore.SetDiscardTs(snap.BaseTs)
		return nil

	case proposal.DeleteNs != nil:
		x.AssertTrue(proposal.DeleteNs.Namespace != x.GalaxyNamespace)
		n.elog.Printf("Deleting namespace: %d", proposal.DeleteNs.Namespace)
		return posting.DeleteNamespace(proposal.DeleteNs.Namespace)

	case proposal.CdcState != nil:
		n.cdcTracker.updateCDCState(proposal.CdcState)
		return nil
	}
	x.Fatalf("Unknown proposal: %+v", proposal)
	return nil
}

func (n *node) processTabletSizes() {
	defer n.closer.Done()                   // CLOSER:1
	tick := time.NewTicker(5 * time.Minute) // Once every 5 minutes seems alright.
	defer tick.Stop()

	for {
		select {
		case <-n.closer.HasBeenClosed():
			return
		case <-tick.C:
			n.calculateTabletSizes()
		}
	}
}

func getProposal(e raftpb.Entry) *pb.Proposal {
	p := &pb.Proposal{}
	key := binary.BigEndian.Uint64(e.Data[:8])
	x.Check(p.Unmarshal(e.Data[8:]))
	p.Key = key
	p.Index = e.Index
	p.EntrySize = int64(e.Size())
	return p
}

func propResult(err error) conn.ProposalResult {
	return conn.ProposalResult{Err: err}
}

func (n *node) processApplyCh() {
	defer n.closer.Done() // CLOSER:1

	type P struct {
		err  error
		size int
		seen time.Time
	}
	previous := make(map[uint64]*P)

	// This function must be run serially.
	handle := func(prop pb.Proposal) {
		var perr error
		prev, ok := previous[prop.Key]
		if ok && prev.err == nil {
			msg := fmt.Sprintf("Proposal with key: %d already applied. Skipping index: %d."+
				" Snapshot: %+v.\n", prop.Key, prop.Index, prop.Snapshot)
			n.elog.Printf(msg)
			glog.Infof(msg)
			previous[prop.Key].seen = time.Now() // Update the ts.
			// Don't break here. We still need to call the Done below.

		} else {
			// if this applyCommited fails, how do we ensure
			start := time.Now()

			perr = n.applyCommitted(&prop) // LOGIC is run here.

			if prop.Key != 0 {
				p := &P{err: perr, seen: time.Now()}
				previous[prop.Key] = p
			}
			if perr != nil {
				glog.Errorf("Applying proposal. Error: %v. Proposal: %q.", perr, prop.String())
			}
			n.elog.Printf("Applied proposal with key: %d, index: %d. Err: %v",
				prop.Key, prop.Index, perr)

			var tags []tag.Mutator
			switch {
			case prop.Mutations != nil:
				if len(prop.Mutations.Schema) == 0 {
					// Don't capture schema updates.
					tags = append(tags, tag.Upsert(x.KeyMethod, "apply.Mutations"))
				}
			}
			ms := x.SinceMs(start)
			if err := ostats.RecordWithTags(context.Background(),
				tags, x.LatencyMs.M(ms)); err != nil {
				glog.Errorf("Error recording stats: %+v", err)
			}
		}

		txn := posting.GetTxn(prop.CommitTs)
		n.Proposals.Done(prop.Key, conn.ProposalResult{Err: perr, Data: txn})

		n.Applied.Done(prop.Index)
		posting.DoneTimestamp(prop.CommitTs)
		ostats.Record(context.Background(), x.RaftAppliedIndex.M(int64(n.Applied.DoneUntil())))
	}

	maxAge := 2 * time.Minute
	tick := time.NewTicker(maxAge / 2)
	defer tick.Stop()

	var counter int
	for {
		select {
		case <-n.drainApplyCh:
			numDrained := 0

			var done bool
			for !done {
				select {
				case props := <-n.applyCh:
					numDrained += len(props)
					for _, p := range props {
						n.Proposals.Done(p.Key, propResult(nil))
						n.Applied.Done(p.Index)
					}
				default:
					done = true
				}
			}
			glog.Infof("Drained %d entries. Size of applyCh: %d\n", numDrained, len(n.applyCh))

		case props, ok := <-n.applyCh:
			if !ok {
				return
			}
			var totalSize int64
			for _, p := range props {
				handle(*p) // Run the logic.

				totalSize += p.EntrySize
			}
			if sz := atomic.AddInt64(&n.pendingSize, -totalSize); sz < 0 {
				glog.Warningf("Pending size should remain above zero: %d", sz)
			}

		case <-tick.C:
			// We use this ticker to clear out previous map.
			counter++
			now := time.Now()
			for key, p := range previous {
				if now.Sub(p.seen) > maxAge {
					delete(previous, key)
				}
			}
			n.elog.Printf("Size of previous map: %d", len(previous))

			kw := n.keysWritten
			minStart := posting.Oracle().MinStartTs()
			before := len(kw.keyCommitTs)
			for k, commitTs := range kw.keyCommitTs {
				// If commitTs is less than the min of all pending Txn's StartTs, then we
				// can safely delete the key. StillValid would only consider the commits with ts >
				// min start ts.
				if commitTs < minStart {
					delete(kw.keyCommitTs, k)
				}
			}
			if counter%5 == 0 {
				// Once in 5 minutes.
				glog.V(2).Infof("Still valid: %d Invalid: %d. Size of commit map: %d -> %d."+
					" Total keys written: %d\n",
					kw.validTxns, kw.invalidTxns, before, len(kw.keyCommitTs), kw.totalKeys)
			}
		}
	}
}

func (n *node) commit(txn *posting.Txn) error {
	x.AssertTrue(txn != nil)
	start := time.Now()

	c := txn.Cache()
	c.RLock()
	for k := range c.Deltas() {
		n.keysWritten.keyCommitTs[z.MemHashString(k)] = txn.CommitTs
	}
	numKeys := len(c.Deltas())
	c.RUnlock()

	n.keysWritten.totalKeys += numKeys

	// span.Annotatef(nil, "Num keys: %d\n", numKeys)
	ostats.Record(n.ctx, x.NumEdges.M(int64(numKeys)))

	// This would be used for callback via Badger when skiplist is pushed to
	// disk.
	deleteTxn := func() {
		posting.DeleteTxnAndRollupKeys(txn)
	}

	err := x.RetryUntilSuccess(3600, time.Second, func() error {
		if numKeys == 0 {
			deleteTxn()
			return nil
		}
		// We do the pending txn deletion in the callback, so that our snapshot and checkpoint
		// tracking would only consider the txns which have been successfully pushed to disk.
		return pstore.HandoverSkiplist(txn.Skiplist(), deleteTxn)
	})
	if err != nil {
		glog.Errorf("while handing over skiplist: %v\n", err)
	}
	txn.UpdateCachedKeys()

	// Before, we used to call pstore.Sync() here. We don't need to do that
	// anymore because we're not using Badger's WAL.

	ms := x.SinceMs(start)
	tags := []tag.Mutator{tag.Upsert(x.KeyMethod, "commit")}
	x.Check(ostats.RecordWithTags(context.Background(), tags, x.LatencyMs.M(ms)))
	return nil
}

func (n *node) leaderBlocking() (*conn.Pool, error) {
	if _, err := zero.LatestMembershipState(n.ctx); err != nil {
		return nil, errors.Wrapf(err, "while getting latest membership state")
	}
	pool := groups().Leader(groups().groupId())
	if pool == nil {
		return nil, errors.Errorf("Unable to reach leader in group %d", n.gid)
	}
	return pool, nil
}

func (n *node) Snapshot() (*pb.Snapshot, error) {
	if n == nil || n.Store == nil {
		return nil, conn.ErrNoNode
	}
	snap, err := n.Store.Snapshot()
	if err != nil {
		return nil, err
	}
	res := &pb.Snapshot{}
	if err := res.Unmarshal(snap.Data); err != nil {
		return nil, err
	}
	return res, nil
}

func (n *node) retrieveSnapshot(snap pb.Snapshot) error {
	closer, err := n.startTask(opSnapshot)
	if err != nil {
		return err
	}
	defer closer.Done()

	// In some edge cases, the Zero leader might not have been able to update
	// the status of Alpha leader. So, instead of blocking forever on waiting
	// for Zero to send us the updates info about the leader, we can just use
	// the Snapshot RaftContext, which contains the address of the leader.
	var pool *conn.Pool
	addr := snap.Context.GetAddr()
	glog.V(2).Infof("Snapshot.RaftContext.Addr: %q", addr)
	if len(addr) > 0 {
		p, err := conn.GetPools().Get(addr)
		if err != nil {
			glog.V(2).Infof("conn.Get(%q) Error: %v", addr, err)
		} else {
			pool = p
			glog.V(2).Infof("Leader connection picked from RaftContext")
		}
	}
	if pool == nil {
		glog.V(2).Infof("No leader conn from RaftContext. Using membership state.")
		p, err := n.leaderBlocking()
		if err != nil {
			return err
		}
		pool = p
	}

	// Need to clear pl's stored in memory for the case when retrieving snapshot with
	// index greater than this node's last index
	// Should invalidate/remove pl's to this group only ideally
	//
	// We can safely evict posting lists from memory. Because, all the updates corresponding to txn
	// commits up until then have already been written to pstore. And the way we take snapshots, we
	// keep all the pre-writes for a pending transaction, so they will come back to memory, as Raft
	// logs are replayed.
	if err := n.populateSnapshot(snap, pool); err != nil {
		return errors.Wrapf(err, "cannot retrieve snapshot from peer")
	}
	// Populate shard stores the streamed data directly into db, so we need to refresh
	// schema for current group id
	if err := schema.LoadFromDb(); err != nil {
		return errors.Wrapf(err, "while initializing schema")
	}
	groups().triggerMembershipSync()
	return nil
}

func (n *node) proposeCDCState(ts uint64) error {
	proposal := &pb.Proposal{
		CdcState: &pb.CDCState{
			SentTs: ts,
		},
	}
	glog.V(2).Infof("Proposing new CDC state ts: %d\n", ts)
	data := make([]byte, 8+proposal.Size())
	sz, err := proposal.MarshalToSizedBuffer(data[8:])
	data = data[:8+sz]
	x.Check(err)
	return n.Raft().Propose(n.ctx, data)
}

func (n *node) proposeSnapshot() error {
	lastIdx := x.Min(n.Applied.DoneUntil(), n.cdcTracker.getSeenIndex())
	// We can't rely upon the Raft entries to determine the minPendingStart,
	// because there are many cases during mutations where we don't commit or
	// abort the transaction. This might happen due to an early error thrown.
	// Only the mutations which make it to Zero for a commit/abort decision have
	// corresponding Delta entries. So, instead of replicating all that logic
	// here, we just use the MinPendingStartTs tracked by the Oracle, and look
	// for that in the logs.
	//
	// So, we iterate over logs. If we hit MinPendingStartTs, that generates our
	// snapshotIdx. In any case, we continue picking up txn updates, to generate
	// a maxCommitTs, which would become the readTs for the snapshot.
	minPendingStart := n.cdcTracker.getTs()
	snap, err := n.calculateSnapshot(0, lastIdx, minPendingStart)
	if err != nil {
		return err
	}
	if snap == nil {
		return nil
	}
	// proposeSnapshot is only called if we need to run a snapshot. So, we don't
	// need to check if we have sufficient entries here. See checkpointAndClose.
	proposal := &pb.Proposal{
		Snapshot: snap,
	}
	glog.V(2).Infof("Proposing snapshot: %+v. BaseTs: %#x\n", snap, snap.BaseTs)
	data := make([]byte, 8+proposal.Size())
	sz, err := proposal.MarshalToSizedBuffer(data[8:])
	data = data[:8+sz]
	x.Check(err)
	return n.Raft().Propose(n.ctx, data)
}

const (
	maxPendingSize int64 = 256 << 20 // in bytes.
	nodeApplyChan        = "pushing to raft node applyCh"
)

func rampMeter(address *int64, maxSize int64, component string) {
	start := time.Now()
	defer func() {
		if dur := time.Since(start); dur > time.Second {
			glog.Infof("Blocked %s for %v", component, dur.Round(time.Millisecond))
		}
	}()
	for {
		if atomic.LoadInt64(address) <= maxSize {
			return
		}
		time.Sleep(3 * time.Millisecond)
	}
}

func (n *node) updateRaftProgress() error {
	// Both leader and followers can independently update their Raft progress. We don't store
	// this in Raft WAL. Instead, this is used to just skip over log records that this Alpha
	// has already applied, to speed up things on a restart.
	//
	// Let's check what we already have. And only update if the new snap.Index is ahead of the last
	// stored applied.
	applied := n.Store.Uint(raftwal.CheckpointIndex)

	snap, err := n.calculateSnapshot(applied, n.Applied.DoneUntil(), math.MaxUint64)
	if err != nil || snap == nil || snap.Index <= applied {
		return err
	}
	atomic.StoreUint64(&n.checkpointTs, snap.BaseTs)

	n.Store.SetUint(raftwal.CheckpointIndex, snap.GetIndex())
	glog.V(2).Infof("[%#x] Set Raft checkpoint to index: %d, ts: %#x.",
		n.Id, snap.Index, snap.BaseTs)
	return nil
}

// proposeBaseTimestamp proposes a base timestamp to use across all replicas.
// This ensures that the commit timestamps used by all replicas are exactly the
// same.
func (n *node) proposeBaseTimestamp() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	propose := func() {
		if !n.AmLeader() {
			return
		}
		now := uint64(time.Now().Unix())
		now = now << 32 // Make space for lower 32 bits.
		prop := &pb.Proposal{BaseTimestamp: now}
		err := x.RetryUntilSuccess(10, time.Second, func() error {
			_, err := n.proposeAndWait(n.closer.Ctx(), prop)
			return err
		})
		if err != nil {
			glog.Warningf("Unable to propose base timestamp: %v", err)
		}
	}

	propose() // Immediately after start.
	for {
		select {
		case <-ticker.C:
			propose()

		case <-n.closer.HasBeenClosed():
			glog.Infof("Returning from proposeBaseTimestamp")
			return
		}
	}
}

var lastSnapshotTime int64 = time.Now().Unix()

func (n *node) checkpointAndClose(done chan struct{}) {
	defer glog.Infof("Returning from checkpointAndClose")

	snapshotAfterEntries := x.WorkerConfig.Raft.GetUint64("snapshot-after-entries")
	x.AssertTruef(snapshotAfterEntries > 10, "raft.snapshot-after must be a number greater than 10")

	slowTicker := time.NewTicker(time.Minute)
	defer slowTicker.Stop()

	exceededSnapshotByEntries := func() bool {
		if snapshotAfterEntries == 0 {
			// If snapshot-after isn't set, return true always.
			return true
		}
		chk, err := n.Store.Checkpoint()
		if err != nil {
			glog.Errorf("While reading checkpoint: %v", err)
			return false
		}
		first, err := n.Store.FirstIndex()
		if err != nil {
			glog.Errorf("While reading first index: %v", err)
			return false
		}
		// If we're over snapshotAfterEntries, calculate would be true.
		glog.V(3).Infof("Evaluating snapshot first:%d chk:%d (chk-first:%d) "+
			"snapshotAfterEntries:%d", first, chk, chk-first,
			snapshotAfterEntries)
		return chk-first >= snapshotAfterEntries
	}

	snapshotFrequency := x.WorkerConfig.Raft.GetDuration("snapshot-after-duration")
	for {
		select {
		case <-slowTicker.C:
			// Do these operations asynchronously away from the main Run loop to allow heartbeats to
			// be sent on time. Otherwise, followers would just keep running elections.

			n.elog.Printf("Size of applyCh: %d", len(n.applyCh))
			if err := n.updateRaftProgress(); err != nil {
				glog.Errorf("While updating Raft progress: %v", err)
			}

			if n.AmLeader() {
				// If leader doesn't have a snapshot, we should create one immediately. This is very
				// useful when you bring up the cluster from bulk loader. If you remove an alpha and
				// add a new alpha, the new follower won't get a snapshot if the leader doesn't have
				// one.
				snap, err := n.Store.Snapshot()
				if err != nil {
					glog.Errorf("While retrieving snapshot from Store: %v\n", err)
					continue
				}

				// calculate would be true if:
				// - snapshot is empty [#0]
				// - we have more than 4 log files in Raft WAL [#0]
				//
				// If snapshot entries is set (no frequency):
				// - Just use entries [#1]
				//
				// If snapshot frequency is set (no entries):
				// - Just use frequency based threshold time [#2]
				//
				// If both entries and frequency is set:
				// - Take a snapshot after BOTH time and entries are exceeded [#3]
				//
				// Note: In case we're exceeding threshold entries, but have not exceeded the
				// threshold time since last snapshot, calculate would be false.
				calculate := raft.IsEmptySnap(snap) || n.Store.NumLogFiles() > 4 // #0
				lastSnapTime := time.Unix(atomic.LoadInt64(&lastSnapshotTime), 0)
				if snapshotFrequency == 0 {
					calculate = calculate || exceededSnapshotByEntries() // #1

				} else if time.Since(lastSnapTime) > snapshotFrequency {
					// If we haven't taken a snapshot since snapshotFrequency, calculate would
					// follow snapshot entries.
					calculate = calculate || exceededSnapshotByEntries() // #2, #3
				}

				// We keep track of the applied index in the p directory. Even if we don't take
				// snapshot for a while and let the Raft logs grow and restart, we would not have to
				// run all the log entries, because we can tell Raft.Config to set Applied to that
				// index.
				// This applied index tracking also covers the case when we have a big index
				// rebuild. The rebuild would be tracked just like others and would not need to be
				// replayed after a restart, because the Applied config would let us skip right
				// through it.
				// We use disk based storage for Raft. So, we're not too concerned about
				// snapshotting.  We just need to do enough, so that we don't have a huge backlog of
				// entries to process on a restart.
				if calculate {
					// We can set discardN argument to zero, because we already know that calculate
					// would be true if either we absolutely needed to calculate the snapshot,
					// or our checkpoint already crossed the SnapshotAfter threshold.
					if err := n.proposeSnapshot(); err != nil {
						glog.Errorf("While calculating and proposing snapshot: %v", err)
					} else {
						atomic.StoreInt64(&lastSnapshotTime, time.Now().Unix())
					}
				}
			}

		case <-n.closer.HasBeenClosed():
			glog.Infof("Stopping checkpointAndClose")
			if peerId, has := groups().MyPeer(); has && n.AmLeader() {
				n.Raft().TransferLeadership(n.ctx, n.Id, peerId)
				time.Sleep(time.Second) // Let transfer happen.
			}
			n.Raft().Stop()
			close(done)
			return
		}
	}
}

const tickDur = 100 * time.Millisecond

func (n *node) Run() {
	defer n.closer.Done() // CLOSER:1

	// lastLead is for detecting leadership changes
	//
	// etcd has a similar mechanism for tracking leader changes, with their
	// raftReadyHandler.getLead() function that returns the previous leader
	lastLead := uint64(math.MaxUint64)

	firstRun := true
	var leader bool
	// See also our configuration of HeartbeatTick and ElectionTick.
	// Before we used to have 20ms ticks, but they would overload the Raft tick channel, causing
	// "tick missed to fire" logs. Etcd uses 100ms and they haven't seen those issues.
	// Additionally, using 100ms for ticks does not cause proposals to slow down, because they get
	// sent out asap and don't rely on ticks. So, setting this to 100ms instead of 20ms is a NOOP.
	ticker := time.NewTicker(tickDur)
	defer ticker.Stop()

	done := make(chan struct{})
	go n.checkpointAndClose(done)
	go n.ReportRaftComms("alpha", n.closer)
	go n.EnsureRegularityOfTicks("alpha", tickDur, n.closer)
	go n.proposeBaseTimestamp()

	if !x.WorkerConfig.HardSync {
		closer := z.NewCloser(2)
		defer closer.SignalAndWait()
		go x.StoreSync(n.Store, closer)
		go x.StoreSync(pstore, closer)
	}

	applied, err := n.Store.Checkpoint()
	if err != nil {
		glog.Errorf("While trying to find raft progress: %v", err)
	} else {
		glog.Infof("Found Raft checkpoint: %d", applied)
	}

	snap, err := n.Snapshot()
	if err != nil {
		glog.Errorf("While trying to find snapshot: %v", err)
	} else {
		glog.Infof("Found snapshot: %+v. BaseTs: %#x", snap, snap.BaseTs)
	}

	baseTimestamp := snap.BaseTs
	posting.InitApplied(baseTimestamp)

	var timer x.Timer
	for {
		select {
		case <-done:
			// We use done channel here instead of closer.HasBeenClosed so that we can transfer
			// leadership in a goroutine. The push to n.applyCh happens in this loop, so the close
			// should happen here too. Otherwise, race condition between push and close happens.
			close(n.applyCh)
			glog.Infoln("Raft node done.")
			return

			// Slow ticker can't be placed here because figuring out checkpoints and snapshots takes
			// time and if the leader does not send heartbeats out during this time, the followers
			// start an election process. And that election process would just continue to happen
			// indefinitely because checkpoints and snapshots are being calculated indefinitely.
		case <-ticker.C:
			n.Tick()

		case rd := <-n.Raft().Ready():
			timer.Start()
			_, span := otrace.StartSpan(n.ctx, "Alpha.RunLoop",
				otrace.WithSampler(otrace.ProbabilitySampler(0.001)))

			if rd.SoftState != nil {
				groups().triggerMembershipSync()
				leader = rd.RaftState == raft.StateLeader
				// create context with group id
				ctx, _ := tag.New(n.ctx, tag.Upsert(x.KeyGroup, fmt.Sprintf("%d", n.gid)))
				// detect leadership changes
				if rd.SoftState.Lead != lastLead {
					lastLead = rd.SoftState.Lead
					ostats.Record(ctx, x.RaftLeaderChanges.M(1))
				}
				if rd.SoftState.Lead != raft.None {
					ostats.Record(ctx, x.RaftHasLeader.M(1))
				} else {
					ostats.Record(ctx, x.RaftHasLeader.M(0))
				}
				if leader {
					ostats.Record(ctx, x.RaftIsLeader.M(1))
				} else {
					ostats.Record(ctx, x.RaftIsLeader.M(0))
				}
			}
			if leader {
				// Leader can send messages in parallel with writing to disk.
				for i := range rd.Messages {
					// NOTE: We can do some optimizations here to drop messages.
					n.Send(&rd.Messages[i])
				}
			}
			if span != nil {
				span.Annotate(nil, "Handled ReadStates and SoftState.")
			}

			// We move the retrieval of snapshot before we store the rd.Snapshot, so that in case
			// this node fails to get the snapshot, the Raft state would reflect that by not having
			// the snapshot on a future probe. This is different from the recommended order in Raft
			// docs where they assume that the Snapshot contains the full data, so even on a crash
			// between n.SaveToStorage and n.retrieveSnapshot, that Snapshot can be applied by the
			// node on a restart. In our case, we don't store the full data in snapshot, only the
			// metadata.  So, we should only store the snapshot received in Raft, iff we actually
			// were able to update the state.
			if !raft.IsEmptySnap(rd.Snapshot) {
				// We don't send snapshots to other nodes. But, if we get one, that means
				// either the leader is trying to bring us up to state; or this is the
				// snapshot that I created. Only the former case should be handled.
				var snap pb.Snapshot
				x.Check(snap.Unmarshal(rd.Snapshot.Data))
				rc := snap.GetContext()
				x.AssertTrue(rc.GetGroup() == n.gid)
				if rc.Id != n.Id {
					// Set node to unhealthy state here while it applies the snapshot.
					x.UpdateHealthStatus(false)

					// We are getting a new snapshot from leader. We need to wait for the applyCh to
					// finish applying the updates, otherwise, we'll end up overwriting the data
					// from the new snapshot that we retrieved.

					// Drain the apply channel. Snapshot will be retrieved next.
					maxIndex := n.Applied.LastIndex()
					glog.Infof("Drain applyCh by reaching %d before"+
						" retrieving snapshot\n", maxIndex)
					n.drainApplyCh <- struct{}{}

					if err := n.Applied.WaitForMark(context.Background(), maxIndex); err != nil {
						glog.Errorf("Error waiting for mark for index %d: %+v", maxIndex, err)
					}

					if currSnap, err := n.Snapshot(); err != nil {
						// Retrieve entire snapshot from leader if node does not have
						// a current snapshot.
						glog.Errorf("Could not retrieve previous snapshot. Setting SinceTs to 0.")
						snap.SinceTs = 0
					} else {
						snap.SinceTs = currSnap.BaseTs
					}

					// It's ok to block ticks while retrieving snapshot, since it's a follower.
					glog.Infof("---> SNAPSHOT: %+v. Group %d from node id %#x\n",
						snap, n.gid, rc.Id)

					for {
						err := n.retrieveSnapshot(snap)
						if err == nil {
							glog.Infoln("---> Retrieve snapshot: OK.")
							break
						}
						glog.Errorf("While retrieving snapshot, error: %v. Retrying...", err)
						time.Sleep(time.Second) // Wait for a bit.
					}
					glog.Infof("---> SNAPSHOT: %+v. Group %d. DONE.\n", snap, n.gid)

					// Set node to healthy state here.
					x.UpdateHealthStatus(true)
				} else {
					glog.Infof("---> SNAPSHOT: %+v. Group %d from node id %#x [SELF]. Ignoring.\n",
						snap, n.gid, rc.Id)
				}
				if span != nil {
					span.Annotate(nil, "Applied or retrieved snapshot.")
				}
			}

			// Store the hardstate and entries. Note that these are not CommittedEntries.
			n.SaveToStorage(&rd.HardState, rd.Entries, &rd.Snapshot)
			timer.Record("disk")
			if span != nil {
				span.Annotatef(nil, "Saved %d entries. Snapshot, HardState empty? (%v, %v)",
					len(rd.Entries),
					raft.IsEmptySnap(rd.Snapshot),
					raft.IsEmptyHardState(rd.HardState))
			}
			for x.WorkerConfig.HardSync && rd.MustSync {
				if err := n.Store.Sync(); err != nil {
					glog.Errorf("Error while calling Store.Sync: %+v", err)
					time.Sleep(10 * time.Millisecond)
					continue
				}
				timer.Record("sync")
				break
			}

			// Now schedule or apply committed entries.
			var entries []raftpb.Entry
			for _, entry := range rd.CommittedEntries {
				// Need applied watermarks for schema mutation also for read linearazibility
				// Applied watermarks needs to be emitted as soon as possible sequentially.
				// If we emit Mark{4, false} and Mark{4, true} before emitting Mark{3, false}
				// then doneUntil would be set as 4 as soon as Mark{4,true} is done and before
				// Mark{3, false} is emitted. So it's safer to emit watermarks as soon as
				// possible sequentially
				n.Applied.Begin(entry.Index)

				switch {
				case entry.Type == raftpb.EntryConfChange:
					n.applyConfChange(entry)
					// Not present in proposal map.
					n.Applied.Done(entry.Index)
					groups().triggerMembershipSync()
				case len(entry.Data) == 0:
					n.elog.Printf("Found empty data at index: %d", entry.Index)
					n.Applied.Done(entry.Index)
				case entry.Index < applied:
					n.elog.Printf("Skipping over already applied entry: %d", entry.Index)
					n.Applied.Done(entry.Index)
				default:
					key := binary.BigEndian.Uint64(entry.Data[:8])
					if pctx := n.Proposals.Get(key); pctx != nil {
						atomic.AddUint32(&pctx.Found, 1)
						if span := otrace.FromContext(pctx.Ctx); span != nil {
							span.Annotate(nil, "Proposal found in CommittedEntries")
						}
					}
					entries = append(entries, entry)
				}
			}
			// Send the whole lot to applyCh in one go, instead of sending proposals one by one.
			if len(entries) > 0 {
				// Apply the meter this before adding size to pending size so some crazy big
				// proposal can be pushed to applyCh. If we do this after adding its size to
				// pending size, we could block forever in rampMeter.
				rampMeter(&n.pendingSize, maxPendingSize, nodeApplyChan)
				var pendingSize int64
				for _, e := range entries {
					pendingSize += int64(e.Size())
				}
				if sz := atomic.AddInt64(&n.pendingSize, pendingSize); sz > 2*maxPendingSize {
					glog.Warningf("Inflight proposal size: %d. There would be some throttling.", sz)
				}

				readTs := posting.ReadTimestamp()

				var props []*pb.Proposal
				for _, e := range entries {
					p := getProposal(e)
					if p.BaseTimestamp > 0 {
						if x.Debug {
							glog.V(2).Infof("Setting base timestamp to: %#x for index: %d\n",
								p.BaseTimestamp, e.Index)
						}
						baseTimestamp = p.BaseTimestamp
						n.Proposals.Done(p.Key, propResult(nil))
						n.Applied.Done(p.Index)
						continue
					}

					p.ReadTs = readTs
					p.CommitTs = x.Timestamp(baseTimestamp, e.Index)
					// glog.Infof("e.Index: %d commit Ts: %#x\n", e.Index, p.CommitTs)

					props = append(props, p)

					if emptyMutation(p.Mutations) {
						glog.Infof("Continuing for mutation: %+v\n", p.Mutations)
						continue
					}
					var skip bool
					for _, e := range p.Mutations.GetEdges() {
						// This is a drop predicate mutation. We should not try to execute it
						// concurrently.
						if isDeletePredicateEdge(e) {
							skip = true
							break
						}
					}
					if skip {
						continue
					}
					// We should register this txn before sending it over for concurrent
					// application.
					txn := posting.RegisterTxn(p.ReadTs, p.CommitTs)
					if x.Debug {
						glog.Infof("RegisterTxn ts: %d commit ts: %d txn: %p. mutation: %+v\n",
							p.ReadTs, p.CommitTs, txn, p.Mutations)
					}

					n.concApplyCh <- p
				}
				n.applyCh <- props
			}

			if span != nil {
				span.Annotatef(nil, "Handled %d committed entries.", len(rd.CommittedEntries))
			}

			if !leader {
				// Followers should send messages later.
				for i := range rd.Messages {
					// NOTE: We can do some optimizations here to drop messages.
					n.Send(&rd.Messages[i])
				}
			}
			if span != nil {
				span.Annotate(nil, "Followed queued messages.")
			}
			timer.Record("proposals")

			n.Raft().Advance()
			timer.Record("advance")

			if firstRun && n.canCampaign {
				go func() {
					if err := n.Raft().Campaign(n.ctx); err != nil {
						glog.Errorf("Error starting campaign for node %v: %+v", n.gid, err)
					}
				}()
				firstRun = false
			}
			if span != nil {
				span.Annotate(nil, "Advanced Raft. Done.")
				span.End()
				if err := ostats.RecordWithTags(context.Background(),
					[]tag.Mutator{tag.Upsert(x.KeyMethod, "alpha.RunLoop")},
					x.LatencyMs.M(float64(timer.Total())/1e6)); err != nil {
					glog.Errorf("Error recording stats: %+v", err)
				}
			}
			if timer.Total() > 5*tickDur {
				glog.Warningf(
					"Raft.Ready took too long to process: %s"+
						" Num entries: %d. MustSync: %v",
					timer.String(), len(rd.Entries), rd.MustSync)
			}
		}
	}
}

func listWrap(kv *bpb.KV) *bpb.KVList {
	return &bpb.KVList{Kv: []*bpb.KV{kv}}
}

// calculateTabletSizes updates the tablet sizes for the keys.
func (n *node) calculateTabletSizes() {
	if !n.AmLeader() {
		// Only leader sends the tablet size updates to Zero. No one else does.
		return
	}
	var total int64
	tablets := make(map[string]*pb.Tablet)
	updateSize := func(tinfo badger.TableInfo) {
		// The error has already been checked by caller.
		left, _ := x.Parse(tinfo.Left)
		pred := left.Attr
		if pred == "" {
			return
		}
		if tablet, ok := tablets[pred]; ok {
			tablet.OnDiskBytes += int64(tinfo.OnDiskSize)
			tablet.UncompressedBytes += int64(tinfo.UncompressedSize)
		} else {
			tablets[pred] = &pb.Tablet{
				GroupId:           n.gid,
				Predicate:         pred,
				OnDiskBytes:       int64(tinfo.OnDiskSize),
				UncompressedBytes: int64(tinfo.UncompressedSize),
			}
		}
		total += int64(tinfo.OnDiskSize)
	}

	tableInfos := pstore.Tables()
	glog.V(2).Infof("Calculating tablet sizes. Found %d tables\n", len(tableInfos))
	for _, tinfo := range tableInfos {
		left, err := x.Parse(tinfo.Left)
		if err != nil {
			glog.V(3).Infof("Unable to parse key: %v", err)
			continue
		}
		right, err := x.Parse(tinfo.Right)
		if err != nil {
			glog.V(3).Infof("Unable to parse key: %v", err)
			continue
		}

		// Count the table only if it is occupied by a single predicate.
		if left.Attr == right.Attr {
			updateSize(tinfo)
		} else {
			glog.V(3).Info("Skipping table not owned by one predicate")
		}
	}

	if len(tablets) == 0 {
		glog.V(2).Infof("No tablets found.")
		return
	}
	// Update Zero with the tablet sizes. If Zero sees a tablet which does not belong to
	// this group, it would send instruction to delete that tablet. There's an edge case
	// here if the followers are still running Rollup, and happen to read a key before and
	// write after the tablet deletion, causing that tablet key to resurface. Then, only the
	// follower would have that key, not the leader.
	// However, if the follower then becomes the leader, we'd be able to get rid of that
	// key then. Alternatively, we could look into cancelling the Rollup if we see a
	// predicate deletion.
	if err := groups().doSendMembership(tablets); err != nil {
		glog.Warningf("While sending membership to Zero. Error: %v", err)
	} else {
		glog.V(2).Infof("Sent tablet size update to Zero. Total size: %s",
			humanize.Bytes(uint64(total)))
	}
}

var errNoConnection = errors.New("No connection exists")

// calculateSnapshot would calculate a snapshot index, considering these factors:
// - We only start discarding once we have at least discardN entries.
// - We are not overshooting the max applied entry. That is, we're not removing
// Raft entries before they get applied.
func (n *node) calculateSnapshot(startIdx, lastIdx, lastCommitTs uint64) (*pb.Snapshot, error) {
	_, span := otrace.StartSpan(n.ctx, "Calculate.Snapshot",
		otrace.WithSampler(otrace.AlwaysSample()))
	defer span.End()
	discardN := 1

	// We do not need to block snapshot calculation because of a pending stream. Badger would have
	// pending iterators which would ensure that the data above their read ts would not be
	// discarded. Secondly, if a new snapshot does get calculated and applied, the follower can just
	// ask for the new snapshot. Blocking snapshot calculation has caused us issues when a follower
	// somehow kept streaming forever. Then, the leader didn't calculate snapshot, instead it
	// kept appending to Raft logs forever causing group wide issues.

	first, err := n.Store.FirstIndex()
	if err != nil {
		span.Annotatef(nil, "Error: %v", err)
		return nil, err
	}
	span.Annotatef(nil, "First index: %d", first)
	if startIdx > first {
		// If we're starting from a higher index, set first to that.
		first = startIdx
		span.Annotatef(nil, "Setting first to: %d", startIdx)
	}

	rsnap, err := n.Store.Snapshot()
	if err != nil {
		return nil, err
	}
	var snap pb.Snapshot
	if len(rsnap.Data) > 0 {
		if err := snap.Unmarshal(rsnap.Data); err != nil {
			return nil, err
		}
	}
	span.Annotatef(nil, "Last snapshot: %+v", snap)

	if lastIdx < first || int(lastIdx-first) < discardN {
		span.Annotate(nil, "Skipping due to insufficient entries")
		return nil, nil
	}
	span.Annotatef(nil, "Found Raft entries: %d", lastIdx-first)

	if num := posting.Oracle().NumPendingTxns(); num > 0 {
		glog.V(2).Infof("Num pending txns: %d", num)
	}

	baseTs := snap.BaseTs
	var snapshotIdx uint64

	// Trying to retrieve all entries at once might cause out-of-memory issues in
	// cases where the raft log is too big to fit into memory. Instead of retrieving
	// all entries at once, retrieve it in batches of 64MB.
	var lastEntry raftpb.Entry
	for batchFirst := first; batchFirst <= lastIdx; {
		entries, err := n.Store.Entries(batchFirst, lastIdx+1, 256<<20)
		if err != nil {
			span.Annotatef(nil, "Error: %v", err)
			return nil, err
		}
		// Exit early from the loop if no entries were found.
		if len(entries) == 0 {
			break
		}

		// Store the last entry (as it might be needed outside the loop) and set the
		// start of the new batch at the entry following it. Also set foundEntries to
		// true to indicate to the code outside the loop that entries were retrieved.
		lastEntry = entries[len(entries)-1]
		batchFirst = lastEntry.Index + 1

		for _, entry := range entries {
			cts := x.Timestamp(baseTs, entry.Index)
			if cts > lastCommitTs {
				break
			}
			snapshotIdx = entry.Index

			if entry.Type != raftpb.EntryNormal || len(entry.Data) == 0 {
				continue
			}
			proposal := getProposal(entry)
			if proposal.BaseTimestamp > 0 {
				baseTs = proposal.BaseTimestamp
				continue
			}
		}
	}

	if baseTs == 0 || snapshotIdx == 0 {
		span.Annotate(nil, "maxCommitTs or snapshotIdx is zero")
		return nil, nil
	}

	numDiscarding := snapshotIdx - first + 1
	span.Annotatef(nil,
		"Got snapshotIdx: %d. baseTs: %#x. Discarding: %d. lastCommitTs: %d",
		snapshotIdx, baseTs, numDiscarding, lastCommitTs)

	if int(numDiscarding) < discardN {
		span.Annotate(nil, "Skipping snapshot because insufficient discard entries")
		glog.Infof("Skipping snapshot at index: %d. Insufficient discard entries: %d."+
			" baseTs: %#x\n", snapshotIdx, numDiscarding, baseTs)
		return nil, nil
	}

	result := &pb.Snapshot{
		Context: n.RaftContext,
		Index:   snapshotIdx,
		BaseTs:  baseTs,
	}
	span.Annotatef(nil, "Got snapshot: %+v", result)
	return result, nil
}

func (n *node) joinPeers() error {
	pl, err := n.leaderBlocking()
	if err != nil {
		return err
	}

	c := pb.NewRaftClient(pl.Get())
	glog.Infof("Calling JoinCluster via leader: %s", pl.Addr)
	if _, err := c.JoinCluster(n.ctx, n.RaftContext); err != nil {
		return errors.Wrapf(err, "error while joining cluster")
	}
	glog.Infof("Done with JoinCluster call\n")
	return nil
}

// Checks if its a peer from the leader of the group.
func (n *node) isMember() (bool, error) {
	pl, err := n.leaderBlocking()
	if err != nil {
		return false, err
	}

	gconn := pl.Get()
	c := pb.NewRaftClient(gconn)
	glog.Infof("Calling IsPeer")
	pr, err := c.IsPeer(n.ctx, n.RaftContext)
	if err != nil {
		return false, errors.Wrapf(err, "error while joining cluster")
	}
	glog.Infof("Done with IsPeer call\n")
	return pr.Status, nil
}

func (n *node) retryUntilSuccess(fn func() error, pause time.Duration) {
	var err error
	for {
		if err = fn(); err == nil {
			break
		}
		glog.Errorf("Error while calling fn: %v. Retrying...\n", err)
		time.Sleep(pause)
	}
}

// InitAndStartNode gets called after having at least one membership sync with the cluster.
func (n *node) InitAndStartNode() {
	x.Check(initProposalKey(n.Id))
	_, restart, err := n.PastLife()
	x.Check(err)

	_, hasPeer := groups().MyPeer()
	if !restart && hasPeer {
		// The node has other peers, it might have crashed after joining the cluster and before
		// writing a snapshot. Check from leader, if it is part of the cluster. Consider this a
		// restart if it is part of the cluster, else start a new node.
		for {
			if restart, err = n.isMember(); err == nil {
				break
			}
			glog.Errorf("Error while calling hasPeer: %v. Retrying...\n", err)
			time.Sleep(time.Second)
		}
	}

	if n.RaftContext.IsLearner && !hasPeer {
		glog.Fatal("Cannot start a learner node without peer alpha nodes")
	}

	if restart {
		glog.Infof("Restarting node for group: %d\n", n.gid)
		sp, err := n.Store.Snapshot()
		x.Checkf(err, "Unable to get existing snapshot")
		if !raft.IsEmptySnap(sp) {
			// It is important that we pick up the conf state here.
			// Otherwise, we'll lose the store conf state, and it would get
			// overwritten with an empty state when a new snapshot is taken.
			// This causes a node to just hang on restart, because it finds a
			// zero-member Raft group.
			n.SetConfState(&sp.Metadata.ConfState)

			// TODO: Making connections here seems unnecessary, evaluate.
			members := groups().members(n.gid)
			for _, id := range sp.Metadata.ConfState.Nodes {
				m, ok := members[id]
				if ok {
					n.Connect(id, m.Addr)
				}
			}
			for _, id := range sp.Metadata.ConfState.Learners {
				m, ok := members[id]
				if ok {
					n.Connect(id, m.Addr)
				}
			}
		}
		n.SetRaft(raft.RestartNode(n.Cfg))
		glog.V(2).Infoln("Restart node complete")

	} else {
		glog.Infof("New Node for group: %d\n", n.gid)
		if _, hasPeer := groups().MyPeer(); hasPeer {
			// Get snapshot before joining peers as it can take time to retrieve it and we dont
			// want the quorum to be inactive when it happens.
			// Update: This is an optimization, which adds complexity because it requires us to
			// understand the Raft state of the node. Let's instead have the node retrieve the
			// snapshot as needed after joining the group, instead of us forcing one upfront.
			glog.Infoln("Trying to join peers.")
			n.retryUntilSuccess(n.joinPeers, time.Second)
			n.SetRaft(raft.StartNode(n.Cfg, nil))
		} else {
			peers := []raft.Peer{{ID: n.Id}}
			n.SetRaft(raft.StartNode(n.Cfg, peers))
			// Trigger election, so this node can become the leader of this single-node cluster.
			n.canCampaign = true
		}
	}
	go n.processTabletSizes()
	go n.processApplyCh()
	go n.BatchAndSendMessages()
	go n.monitorRaftMetrics()
	go n.cdcTracker.processCDCEvents()
	// Ignoring the error since InitAndStartNode does not return an error and using x.Check would
	// not be the right thing to do.
	_, _ = n.startTask(opRollup)
	go n.stopAllTasks()
	for i := 0; i < 8; i++ {
		go n.mutationWorker(i)
	}
	go n.Run()
}

func (n *node) AmLeader() bool {
	if n.Raft() == nil {
		return false
	}
	r := n.Raft()
	return r.Status().Lead == r.Status().ID
}

func (n *node) monitorRaftMetrics() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		curPendingSize := atomic.LoadInt64(&n.pendingSize)
		ostats.Record(n.ctx, x.RaftPendingSize.M(curPendingSize))
		ostats.Record(n.ctx, x.RaftApplyCh.M(int64(len(n.applyCh))))
	}
}
