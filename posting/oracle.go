// Portions Copyright 2017-2018 Dgraph Labs, Inc. are available under the Apache License v2.0.
// Portions Copyright 2022 Outcaste LLC are available under the Smart License v1.0.

package posting

import (
	"context"
	"encoding/hex"
	"sync"

	"github.com/golang/glog"
	"github.com/outcaste-io/badger/v3/skl"
	"github.com/outcaste-io/badger/v3/y"
	"github.com/outcaste-io/outserv/x"
	"github.com/outcaste-io/ristretto/z"
	otrace "go.opencensus.io/trace"
)

var o *oracle

// Oracle returns the global oracle instance.
// TODO: Oracle should probably be located in worker package, instead of posting
// package now that we don't run inSnapshot anymore.
func Oracle() *oracle {
	return o
}

func init() {
	o = new(oracle)
	o.init()
}

// Txn represents a transaction.
type Txn struct {
	StartTs          uint64 // This does not get modified.
	CommitTs         uint64
	MaxAssignedSeen  uint64 // atomic
	AppliedIndexSeen uint64 // atomic
	Uids             map[string]string

	// Fields which can changed after init
	sync.Mutex

	cache *LocalCache // This pointer does not get modified.
	ErrCh chan error

	slWait sync.WaitGroup
	sl     *skl.Skiplist
}

// NewTxn returns a new Txn instance.
func NewTxn(startTs uint64) *Txn {
	return &Txn{
		StartTs: startTs,
		cache:   NewLocalCache(startTs),
		ErrCh:   make(chan error, 1),
	}
}

// Get retrieves the posting list for the given list from the local cache.
func (txn *Txn) Get(key []byte) (*List, error) {
	return txn.cache.Get(key)
}

// GetFromDelta retrieves the posting list from delta cache, not from Badger.
func (txn *Txn) GetFromDelta(key []byte) (*List, error) {
	return txn.cache.GetFromDelta(key)
}

func (txn *Txn) Skiplist() *skl.Skiplist {
	txn.slWait.Wait()
	return txn.sl
}

// Update calls UpdateDeltasAndDiscardLists on the local cache.
func (txn *Txn) Update(ctx context.Context, resolved map[string]string) {
	txn.Lock()
	defer txn.Unlock()
	txn.cache.UpdateDeltasAndDiscardLists()

	// If we already have a pending Update, then wait for it to be done first. So it does not end up
	// overwriting the skiplist that we generate here.
	txn.slWait.Wait()
	txn.slWait.Add(1)
	txn.Uids = resolved
	go func() {
		if err := txn.ToSkiplist(); err != nil {
			glog.Errorf("While creating skiplist: %v\n", err)
		}
		span := otrace.FromContext(ctx)
		span.Annotate(nil, "ToSkiplist done")
		txn.slWait.Done()
	}()
}

// Store is used by tests.
func (txn *Txn) Store(pl *List) *List {
	return txn.cache.SetIfAbsent(string(pl.key), pl)
}

type oracle struct {
	x.SafeMutex

	closer  *z.Closer
	applied y.WaterMark

	// Keeps track of all the startTs we have seen so far, based on the mutations. Then as
	// transactions are committed or aborted, we delete entries from the startTs map. When taking a
	// snapshot, we need to know the minimum start ts present in the map, which represents a
	// mutation which has not yet been committed or aborted.  As we iterate over entries, we should
	// only discard those whose StartTs is below this minimum pending start ts.
	pendingTxns map[uint64]*Txn

	// Used for waiting logic for transactions with startTs > maxpending so that we don't read an
	// uncommitted transaction.
	waiters map[uint64][]chan struct{}
}

func RegisterTimestamp(ts uint64) {
	o.applied.Begin(ts)
}
func DoneTimestamp(ts uint64) {
	o.applied.Done(ts)
}
func ReadTimestamp() uint64 {
	// Return +1 from whatever the commit timestamp is. This way, we can also
	// read the rolled up posting lists, which are written at commit ts + 1.
	return o.applied.DoneUntil() + 1
}
func InitApplied(ts uint64) {
	o.applied.SetDoneUntil(ts)
}

func (o *oracle) init() {
	o.closer = z.NewCloser(1)
	o.applied.Init(o.closer)
	o.waiters = make(map[uint64][]chan struct{})
	o.pendingTxns = make(map[uint64]*Txn)
}

// RegisterCommitTs would return a txn and a bool.
// If the bool is true, the txn was already present. If false, it is new.
func RegisterTxn(startTs, commitTs uint64) *Txn {
	o.Lock()
	defer o.Unlock()

	_, ok := o.pendingTxns[commitTs]
	x.AssertTrue(!ok)

	txn := NewTxn(startTs)
	txn.CommitTs = commitTs

	RegisterTimestamp(commitTs)

	o.pendingTxns[commitTs] = txn
	return txn
}

func (o *oracle) ResetTxn(ts uint64) *Txn {
	o.Lock()
	defer o.Unlock()

	txn := NewTxn(ts)
	o.pendingTxns[ts] = txn
	return txn
}

func (o *oracle) MinStartTs() uint64 {
	o.RLock()
	defer o.RUnlock()
	min := ReadTimestamp()
	for _, txn := range o.pendingTxns {
		if ts := txn.StartTs; ts < min {
			min = ts
		}
	}
	return min
}

func (o *oracle) NumPendingTxns() int {
	o.RLock()
	defer o.RUnlock()
	return len(o.pendingTxns)
}

func (o *oracle) WaitForTs(ctx context.Context, startTs uint64) error {
	// TODO: Add a wait here based on the timestamp.
	return nil
}

func DeleteTxnWithCommitTs(ts uint64) {
	o.Lock()
	delete(o.pendingTxns, ts)
	o.Unlock()
}

// DeleteTxnsAndRollupKeys is called via a callback when Skiplist is handled
// over to Badger with latest commits in it.
func DeleteTxnAndRollupKeys(txn *Txn) {
	o.Lock()
	if txn != nil && txn.CommitTs > 0 {
		c := txn.Cache()
		c.RLock()
		for k := range c.Deltas() {
			IncrRollup.addKeyToBatch([]byte(k), 0)
		}
		c.RUnlock()
		delete(o.pendingTxns, txn.CommitTs)
	}
	o.Unlock()
}

func (o *oracle) ResetTxns() {
	o.Lock()
	defer o.Unlock()
	o.pendingTxns = make(map[uint64]*Txn)
}

// ResetTxnForNs deletes all the pending transactions for a given namespace.
func (o *oracle) ResetTxnsForNs(ns uint64) {
	txns := o.IterateTxns(func(key []byte) bool {
		pk, err := x.Parse(key)
		if err != nil {
			glog.Errorf("error %v while parsing key %v", err, hex.EncodeToString(key))
			return false
		}
		return x.ParseNamespace(pk.Attr) == ns
	})
	o.Lock()
	defer o.Unlock()
	for _, txn := range txns {
		delete(o.pendingTxns, txn)
	}
}

func GetTxn(commitTs uint64) *Txn {
	o.RLock()
	defer o.RUnlock()
	return o.pendingTxns[commitTs]
}

func (txn *Txn) matchesDelta(ok func(key []byte) bool) bool {
	txn.Lock()
	defer txn.Unlock()
	for key := range txn.cache.deltas {
		if ok([]byte(key)) {
			return true
		}
	}
	return false
}

// IterateTxns returns a list of start timestamps for currently pending transactions, which match
// the provided function.
func (o *oracle) IterateTxns(ok func(key []byte) bool) []uint64 {
	o.RLock()
	defer o.RUnlock()
	var timestamps []uint64
	for startTs, txn := range o.pendingTxns {
		if txn.matchesDelta(ok) {
			timestamps = append(timestamps, startTs)
		}
	}
	return timestamps
}
