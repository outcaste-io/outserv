// Portions Copyright 2016-2018 Dgraph Labs, Inc. are available under the Apache 2.0 license.
// Portions Copyright 2022 Outcaste, Inc. are available under the Smart license.

package worker

import (
	"sync/atomic"

	"github.com/outcaste-io/outserv/posting"
)

// keysWritten is always accessed serially via applyCh. So, we don't need to make it thread-safe.
type keysWritten struct {
	rejectBeforeIndex uint64
	keyCommitTs       map[uint64]uint64
	validTxns         int64
	invalidTxns       int64
	totalKeys         int
}

func newKeysWritten() *keysWritten {
	return &keysWritten{
		keyCommitTs: make(map[uint64]uint64),
	}
}

// We use keysWritten structure to allow mutations to be run concurrently. Consider this:
// 1. We receive a txn with mutation at start ts = Ts.
// 2. The server is at MaxAssignedTs Tm < Ts.
// 3. Before, we would block proposing until Tm >= Ts.
// 4. Now, we propose the mutation immediately.
// 5. Once the mutation goes through raft, it is executed concurrently, and the "seen" MaxAssignedTs
//    is registered as Tm-seen.
// 6. The same mutation is also pushed to applyCh.
// 7. When applyCh sees the mutation, it checks if any reads the txn incurred, have been written to
//    with a commit ts in the range (Tm-seen, Ts]. If so, the mutation is re-run. In 21M live load,
//    this happens about 3.6% of the time.
// 8. If no commits have happened for the read key set, we are done. This happens 96.4% of the time.
// 9. If multiple mutations happen for the same txn, the sequential mutations are always run
//    serially by applyCh. This is to avoid edge cases.
func (kw *keysWritten) StillValid(txn *posting.Txn) bool {
	if atomic.LoadUint64(&txn.AppliedIndexSeen) < kw.rejectBeforeIndex {
		kw.invalidTxns++
		return false
	}
	if atomic.LoadUint64(&txn.MaxAssignedSeen) >= txn.StartTs {
		kw.validTxns++
		return true
	}

	c := txn.Cache()
	c.Lock()
	defer c.Unlock()
	for hash := range c.ReadKeys() {
		// If the commitTs is between (MaxAssignedSeen, StartTs], the txn reads were invalid. If the
		// commitTs is > StartTs, then it doesn't matter for reads. If the commit ts is <
		// MaxAssignedSeen, that means our reads are valid.
		commitTs := kw.keyCommitTs[hash]
		if commitTs > atomic.LoadUint64(&txn.MaxAssignedSeen) && commitTs <= txn.StartTs {
			kw.invalidTxns++
			return false
		}
	}
	kw.validTxns++
	return true
}
