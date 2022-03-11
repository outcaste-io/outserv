// Portions Copyright 2016-2018 Dgraph Labs, Inc. are available under the Apache License v2.0.
// Portions Copyright 2022 Outcaste LLC are available under the Smart License v1.0.

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
// 1. We receive a mutation proposal (CommittedEntries in Raft).
// 2. We set a StartTs and CommitTs for the proposal. Both of them are based on
// Raft Index. StartTs is calculated based on what applyCh has processed so far.
// CommitTs is what the timestamp would be based on Raft index.
// 3. It is executed concurrently.
// 4. It is also pushed to applyCh.
// 5. When applych receives the mutation, it checks if any reads the txn
// incurred, have been written to in the range (Tstart, inf).
// 6. If the mutation was applied serially, it would have read everything below
// Tcommit. So, it's Tstart would be Tcommit - 1.
// 7. If something got written after Tstart, then we need to redo the update
// serially. So, concurrent processing done would be invalid.
// 8. If nothing was written, then the processing done is valid.
func (kw *keysWritten) StillValid(txn *posting.Txn) bool {
	if atomic.LoadUint64(&txn.AppliedIndexSeen) < kw.rejectBeforeIndex {
		kw.invalidTxns++
		return false
	}
	// TODO: Need to understand this better.
	// if atomic.LoadUint64(&txn.MaxAssignedSeen) >= txn.StartTs {
	// 	kw.validTxns++
	// 	return true
	// }

	c := txn.Cache()
	c.Lock()
	defer c.Unlock()
	for hash := range c.ReadKeys() {
		// If the commitTs is between (MaxAssignedSeen, StartTs], the txn reads were invalid. If the
		// commitTs is > StartTs, then it doesn't matter for reads. If the commit ts is <
		// MaxAssignedSeen, that means our reads are valid.
		commitTs := kw.keyCommitTs[hash]
		if commitTs > txn.StartTs {
			kw.invalidTxns++
			return false
		}
	}
	kw.validTxns++
	return true
}
