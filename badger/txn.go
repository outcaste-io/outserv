// Portions Copyright 2020 Dgraph Labs, Inc. are available under the Apache License v2.0.
// Portions Copyright 2022 Outcaste LLC are available under the Apache License v2.0.

package badger

import (
	"bytes"
	"encoding/hex"
	"math"
	"sync/atomic"

	"github.com/outcaste-io/outserv/badger/y"
	"github.com/pkg/errors"
)

const maxKeySize = 65000
const maxValSize = 1 << 20

func exceedsSize(prefix string, max int64, key []byte) error {
	return errors.Errorf("%s with size %d exceeded %d limit. %s:\n%s",
		prefix, len(key), max, prefix, hex.Dump(key[:1<<10]))
}

func ValidEntry(db *DB, key, val []byte) error {
	switch {
	case len(key) == 0:
		return ErrEmptyKey
	case bytes.HasPrefix(key, badgerPrefix):
		return ErrInvalidKey
	case len(key) > maxKeySize:
		// Key length can't be more than uint16, as determined by table::header.  To
		// keep things safe and allow badger move prefix and a timestamp suffix, let's
		// cut it down to 65000, instead of using 65536.
		return exceedsSize("Key", maxKeySize, key)
	case int64(len(val)) > maxValSize:
		return exceedsSize("Value", maxValSize, val)
	}
	if err := db.isBanned(key); err != nil {
		return err
	}
	return nil
}

// Txn represents a Badger transaction.
type Txn struct {
	readTs uint64
	db     *DB

	numIterators int32
	discarded    bool
}

// NewReadTxn is a read-only transaction.
// For read-only transactions, set update to false. In this mode, we don't track the rows read for
// any changes. Thus, any long running iterations done in this mode wouldn't pay this overhead.
//
// Running transactions concurrently is OK. However, a transaction itself isn't thread safe, and
// should only be run serially. It doesn't matter if a transaction is created by one goroutine and
// passed down to other, as long as the Txn APIs are called serially.
//
// When you create a new transaction, it is absolutely essential to call
// Discard(). This should be done irrespective of what the update param is set
// to. Commit API internally runs Discard, but running it twice wouldn't cause
// any issues.
//
//  txn := db.NewTransaction(false)
//  defer txn.Discard()
//  // Call various APIs.
func (db *DB) NewReadTxn(readTs uint64) *Txn {
	return &Txn{readTs: readTs, db: db}
}

// Get looks for key and returns corresponding Item.
// If key is not found, ErrKeyNotFound is returned.
func (txn *Txn) Get(key []byte) (item *Item, rerr error) {
	if len(key) == 0 {
		return nil, ErrEmptyKey
	} else if txn.discarded {
		return nil, ErrDiscardedTxn
	}

	if err := txn.db.isBanned(key); err != nil {
		return nil, err
	}

	item = new(Item)
	seek := y.KeyWithTs(key, txn.readTs)
	vs, err := txn.db.get(seek)
	if err != nil {
		return nil, y.Wrapf(err, "DB::Get key: %q", key)
	}
	if vs.Value == nil && vs.Meta == 0 {
		return nil, ErrKeyNotFound
	}
	if isDeletedOrExpired(vs.Meta) {
		return nil, ErrKeyNotFound
	}

	item.key = key
	item.version = vs.Version
	item.meta = vs.Meta
	item.userMeta = vs.UserMeta
	item.vptr = y.SafeCopy(item.vptr, vs.Value)
	item.txn = txn
	return item, nil
}

// Discard discards a created transaction. This method is very important and must be called. Commit
// method calls this internally, however, calling this multiple times doesn't cause any issues. So,
// this can safely be called via a defer right when transaction is created.
//
// NOTE: If any operations are run on a discarded transaction, ErrDiscardedTxn is returned.
func (txn *Txn) Discard() {
	if txn.discarded { // Avoid a re-run.
		return
	}
	if atomic.LoadInt32(&txn.numIterators) > 0 {
		panic("Unclosed iterator at time of Txn.Discard.")
	}
	txn.discarded = true
}

// ReadTs returns the read timestamp of the transaction.
func (txn *Txn) ReadTs() uint64 {
	return txn.readTs
}

// Discarded returns the discarded value of the transaction.
func (txn *Txn) Discarded() bool {
	return txn.discarded
}

// View executes a function creating and managing a read-only transaction for the user. Error
// returned by the function is relayed by the View method.
// If View is used with managed transactions, it would assume a read timestamp of MaxUint64.
func (db *DB) View(fn func(txn *Txn) error) error {
	if db.IsClosed() {
		return ErrDBClosed
	}
	txn := db.NewReadTxn(math.MaxUint64)
	defer txn.Discard()

	return fn(txn)
}
