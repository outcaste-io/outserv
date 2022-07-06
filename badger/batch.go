/*
 * Copyright 2018 Dgraph Labs, Inc. and Contributors
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

package badger

import (
	"sort"
	"sync"
	"sync/atomic"

	"github.com/outcaste-io/outserv/badger/pb"
	"github.com/outcaste-io/outserv/badger/skl"
	"github.com/outcaste-io/outserv/badger/y"
	"github.com/outcaste-io/ristretto/z"
	"github.com/pkg/errors"
)

// WriteBatch holds the necessary info to perform batched writes.
// TODO(mrjn): Move this to use Skiplists directly. That's all we need.
type WriteBatch struct {
	sync.Mutex
	sz       int64
	entries  []*Entry
	db       *DB
	throttle *y.Throttle
	err      atomic.Value
	finished bool
}

func (db *DB) NewWriteBatch() *WriteBatch {
	return &WriteBatch{
		db:       db,
		throttle: y.NewThrottle(16),
	}
}

// SetMaxPendingTxns sets a limit on maximum number of pending transactions while writing batches.
// This function should be called before using WriteBatch. Default value of MaxPendingTxns is
// 16 to minimise memory usage.
func (wb *WriteBatch) SetMaxPendingTxns(max int) {
	wb.throttle = y.NewThrottle(max)
}

// Cancel function must be called if there's a chance that Flush might not get
// called. If neither Flush or Cancel is called, the transaction oracle would
// never get a chance to clear out the row commit timestamp map, thus causing an
// unbounded memory consumption. Typically, you can call Cancel as a defer
// statement right after NewWriteBatch is called.
//
// Note that any committed writes would still go through despite calling Cancel.
func (wb *WriteBatch) Cancel() {
	wb.Lock()
	defer wb.Unlock()
	wb.finished = true
	if err := wb.throttle.Finish(); err != nil {
		wb.db.opt.Errorf("WatchBatch.Cancel error while finishing: %v", err)
	}
}

func (wb *WriteBatch) callback(err error) {
	// sync.WaitGroup is thread-safe, so it doesn't need to be run inside wb.Lock.
	defer wb.throttle.Done(err)
	if err == nil {
		return
	}
	if err := wb.Error(); err != nil {
		return
	}
	wb.err.Store(err)
}

// Caller to writeKV must hold a write lock.
func (wb *WriteBatch) writeKV(kv *pb.KV) error {
	e := Entry{Key: kv.Key, Value: kv.Value}
	if len(kv.UserMeta) > 0 {
		e.UserMeta = kv.UserMeta[0]
	}
	y.AssertTrue(kv.Version != 0)
	e.version = kv.Version
	return wb.handleEntry(&e)
}

func (wb *WriteBatch) Write(buf *z.Buffer) error {
	wb.Lock()
	defer wb.Unlock()

	err := buf.SliceIterate(func(s []byte) error {
		kv := &pb.KV{}
		if err := kv.Unmarshal(s); err != nil {
			return err
		}
		return wb.writeKV(kv)
	})
	return err
}

func (wb *WriteBatch) WriteList(kvList *pb.KVList) error {
	wb.Lock()
	defer wb.Unlock()
	for _, kv := range kvList.Kv {
		if err := wb.writeKV(kv); err != nil {
			return err
		}
	}
	return nil
}

// Should be called with lock acquired.
func (wb *WriteBatch) handleEntry(e *Entry) error {
	if err := ValidEntry(wb.db, e.Key, e.Value); err != nil {
		return errors.Wrapf(err, "invalid entry")
	}
	wb.entries = append(wb.entries, e)
	wb.sz += e.estimateSize()
	if wb.sz < wb.db.opt.MemTableSize {
		return nil
	}
	return wb.commit()
}

// SetEntryAt is the equivalent of Txn.SetEntry but it also allows setting version for the entry.
// SetEntryAt can be used only in managed mode.
func (wb *WriteBatch) SetEntryAt(e *Entry, ts uint64) error {
	e.version = ts
	return wb.SetEntry(e)
}

// SetEntry is the equivalent of Txn.SetEntry.
func (wb *WriteBatch) SetEntry(e *Entry) error {
	wb.Lock()
	defer wb.Unlock()
	return wb.handleEntry(e)
}

// SetAt sets key at given ts.
func (wb *WriteBatch) SetAt(k []byte, v []byte, ts uint64) error {
	e := Entry{Key: k, Value: v, version: ts}
	return wb.SetEntry(&e)
}

// DeleteAt is equivalent of Txn.Delete but accepts a delete timestamp.
func (wb *WriteBatch) DeleteAt(k []byte, ts uint64) error {
	e := Entry{Key: k, meta: bitDelete, version: ts}
	return wb.SetEntry(&e)
}

// Caller to commit must hold a write lock.
func (wb *WriteBatch) commit() error {
	if err := wb.Error(); err != nil {
		return err
	}
	if wb.finished {
		return y.ErrCommitAfterFinish
	}
	// Set the versions to the keys.
	for _, e := range wb.entries {
		if e.version > 0 {
			e.Key = y.KeyWithTs(e.Key, e.version)
		} else {
			// We have to assume that e.Key already has version in it.
		}
	}

	// Sort all the keys with their timestamps attached.
	sort.Slice(wb.entries, func(i, j int) bool {
		return y.CompareKeys(wb.entries[i].Key, wb.entries[j].Key) < 0
	})

	// With the keys sorted, we can build the Skiplist quickly.
	if wb.sz < 1024 {
		wb.sz = 1024
	}
	b := skl.NewBuilder(int64(float64(wb.sz) * 1.2))
	for _, e := range wb.entries {
		vs := y.ValueStruct{
			Value:    e.Value,
			UserMeta: e.UserMeta,
			Meta:     e.meta,
		}
		b.Add(e.Key, vs)
	}
	sl := b.Skiplist()

	if err := wb.throttle.Do(); err != nil {
		wb.err.Store(err)
		return err
	}
	// Handover the skiplist to DB.
	err := wb.db.HandoverSkiplist(sl, func() {
		wb.callback(nil)
	})
	if err != nil {
		wb.err.Store(err)
		return err
	}

	wb.entries = wb.entries[:0]
	wb.sz = 0
	return wb.Error()
}

func (wb *WriteBatch) FlushWith(cb func(err error)) error {
	wb.Lock()
	err := wb.commit()
	if err != nil {
		wb.Unlock()
		return err
	}
	wb.finished = true
	wb.Unlock()

	done := func() error {
		if err := wb.throttle.Finish(); err != nil {
			if wb.Error() != nil {
				return errors.Errorf("wb.err: %s err: %s", wb.Error(), err)
			}
			return err
		}
		return wb.Error()
	}

	go func() {
		err := done()
		cb(err)
	}()
	return nil
}

// Flush must be called at the end to ensure that any pending writes get committed to Badger. Flush
// returns any error stored by WriteBatch.
func (wb *WriteBatch) Flush() error {
	var wg sync.WaitGroup
	wg.Add(1)

	var ferr error
	if err := wb.FlushWith(func(err error) {
		ferr = err
		wg.Done()
	}); err != nil {
		return err
	}
	wg.Wait()
	return ferr
}

// Error returns any errors encountered so far. No commits would be run once an error is detected.
func (wb *WriteBatch) Error() error {
	// If the interface conversion fails, the err will be nil.
	err, _ := wb.err.Load().(error)
	return err
}
