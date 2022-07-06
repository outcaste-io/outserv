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

package posting

import (
	"github.com/outcaste-io/outserv/badger"
	"github.com/outcaste-io/outserv/badger/pb"
)

// TxnWriter is in charge or writing transactions to badger.
type TxnWriter struct {
	wb *badger.WriteBatch
}

// NewTxnWriter returns a new TxnWriter instance.
func NewTxnWriter(db *badger.DB) *TxnWriter {
	return &TxnWriter{
		wb: db.NewWriteBatch(),
	}
}

// Write stores the given key-value pairs in badger.
func (w *TxnWriter) Write(kvs *pb.KVList) error {
	for _, kv := range kvs.Kv {
		var meta byte
		if len(kv.UserMeta) > 0 {
			meta = kv.UserMeta[0]
		}
		if err := w.SetAt(kv.Key, kv.Value, meta, kv.Version); err != nil {
			return err
		}
	}
	return nil
}

// SetAt writes a key-value pair at the given timestamp.
func (w *TxnWriter) SetAt(key, val []byte, meta byte, ts uint64) error {
	entry := &badger.Entry{
		Key:      key,
		Value:    val,
		UserMeta: meta,
	}
	switch meta {
	case BitCompletePosting, BitEmptyPosting, BitForbidPosting:
		entry = entry.WithDiscard()
	default:
	}
	return w.wb.SetEntryAt(entry, ts)
}

// Flush waits until all operations are done and all data is written to disk.
func (w *TxnWriter) Flush() error {
	// No need to call Sync here.
	return w.wb.Flush()
}
