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
	"fmt"
	"testing"
	"time"

	"github.com/outcaste-io/outserv/badger/y"

	"github.com/stretchr/testify/require"
)

func TestWriteBatch(t *testing.T) {
	key := func(i int) []byte {
		return []byte(fmt.Sprintf("%10d", i))
	}
	val := func(i int) []byte {
		return []byte(fmt.Sprintf("%128d", i))
	}

	test := func(t *testing.T, db *DB) {
		wb := db.NewWriteBatch()
		defer wb.Cancel()

		// Sanity check for SetEntryAt.
		require.Error(t, wb.SetEntryAt(&Entry{}, 12))

		N, M := 50000, 1000
		start := time.Now()
		var version uint64

		for i := 0; i < N; i++ {
			version++
			require.NoError(t, wb.SetAt(key(i), val(i), version))
		}
		for i := 0; i < M; i++ {
			version++
			require.NoError(t, wb.DeleteAt(key(i), version))
		}
		require.NoError(t, wb.Flush())
		t.Logf("Time taken for %d writes (w/ test options): %s\n", N+M, time.Since(start))

		err := db.View(func(txn *Txn) error {
			itr := txn.NewIterator(DefaultIteratorOptions)
			defer itr.Close()

			i := M
			for itr.Rewind(); itr.Valid(); itr.Next() {
				item := itr.Item()
				require.Equal(t, string(key(i)), string(item.Key()))
				valcopy, err := item.ValueCopy(nil)
				require.NoError(t, err)
				require.Equal(t, val(i), valcopy)
				i++
			}
			require.Equal(t, N, i)
			return nil
		})
		require.NoError(t, err)
	}
	t.Run("disk mode", func(t *testing.T) {
		opt := getTestOptions("")
		runBadgerTest(t, &opt, func(t *testing.T, db *DB) {
			test(t, db)
		})
		t.Logf("Disk mode done\n")
	})
	t.Run("InMemory mode", func(t *testing.T) {
		opt := getTestOptions("")
		opt.InMemory = true
		db, err := Open(opt)
		require.NoError(t, err)
		test(t, db)
		t.Logf("Disk mode done\n")
		require.NoError(t, db.Close())
	})
}

// This test ensures we don't end up in deadlock in case of empty writebatch.
func TestEmptyWriteBatch(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		wb := db.NewWriteBatch()
		require.NoError(t, wb.Flush())
		wb = db.NewWriteBatch()
		require.NoError(t, wb.Flush())
		wb = db.NewWriteBatch()
		require.NoError(t, wb.Flush())
	})
}

// This test ensures we don't panic during flush.
// See issue: https://github.com/dgraph-io/badger/issues/1394
func TestFlushPanic(t *testing.T) {
	t.Run("flush after flush", func(t *testing.T) {
		runBadgerTest(t, nil, func(t *testing.T, db *DB) {
			wb := db.NewWriteBatch()
			wb.Flush()
			require.Error(t, y.ErrCommitAfterFinish, wb.Flush())
		})
	})
	t.Run("flush after cancel", func(t *testing.T) {
		runBadgerTest(t, nil, func(t *testing.T, db *DB) {
			wb := db.NewWriteBatch()
			wb.Cancel()
			require.Error(t, y.ErrCommitAfterFinish, wb.Flush())
		})
	})
}
