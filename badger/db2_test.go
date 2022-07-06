// Portions Copyright 2020 Dgraph Labs, Inc. are available under the Apache License v2.0.
// Portions Copyright 2022 Outcaste LLC are available under the Apache License v2.0.

package badger

import (
	"bytes"
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/outcaste-io/outserv/badger/options"
	"github.com/pkg/errors"

	"github.com/outcaste-io/outserv/badger/pb"
	"github.com/outcaste-io/outserv/badger/table"
	"github.com/outcaste-io/outserv/badger/y"
	"github.com/outcaste-io/ristretto/z"
	"github.com/stretchr/testify/require"
)

var manual = flag.Bool("manual", false, "Set when manually running some tests.")

// Badger dir to be used for performing db.Open benchmark.
var benchDir = flag.String("benchdir", "", "Set when running db.Open benchmark")

// The following benchmark test is supposed to be run against a badger directory with some data.
// Use badger fill to create data if it doesn't exist.
func BenchmarkDBOpen(b *testing.B) {
	if *benchDir == "" {
		b.Skip("Please set -benchdir to badger directory")
	}
	dir := *benchDir
	// Passing an empty directory since it will be filled by runBadgerTest.
	opt := DefaultOptions(dir).
		WithReadOnly(true)
	for i := 0; i < b.N; i++ {
		db, err := Open(opt)
		require.NoError(b, err)
		require.NoError(b, db.Close())
	}
}

// Test for values of size uint32.
func TestBigValues(t *testing.T) {
	if !*manual {
		t.Skip("Skipping test meant to be run manually.")
		return
	}
	opts := DefaultOptions("")
	test := func(t *testing.T, db *DB) {
		keyCount := 1000

		data := bytes.Repeat([]byte("a"), (1 << 20)) // Valuesize 1 MB.
		key := func(i int) string {
			return fmt.Sprintf("%65000d", i)
		}

		getByKey := func(key string) error {
			return db.View(func(txn *Txn) error {
				item, err := txn.Get([]byte(key))
				if err != nil {
					return err
				}
				return item.Value(func(val []byte) error {
					if len(val) == 0 || len(val) != len(data) || !bytes.Equal(val, []byte(data)) {
						log.Fatalf("key not found %q", len(key))
					}
					return nil
				})
			})
		}

		wb := db.NewWriteBatch()
		for i := 0; i < keyCount; i++ {
			require.NoError(t, wb.SetEntryAt(
				NewEntry([]byte(key(i)), []byte(data)), 1))
		}
		require.NoError(t, wb.Flush())

		for i := 0; i < keyCount; i++ {
			require.NoError(t, getByKey(key(i)))
		}
	}
	t.Run("disk mode", func(t *testing.T) {
		runBadgerTest(t, &opts, func(t *testing.T, db *DB) {
			test(t, db)
		})
	})
	t.Run("InMemory mode", func(t *testing.T) {
		opts.InMemory = true
		opts.Dir = ""
		db, err := Open(opts)
		require.NoError(t, err)
		test(t, db)
		require.NoError(t, db.Close())
	})
}

// This test is for compaction file picking testing. We are creating db with two levels. We have 10
// tables on level 3 and 3 tables on level 2. Tables on level 2 have overlap with 2, 4, 3 tables on
// level 3.
func TestCompactionFilePicking(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)

	db, err := Open(DefaultOptions(dir))
	require.NoError(t, err, "error while opening db")
	defer func() {
		require.NoError(t, db.Close())
	}()

	l3 := db.lc.levels[3]
	for i := 1; i <= 10; i++ {
		// Each table has difference of 1 between smallest and largest key.
		tab := createTableWithRange(t, db, 2*i-1, 2*i)
		addToManifest(t, db, tab, 3)
		require.NoError(t, l3.replaceTables([]*table.Table{}, []*table.Table{tab}))
	}

	l2 := db.lc.levels[2]
	// First table has keys 1 and 4.
	tab := createTableWithRange(t, db, 1, 4)
	addToManifest(t, db, tab, 2)
	require.NoError(t, l2.replaceTables([]*table.Table{}, []*table.Table{tab}))

	// Second table has keys 5 and 12.
	tab = createTableWithRange(t, db, 5, 12)
	addToManifest(t, db, tab, 2)
	require.NoError(t, l2.replaceTables([]*table.Table{}, []*table.Table{tab}))

	// Third table has keys 13 and 18.
	tab = createTableWithRange(t, db, 13, 18)
	addToManifest(t, db, tab, 2)
	require.NoError(t, l2.replaceTables([]*table.Table{}, []*table.Table{tab}))

	cdef := &compactDef{
		thisLevel: db.lc.levels[2],
		nextLevel: db.lc.levels[3],
	}

	tables := db.lc.levels[2].tables
	db.lc.sortByHeuristic(tables, cdef)

	var expKey [8]byte
	// First table should be with smallest and biggest keys as 1 and 4 which
	// has the lowest version.
	binary.BigEndian.PutUint64(expKey[:], uint64(1))
	require.Equal(t, expKey[:], y.ParseKey(tables[0].Smallest()))
	binary.BigEndian.PutUint64(expKey[:], uint64(4))
	require.Equal(t, expKey[:], y.ParseKey(tables[0].Biggest()))

	// Second table should be with smallest and biggest keys as 13 and 18
	// which has the second lowest version.
	binary.BigEndian.PutUint64(expKey[:], uint64(13))
	require.Equal(t, expKey[:], y.ParseKey(tables[2].Smallest()))
	binary.BigEndian.PutUint64(expKey[:], uint64(18))
	require.Equal(t, expKey[:], y.ParseKey(tables[2].Biggest()))

	// Third table should be with smallest and biggest keys as 5 and 12 which
	// has the maximum version.
	binary.BigEndian.PutUint64(expKey[:], uint64(5))
	require.Equal(t, expKey[:], y.ParseKey(tables[1].Smallest()))
	binary.BigEndian.PutUint64(expKey[:], uint64(12))
	require.Equal(t, expKey[:], y.ParseKey(tables[1].Biggest()))
}

// addToManifest function is used in TestCompactionFilePicking. It adds table to db manifest.
func addToManifest(t *testing.T, db *DB, tab *table.Table, level uint32) {
	change := &pb.ManifestChange{
		Id:          tab.ID(),
		Op:          pb.ManifestChange_CREATE,
		Level:       level,
		Compression: uint32(tab.CompressionType()),
	}
	require.NoError(t, db.manifest.addChanges([]*pb.ManifestChange{change}),
		"unable to add to manifest")
}

// createTableWithRange function is used in TestCompactionFilePicking. It creates
// a table with key starting from start and ending with end.
func createTableWithRange(t *testing.T, db *DB, start, end int) *table.Table {
	bopts := buildTableOptions(db)
	b := table.NewTableBuilder(bopts)
	defer b.Close()
	nums := []int{start, end}
	for _, i := range nums {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key[:], uint64(i))
		key = y.KeyWithTs(key, uint64(0))
		val := y.ValueStruct{Value: []byte(fmt.Sprintf("%d", i))}
		b.Add(key, val)
	}

	fileID := db.lc.reserveFileID()
	tab, err := table.CreateTable(table.NewFilename(fileID, db.opt.Dir), b)
	require.NoError(t, err)
	return tab
}

func TestReadSameVlog(t *testing.T) {
	key := func(i int) []byte {
		return []byte(fmt.Sprintf("%d%10d", i, i))
	}
	testReadingSameKey := func(t *testing.T, db *DB) {
		// Forcing to read all values from vlog.
		wb := db.NewWriteBatch()
		for i := 0; i < 50; i++ {
			require.NoError(t, wb.SetAt(key(i), key(i), 1))
		}
		require.NoError(t, wb.Flush())

		// reading it again several times
		for i := 0; i < 50; i++ {
			for j := 0; j < 10; j++ {
				err := db.View(func(txn *Txn) error {
					item, err := txn.Get(key(i))
					require.NoError(t, err)
					require.Equal(t, key(i), getItemValue(t, item))
					return nil
				})
				require.NoError(t, err)
			}
		}
	}

	t.Run("Test Read Again Plain Text", func(t *testing.T) {
		runBadgerTest(t, nil, func(t *testing.T, db *DB) {
			testReadingSameKey(t, db)
		})
	})

	t.Run("Test Read Again Encryption", func(t *testing.T) {
		opt := getTestOptions("")
		// Generate encryption key.
		eKey := make([]byte, 32)
		_, err := rand.Read(eKey)
		require.NoError(t, err)
		opt.EncryptionKey = eKey
		runBadgerTest(t, nil, func(t *testing.T, db *DB) {
			testReadingSameKey(t, db)
		})
	})
}

func TestDropAllDropPrefix(t *testing.T) {
	key := func(i int) []byte {
		return []byte(fmt.Sprintf("%10d", i))
	}
	val := func(i int) []byte {
		return []byte(fmt.Sprintf("%128d", i))
	}
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		wb := db.NewWriteBatch()
		defer wb.Cancel()

		N := 50000

		for i := 0; i < N; i++ {
			require.NoError(t, wb.SetAt(key(i), val(i), 1))
		}
		require.NoError(t, wb.Flush())

		var wg sync.WaitGroup
		wg.Add(3)
		go func() {
			defer wg.Done()
			err := db.DropPrefix([]byte("000"))
			for errors.Is(err, ErrBlockedWrites) {
				err = db.DropPrefix([]byte("000"))
				time.Sleep(time.Millisecond * 500)
			}
			require.NoError(t, err)
		}()
		go func() {
			defer wg.Done()
			err := db.DropPrefix([]byte("111"))
			for errors.Is(err, ErrBlockedWrites) {
				err = db.DropPrefix([]byte("111"))
				time.Sleep(time.Millisecond * 500)
			}
			require.NoError(t, err)
		}()
		go func() {
			time.Sleep(time.Millisecond) // Let drop prefix run first.
			defer wg.Done()
			err := db.DropAll()
			for errors.Is(err, ErrBlockedWrites) {
				err = db.DropAll()
				time.Sleep(time.Millisecond * 300)
			}
			require.NoError(t, err)
		}()
		wg.Wait()
	})
}

func TestIsClosed(t *testing.T) {
	test := func(inMemory bool) {
		opt := DefaultOptions("")
		if inMemory {
			opt.InMemory = true
		} else {
			dir, err := ioutil.TempDir("", "badger-test")
			require.NoError(t, err)
			defer removeDir(dir)

			opt.Dir = dir
		}

		db, err := Open(opt)
		require.NoError(t, err)
		require.False(t, db.IsClosed())
		require.NoError(t, db.Close())
		require.True(t, db.IsClosed())
	}

	t.Run("normal", func(t *testing.T) {
		test(false)
	})
	t.Run("in-memory", func(t *testing.T) {
		test(true)
	})

}

// This test is failing currently because we're returning version+1 from MaxVersion()
func TestMaxVersion(t *testing.T) {
	N := 10000
	t.Run("multiple versions", func(t *testing.T) {
		dir, err := ioutil.TempDir("", "badger-test")
		require.NoError(t, err)
		defer removeDir(dir)

		opt := getTestOptions(dir)
		opt.NumVersionsToKeep = 100
		db, err := Open(opt)
		require.NoError(t, err)

		wb := db.NewWriteBatch()
		defer wb.Cancel()

		k := make([]byte, 100)
		rand.Read(k)
		// Create multiple version of the same key.
		for i := 1; i <= N; i++ {
			wb.SetEntryAt(&Entry{Key: k}, uint64(i))
		}
		require.NoError(t, wb.Flush())

		ver := db.MaxVersion()
		require.Equal(t, N, int(ver))

		require.NoError(t, db.Close())
	})
	t.Run("Managed mode", func(t *testing.T) {
		dir, err := ioutil.TempDir("", "badger-test")
		require.NoError(t, err)
		defer removeDir(dir)

		opt := getTestOptions(dir)
		db, err := Open(opt)
		require.NoError(t, err)

		wb := db.NewWriteBatch()
		defer wb.Cancel()

		// This will create commits from 1 to N.
		for i := 1; i <= N; i++ {
			wb.SetEntryAt(&Entry{Key: []byte(fmt.Sprintf("%d", i))}, uint64(i))
		}
		require.NoError(t, wb.Flush())

		ver := db.MaxVersion()
		require.NoError(t, err)
		require.Equal(t, N, int(ver))

		require.NoError(t, db.Close())
	})
}

// This tests failed for stream writer with jemalloc and compression enabled.
func TestKeyCount(t *testing.T) {
	if !*manual {
		t.Skip("Skipping test meant to be run manually.")
		return
	}

	writeSorted := func(db *DB, num uint64) {
		valSz := 128
		value := make([]byte, valSz)
		y.Check2(rand.Read(value))
		es := 8 + valSz // key size is 8 bytes and value size is valSz

		writer := db.NewStreamWriter()
		require.NoError(t, writer.Prepare())

		wg := &sync.WaitGroup{}
		writeCh := make(chan *pb.KVList, 3)
		writeRange := func(start, end uint64, streamId uint32) {
			// end is not included.
			defer wg.Done()
			kvs := &pb.KVList{}
			var sz int
			for i := start; i < end; i++ {
				key := make([]byte, 8)
				binary.BigEndian.PutUint64(key, i)
				kvs.Kv = append(kvs.Kv, &pb.KV{
					Key:      key,
					Value:    value,
					Version:  1,
					StreamId: streamId,
				})

				sz += es

				if sz >= 4<<20 { // 4 MB
					writeCh <- kvs
					kvs = &pb.KVList{}
					sz = 0
				}
			}
			writeCh <- kvs
		}

		// Let's create some streams.
		width := num / 16
		streamID := uint32(0)
		for start := uint64(0); start < num; start += width {
			end := start + width
			if end > num {
				end = num
			}
			streamID++
			wg.Add(1)
			go writeRange(start, end, streamID)
		}
		go func() {
			wg.Wait()
			close(writeCh)
		}()

		write := func(kvs *pb.KVList) error {
			buf := z.NewBuffer(1<<20, "test")
			defer buf.Release()

			for _, kv := range kvs.Kv {
				KVToBuffer(kv, buf)
			}
			writer.Write(buf)
			return nil
		}

		for kvs := range writeCh {
			require.NoError(t, write(kvs))
		}
		require.NoError(t, writer.Flush())
	}

	N := uint64(10 * 1e6) // 10 million entries
	dir, err := ioutil.TempDir("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)
	opt := DefaultOptions(dir).
		WithBlockCacheSize(100 << 20).
		WithCompression(options.ZSTD)

	db, err := Open(opt)
	y.Check(err)
	defer db.Close()
	writeSorted(db, N)
	require.NoError(t, db.Close())
	t.Logf("Writing DONE\n")

	// Read the db
	db2, err := Open(DefaultOptions(dir))
	y.Check(err)
	defer db.Close()
	lastKey := -1
	count := 0

	streams := make(map[uint32]int)
	stream := db2.NewStreamAt(1)
	stream.Send = func(buf *z.Buffer) error {
		list, err := BufferToKVList(buf)
		if err != nil {
			return err
		}
		for _, kv := range list.Kv {
			last := streams[kv.StreamId]
			key := binary.BigEndian.Uint64(kv.Key)
			// The following should happen as we're writing sorted data.
			if last > 0 {
				require.Equalf(t, last+1, int(key), "Expected key: %d, Found Key: %d", lastKey+1, int(key))
			}
			streams[kv.StreamId] = int(key)
		}
		count += len(list.Kv)
		return nil
	}
	require.NoError(t, stream.Orchestrate(context.Background()))
	require.Equal(t, N, uint64(count))
}

func TestDropPrefixNonBlocking(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)

	db, err := Open(DefaultOptions(dir).WithAllowStopTheWorld(false))
	require.NoError(t, err)
	defer db.Close()

	val := []byte("value")

	// Insert key-values
	write := func() {
		wb := db.NewWriteBatch()
		require.NoError(t, wb.SetAt([]byte("aaa"), val, 2))
		require.NoError(t, wb.SetAt([]byte("aab"), val, 2))
		require.NoError(t, wb.SetAt([]byte("aba"), val, 2))
		require.NoError(t, wb.SetAt([]byte("aca"), val, 2))
		require.NoError(t, wb.Flush())
	}

	read := func() {
		txn := db.NewReadTxn(6)
		defer txn.Discard()
		iterOpts := DefaultIteratorOptions
		iterOpts.Prefix = []byte("aa")
		it := txn.NewIterator(iterOpts)
		defer it.Close()

		cnt := 0
		for it.Rewind(); it.Valid(); it.Next() {
			fmt.Printf("%+v", it.Item())
			cnt++
		}

		require.Equal(t, 0, cnt)
	}

	write()
	prefixes := [][]byte{[]byte("aa")}
	require.NoError(t, db.DropPrefix(prefixes...))
	read()
}

func TestDropPrefixNonBlockingNoError(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)

	opt := DefaultOptions(dir)
	db, err := Open(opt)
	require.NoError(t, err)
	defer db.Close()

	clock := uint64(1)

	writer := func(db *DB, shouldFail bool, closer *z.Closer) {
		val := []byte("value")
		defer closer.Done()
		// Insert key-values
		for {
			select {
			case <-closer.HasBeenClosed():
				return
			default:
				wb := db.NewWriteBatch()
				vs := atomic.AddUint64(&clock, 2)
				require.NoError(t, wb.SetEntryAt(NewEntry([]byte("aaa"), val), vs))
				err := wb.Flush()

				if shouldFail && err != nil {
					require.Error(t, err, ErrBlockedWrites)
				} else if !shouldFail {
					require.NoError(t, err)
				}
			}
		}
	}

	closer := z.NewCloser(1)
	go writer(db, true, closer)
	time.Sleep(time.Millisecond * 100)
	require.NoError(t, db.DropPrefixBlocking([]byte("aa")))
	closer.SignalAndWait()

	closer2 := z.NewCloser(1)
	go writer(db, false, closer2)
	time.Sleep(time.Millisecond * 50)
	prefixes := [][]byte{[]byte("aa")}
	require.NoError(t, db.DropPrefixNonBlocking(prefixes...))
	closer2.SignalAndWait()
}
