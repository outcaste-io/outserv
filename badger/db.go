/*
 * Copyright 2017 Dgraph Labs, Inc. and Contributors
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
	"bytes"
	"context"
	"expvar"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	humanize "github.com/dustin/go-humanize"
	"github.com/outcaste-io/badger/v3/options"
	"github.com/outcaste-io/badger/v3/pb"
	"github.com/outcaste-io/badger/v3/skl"
	"github.com/outcaste-io/badger/v3/table"
	"github.com/outcaste-io/badger/v3/y"
	"github.com/outcaste-io/ristretto"
	"github.com/outcaste-io/ristretto/z"
	"github.com/pkg/errors"
)

// Values have their first byte being byteData or byteDelete. This helps us distinguish between
// a key that has never been seen and a key that has been explicitly deleted.
const (
	bitDelete                 byte = 1 << 0 // Set if the key has been deleted.
	BitDiscardEarlierVersions byte = 1 << 2 // Set if earlier versions can be discarded.
)

var (
	badgerPrefix = []byte("!badger!")       // Prefix for internal keys used by badger.
	bannedNsKey  = []byte("!badger!banned") // For storing the banned namespaces.
)

type closers struct {
	updateSize  *z.Closer
	compactors  *z.Closer
	memtable    *z.Closer
	writes      *z.Closer
	pub         *z.Closer
	cacheHealth *z.Closer
}

type lockedKeys struct {
	sync.RWMutex
	keys map[uint64]struct{}
}

func (lk *lockedKeys) add(key uint64) {
	lk.Lock()
	defer lk.Unlock()
	lk.keys[key] = struct{}{}
}

func (lk *lockedKeys) has(key uint64) bool {
	lk.RLock()
	defer lk.RUnlock()
	_, ok := lk.keys[key]
	return ok
}

func (lk *lockedKeys) all() []uint64 {
	lk.RLock()
	defer lk.RUnlock()
	keys := make([]uint64, 0, len(lk.keys))
	for key := range lk.keys {
		keys = append(keys, key)
	}
	return keys
}

// DB provides the various functions required to interact with Badger.
// DB is thread-safe.
type DB struct {
	lock sync.RWMutex // Guards list of inmemory tables, not individual reads and writes.

	dirLockGuard *directoryLockGuard
	// nil if Dir and ValueDir are the same
	valueDirGuard *directoryLockGuard

	closers closers

	imm []*skl.Skiplist // Add here only AFTER pushing to flushChan.

	discardTs uint64
	opt       Options
	manifest  *manifestFile
	lc        *levelsController
	sklCh     chan *handoverRequest
	flushChan chan flushTask // For flushing memtables.
	closeOnce sync.Once      // For closing DB only once.

	blockWrites int32
	isClosed    uint32

	bannedNamespaces *lockedKeys

	pub        *publisher
	registry   *KeyRegistry
	blockCache *ristretto.Cache
	indexCache *ristretto.Cache
	allocPool  *z.AllocatorPool
}

func checkAndSetOptions(opt *Options) error {
	// It's okay to have zero compactors which will disable all compactions but
	// we cannot have just one compactor otherwise we will end up with all data
	// on level 2.
	if opt.NumCompactors == 1 {
		return errors.New("Cannot have 1 compactor. Need at least 2")
	}

	if opt.InMemory && opt.Dir != "" {
		return errors.New("Cannot use badger in Disk-less mode with Dir set")
	}
	opt.maxBatchSize = (15 * opt.MemTableSize) / 100
	opt.maxBatchCount = opt.maxBatchSize / int64(skl.MaxNodeSize)

	// This is the maximum value, vlogThreshold can have if dynamic thresholding is enabled.
	opt.maxValueThreshold = math.Min(maxValueThreshold, float64(opt.maxBatchSize))
	if opt.ReadOnly {
		// Do not perform compaction in read only mode.
		opt.CompactL0OnClose = false
	}

	needCache := (opt.Compression != options.None) || (len(opt.EncryptionKey) > 0)
	if needCache && opt.BlockCacheSize == 0 {
		panic("BlockCacheSize should be set since compression/encryption are enabled")
	}
	return nil
}

// Open returns a new DB, which allows more control over setting
// transaction timestamps, aka managed mode.
//
// This is only useful for databases built on top of Badger (like Dgraph), and
// can be ignored by most users.
func Open(opt Options) (*DB, error) {
	if err := checkAndSetOptions(&opt); err != nil {
		return nil, err
	}
	var dirLockGuard, valueDirLockGuard *directoryLockGuard

	// Create directories and acquire lock on it only if badger is not running in InMemory mode.
	// We don't have any directories/files in InMemory mode so we don't need to acquire
	// any locks on them.
	if !opt.InMemory {
		if err := createDirs(opt); err != nil {
			return nil, err
		}
		var err error
		if !opt.BypassLockGuard {
			dirLockGuard, err = acquireDirectoryLock(opt.Dir, lockFile, opt.ReadOnly)
			if err != nil {
				return nil, err
			}
			defer func() {
				if dirLockGuard != nil {
					_ = dirLockGuard.release()
				}
			}()
		}
	}

	manifestFile, manifest, err := openOrCreateManifestFile(opt)
	if err != nil {
		return nil, err
	}
	defer func() {
		if manifestFile != nil {
			_ = manifestFile.close()
		}
	}()

	db := &DB{
		imm:              make([]*skl.Skiplist, 0, opt.NumMemtables),
		flushChan:        make(chan flushTask, opt.NumMemtables),
		sklCh:            make(chan *handoverRequest),
		opt:              opt,
		manifest:         manifestFile,
		dirLockGuard:     dirLockGuard,
		valueDirGuard:    valueDirLockGuard,
		pub:              newPublisher(),
		allocPool:        z.NewAllocatorPool(8),
		bannedNamespaces: &lockedKeys{keys: make(map[uint64]struct{})},
	}
	// Cleanup all the goroutines started by badger in case of an error.
	defer func() {
		if err != nil {
			opt.Errorf("Received err: %v. Cleaning up...", err)
			db.cleanup()
			db = nil
		}
	}()

	if opt.BlockCacheSize > 0 {
		numInCache := opt.BlockCacheSize / int64(opt.BlockSize)
		if numInCache == 0 {
			// Make the value of this variable at least one since the cache requires
			// the number of counters to be greater than zero.
			numInCache = 1
		}

		config := ristretto.Config{
			NumCounters: numInCache * 8,
			MaxCost:     opt.BlockCacheSize,
			BufferItems: 64,
			Metrics:     true,
			OnExit:      table.BlockEvictHandler,
		}
		db.blockCache, err = ristretto.NewCache(&config)
		if err != nil {
			return nil, y.Wrap(err, "failed to create data cache")
		}
	}

	if opt.IndexCacheSize > 0 {
		// Index size is around 5% of the table size.
		indexSz := int64(float64(opt.MemTableSize) * 0.05)
		numInCache := opt.IndexCacheSize / indexSz
		if numInCache == 0 {
			// Make the value of this variable at least one since the cache requires
			// the number of counters to be greater than zero.
			numInCache = 1
		}

		config := ristretto.Config{
			NumCounters: numInCache * 8,
			MaxCost:     opt.IndexCacheSize,
			BufferItems: 64,
			Metrics:     true,
		}
		db.indexCache, err = ristretto.NewCache(&config)
		if err != nil {
			return nil, y.Wrap(err, "failed to create bf cache")
		}
	}

	db.closers.cacheHealth = z.NewCloser(1)
	go db.monitorCache(db.closers.cacheHealth)

	krOpt := KeyRegistryOptions{
		ReadOnly:                      opt.ReadOnly,
		Dir:                           opt.Dir,
		EncryptionKey:                 opt.EncryptionKey,
		EncryptionKeyRotationDuration: opt.EncryptionKeyRotationDuration,
		InMemory:                      opt.InMemory,
	}

	if db.registry, err = OpenKeyRegistry(krOpt); err != nil {
		return db, err
	}
	db.calculateSize()
	db.closers.updateSize = z.NewCloser(1)
	go db.updateSize(db.closers.updateSize)

	// newLevelsController potentially loads files in directory.
	if db.lc, err = newLevelsController(db, &manifest); err != nil {
		return db, err
	}

	if !opt.ReadOnly {
		db.closers.compactors = z.NewCloser(1)
		db.lc.startCompact(db.closers.compactors)

		db.closers.memtable = z.NewCloser(1)
		go func() {
			_ = db.flushMemtable(db.closers.memtable) // Need levels controller to be up.
		}()
		// Flush them to disk asap.
		for _, mt := range db.imm {
			db.flushChan <- flushTask{sl: mt}
		}
	}

	if err := db.initBannedNamespaces(); err != nil {
		return db, errors.Wrapf(err, "While setting banned keys")
	}

	db.closers.writes = z.NewCloser(1)
	go db.handleHandovers(db.closers.writes)

	db.closers.pub = z.NewCloser(1)
	go db.pub.listenForUpdates(db.closers.pub)

	valueDirLockGuard = nil
	dirLockGuard = nil
	manifestFile = nil
	return db, nil
}

// initBannedNamespaces retrieves the banned namepsaces from the DB and updates in-memory structure.
func (db *DB) initBannedNamespaces() error {
	if db.opt.NamespaceOffset < 0 {
		return nil
	}
	return db.View(func(txn *Txn) error {
		iopts := DefaultIteratorOptions
		iopts.Prefix = bannedNsKey
		iopts.PrefetchValues = false
		iopts.InternalAccess = true
		itr := txn.NewIterator(iopts)
		defer itr.Close()
		for itr.Rewind(); itr.Valid(); itr.Next() {
			key := y.BytesToU64(itr.Item().Key()[len(bannedNsKey):])
			db.bannedNamespaces.add(key)
		}
		return nil
	})
}

func (db *DB) MaxVersion() uint64 {
	var maxVersion uint64
	update := func(a uint64) {
		if a > maxVersion {
			maxVersion = a
		}
	}
	for _, ti := range db.Tables() {
		update(ti.MaxVersion)
	}
	return maxVersion
}

func (db *DB) monitorCache(c *z.Closer) {
	defer c.Done()
	count := 0
	analyze := func(name string, metrics *ristretto.Metrics) {
		// If the mean life expectancy is less than 10 seconds, the cache
		// might be too small.
		le := metrics.LifeExpectancySeconds()
		if le == nil {
			return
		}
		lifeTooShort := le.Count > 0 && float64(le.Sum)/float64(le.Count) < 10
		hitRatioTooLow := metrics.Ratio() > 0 && metrics.Ratio() < 0.4
		if lifeTooShort && hitRatioTooLow {
			db.opt.Warningf("%s might be too small. Metrics: %s\n", name, metrics)
			db.opt.Warningf("Cache life expectancy (in seconds): %+v\n", le)

		} else if le.Count > 1000 && count%5 == 0 {
			db.opt.Infof("%s metrics: %s\n", name, metrics)
		}
	}

	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-c.HasBeenClosed():
			return
		case <-ticker.C:
		}

		analyze("Block cache", db.BlockCacheMetrics())
		analyze("Index cache", db.IndexCacheMetrics())
		count++
	}
}

// cleanup stops all the goroutines started by badger. This is used in open to
// cleanup goroutines in case of an error.
func (db *DB) cleanup() {
	db.stopMemoryFlush()
	db.stopCompactions()

	db.blockCache.Close()
	db.indexCache.Close()
	if db.closers.updateSize != nil {
		db.closers.updateSize.Signal()
	}
	if db.closers.writes != nil {
		db.closers.writes.Signal()
	}
	if db.closers.pub != nil {
		db.closers.pub.Signal()
	}
}

// BlockCacheMetrics returns the metrics for the underlying block cache.
func (db *DB) BlockCacheMetrics() *ristretto.Metrics {
	if db.blockCache != nil {
		return db.blockCache.Metrics
	}
	return nil
}

// IndexCacheMetrics returns the metrics for the underlying index cache.
func (db *DB) IndexCacheMetrics() *ristretto.Metrics {
	if db.indexCache != nil {
		return db.indexCache.Metrics
	}
	return nil
}

// Close closes a DB. It's crucial to call it to ensure all the pending updates make their way to
// disk. Calling DB.Close() multiple times would still only close the DB once.
func (db *DB) Close() error {
	var err error
	db.closeOnce.Do(func() {
		err = db.close()
	})
	return err
}

// IsClosed denotes if the badger DB is closed or not. A DB instance should not
// be used after closing it.
func (db *DB) IsClosed() bool {
	return atomic.LoadUint32(&db.isClosed) == 1
}

func (db *DB) close() (err error) {
	defer db.allocPool.Release()

	db.opt.Debugf("Closing database")
	db.opt.Infof("Lifetime L0 stalled for: %s\n", time.Duration(atomic.LoadInt64(&db.lc.l0stallsMs)))

	atomic.StoreInt32(&db.blockWrites, 1)

	// Stop writes next.
	db.closers.writes.SignalAndWait()

	// Don't accept any more write.
	close(db.sklCh)

	db.closers.pub.SignalAndWait()
	db.closers.cacheHealth.Signal()

	db.stopMemoryFlush()
	db.stopCompactions()

	// Force Compact L0
	// We don't need to care about cstatus since no parallel compaction is running.
	if db.opt.CompactL0OnClose {
		err := db.lc.doCompact(173, compactionPriority{level: 0, score: 1.73})
		switch {
		case errors.Is(err, errFillTables):
			// This error only means that there might be enough tables to do a compaction. So, we
			// should not report it to the end user to avoid confusing them.
		case err == nil:
			db.opt.Debugf("Force compaction on level 0 done")
		default:
			db.opt.Warningf("While forcing compaction on level 0: %v", err)
		}
	}

	db.opt.Infof(db.LevelsToString())
	if lcErr := db.lc.close(); err == nil {
		err = y.Wrap(lcErr, "DB.Close")
	}
	db.opt.Debugf("Waiting for closer")
	db.closers.updateSize.SignalAndWait()
	db.blockCache.Close()
	db.indexCache.Close()

	atomic.StoreUint32(&db.isClosed, 1)

	if db.opt.InMemory {
		return
	}

	if db.dirLockGuard != nil {
		if guardErr := db.dirLockGuard.release(); err == nil {
			err = y.Wrap(guardErr, "DB.Close")
		}
	}
	if db.valueDirGuard != nil {
		if guardErr := db.valueDirGuard.release(); err == nil {
			err = y.Wrap(guardErr, "DB.Close")
		}
	}
	if manifestErr := db.manifest.close(); err == nil {
		err = y.Wrap(manifestErr, "DB.Close")
	}
	if registryErr := db.registry.Close(); err == nil {
		err = y.Wrap(registryErr, "DB.Close")
	}

	// Fsync directories to ensure that lock file, and any other removed files whose directory
	// we haven't specifically fsynced, are guaranteed to have their directory entry removal
	// persisted to disk.
	if syncErr := db.syncDir(db.opt.Dir); err == nil {
		err = y.Wrap(syncErr, "DB.Close")
	}
	return err
}

// VerifyChecksum verifies checksum for all tables on all levels.
// This method can be used to verify checksum, if opt.ChecksumVerificationMode is NoVerification.
func (db *DB) VerifyChecksum() error {
	return db.lc.verifyChecksum()
}

const (
	lockFile = "LOCK"
)

// Sync is a NOOP. SSTables are always synced to disk.
func (db *DB) Sync() error { return nil }

// getMemtables returns the current memtables and get references.
func (db *DB) getMemTables() ([]*skl.Skiplist, func()) {
	db.lock.RLock()
	defer db.lock.RUnlock()

	var tables []*skl.Skiplist

	// Get immutable memtables.
	last := len(db.imm) - 1
	for i := range db.imm {
		tables = append(tables, db.imm[last-i])
		db.imm[last-i].IncrRef()
	}
	return tables, func() {
		for _, tbl := range tables {
			tbl.DecrRef()
		}
	}
}

// get returns the value in memtable or disk for given key.
// Note that value will include meta byte.
//
// IMPORTANT: We should never write an entry with an older timestamp for the same key, We need to
// maintain this invariant to search for the latest value of a key, or else we need to search in all
// tables and find the max version among them.  To maintain this invariant, we also need to ensure
// that all versions of a key are always present in the same table from level 1, because compaction
// can push any table down.
//
// Update(23/09/2020) - We have dropped the move key implementation. Earlier we
// were inserting move keys to fix the invalid value pointers but we no longer
// do that. For every get("fooX") call where X is the version, we will search
// for "fooX" in all the levels of the LSM tree. This is expensive but it
// removes the overhead of handling move keys completely.
func (db *DB) get(key []byte) (y.ValueStruct, error) {
	if db.IsClosed() {
		return y.ValueStruct{}, ErrDBClosed
	}
	tables, decr := db.getMemTables() // Lock should be released.
	defer decr()

	var maxVs y.ValueStruct
	version := y.ParseTs(key)

	y.NumGetsAdd(db.opt.MetricsEnabled, 1)
	for i := 0; i < len(tables); i++ {
		vs := tables[i].Get(key)
		y.NumMemtableGetsAdd(db.opt.MetricsEnabled, 1)
		if vs.Meta == 0 && vs.Value == nil {
			continue
		}
		// Found the required version of the key, return immediately.
		if vs.Version == version {
			return vs, nil
		}
		if maxVs.Version < vs.Version {
			maxVs = vs
		}
	}
	return db.lc.get(key, maxVs, 0)
}

func (db *DB) handleHandovers(lc *z.Closer) {
	defer lc.Done()
	for {
		select {
		case r := <-db.sklCh:
			for {
				db.lock.Lock()
				r.err = db.handoverSkiplist(r, isLocked{})
				db.lock.Unlock()
				if r.err != errNoRoom {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			r.wg.Done()
		case <-lc.HasBeenClosed():
			return
		}
	}
}

// batchSetAsync is the asynchronous version of batchSet. It accepts a callback
// function which is called when all the sets are complete. If a request level
// error occurs, it will be passed back via the callback.
//   err := kv.BatchSetAsync(entries, func(err error)) {
//      Check(err)
//   }
func (db *DB) BatchSet(entries []*Entry) error {
	wb := db.NewWriteBatch()
	for _, e := range entries {
		if err := wb.SetEntry(e); err != nil {
			return err
		}
	}
	return wb.Flush()
}

var errNoRoom = errors.New("No room for write")

type isLocked struct{}

func (db *DB) handoverSkiplist(r *handoverRequest, _ isLocked) error {
	sl, callback := r.skl, r.callback
	req := &request{
		Skl: sl,
	}
	reqs := []*request{req}

	select {
	case db.flushChan <- flushTask{sl: sl, cb: callback}:
		db.imm = append(db.imm, sl)
		db.pub.sendUpdates(reqs)
		return nil
	default:
		return errNoRoom
	}
}

func (db *DB) HandoverSkiplist(skl *skl.Skiplist, callback func()) error {
	if atomic.LoadInt32(&db.blockWrites) == 1 {
		return ErrBlockedWrites
	}

	req := &handoverRequest{skl: skl, callback: callback}
	req.wg.Add(1)
	db.sklCh <- req
	req.wg.Wait()
	return req.err
}

func arenaSize(opt Options) int64 {
	return opt.MemTableSize + opt.maxBatchSize + opt.maxBatchCount*int64(skl.MaxNodeSize)
}

func (db *DB) NewSkiplist() *skl.Skiplist {
	return skl.NewSkiplist(arenaSize(db.opt))
}

// buildL0Table builds a new table from the memtable.
func buildL0Table(ft flushTask, bopts table.Options) *table.Builder {
	var iter y.Iterator
	if ft.itr != nil {
		iter = ft.itr
	} else {
		iter = ft.sl.NewUniIterator(false)
	}
	defer iter.Close()

	b := table.NewTableBuilder(bopts)
	for iter.Rewind(); iter.Valid(); iter.Next() {
		if len(ft.dropPrefixes) > 0 && hasAnyPrefixes(iter.Key(), ft.dropPrefixes) {
			continue
		}
		b.Add(iter.Key(), iter.Value())
	}
	return b
}

type flushTask struct {
	sl           *skl.Skiplist
	cb           func()
	itr          y.Iterator
	dropPrefixes [][]byte
}

// handleFlushTask must be run serially.
func (db *DB) handleFlushTask(ft flushTask) error {
	// ft.mt could be nil with ft.itr being the valid field.
	bopts := buildTableOptions(db)
	builder := buildL0Table(ft, bopts)
	defer builder.Close()

	// buildL0Table can return nil if the none of the items in the skiplist are
	// added to the builder. This can happen when drop prefix is set and all
	// the items are skipped.
	if builder.Empty() {
		builder.Finish()
		return nil
	}

	fileID := db.lc.reserveFileID()
	var tbl *table.Table
	var err error
	if db.opt.InMemory {
		data := builder.Finish()
		tbl, err = table.OpenInMemoryTable(data, fileID, &bopts)
	} else {
		tbl, err = table.CreateTable(table.NewFilename(fileID, db.opt.Dir), builder)
	}
	if err != nil {
		return y.Wrap(err, "error while creating table")
	}
	// We own a ref on tbl.
	err = db.lc.addLevel0Table(tbl) // This will incrRef
	_ = tbl.DecrRef()               // Releases our ref.
	return err
}

// flushMemtable must keep running until we send it an empty flushTask. If there
// are errors during handling the flush task, we'll retry indefinitely.
func (db *DB) flushMemtable(lc *z.Closer) error {
	defer lc.Done()

	var sz int64
	var itrs []y.Iterator
	var mts []*skl.Skiplist
	var cbs []func()
	slurp := func() {
		for {
			select {
			case more := <-db.flushChan:
				if more.sl == nil {
					return
				}
				sl := more.sl
				itrs = append(itrs, sl.NewUniIterator(false))
				mts = append(mts, more.sl)
				cbs = append(cbs, more.cb)

				sz += sl.MemSize()
				if sz > db.opt.MemTableSize {
					return
				}
			default:
				return
			}
		}
	}

	for ft := range db.flushChan {
		if ft.sl == nil {
			// We close db.flushChan now, instead of sending a nil ft.mt.
			continue
		}
		sz = ft.sl.MemSize()
		// Reset of itrs, mts etc. is being done below.
		y.AssertTrue(len(itrs) == 0 && len(mts) == 0 && len(cbs) == 0)
		itrs = append(itrs, ft.sl.NewUniIterator(false))
		mts = append(mts, ft.sl)
		cbs = append(cbs, ft.cb)

		// Pick more memtables, so we can really fill up the L0 table.
		slurp()

		// db.opt.Infof("Picked %d memtables. Size: %d\n", len(itrs), sz)
		ft.sl = nil
		ft.itr = table.NewMergeIterator(itrs, false)
		ft.cb = nil

		for {
			err := db.handleFlushTask(ft)
			if err == nil {
				// Update s.imm. Need a lock.
				db.lock.Lock()
				// This is a single-threaded operation. ft.mt corresponds to the head of
				// db.imm list. Once we flush it, we advance db.imm. The next ft.mt
				// which would arrive here would match db.imm[0], because we acquire a
				// lock over DB when pushing to flushChan.
				// TODO: This logic is dirty AF. Any change and this could easily break.
				for _, mt := range mts {
					y.AssertTrue(mt == db.imm[0])
					db.imm = db.imm[1:]
					mt.DecrRef() // Return memory.
				}
				db.lock.Unlock()

				for _, cb := range cbs {
					if cb != nil {
						cb()
					}
				}
				break
			}
			// Encountered error. Retry indefinitely.
			db.opt.Errorf("Failure while flushing memtable to disk: %v. Retrying...\n", err)
			time.Sleep(time.Second)
		}
		// Reset everything.
		itrs, mts, cbs, sz = itrs[:0], mts[:0], cbs[:0], 0
	}
	return nil
}

func exists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return true, err
}

// This function does a filewalk, calculates the size of vlog and sst files and stores it in
// y.LSMSize and y.VlogSize.
func (db *DB) calculateSize() {
	if db.opt.InMemory {
		return
	}
	newInt := func(val int64) *expvar.Int {
		v := new(expvar.Int)
		v.Add(val)
		return v
	}

	totalSize := func(dir string) int64 {
		var lsmSize int64
		err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			ext := filepath.Ext(path)
			switch ext {
			case ".sst":
				lsmSize += info.Size()
			}
			return nil
		})
		if err != nil {
			db.opt.Debugf("Got error while calculating total size of directory: %s", dir)
		}
		return lsmSize
	}

	lsmSize := totalSize(db.opt.Dir)
	y.LSMSizeSet(db.opt.MetricsEnabled, db.opt.Dir, newInt(lsmSize))
}

func (db *DB) updateSize(lc *z.Closer) {
	defer lc.Done()
	if db.opt.InMemory {
		return
	}

	metricsTicker := time.NewTicker(time.Minute)
	defer metricsTicker.Stop()

	for {
		select {
		case <-metricsTicker.C:
			db.calculateSize()
		case <-lc.HasBeenClosed():
			return
		}
	}
}

// Size returns the size of lsm and value log files in bytes. It can be used to decide how often to
// call RunValueLogGC.
func (db *DB) Size() (lsm int64) {
	if y.LSMSizeGet(db.opt.MetricsEnabled, db.opt.Dir) == nil {
		lsm = 0
		return
	}
	lsm = y.LSMSizeGet(db.opt.MetricsEnabled, db.opt.Dir).(*expvar.Int).Value()
	return
}

// Tables gets the TableInfo objects from the level controller. If withKeysCount
// is true, TableInfo objects also contain counts of keys for the tables.
func (db *DB) Tables() []TableInfo {
	return db.lc.getTableInfo()
}

// Levels gets the LevelInfo.
func (db *DB) Levels() []LevelInfo {
	return db.lc.getLevelInfo()
}

// EstimateSize can be used to get rough estimate of data size for a given prefix.
func (db *DB) EstimateSize(prefix []byte) (uint64, uint64) {
	var onDiskSize, uncompressedSize uint64
	tables := db.Tables()
	for _, ti := range tables {
		if bytes.HasPrefix(ti.Left, prefix) && bytes.HasPrefix(ti.Right, prefix) {
			onDiskSize += uint64(ti.OnDiskSize)
			uncompressedSize += uint64(ti.UncompressedSize)
		}
	}
	return onDiskSize, uncompressedSize
}

// Ranges can be used to get rough key ranges to divide up iteration over the DB. The ranges here
// would consider the prefix, but would not necessarily start or end with the prefix. In fact, the
// first range would have nil as left key, and the last range would have nil as the right key.
func (db *DB) Ranges(prefix []byte, numRanges int) []*keyRange {
	var splits []string
	tables := db.Tables()

	// We just want table ranges here and not keys count.
	for _, ti := range tables {
		// We don't use ti.Left, because that has a tendency to store !badger keys. Skip over tables
		// at upper levels. Only choose tables from the last level.
		if ti.Level != db.opt.MaxLevels-1 {
			continue
		}
		if bytes.HasPrefix(ti.Right, prefix) {
			splits = append(splits, string(ti.Right))
		}
	}

	// If the number of splits is low, look at the offsets inside the
	// tables to generate more splits.
	if len(splits) < 32 {
		numTables := len(tables)
		if numTables == 0 {
			numTables = 1
		}
		numPerTable := 32 / numTables
		if numPerTable == 0 {
			numPerTable = 1
		}
		splits = db.lc.keySplits(numPerTable, prefix)
	}

	// If the number of splits is still < 32, then look at the memtables.
	if len(splits) < 32 {
		maxPerSplit := 10000
		mtSplits := func(sl *skl.Skiplist) {
			if sl == nil {
				return
			}
			count := 0
			iter := sl.NewIterator()
			for iter.SeekToFirst(); iter.Valid(); iter.Next() {
				if count%maxPerSplit == 0 {
					// Add a split every maxPerSplit keys.
					if bytes.HasPrefix(iter.Key(), prefix) {
						splits = append(splits, string(iter.Key()))
					}
				}
				count += 1
			}
			_ = iter.Close()
		}

		db.lock.Lock()
		defer db.lock.Unlock()
		var memTables []*skl.Skiplist
		memTables = append(memTables, db.imm...)
		for _, mt := range memTables {
			mtSplits(mt)
		}
	}

	// We have our splits now. Let's convert them to ranges.
	sort.Strings(splits)
	var ranges []*keyRange
	var start []byte
	for _, key := range splits {
		ranges = append(ranges, &keyRange{left: start, right: y.SafeCopy(nil, []byte(key))})
		start = y.SafeCopy(nil, []byte(key))
	}
	ranges = append(ranges, &keyRange{left: start})

	// Figure out the approximate table size this range has to deal with.
	for _, t := range tables {
		tr := keyRange{left: t.Left, right: t.Right}
		for _, r := range ranges {
			if len(r.left) == 0 || len(r.right) == 0 {
				continue
			}
			if r.overlapsWith(tr) {
				r.size += int64(t.UncompressedSize)
			}
		}
	}

	var total int64
	for _, r := range ranges {
		total += r.size
	}
	if total == 0 {
		return ranges
	}
	// Figure out the average size, so we know how to bin the ranges together.
	avg := total / int64(numRanges)

	var out []*keyRange
	var i int
	for i < len(ranges) {
		r := ranges[i]
		cur := &keyRange{left: r.left, size: r.size, right: r.right}
		i++
		for ; i < len(ranges); i++ {
			next := ranges[i]
			if cur.size+next.size > avg {
				break
			}
			cur.right = next.right
			cur.size += next.size
		}
		out = append(out, cur)
	}
	return out
}

// MaxBatchCount returns max possible entries in batch
func (db *DB) MaxBatchCount() int64 {
	return db.opt.maxBatchCount
}

// MaxBatchSize returns max possible batch size
func (db *DB) MaxBatchSize() int64 {
	return db.opt.maxBatchSize
}

func (db *DB) stopMemoryFlush() {
	// Stop memtable flushes.
	if db.closers.memtable != nil {
		close(db.flushChan)
		db.closers.memtable.SignalAndWait()
	}
}

func (db *DB) stopCompactions() {
	// Stop compactions.
	if db.closers.compactors != nil {
		db.closers.compactors.SignalAndWait()
	}
}

func (db *DB) startCompactions() {
	// Resume compactions.
	if db.closers.compactors != nil {
		db.closers.compactors = z.NewCloser(1)
		db.lc.startCompact(db.closers.compactors)
	}
}

func (db *DB) startMemoryFlush() {
	// Start memory fluhser.
	if db.closers.memtable != nil {
		db.flushChan = make(chan flushTask, db.opt.NumMemtables)
		db.closers.memtable = z.NewCloser(1)
		go func() {
			_ = db.flushMemtable(db.closers.memtable)
		}()
	}
}

// Flatten can be used to force compactions on the LSM tree so all the tables fall on the same
// level. This ensures that all the versions of keys are colocated and not split across multiple
// levels, which is necessary after a restore from backup. During Flatten, live compactions are
// stopped. Ideally, no writes are going on during Flatten. Otherwise, it would create competition
// between flattening the tree and new tables being created at level zero.
func (db *DB) Flatten(workers int) error {

	db.stopCompactions()
	defer db.startCompactions()

	compactAway := func(cp compactionPriority) error {
		db.opt.Infof("Attempting to compact with %+v\n", cp)
		errCh := make(chan error, 1)
		for i := 0; i < workers; i++ {
			go func() {
				errCh <- db.lc.doCompact(175, cp)
			}()
		}
		var success int
		var rerr error
		for i := 0; i < workers; i++ {
			err := <-errCh
			if err != nil {
				rerr = err
				db.opt.Warningf("While running doCompact with %+v. Error: %v\n", cp, err)
			} else {
				success++
			}
		}
		if success == 0 {
			return rerr
		}
		// We could do at least one successful compaction. So, we'll consider this a success.
		db.opt.Infof("%d compactor(s) succeeded. One or more tables from level %d compacted.\n",
			success, cp.level)
		return nil
	}

	hbytes := func(sz int64) string {
		return humanize.IBytes(uint64(sz))
	}

	t := db.lc.levelTargets()
	for {
		db.opt.Infof("\n")
		var levels []int
		for i, l := range db.lc.levels {
			sz := l.getTotalSize()
			db.opt.Infof("Level: %d. %8s Size. %8s Max.\n",
				i, hbytes(l.getTotalSize()), hbytes(t.targetSz[i]))
			if sz > 0 {
				levels = append(levels, i)
			}
		}
		if len(levels) <= 1 {
			prios := db.lc.pickCompactLevels()
			if len(prios) == 0 || prios[0].score <= 1.0 {
				db.opt.Infof("All tables consolidated into one level. Flattening done.\n")
				return nil
			}
			if err := compactAway(prios[0]); err != nil {
				return err
			}
			continue
		}
		// Create an artificial compaction priority, to ensure that we compact the level.
		cp := compactionPriority{level: levels[0], score: 1.71}
		if err := compactAway(cp); err != nil {
			return err
		}
	}
}

func (db *DB) blockWrite() error {
	// Stop accepting new writes.
	if !atomic.CompareAndSwapInt32(&db.blockWrites, 0, 1) {
		return ErrBlockedWrites
	}

	// Make all pending writes finish. The following will also close writeCh.
	db.closers.writes.SignalAndWait()
	db.opt.Infof("Writes flushed. Stopping compactions now...")
	return nil
}

func (db *DB) unblockWrite() {
	db.closers.writes = z.NewCloser(1)
	go db.handleHandovers(db.closers.writes)

	// Resume writes.
	atomic.StoreInt32(&db.blockWrites, 0)
}

func (db *DB) prepareToDrop() (func(), error) {
	if db.opt.ReadOnly {
		panic("Attempting to drop data in read-only mode.")
	}
	// In order prepare for drop, we need to block the incoming writes and
	// write it to db. Then, flush all the pending flushtask. So that, we
	// don't miss any entries.
	if err := db.blockWrite(); err != nil {
		return func() {}, err
	}
	skls := make([]*handoverRequest, 0, 5)
	for {
		select {
		case skl := <-db.sklCh:
			skls = append(skls, skl)
		default:
			for _, skl := range skls {
				// This isn't locked. But, no writes are happening, so it's OK.
				skl.err = db.handoverSkiplist(skl, isLocked{})
				skl.wg.Done()
				if skl.err != nil {
					db.opt.Errorf("handoverSkiplists: %v", skl.err)
				}
			}
			db.stopMemoryFlush()
			return func() {
				db.opt.Infof("Resuming writes")
				db.startMemoryFlush()
				db.unblockWrite()
			}, nil
		}
	}
}

// DropAll would drop all the data stored in Badger. It does this in the following way.
// - Stop accepting new writes.
// - Pause memtable flushes and compactions.
// - Pick all tables from all levels, create a changeset to delete all these
// tables and apply it to manifest.
// - Pick all log files from value log, and delete all of them. Restart value log files from zero.
// - Resume memtable flushes and compactions.
//
// NOTE: DropAll is resilient to concurrent writes, but not to reads. It is up to the user to not do
// any reads while DropAll is going on, otherwise they may result in panics. Ideally, both reads and
// writes are paused before running DropAll, and resumed after it is finished.
func (db *DB) DropAll() error {
	f, err := db.dropAll()
	if f != nil {
		f()
	}
	return err
}

func (db *DB) dropAll() (func(), error) {
	db.opt.Infof("DropAll called. Blocking writes...")
	f, err := db.prepareToDrop()
	if err != nil {
		return f, err
	}
	// prepareToDrop will stop all the incomming write and flushes any pending flush tasks.
	// Before we drop, we'll stop the compaction because anyways all the datas are going to
	// be deleted.
	db.stopCompactions()
	resume := func() {
		db.startCompactions()
		f()
	}
	// Block all foreign interactions with memory tables.
	db.lock.Lock()
	defer db.lock.Unlock()

	// Remove inmemory tables. Calling DecrRef for safety. Not sure if they're absolutely needed.
	for _, mt := range db.imm {
		mt.DecrRef()
	}
	db.imm = db.imm[:0]

	num, err := db.lc.dropTree()
	if err != nil {
		return resume, err
	}
	db.opt.Infof("Deleted %d SSTables. Now deleting value logs...\n", num)

	db.lc.nextFileID = 1
	db.blockCache.Clear()
	db.indexCache.Clear()
	return resume, nil
}

// DropPrefixNonBlocking would logically drop all the keys with the provided prefix. The data would
// not be cleared from LSM tree immediately. It would be deleted eventually through compactions.
// This operation is useful when we don't want to block writes while we delete the prefixes.
// It does this in the following way:
// - Stream the given prefixes at a given ts.
// - Write them to skiplist at the specified ts and handover that skiplist to DB.
func (db *DB) DropPrefixNonBlocking(prefixes ...[]byte) error {
	if db.opt.ReadOnly {
		return errors.New("Attempting to drop data in read-only mode.")
	}

	if len(prefixes) == 0 {
		return nil
	}
	db.opt.Infof("Non-blocking DropPrefix called for %s", prefixes)

	cbuf := z.NewBuffer(int(db.opt.MemTableSize), "DropPrefixNonBlocking")
	defer func() {
		_ = cbuf.Release()
	}()

	var wg sync.WaitGroup
	handover := func(force bool) error {
		if !force && int64(cbuf.LenNoPadding()) < db.opt.MemTableSize {
			return nil
		}

		// Sort the kvs, add them to the builder, and hand it over to DB.
		cbuf.SortSlice(func(left, right []byte) bool {
			return y.CompareKeys(left, right) < 0
		})

		b := skl.NewBuilder(db.opt.MemTableSize)
		err := cbuf.SliceIterate(func(s []byte) error {
			b.Add(s, y.ValueStruct{Meta: bitDelete})
			return nil
		})
		if err != nil {
			return err
		}
		cbuf.Reset()
		wg.Add(1)
		return db.HandoverSkiplist(b.Skiplist(), wg.Done)
	}

	dropPrefix := func(prefix []byte) error {
		stream := db.NewStreamAt(math.MaxUint64)
		stream.LogPrefix = fmt.Sprintf("Dropping prefix: %#x", prefix)
		stream.Prefix = prefix
		// We don't need anything except key and version.
		stream.KeyToList = func(key []byte, itr *Iterator) (*pb.KVList, error) {
			if !itr.Valid() {
				return nil, nil
			}
			item := itr.Item()
			if item.IsDeletedOrExpired() {
				return nil, nil
			}
			if !bytes.Equal(key, item.Key()) {
				// Return on the encounter with another key.
				return nil, nil
			}

			a := itr.Alloc
			ka := a.Copy(key)
			list := &pb.KVList{}
			// We need to generate only a single delete marker per key. All the versions for this
			// key will be considered deleted, if we delete the one at highest version.
			kv := y.NewKV(a)
			kv.Key = y.KeyWithTs(ka, item.Version())
			list.Kv = append(list.Kv, kv)
			itr.Next()
			return list, nil
		}

		stream.Send = func(buf *z.Buffer) error {
			kv := pb.KV{}
			err := buf.SliceIterate(func(s []byte) error {
				kv.Reset()
				if err := kv.Unmarshal(s); err != nil {
					return err
				}
				cbuf.WriteSlice(kv.Key)
				return nil
			})
			if err != nil {
				return err
			}
			return handover(false)
		}
		if err := stream.Orchestrate(context.Background()); err != nil {
			return err
		}
		// Flush the remaining skiplists if any.
		return handover(true)
	}

	// Iterate over all the prefixes and logically drop them.
	for _, prefix := range prefixes {
		if err := dropPrefix(prefix); err != nil {
			return errors.Wrapf(err, "While dropping prefix: %#x", prefix)
		}
	}

	wg.Wait()
	return nil
}

// DropPrefix would drop all the keys with the provided prefix. Based on DB options, it either drops
// the prefixes by blocking the writes or doing a logical drop.
// See DropPrefixBlocking and DropPrefixNonBlocking for more information.
func (db *DB) DropPrefix(prefixes ...[]byte) error {
	if db.opt.AllowStopTheWorld {
		return db.DropPrefixBlocking(prefixes...)
	}
	return db.DropPrefixNonBlocking(prefixes...)
}

// DropPrefix would drop all the keys with the provided prefix. It does this in the following way:
// - Stop accepting new writes.
// - Stop memtable flushes before acquiring lock. Because we're acquring lock here
//   and memtable flush stalls for lock, which leads to deadlock
// - Flush out all memtables, skipping over keys with the given prefix, Kp.
// - Write out the value log header to memtables when flushing, so we don't accidentally bring Kp
//   back after a restart.
// - Stop compaction.
// - Compact L0->L1, skipping over Kp.
// - Compact rest of the levels, Li->Li, picking tables which have Kp.
// - Resume memtable flushes, compactions and writes.
func (db *DB) DropPrefixBlocking(prefixes ...[]byte) error {
	if len(prefixes) == 0 {
		return nil
	}
	db.opt.Infof("DropPrefix called for %s", prefixes)
	f, err := db.prepareToDrop()
	if err != nil {
		return err
	}
	defer f()

	var filtered [][]byte
	if filtered, err = db.filterPrefixesToDrop(prefixes); err != nil {
		return err
	}
	// If there is no prefix for which the data already exist, do not do anything.
	if len(filtered) == 0 {
		db.opt.Infof("No prefixes to drop")
		return nil
	}
	// Block all foreign interactions with memory tables.
	db.lock.Lock()
	defer db.lock.Unlock()

	for _, sl := range db.imm {
		if sl.Empty() {
			sl.DecrRef()
			continue
		}
		task := flushTask{
			sl: sl,
			// Ensure that the head of value log gets persisted to disk.
			dropPrefixes: filtered,
		}
		db.opt.Debugf("Flushing memtable")
		if err := db.handleFlushTask(task); err != nil {
			db.opt.Errorf("While trying to flush memtable: %v", err)
			return err
		}
		sl.DecrRef()
	}
	db.stopCompactions()
	defer db.startCompactions()
	db.imm = db.imm[:0]
	if err != nil {
		return y.Wrapf(err, "cannot create new mem table")
	}

	// Drop prefixes from the levels.
	if err := db.lc.dropPrefixes(filtered); err != nil {
		return err
	}
	db.opt.Infof("DropPrefix done")
	return nil
}

func (db *DB) filterPrefixesToDrop(prefixes [][]byte) ([][]byte, error) {
	var filtered [][]byte
	for _, prefix := range prefixes {
		err := db.View(func(txn *Txn) error {
			iopts := DefaultIteratorOptions
			iopts.Prefix = prefix
			iopts.PrefetchValues = false
			itr := txn.NewIterator(iopts)
			defer itr.Close()
			itr.Rewind()
			if itr.ValidForPrefix(prefix) {
				filtered = append(filtered, prefix)
			}
			return nil
		})
		if err != nil {
			return filtered, err
		}
	}
	return filtered, nil
}

// Checks if the key is banned. Returns the respective error if the key belongs to any of the banned
// namepspaces. Else it returns nil.
func (db *DB) isBanned(key []byte) error {
	if db.opt.NamespaceOffset < 0 {
		return nil
	}
	if len(key) <= db.opt.NamespaceOffset+8 {
		return nil
	}
	if db.bannedNamespaces.has(y.BytesToU64(key[db.opt.NamespaceOffset:])) {
		return ErrBannedKey
	}
	return nil
}

// BanNamespace bans a namespace. Read/write to keys belonging to any of such namespace is denied.
func (db *DB) BanNamespace(ns uint64) error {
	if db.opt.NamespaceOffset < 0 {
		return ErrNamespaceMode
	}
	db.opt.Infof("Banning namespace: %d", ns)
	// First set the banned namespaces in DB and then update the in-memory structure.
	key := y.KeyWithTs(append(bannedNsKey, y.U64ToBytes(ns)...), 1)
	entry := []*Entry{{
		Key:   key,
		Value: nil,
	}}
	if err := db.BatchSet(entry); err != nil {
		return err
	}
	db.bannedNamespaces.add(ns)
	return nil
}

// BannedNamespaces returns the list of prefixes banned for DB.
func (db *DB) BannedNamespaces() []uint64 {
	return db.bannedNamespaces.all()
}

// KVList contains a list of key-value pairs.
type KVList = pb.KVList

// Subscribe can be used to watch key changes for the given key prefixes and the ignore string.
// At least one prefix should be passed, or an error will be returned.
// You can use an empty prefix to monitor all changes to the DB.
// Ignore string is the byte ranges for which prefix matching will be ignored.
// For example: ignore = "2-3", and prefix = "abc" will match for keys "abxxc", "abdfc" etc.
// This function blocks until the given context is done or an error occurs.
// The given function will be called with a new KVList containing the modified keys and the
// corresponding values.
func (db *DB) Subscribe(ctx context.Context, cb func(kv *KVList) error, matches []pb.Match) error {
	if cb == nil {
		return ErrNilCallback
	}

	c := z.NewCloser(1)
	s, err := db.pub.newSubscriber(c, matches)
	if err != nil {
		return y.Wrapf(err, "error adding subscriber")
	}
	slurp := func(batch *pb.KVList) error {
		for {
			select {
			case kvs := <-s.sendCh:
				batch.Kv = append(batch.Kv, kvs.Kv...)
			default:
				if len(batch.GetKv()) > 0 {
					return cb(batch)
				}
				return nil
			}
		}
	}

	drain := func() {
		for {
			select {
			case <-s.sendCh:
			default:
				return
			}
		}
	}
	for {
		select {
		case <-c.HasBeenClosed():
			// No need to delete here. Closer will be called only while
			// closing DB. Subscriber will be deleted by cleanSubscribers.
			err := slurp(new(pb.KVList))
			// Drain if any pending updates.
			c.Done()
			return err
		case <-ctx.Done():
			c.Done()
			atomic.StoreUint64(s.active, 0)
			drain()
			db.pub.deleteSubscriber(s.id)
			// Delete the subscriber to avoid further updates.
			return ctx.Err()
		case batch := <-s.sendCh:
			err := slurp(batch)
			if err != nil {
				c.Done()
				atomic.StoreUint64(s.active, 0)
				drain()
				// Delete the subscriber if there is an error by the callback.
				db.pub.deleteSubscriber(s.id)
				return err
			}
		}
	}
}

func (db *DB) syncDir(dir string) error {
	if db.opt.InMemory {
		return nil
	}
	return syncDir(dir)
}

func createDirs(opt Options) error {
	for _, path := range []string{opt.Dir} {
		dirExists, err := exists(path)
		if err != nil {
			return y.Wrapf(err, "Invalid Dir: %q", path)
		}
		if !dirExists {
			if opt.ReadOnly {
				return errors.Errorf("Cannot find directory %q for read-only open", path)
			}
			// Try to create the directory
			err = os.MkdirAll(path, 0700)
			if err != nil {
				return y.Wrapf(err, "Error Creating Dir: %q", path)
			}
		}
	}
	return nil
}

// Stream the contents of this DB to a new DB with options outOptions that will be
// created in outDir.
func (db *DB) StreamDB(outOptions Options) error {
	outDir := outOptions.Dir

	// Open output DB.
	outDB, err := Open(outOptions)
	if err != nil {
		return y.Wrapf(err, "cannot open out DB at %s", outDir)
	}
	defer outDB.Close()
	writer := outDB.NewStreamWriter()
	if err := writer.Prepare(); err != nil {
		return y.Wrapf(err, "cannot create stream writer in out DB at %s", outDir)
	}

	// Stream contents of DB to the output DB.
	stream := db.NewStreamAt(math.MaxUint64)
	stream.LogPrefix = fmt.Sprintf("Streaming DB to new DB at %s", outDir)
	stream.FullCopy = true

	stream.Send = func(buf *z.Buffer) error {
		return writer.Write(buf)
	}
	if err := stream.Orchestrate(context.Background()); err != nil {
		return y.Wrapf(err, "cannot stream DB to out DB at %s", outDir)
	}
	if err := writer.Flush(); err != nil {
		return y.Wrapf(err, "cannot flush writer")
	}
	return nil
}

// Opts returns a copy of the DB options.
func (db *DB) Opts() Options {
	return db.opt
}

type CacheType int

const (
	BlockCache CacheType = iota
	IndexCache
)

// CacheMaxCost updates the max cost of the given cache (either block or index cache).
// The call will have an effect only if the DB was created with the cache. Otherwise it is
// a no-op. If you pass a negative value, the function will return the current value
// without updating it.
func (db *DB) CacheMaxCost(cache CacheType, maxCost int64) (int64, error) {
	if db == nil {
		return 0, nil
	}

	if maxCost < 0 {
		switch cache {
		case BlockCache:
			return db.blockCache.MaxCost(), nil
		case IndexCache:
			return db.indexCache.MaxCost(), nil
		default:
			return 0, errors.Errorf("invalid cache type")
		}
	}

	switch cache {
	case BlockCache:
		db.blockCache.UpdateMaxCost(maxCost)
		return maxCost, nil
	case IndexCache:
		db.indexCache.UpdateMaxCost(maxCost)
		return maxCost, nil
	default:
		return 0, errors.Errorf("invalid cache type")
	}
}

func (db *DB) LevelsToString() string {
	levels := db.Levels()
	h := func(sz int64) string {
		return humanize.IBytes(uint64(sz))
	}
	base := func(b bool) string {
		if b {
			return "B"
		}
		return " "
	}

	var b strings.Builder
	b.WriteRune('\n')
	for _, li := range levels {
		b.WriteString(fmt.Sprintf(
			"Level %d [%s]: NumTables: %02d. Size: %s of %s. Score: %.2f->%.2f"+
				" StaleData: %s Target FileSize: %s\n",
			li.Level, base(li.IsBaseLevel), li.NumTables,
			h(li.Size), h(li.TargetSize), li.Score, li.Adjusted, h(li.StaleDatSize),
			h(li.TargetFileSize)))
	}
	b.WriteString("Level Done\n")
	return b.String()
}

// SetDiscardTs sets a timestamp at or below which, any invalid or deleted
// versions can be discarded from the LSM tree, and thence from the value log to
// reclaim disk space. Can only be used with managed transactions.
func (db *DB) SetDiscardTs(ts uint64) {
	atomic.StoreUint64(&db.discardTs, ts)
}

func (db *DB) discardAtOrBelow() uint64 {
	return atomic.LoadUint64(&db.discardTs)
}
