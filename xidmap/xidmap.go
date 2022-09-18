// Portions Copyright 2017-2018 Dgraph Labs, Inc. are available under the Apache License v2.0.
// Portions Copyright 2022 Outcaste LLC are available under the Sustainable License v1.0.

package xidmap

import (
	"encoding/binary"
	"regexp"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc"

	"github.com/dgryski/go-farm"
	"github.com/golang/glog"
	"github.com/outcaste-io/dgo/v210"
	"github.com/outcaste-io/outserv/badger"
	"github.com/outcaste-io/outserv/protos/pb"
	"github.com/outcaste-io/outserv/x"
	"github.com/outcaste-io/ristretto/z"
)

var maxLeaseRegex = regexp.MustCompile(`currMax:([0-9]+)`)

// XidMapOptions specifies the options for creating a new xidmap.
type XidMapOptions struct {
	UidAssigner *grpc.ClientConn
	DgClient    *dgo.Dgraph
	DB          *badger.DB
	Dir         string
}

// XidMap allocates and tracks mappings between Xids and Uids in a threadsafe
// manner. It's memory friendly because the mapping is stored on disk, but fast
// because it uses an LRU cache.
type XidMap struct {
	shards    []*shard
	newRanges chan *pb.AssignedIds
	nextUid   uint64

	// Optionally, these can be set to persist the mappings.
	writer *badger.WriteBatch
	wg     sync.WaitGroup

	kvBuf  []kv
	kvChan chan []kv
}

type shard struct {
	sync.RWMutex

	tree *z.Tree
}

type kv struct {
	key, value []byte
}

// New creates an XidMap. zero conn must be valid for UID allocations to happen. Optionally, a
// badger.DB can be provided to persist the xid to uid allocations. This would add latency to the
// assignment operations. XidMap creates the temporary buffers inside dir directory. The caller must
// ensure that the dir exists.
func New(opts XidMapOptions) *XidMap {
	numShards := 32
	xm := &XidMap{
		newRanges: make(chan *pb.AssignedIds, numShards),
		shards:    make([]*shard, numShards),
		kvChan:    make(chan []kv, 64),
	}
	for i := range xm.shards {
		xm.shards[i] = &shard{
			tree: z.NewTree("XidMap"),
		}
	}

	if opts.DB != nil {
		// If DB is provided, let's load up all the xid -> uid mappings in memory.
		xm.writer = opts.DB.NewWriteBatch()

		for i := 0; i < 16; i++ {
			xm.wg.Add(1)
			go xm.dbWriter()
		}

		err := opts.DB.View(func(txn *badger.Txn) error {
			var count int
			opt := badger.DefaultIteratorOptions
			opt.PrefetchValues = false
			itr := txn.NewIterator(opt)
			defer itr.Close()
			for itr.Rewind(); itr.Valid(); itr.Next() {
				item := itr.Item()
				key := string(item.Key())
				sh := xm.shardFor(key)
				err := item.Value(func(val []byte) error {
					uid := binary.BigEndian.Uint64(val)
					// No need to acquire a lock. This is all serial access.
					sh.tree.Set(farm.Fingerprint64([]byte(key)), uid)
					return nil
				})
				if err != nil {
					return err
				}
				count++
			}
			glog.Infof("Loaded up %d xid to uid mappings", count)
			return nil
		})
		x.Check(err)
	}
	return xm
}

func (m *XidMap) shardFor(xid string) *shard {
	fp := z.MemHashString(xid)
	idx := fp % uint64(len(m.shards))
	return m.shards[idx]
}

func (m *XidMap) CheckUid(xid string) bool {
	sh := m.shardFor(xid)
	sh.RLock()
	defer sh.RUnlock()
	uid := sh.tree.Get(farm.Fingerprint64([]byte(xid)))
	return uid != 0
}

func (m *XidMap) SetUid(xid string, uid uint64) {
	sh := m.shardFor(xid)
	sh.Lock()
	defer sh.Unlock()
	sh.tree.Set(farm.Fingerprint64([]byte(xid)), uid)
}

func (m *XidMap) dbWriter() {
	defer m.wg.Done()
	for buf := range m.kvChan {
		for _, kv := range buf {
			x.Panic(m.writer.SetAt(kv.key, kv.value, 1))
		}
	}
}

// AssignUid creates new or looks up existing XID to UID mappings. It also returns if
// UID was created.
func (m *XidMap) AssignUid(xid string) (uint64, bool) {
	sh := m.shardFor(xid)
	sh.RLock()

	fp := farm.Fingerprint64([]byte(xid))
	uid := sh.tree.Get(fp)
	sh.RUnlock()
	if uid > 0 {
		return uid, false
	}

	sh.Lock()
	defer sh.Unlock()

	uid = sh.tree.Get(fp)
	if uid > 0 {
		return uid, false
	}

	newUid := atomic.AddUint64(&m.nextUid, 1)
	sh.tree.Set(fp, newUid)
	// if fp%1000 == 0 {
	// 	fmt.Printf("Assigned UID: %x for XID: %s\n", newUid, xid)
	// }

	if m.writer != nil {
		var uidBuf [8]byte
		binary.BigEndian.PutUint64(uidBuf[:], newUid)
		m.kvBuf = append(m.kvBuf, kv{key: []byte(xid), value: uidBuf[:]})

		if len(m.kvBuf) == 64 {
			m.kvChan <- m.kvBuf
			m.kvBuf = make([]kv, 0, 64)
		}
	}

	return newUid, true
}

func (m *XidMap) BumpPast(max uint64) {
	for {
		prev := atomic.LoadUint64(&m.nextUid)
		if prev >= max {
			return
		}
		if atomic.CompareAndSwapUint64(&m.nextUid, prev, max+1) {
			return
		}
	}
}

// AllocateUid gives a single uid without creating an xid to uid mapping.
func (m *XidMap) AllocateUid() uint64 {
	return atomic.AddUint64(&m.nextUid, 1)
}

// Flush must be called if DB is provided to XidMap.
func (m *XidMap) Flush() error {
	// While running bulk loader, this method is called at the completion of map phase. After this
	// method returns xidmap of bulk loader is made nil. But xidmap still show up in memory profiles
	// even during reduce phase. If bulk loader is running on large dataset, this occupies lot of
	// memory and causing OOM sometimes. Making shards explicitly nil in this method fixes this.
	// TODO: find why xidmap is not getting GCed without below line.
	for _, shards := range m.shards {
		shards.tree.Close()
	}
	m.shards = nil
	if m.writer == nil {
		return nil
	}
	glog.Infof("Writing xid map to DB")
	defer func() {
		glog.Infof("Finished writing xid map to DB")
	}()

	if len(m.kvBuf) > 0 {
		m.kvChan <- m.kvBuf
	}
	close(m.kvChan)
	m.wg.Wait()

	return m.writer.Flush()
}
