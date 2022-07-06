// Portions Copyright 2020 Dgraph Labs, Inc. are available under the Apache License v2.0.
// Portions Copyright 2022 Outcaste LLC are available under the Apache License v2.0.
package badger

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"

	"github.com/stretchr/testify/require"

	"github.com/outcaste-io/outserv/badger/pb"
)

// This test will result in deadlock for commits before this.
// Exiting this test gracefully will be the proof that the
// publisher is no longer stuck in deadlock.
func TestPublisherDeadlock(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		var subWg sync.WaitGroup
		subWg.Add(1)

		var firstUpdate sync.WaitGroup
		firstUpdate.Add(1)

		var subDone sync.WaitGroup
		subDone.Add(1)
		go func() {
			subWg.Done()
			match := pb.Match{Prefix: []byte("ke"), IgnoreBytes: ""}
			err := db.Subscribe(context.Background(), func(kvs *pb.KVList) error {
				firstUpdate.Done()
				time.Sleep(time.Second * 20)
				return errors.New("error returned")
			}, []pb.Match{match})
			require.Error(t, err, errors.New("error returned"))
			subDone.Done()
		}()
		subWg.Wait()
		go func() {
			wb := db.NewWriteBatch()
			e := NewEntry([]byte(fmt.Sprintf("key%d", 0)), []byte(fmt.Sprintf("value%d", 0)))
			require.NoError(t, wb.SetEntryAt(e, 1))
			require.NoError(t, wb.Flush())
		}()

		firstUpdate.Wait()
		req := int64(0)
		for i := 1; i < 111; i++ {
			time.Sleep(time.Millisecond * 10)
			go func(i int) {
				wb := db.NewWriteBatch()
				e := NewEntry([]byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("value%d", i)))
				require.NoError(t, wb.SetEntryAt(e, 1))
				require.NoError(t, wb.Flush())
				atomic.AddInt64(&req, 1)
			}(i)
		}
		for {
			if atomic.LoadInt64(&req) == 110 {
				break
			}
			time.Sleep(time.Second)
		}
		subDone.Wait()
	})
}

func TestPublisherOrdering(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		order := []string{}
		var wg sync.WaitGroup
		wg.Add(1)
		var subWg sync.WaitGroup
		subWg.Add(1)
		go func() {
			subWg.Done()
			updates := 0
			match := pb.Match{Prefix: []byte("ke"), IgnoreBytes: ""}
			err := db.Subscribe(context.Background(), func(kvs *pb.KVList) error {
				updates += len(kvs.GetKv())
				for _, kv := range kvs.GetKv() {
					order = append(order, string(kv.Value))
				}
				if updates == 5 {
					wg.Done()
				}
				return nil
			}, []pb.Match{match})
			if err != nil {
				require.Equal(t, err.Error(), context.Canceled.Error())
			}
		}()
		subWg.Wait()
		for i := 0; i < 5; i++ {
			wb := db.NewWriteBatch()
			e := NewEntry([]byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("value%d", i)))
			require.NoError(t, wb.SetEntryAt(e, 1))
			require.NoError(t, wb.Flush())
		}
		wg.Wait()
		for i := 0; i < 5; i++ {
			require.Equal(t, fmt.Sprintf("value%d", i), order[i])
		}
	})
}

func TestMultiplePrefix(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		var wg sync.WaitGroup
		wg.Add(1)
		var subWg sync.WaitGroup
		subWg.Add(1)
		go func() {
			subWg.Done()
			updates := 0
			match1 := pb.Match{Prefix: []byte("ke"), IgnoreBytes: ""}
			match2 := pb.Match{Prefix: []byte("hel"), IgnoreBytes: ""}
			err := db.Subscribe(context.Background(), func(kvs *pb.KVList) error {
				updates += len(kvs.GetKv())
				for _, kv := range kvs.GetKv() {
					if string(kv.Key) == "key" {
						require.Equal(t, string(kv.Value), "value")
					} else {
						require.Equal(t, string(kv.Value), "badger")
					}
				}
				if updates == 2 {
					wg.Done()
				}
				return nil
			}, []pb.Match{match1, match2})
			if err != nil {
				require.Equal(t, err.Error(), context.Canceled.Error())
			}
		}()
		subWg.Wait()

		e := NewEntry([]byte("key"), []byte("value"))
		e.version = 1
		require.NoError(t, db.BatchSet([]*Entry{e}))

		e = NewEntry([]byte("hello"), []byte("badger"))
		e.version = 1
		require.NoError(t, db.BatchSet([]*Entry{e}))

		wg.Wait()
	})
}
