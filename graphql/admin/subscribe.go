package admin

import (
	badgerpb "github.com/outcaste-io/badger/v3/pb"
	"github.com/outcaste-io/outserv/protos/pb"
	"github.com/outcaste-io/outserv/worker"
	"github.com/outcaste-io/outserv/x"
	"github.com/outcaste-io/ristretto/z"
	"github.com/pkg/errors"
)

// TODO(schartey/wasm): rewrite to goroutines instead of callbacks

// Async subscription for individual updates
func SubscribeUpdates(prefixes [][]byte, ignore string, cb func(ns uint64, uid uint64, prefix []byte, val []byte,
	err error), group uint32, closer *z.Closer) {
	for _, prefix := range prefixes {
		go SubscribeForUpdate(prefix, ignore, func(ns uint64, uid uint64, pre []byte, val []byte, err error) {
			cb(ns, uid, pre, val, err)
		}, group, closer)
	}
}

// Async subscription for single update
func SubscribeForUpdate(prefix []byte, ignore string, cb func(ns uint64, uid uint64, prefix []byte, val []byte,
	err error), group uint32, closer *z.Closer) {
	go SubscribeForAnyUpdates([][]byte{prefix}, ignore, func(ns uint64, uid uint64, val []byte, err error) {
		cb(ns, uid, prefix, val, err)
	}, group, closer)
}

// Async subscription for any update of prefix
// Would it be more Go idiomatic to use channels instead of callbacks?
func SubscribeForAnyUpdates(prefixes [][]byte, ignore string, cb func(ns uint64, uid uint64, val []byte,
	err error), group uint32, closer *z.Closer) {
	go worker.SubscribeForUpdates(prefixes, x.IgnoreBytes, func(kvs *badgerpb.KVList) {
		kv := x.KvWithMaxVersion(kvs, prefixes)

		// Unmarshal the incoming posting list.
		pl := &pb.PostingList{}
		err := pl.Unmarshal(kv.GetValue())
		if err != nil {
			err = errors.Wrapf(err, "Unable to unmarshal the posting list for lambda update")
			return
		}

		// There should be only one posting.
		if len(pl.Postings) != 1 {
			err = errors.Wrapf(err, "Only one posting is expected in the lambda posting list but got %d",
				len(pl.Postings))
			return
		}

		pk, err := x.Parse(kv.GetKey())
		if err != nil {
			err = errors.Wrapf(err, "Unable to find uid of updated lambda")
			return
		}
		ns, _ := x.ParseNamespaceAttr(pk.Attr)

		// Ideally we also send back the prefix, so we know which prefix got an update
		cb(ns, pk.Uid, pl.Postings[0].Value, nil)
	}, group, closer)
}
