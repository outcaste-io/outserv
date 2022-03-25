package admin

import (
	badgerpb "github.com/outcaste-io/badger/v3/pb"
	"github.com/outcaste-io/outserv/protos/pb"
	"github.com/outcaste-io/outserv/worker"
	"github.com/outcaste-io/outserv/x"
	"github.com/outcaste-io/ristretto/z"
	"github.com/pkg/errors"
)

func SubscribeForUpdates(prefixes [][]byte, ignore string, cb func(uid uint64, prefix []byte, val []byte,
	err error), group uint32, closer *z.Closer) {
	worker.SubscribeForUpdates(prefixes, x.IgnoreBytes, func(kvs *badgerpb.KVList) {
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

		// Ideally we also send back the prefix, so we know which prefix got an update
		cb(pk.Uid, nil, pl.Postings[0].Value, nil)
	}, group, closer)
}
