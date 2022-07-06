//go:build !debug

// Portions Copyright 2020 Dgraph Labs, Inc. are available under the Apache License v2.0.
// Portions Copyright 2022 Outcaste LLC are available under the Sustainable License v1.0.

package x

import (
	bpb "github.com/outcaste-io/outserv/badger/pb"

	"github.com/outcaste-io/outserv/badger"
	"github.com/outcaste-io/outserv/protos/pb"
)

var Debug bool = false

// VerifyPack works in debug mode. Check out the comment in debug_on.go
func VerifyPack(plist *pb.PostingList) {}

// VerifySnapshot works in debug mode. Check out the comment in debug_on.go
func VerifySnapshot(pstore *badger.DB, readTs uint64) {}

// VerifyPostingSplits works in debug mode. Check out the comment in debug_on.go
func VerifyPostingSplits(kvs []*bpb.KV, plist *pb.PostingList,
	parts map[uint64]*pb.PostingList, baseKey []byte) {
}
