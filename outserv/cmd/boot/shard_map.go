// Portions Copyright 2017-2018 Dgraph Labs, Inc. are available under the Apache License v2.0.
// Portions Copyright 2022 Outcaste LLC are available under the Sustainable License v1.0.

package boot

import (
	"sync"

	"github.com/outcaste-io/outserv/x"
)

type shardMap struct {
	sync.RWMutex
	numShards   int
	predToShard map[string]int
	nextShard   int
}

func newShardMap(numShards int) *shardMap {
	return &shardMap{
		numShards:   numShards,
		predToShard: make(map[string]int),
	}
}

func (m *shardMap) shardFor(pred string) int {
	// Always assign NQuads with reserved predicates to the first map shard.
	if x.IsReservedPredicate(pred) {
		return 0
	}

	m.RLock()
	shard, ok := m.predToShard[pred]
	m.RUnlock()
	if ok {
		return shard
	}

	m.Lock()
	defer m.Unlock()
	shard, ok = m.predToShard[pred]
	if ok {
		return shard
	}

	shard = m.nextShard
	m.predToShard[pred] = shard
	m.nextShard = (m.nextShard + 1) % m.numShards
	return shard
}
