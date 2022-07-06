/*
 * Copyright 2019 Dgraph Labs, Inc. and Contributors
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
	"sync"
	"sync/atomic"

	"github.com/outcaste-io/badger/v3/skl"
)

// Entry provides Key, Value, UserMeta and ExpiresAt. This struct can be used by
// the user to set data.
type Entry struct {
	Key      []byte
	Value    []byte
	version  uint64
	offset   uint32 // offset is an internal field.
	UserMeta byte
	meta     byte
}

func (e *Entry) estimateSize() int64 {
	k := int64(len(e.Key))
	v := int64(len(e.Value))
	return k + v + 2 // Meta, UserMeta
}

func (e Entry) String() {
	fmt.Printf("Key: %s Meta: %d UserMeta: %d Offset: %d len(val)=%d",
		e.Key, e.meta, e.UserMeta, e.offset, len(e.Value))
}

// NewEntry creates a new entry with key and value passed in args. This newly created entry can be
// set in a transaction by calling txn.SetEntry(). All other properties of Entry can be set by
// calling WithMeta, WithDiscard, WithTTL methods on it.
// This function uses key and value reference, hence users must
// not modify key and value until the end of transaction.
func NewEntry(key, value []byte) *Entry {
	return &Entry{
		Key:   key,
		Value: value,
	}
}

// WithMeta adds meta data to Entry e. This byte is stored alongside the key
// and can be used as an aid to interpret the value or store other contextual
// bits corresponding to the key-value pair of entry.
func (e *Entry) WithMeta(meta byte) *Entry {
	e.UserMeta = meta
	return e
}

// WithDiscard adds a marker to Entry e. This means all the previous versions of the key (of the
// Entry) will be eligible for garbage collection.
// This method is only useful if you have set a higher limit for options.NumVersionsToKeep. The
// default setting is 1, in which case, this function doesn't add any more benefit. If however, you
// have a higher setting for NumVersionsToKeep (in Dgraph, we set it to infinity), you can use this
// method to indicate that all the older versions can be discarded and removed during compactions.
func (e *Entry) WithDiscard() *Entry {
	e.meta = BitDiscardEarlierVersions
	return e
}

type request struct {
	// Input values
	Skl     *skl.Skiplist
	Entries []*Entry
	ref     int32
}

type handoverRequest struct {
	skl      *skl.Skiplist
	callback func()
	err      error
	wg       sync.WaitGroup
}

func (req *request) IncrRef() {
	atomic.AddInt32(&req.ref, 1)
}

func (req *request) DecrRef() {
	nRef := atomic.AddInt32(&req.ref, -1)
	if nRef > 0 {
		return
	}
	req.Entries = nil
}

type requests []*request

func (reqs requests) DecrRef() {
	for _, req := range reqs {
		req.DecrRef()
	}
}

func (reqs requests) IncrRef() {
	for _, req := range reqs {
		req.IncrRef()
	}
}
