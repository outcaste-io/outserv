// Portions Copyright 2019 Dgraph Labs, Inc. are available under the Apache License v2.0.
// Portions Copyright 2022 Outcaste LLC are available under the Sustainable License v1.0.

package posting

import (
	"math"
	"reflect"
	"unsafe"

	"github.com/outcaste-io/outserv/protos/pb"
)

const sizeOfBucket = 144

// DeepSize computes the memory taken by a Posting List
func (l *List) DeepSize() uint64 {
	if l == nil {
		return 0
	}

	l.RLock()
	defer l.RUnlock()

	var size uint64 = 4*8 + // safe mutex consists of 4 words.
		1*8 + // plist pointer consists of 1 word.
		1*8 + // mutation map pointer  consists of 1 word.
		2*8 + // minTs and maxTs take 1 word each.
		3*8 + // array take 3 words. so key array is 3 words.
		1*8 // So far 11 words, in order to round the slab we're adding one more word.
	// so far basic struct layout has been calculated.

	// Add each entry size of key array.
	size += uint64(cap(l.key))

	// add the posting list size.
	size += calculatePostingListSize(l.plist)
	if l.mutationMap != nil {
		// add the List.mutationMap size.
		// map has maptype and hmap
		// maptype is defined at compile time and is hardcoded in the compiled code.
		// Hence, it doesn't consume any extra memory.
		// Ref: https://bit.ly/2NQU8Jq
		// Now, let's look at hmap struct.
		// size of hmap struct
		// Ref: https://golang.org/src/runtime/map.go?#L114
		size += 6 * 8
		// we'll calculate the number of buckets based on pointer arithmetic in hmap struct.
		// reflect value give us access to the hmap struct.
		hmap := reflect.ValueOf(l.mutationMap)
		numBuckets := int(math.Pow(2, float64((*(*uint8)(
			unsafe.Pointer(hmap.Pointer() + uintptr(9))))))) // skipcq: GSC-G103
		// skipcq: GSC-G103
		numOldBuckets := (*(*uint16)(unsafe.Pointer(hmap.Pointer() + uintptr(10))))
		size += uint64(numOldBuckets * sizeOfBucket)
		if len(l.mutationMap) > 0 || numBuckets > 1 {
			size += uint64(numBuckets * sizeOfBucket)
		}
	}
	// adding the size of all the entries in the map.
	for _, v := range l.mutationMap {
		size += calculatePostingListSize(v)
	}

	return size
}

// calculatePostingListSize is used to calculate the size of posting list
func calculatePostingListSize(list *pb.PostingList) uint64 {
	if list == nil {
		return 0
	}

	var size uint64 = 1*8 + // Pack consists of 1 word.
		3*8 + // Postings array consists of 3 words.
		1*8 + // CommitTs consists of 1 word.
		3*8 // Splits array consists of 3 words.

	// add bitmap size.
	size += uint64(cap(list.Bitmap))

	// Each entry take one word.
	// Adding each entry reference allocation.
	size += uint64(cap(list.Postings)) * 8
	for _, p := range list.Postings {
		// add the size of each posting.
		size += calculatePostingSize(p)
	}

	// Each entry take one word.
	// Adding each entry size.
	size += uint64(cap(list.Splits)) * 8

	return size
}

// calculatePostingSize is used to calculate the size of a posting
func calculatePostingSize(posting *pb.Posting) uint64 {
	if posting == nil {
		return 0
	}

	var size uint64 = 1*8 + // Uid consists of 1 word.
		3*8 + // Value byte array take 3 words.
		1*8 + // ValType consists 1 word.
		1*8 + // PostingType consists of 1 word.
		3*8 + // LangTag array consists of 3 words.
		1*8 + // Label consists of 1 word.
		3*8 + // Facets array consists of 3 word.
		1*8 + // Op consists of 1 word.
		1*8 + // StartTs consists of 1 word.
		1*8 // CommitTs consists of 1 word..
	size += uint64(cap(posting.Value))

	return size
}
