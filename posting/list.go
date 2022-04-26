// Portions Copyright 2015-2018 Dgraph Labs, Inc. are available under the Apache License v2.0.
// Portions Copyright 2022 Outcaste LLC are available under the Smart License v1.0.

package posting

import (
	"bytes"
	"context"
	"encoding/hex"
	"log"
	"math"
	"sort"

	"github.com/dgryski/go-farm"
	"github.com/pkg/errors"

	"github.com/golang/protobuf/proto"
	bpb "github.com/outcaste-io/badger/v3/pb"
	"github.com/outcaste-io/badger/v3/y"
	"github.com/outcaste-io/outserv/codec"
	"github.com/outcaste-io/outserv/protos/pb"
	"github.com/outcaste-io/outserv/schema"
	"github.com/outcaste-io/outserv/types"
	"github.com/outcaste-io/outserv/x"
	"github.com/outcaste-io/ristretto/z"
	"github.com/outcaste-io/sroar"
)

var (
	// ErrRetry can be triggered if the posting list got deleted from memory due to a hard commit.
	// In such a case, retry.
	ErrRetry = errors.New("Temporary error. Please retry")
	// ErrNoValue would be returned if no value was found in the posting list.
	ErrNoValue = errors.New("No value found")
	// ErrStopIteration is returned when an iteration is terminated early.
	ErrStopIteration = errors.New("Stop iteration")
	emptyPosting     = &pb.Posting{}
	maxListSize      = mb / 2
)

const (
	// Set means overwrite in mutation layer. It contributes 0 in Length.
	Set uint32 = 0x01
	// Del means delete in mutation layer. It contributes -1 in Length.
	Del uint32 = 0x02

	// BitSchemaPosting signals that the value stores a schema or type.
	BitSchemaPosting byte = 0x01
	// BitDeltaPosting signals that the value stores the delta of a posting list.
	BitDeltaPosting byte = 0x04
	// BitCompletePosting signals that the values stores a complete posting list.
	BitCompletePosting byte = 0x08
	// BitEmptyPosting signals that the value stores an empty posting list.
	BitEmptyPosting byte = 0x10
	// BitForbidPosting signals that key should NEVER have postings. This would
	// typically be due to this being considered a Jupiter key, i.e. the key has
	// some very heavy fan-out, which we don't want to process.
	BitForbidPosting byte = 0x20 | BitEmptyPosting
)

// List stores the in-memory representation of a posting list.
type List struct {
	x.SafeMutex
	key         []byte
	plist       *pb.PostingList
	mutationMap map[uint64]*pb.PostingList
	minTs       uint64 // commit timestamp of immutable layer, reject reads before this ts.
	maxTs       uint64 // max commit timestamp seen for this list.
	forbid      bool
}

// NewList returns a new list with an immutable layer set to plist and the
// timestamp of the immutable layer set to minTs.
func NewList(key []byte, plist *pb.PostingList, minTs uint64) *List {
	return &List{
		key:         key,
		plist:       plist,
		mutationMap: make(map[uint64]*pb.PostingList),
		minTs:       minTs,
	}
}

func (l *List) maxVersion() uint64 {
	l.RLock()
	defer l.RUnlock()
	return l.maxTs
}

// pIterator only iterates over Postings. Not UIDs.
type pIterator struct {
	l     *List
	plist *pb.PostingList
	pidx  int // index of postings

	afterUid uint64
	splitIdx int
	// The timestamp of a delete marker in the mutable layer. If this value is greater
	// than zero, then the immutable posting list should not be traversed.
	deleteBelowTs uint64
}

func (it *pIterator) seek(l *List, afterUid, deleteBelowTs uint64) error {
	if deleteBelowTs > 0 && deleteBelowTs <= l.minTs {
		return errors.Errorf("deleteBelowTs (%d) must be greater than the minTs in the list (%d)",
			deleteBelowTs, l.minTs)
	}

	it.l = l
	it.splitIdx = it.selectInitialSplit(afterUid)
	if len(it.l.plist.Splits) > 0 {
		plist, err := l.readListPart(it.l.plist.Splits[it.splitIdx])
		if err != nil {
			return errors.Wrapf(err, "cannot read initial list part for list with base key %s",
				hex.EncodeToString(l.key))
		}
		it.plist = plist
	} else {
		it.plist = l.plist
	}

	it.afterUid = afterUid
	it.deleteBelowTs = deleteBelowTs
	if deleteBelowTs > 0 {
		// We don't need to iterate over the immutable layer if this is > 0. Returning here would
		// mean it.uids is empty and valid() would return false.
		return nil
	}

	it.pidx = sort.Search(len(it.plist.Postings), func(idx int) bool {
		p := it.plist.Postings[idx]
		return it.afterUid < p.Uid
	})
	return nil
}

func (it *pIterator) selectInitialSplit(afterUid uint64) int {
	return it.l.splitIdx(afterUid)
}

// moveToNextValidPart moves the iterator to the next part that contains valid data.
// This is used to skip over parts of the list that might not contain postings.
func (it *pIterator) moveToNextValidPart() error {
	// Not a multi-part list, the iterator has reached the end of the list.
	splits := it.l.plist.Splits
	it.splitIdx++

	for ; it.splitIdx < len(splits); it.splitIdx++ {
		plist, err := it.l.readListPart(splits[it.splitIdx])
		if err != nil {
			return errors.Wrapf(err,
				"cannot move to next list part in iterator for list with key %s",
				hex.EncodeToString(it.l.key))
		}
		it.plist = plist
		if len(plist.Postings) == 0 {
			continue
		}
		if plist.Postings[0].Uid > it.afterUid {
			it.pidx = 0
			return nil
		}
		it.pidx = sort.Search(len(plist.Postings), func(idx int) bool {
			p := plist.Postings[idx]
			return it.afterUid < p.Uid
		})
		if it.pidx == len(plist.Postings) {
			continue
		}
		return nil
	}
	return nil
}

// valid asserts that pIterator has valid uids, or advances it to the next valid part.
// It returns false if there are no more valid parts.
func (it *pIterator) valid() (bool, error) {
	if it.deleteBelowTs > 0 {
		return false, nil
	}
	if it.pidx < len(it.plist.Postings) {
		return true, nil
	}

	err := it.moveToNextValidPart()
	switch {
	case err != nil:
		return false, errors.Wrapf(err, "cannot advance iterator when calling pIterator.valid")
	case it.pidx < len(it.plist.Postings):
		return true, nil
	default:
		return false, nil
	}
}

func (it *pIterator) posting() *pb.Posting {
	p := it.plist.Postings[it.pidx]
	return p
}

// ListOptions is used in List.Uids (in posting) to customize our output list of
// UIDs, for each posting list. It should be pb.to this package.
type ListOptions struct {
	ReadTs    uint64
	AfterUid  uint64   // Any UIDs returned must be after this value.
	Intersect *pb.List // Intersect results with this list of UIDs.
	First     int
}

// NewPosting takes the given edge and returns its equivalent representation as a posting.
func NewPosting(t *pb.Edge) (*pb.Posting, error) {
	var op uint32
	switch t.Op {
	case pb.Edge_SET:
		op = Set
	case pb.Edge_DEL:
		op = Del
	default:
		x.Fatalf("Unhandled operation: %+v", t)
	}

	var postingType pb.Posting_PostingType
	switch {
	case len(t.ObjectId) == 0:
		postingType = pb.Posting_VALUE
	default:
		postingType = pb.Posting_REF
	}

	p := &pb.Posting{
		Uid:         x.FromHex(t.ObjectId),
		Value:       t.ObjectValue,
		PostingType: postingType,
		Op:          op,
	}
	return p, nil
}

func hasDeleteAll(mpost *pb.Posting) bool {
	return mpost.Op == Del && bytes.Equal(mpost.Value, []byte(x.Star))
}

// Ensure that you either abort the uncommitted postings or commit them before calling me.
func (l *List) updateMutationLayer(mpost *pb.Posting, singleUidUpdate bool) error {
	l.AssertLock()
	x.AssertTrue(mpost.Op == Set || mpost.Op == Del)

	// If we have a delete all, then we replace the map entry with just one.
	if hasDeleteAll(mpost) {
		plist := &pb.PostingList{}
		plist.Postings = append(plist.Postings, mpost)
		if l.mutationMap == nil {
			l.mutationMap = make(map[uint64]*pb.PostingList)
		}
		l.mutationMap[mpost.StartTs] = plist
		return nil
	}

	plist, ok := l.mutationMap[mpost.StartTs]
	if !ok {
		plist = &pb.PostingList{}
		if l.mutationMap == nil {
			l.mutationMap = make(map[uint64]*pb.PostingList)
		}
		l.mutationMap[mpost.StartTs] = plist
	}

	if singleUidUpdate {
		// This handles the special case when adding a value to predicates of type uid.
		// The current value should be deleted in favor of this value. This needs to
		// be done because the fingerprint for the value is not math.MaxUint64 as is
		// the case with the rest of the scalar predicates.
		newPlist := &pb.PostingList{}
		newPlist.Postings = append(newPlist.Postings, mpost)

		// Add the deletions in the existing plist because those postings are not picked
		// up by iterating. Not doing so would result in delete operations that are not
		// applied when the transaction is committed.
		for _, post := range plist.Postings {
			if post.Op == Del && post.Uid != mpost.Uid {
				newPlist.Postings = append(newPlist.Postings, post)
			}
		}

		err := l.iterateAll(mpost.StartTs, 0, func(obj *pb.Posting) error {
			// Ignore values which have the same uid as they will get replaced
			// by the current value.
			if obj.Uid == mpost.Uid {
				return nil
			}

			// Mark all other values as deleted. By the end of the iteration, the
			// list of postings will contain deleted operations and only one set
			// for the mutation stored in mpost.
			objCopy := proto.Clone(obj).(*pb.Posting)
			objCopy.Op = Del
			newPlist.Postings = append(newPlist.Postings, objCopy)
			return nil
		})
		if err != nil {
			return err
		}

		// Update the mutation map with the new plist. Return here since the code below
		// does not apply for predicates of type uid.
		l.mutationMap[mpost.StartTs] = newPlist
		return nil
	}

	// Even if we have a delete all in this transaction, we should still pick up any updates since.
	// Note: If we have a big transaction of say 1M postings, then this loop would be taking up all
	// the time, because it is O(N^2), where N = number of postings added.
	for i, prev := range plist.Postings {
		if prev.Uid == mpost.Uid {
			plist.Postings[i] = mpost
			return nil
		}
	}
	plist.Postings = append(plist.Postings, mpost)
	return nil
}

// TypeID returns the typeid of destination vertex
func TypeID(edge *pb.Edge) types.TypeID {
	if len(edge.ObjectId) != 0 {
		return types.TypeUid
	}
	if len(edge.ObjectValue) == 0 {
		return types.TypeUndefined
	}
	return types.TypeID(edge.ObjectValue[0])
}

func fingerprintEdge(t *pb.Edge) uint64 {
	// There could be a collision if the user gives us a value with Lang = "en" and later gives
	// us a value = "en" for the same predicate. We would end up overwriting his older lang
	// value.

	// All edges with a value without LANGTAG, have the same UID. In other words,
	// an (entity, attribute) can only have one untagged value.
	var id uint64 = math.MaxUint64

	if schema.State().IsList(t.Predicate) {
		id = farm.Fingerprint64(t.ObjectValue)
	}
	return id
}

func (l *List) addMutation(ctx context.Context, txn *Txn, t *pb.Edge) error {
	l.Lock()
	defer l.Unlock()
	return l.addMutationInternal(ctx, txn, t)
}

func GetConflictKey(pk x.ParsedKey, key []byte, t *pb.Edge) uint64 {
	getKey := func(key []byte, uid uint64) uint64 {
		// Instead of creating a string first and then doing a fingerprint, let's do a fingerprint
		// here to save memory allocations.
		// Not entirely sure about effect on collision chances due to this simple XOR with uid.
		return farm.Fingerprint64(key) ^ uid
	}

	var conflictKey uint64
	switch {
	case schema.State().HasUpsert(t.Predicate):
		// Consider checking to see if a email id is unique. A user adds:
		// <uid> <email> "email@email.org", and there's a string equal tokenizer
		// and upsert directive on the schema.
		// Then keys are "<email> <uid>" and "<email> email@email.org"
		// The first key won't conflict, because two different UIDs can try to
		// get the same email id. But, the second key would. Thus, we ensure
		// that two users don't set the same email id.
		conflictKey = getKey(key, 0)

	case pk.IsData() && schema.State().IsList(t.Predicate):
		// Data keys, irrespective of whether they are UID or values, should be judged based on
		// whether they are lists or not. For UID, t.ValueId = UID. For value, t.ValueId =
		// fingerprint(value) or could be fingerprint(lang) or something else.
		//
		// For singular uid predicate, like partner: uid // no list.
		// a -> b
		// a -> c
		// Run concurrently, only one of them should succeed.
		// But for friend: [uid], both should succeed.
		//
		// Similarly, name: string
		// a -> "x"
		// a -> "y"
		// This should definitely have a conflict.
		// But, if name: [string], then they can both succeed.
		conflictKey = getKey(key, x.FromHex(t.ObjectId))

	case pk.IsData(): // NOT a list. This case must happen after the above case.
		conflictKey = getKey(key, 0)

	case pk.IsIndex() || pk.IsCount():
		// Index keys are by default of type [uid].
		conflictKey = getKey(key, x.FromHex(t.ObjectId))

	default:
		// Don't assign a conflictKey.
	}

	return conflictKey
}

func (l *List) addMutationInternal(ctx context.Context, txn *Txn, t *pb.Edge) error {
	l.AssertLock()

	mpost, err := NewPosting(t)
	if err != nil {
		return err
	}
	mpost.StartTs = txn.ReadTs
	if mpost.PostingType != pb.Posting_REF {
		mpost.Uid = fingerprintEdge(t)
	}

	// Check whether this mutation is an update for a predicate of type uid.
	pk, err := x.Parse(l.key)
	if err != nil {
		return errors.Wrapf(err, "cannot parse key when adding mutation to list with key %s",
			hex.EncodeToString(l.key))
	}
	pred, ok := schema.State().Get(ctx, t.Predicate)
	isSingleUidUpdate := ok && !pred.GetList() && pred.GetValueType() == types.TypeUid.Int() &&
		pk.IsData() && mpost.Op == Set && mpost.PostingType == pb.Posting_REF

	if err != l.updateMutationLayer(mpost, isSingleUidUpdate) {
		return errors.Wrapf(err, "cannot update mutation layer of key %s with value %+v",
			hex.EncodeToString(l.key), mpost)
	}
	return nil
}

// getMutation returns a marshaled version of posting list mutation stored internally.
func (l *List) getMutation(startTs uint64) []byte {
	l.RLock()
	defer l.RUnlock()
	if pl, ok := l.mutationMap[startTs]; ok {
		data, err := pl.Marshal()
		x.Check(err)
		return data
	}
	return nil
}

func (l *List) setMutation(startTs uint64, data []byte) {
	pl := new(pb.PostingList)
	x.Check(pl.Unmarshal(data))

	l.Lock()
	if l.mutationMap == nil {
		l.mutationMap = make(map[uint64]*pb.PostingList)
	}
	l.mutationMap[startTs] = pl
	l.Unlock()
}

func (l *List) splitIdx(afterUid uint64) int {
	if afterUid == 0 || len(l.plist.Splits) == 0 {
		return 0
	}
	for i, startUid := range l.plist.Splits {
		// If startUid == afterUid, the current block should be selected.
		if startUid == afterUid {
			return i
		}
		// If this split starts at an UID greater than afterUid, there might be
		// elements in the previous split that need to be checked.
		if startUid > afterUid {
			return i - 1
		}
	}
	// In case no split's startUid is greater or equal than afterUid, start the
	// iteration at the start of the last split.
	return len(l.plist.Splits) - 1
}

func (l *List) Bitmap(opt ListOptions) (*sroar.Bitmap, error) {
	l.RLock()
	defer l.RUnlock()
	return l.bitmap(opt)
}

// Bitmap would generate a sroar.Bitmap from the list.
// It works on split posting lists as well.
func (l *List) bitmap(opt ListOptions) (*sroar.Bitmap, error) {
	deleteBelow, posts := l.pickPostings(opt.ReadTs)

	var iw *sroar.Bitmap
	if opt.Intersect != nil {
		iw = codec.FromListNoCopy(opt.Intersect)
	}
	r := sroar.NewBitmap()
	if deleteBelow == 0 {
		r = sroar.FromBufferWithCopy(l.plist.Bitmap)
		if iw != nil {
			r.And(iw)
		}
		codec.RemoveRange(r, 0, opt.AfterUid)

		si := l.splitIdx(opt.AfterUid)
		for _, startUid := range l.plist.Splits[si:] {
			// We could skip over some splits, if they won't have the Uid range we care about.
			split, err := l.readListPart(startUid)
			if err != nil {
				return nil, errors.Wrapf(err, "while reading a split with startUid: %d", startUid)
			}
			s := sroar.FromBufferWithCopy(split.Bitmap)

			// Intersect with opt.Intersect.
			if iw != nil {
				s.And(iw)
			}
			if startUid < opt.AfterUid {
				// Only keep the Uids after opt.AfterUid.
				codec.RemoveRange(s, 0, opt.AfterUid)
			}
			r.Or(s)
		}
	}

	prev := uint64(0)
	for _, p := range posts {
		if p.Uid == prev {
			continue
		}
		if p.Op == Set {
			r.Set(p.Uid)
		} else if p.Op == Del {
			r.Remove(p.Uid)
		}
		prev = p.Uid
	}

	codec.RemoveRange(r, 0, opt.AfterUid)
	if iw != nil {
		r.And(iw)
	}
	return r, nil
}

// Iterate will allow you to iterate over the mutable and immutable layers of
// this posting List, while having acquired a read lock.
// So, please keep this iteration cheap, otherwise mutations would get stuck.
// The iteration will start after the provided UID. The results would not include this uid.
// The function will loop until either the posting List is fully iterated, or you return a false
// in the provided function, which will indicate to the function to break out of the iteration.
//
// 	pl.Iterate(..., func(p *pb.posting) error {
//    // Use posting p
//    return nil // to continue iteration.
//    return errStopIteration // to break iteration.
//  })
func (l *List) Iterate(readTs uint64, afterUid uint64, f func(obj *pb.Posting) error) error {
	l.RLock()
	defer l.RUnlock()
	return l.iterate(readTs, afterUid, f)
}

// IterateAll iterates over all the UIDs and Postings.
// TODO: We should remove this function after merging roaring bitmaps and fixing up how we map
// facetsMatrix to uidMatrix.
func (l *List) iterateAll(readTs uint64, afterUid uint64, f func(obj *pb.Posting) error) error {

	bm, err := l.bitmap(ListOptions{
		ReadTs:   readTs,
		AfterUid: afterUid,
	})
	if err != nil {
		return err
	}

	p := &pb.Posting{}

	uitr := bm.NewIterator()
	var next uint64

	advance := func() {
		next = math.MaxUint64
		if nx := uitr.Next(); nx > 0 {
			next = nx
		}
	}
	advance()

	var maxUid uint64
	fn := func(obj *pb.Posting) error {
		maxUid = x.Max(maxUid, obj.Uid)
		return f(obj)
	}

	fi := func(obj *pb.Posting) error {
		for next < obj.Uid {
			p.Uid = next
			if err := fn(p); err != nil {
				return err
			}
			advance()
		}
		if err := fn(obj); err != nil {
			return err
		}
		if obj.Uid == next {
			advance()
		}
		return nil
	}
	if err := l.iterate(readTs, afterUid, fi); err != nil {
		return err
	}

	codec.RemoveRange(bm, 0, maxUid)
	uitr = bm.NewIterator()
	for u := uitr.Next(); u > 0; u = uitr.Next() {
		p.Uid = u
		f(p)
	}
	return nil
}

func (l *List) IterateAll(readTs uint64, afterUid uint64, f func(obj *pb.Posting) error) error {
	l.RLock()
	defer l.RUnlock()
	return l.iterateAll(readTs, afterUid, f)
}

// pickPostings goes through the mutable layer and returns the appropriate postings,
// along with the timestamp of the delete marker, if any. If this timestamp is greater
// than zero, it indicates that the immutable layer should be ignored during traversals.
// If greater than zero, this timestamp must thus be greater than l.minTs.
func (l *List) pickPostings(readTs uint64) (uint64, []*pb.Posting) {
	// This function would return zero ts for entries above readTs.
	effective := func(start, commit uint64) uint64 {
		if commit > 0 && commit <= readTs {
			// Has been committed and below the readTs.
			return commit
		}
		if start == readTs {
			// This mutation is by ME. So, I must be able to read it.
			return start
		}
		return 0
	}

	// First pick up the postings.
	var deleteBelowTs uint64
	var posts []*pb.Posting
	for startTs, plist := range l.mutationMap {
		// Pick up the transactions which are either committed, or the one which is ME.
		effectiveTs := effective(startTs, plist.CommitTs)
		if effectiveTs > deleteBelowTs {
			// We're above the deleteBelowTs marker. We wouldn't reach here if effectiveTs is zero.
			for _, mpost := range plist.Postings {
				if hasDeleteAll(mpost) {
					deleteBelowTs = effectiveTs
					continue
				}
				posts = append(posts, mpost)
			}
		}
	}

	if deleteBelowTs > 0 {
		// There was a delete all marker. So, trim down the list of postings.
		result := posts[:0]
		for _, post := range posts {
			effectiveTs := effective(post.StartTs, post.CommitTs)
			if effectiveTs < deleteBelowTs { // Do pick the posts at effectiveTs == deleteBelowTs.
				continue
			}
			result = append(result, post)
		}
		posts = result
	}

	// Sort all the postings by UID (inc order), then by commit/startTs in dec order.
	sort.Slice(posts, func(i, j int) bool {
		pi := posts[i]
		pj := posts[j]
		if pi.Uid == pj.Uid {
			ei := effective(pi.StartTs, pi.CommitTs)
			ej := effective(pj.StartTs, pj.CommitTs)
			return ei > ej // Pick the higher, so we can discard older commits for the same UID.
		}
		return pi.Uid < pj.Uid
	})
	return deleteBelowTs, posts
}

func (l *List) iterate(readTs uint64, afterUid uint64, f func(obj *pb.Posting) error) error {
	l.AssertRLock()

	// mposts is the list of mutable postings
	deleteBelowTs, mposts := l.pickPostings(readTs)
	if readTs < l.minTs {
		return errors.Errorf("readTs: %d less than minTs: %d for key: %q", readTs, l.minTs, l.key)
	}

	midx, mlen := 0, len(mposts)
	if afterUid > 0 {
		midx = sort.Search(mlen, func(idx int) bool {
			mp := mposts[idx]
			return afterUid < mp.Uid
		})
	}

	var (
		mp, pp  *pb.Posting
		pitr    pIterator
		prevUid uint64
		err     error
	)

	// pitr iterates through immutable postings
	err = pitr.seek(l, afterUid, deleteBelowTs)
	if err != nil {
		return errors.Wrapf(err, "cannot initialize iterator when calling List.iterate")
	}

loop:
	for err == nil {
		if midx < mlen {
			mp = mposts[midx]
		} else {
			mp = emptyPosting
		}

		valid, err := pitr.valid()
		switch {
		case err != nil:
			break loop
		case valid:
			pp = pitr.posting()
		default:
			pp = emptyPosting
		}

		switch {
		case mp.Uid > 0 && mp.Uid == prevUid:
			// Only pick the latest version of this posting.
			// mp.Uid can be zero if it's an empty posting.
			midx++
		case pp.Uid == 0 && mp.Uid == 0:
			// Reached empty posting for both iterators.
			return nil
		case mp.Uid == 0 || (pp.Uid > 0 && pp.Uid < mp.Uid):
			// Either mp is empty, or pp is lower than mp.
			err = f(pp)
			if err != nil {
				break loop
			}
			pitr.pidx++
		case pp.Uid == 0 || (mp.Uid > 0 && mp.Uid < pp.Uid):
			// Either pp is empty, or mp is lower than pp.
			if mp.Op != Del {
				err = f(mp)
				if err != nil {
					break loop
				}
			}
			prevUid = mp.Uid
			midx++
		case pp.Uid == mp.Uid:
			if mp.Op != Del {
				err = f(mp)
				if err != nil {
					break loop
				}
			}
			prevUid = mp.Uid
			pitr.pidx++
			midx++
		default:
			log.Fatalf("Unhandled case during iteration of posting list.")
		}
	}
	if err == ErrStopIteration {
		return nil
	}
	return err
}

// IsEmpty returns true if there are no uids at the given timestamp after the given UID.
func (l *List) IsEmpty(readTs, afterUid uint64) (bool, error) {
	opt := ListOptions{
		ReadTs:   readTs,
		AfterUid: afterUid,
	}
	bm, err := l.Bitmap(opt)
	if err != nil {
		return false, errors.Wrapf(err, "Failed to get the bitmap")
	}
	return bm.GetCardinality() == 0, nil
}

func (l *List) getPostingAndLength(readTs, afterUid, uid uint64) (int, bool, *pb.Posting) {
	l.AssertRLock()
	var post *pb.Posting
	var bm *sroar.Bitmap
	var err error

	foundPosting := false
	opt := ListOptions{
		ReadTs:   readTs,
		AfterUid: afterUid,
	}
	if bm, err = l.bitmap(opt); err != nil {
		return -1, false, nil
	}
	count := int(bm.GetCardinality())
	found := bm.Contains(uid)

	err = l.iterate(readTs, afterUid, func(p *pb.Posting) error {
		if p.Uid == uid {
			post = p
			foundPosting = true
		}
		return nil
	})
	if err != nil {
		return -1, false, nil
	}

	if found && !foundPosting {
		post = &pb.Posting{Uid: uid}
	}
	return count, found, post
}

// Length iterates over the mutation layer and counts number of elements.
func (l *List) Length(readTs, afterUid uint64) int {
	opt := ListOptions{
		ReadTs:   readTs,
		AfterUid: afterUid,
	}
	bm, err := l.Bitmap(opt)
	if err != nil {
		return -1
	}
	return int(bm.GetCardinality())
}

var MaxSplits int

func init() {
	MaxSplits = int(x.Config.Limit.GetInt64("max-splits"))
}

// Rollup performs the rollup process, merging the immutable and mutable layers
// and outputting the resulting list so it can be written to disk.
// During this process, the list might be split into multiple lists if the main
// list or any of the existing parts become too big.
//
// A normal list has the following format:
// <key> -> <posting list with all the data for this list>
//
// A multi-part list is stored in multiple keys. The keys for the parts will be generated by
// appending the first UID in the part to the key. The list will have the following format:
// <key> -> <posting list that includes no postings but a list of each part's start UID>
// <key, 1> -> <first part of the list with all the data for this part>
// <key, next start UID> -> <second part of the list with all the data for this part>
// ...
// <key, last start UID> -> <last part of the list with all its data>
//
// The first part of a multi-part list always has start UID 1 and will be the last part
// to be deleted, at which point the entire list will be marked for deletion.
// As the list grows, existing parts might be split if they become too big.
func (l *List) Rollup(alloc *z.Allocator) ([]*bpb.KV, error) {
	l.RLock()
	defer l.RUnlock()
	out, err := l.rollup(math.MaxUint64, true)
	if err != nil {
		return nil, errors.Wrapf(err, "failed when calling List.rollup")
	}
	if out == nil {
		return nil, nil
	}

	// deletionKvs returns the KVs corresponding to those splits, that are outdated.
	// If 'all' is set to true, then it returns all the split KVs, else it returns only KVs
	// corresponding to those splits that existed before rollup but not after it.
	deletionKvs := func(all bool) ([]*bpb.KV, error) {
		var delKvs []*bpb.KV
		for _, startUid := range l.plist.Splits {
			if _, ok := out.parts[startUid]; !all && ok {
				// Don't delete this split part because we are sending an update now.
				continue
			}
			key, err := x.SplitKey(l.key, startUid)
			if err != nil {
				return nil, errors.Wrapf(err,
					"cannot generate split key for list with base key %s and start UID %d",
					hex.EncodeToString(l.key), startUid)
			}
			delKvs = append(delKvs, &bpb.KV{
				Key:      key,
				Value:    nil,
				UserMeta: []byte{BitEmptyPosting},
				Version:  out.newMinTs + 1,
			})
		}
		return delKvs, nil
	}

	if l.forbid || len(out.parts) > MaxSplits {
		var kvs []*bpb.KV
		kv := &bpb.KV{
			Key:      alloc.Copy(l.key),
			Value:    nil,
			UserMeta: []byte{BitForbidPosting},
			Version:  out.newMinTs + 1,
		}
		kvs = append(kvs, kv)

		// Send deletion for the parts.
		delKvs, err := deletionKvs(true)
		if err != nil {
			return nil, err
		}
		kvs = append(kvs, delKvs...)
		return kvs, nil
	}
	if len(out.parts) > 0 {
		// The main list for the split postings should not contain postings and bitmap.
		x.AssertTrue(out.plist.Postings == nil)
		x.AssertTrue(out.plist.Bitmap == nil)
	}

	var kvs []*bpb.KV
	kv := MarshalPostingList(out.plist, alloc)
	// We set kv.Version to newMinTs + 1 because if we write the rolled up keys at the same ts as
	// that of the delta, then in case of wal replay the rolled up key would get over-written by the
	// delta which can bring db to an invalid state.
	// It would be fine to write rolled up key at ts+1 and this key won't be overwritten by any
	// other delta because there cannot be commit at ts as well as ts+1 on the same key. The reason
	// is as follows:
	// Suppose there are two inter-leaved txns [s1 s2 c1 c2] where si, ci is the start and commit
	// of the i'th txn. In this case c2 would not have happened because of conflict.
	// Suppose there are two disjoint txns [s1 c1 s2 c2], then c1 and c2 cannot be consecutive.
	kv.Version = out.newMinTs + 1
	kv.Key = alloc.Copy(l.key)
	kvs = append(kvs, kv)

	for startUid, plist := range out.parts {
		// Any empty posting list would still have BitEmpty set. And the main posting list
		// would NOT have that posting list startUid in the splits list.
		kv, err := out.marshalPostingListPart(alloc, l.key, startUid, plist)
		if err != nil {
			return nil, errors.Wrapf(err, "cannot marshaling posting list parts")
		}
		kvs = append(kvs, kv)
	}

	// When split happens, the split boundaries might change. In that case, we need to delete the
	// old split parts from the DB. Otherwise, they would stay as zombie and eat up the memory.
	delKvs, err := deletionKvs(false)
	if err != nil {
		return nil, err
	}
	kvs = append(kvs, delKvs...)

	// Sort the KVs by their key so that the main part of the list is at the
	// start of the list and all other parts appear in the order of their start UID.
	sort.Slice(kvs, func(i, j int) bool {
		return bytes.Compare(kvs[i].Key, kvs[j].Key) <= 0
	})

	x.VerifyPostingSplits(kvs, out.plist, out.parts, l.key)
	return kvs, nil
}

func (out *rollupOutput) marshalPostingListPart(alloc *z.Allocator,
	baseKey []byte, startUid uint64, plist *pb.PostingList) (*bpb.KV, error) {
	key, err := x.SplitKey(baseKey, startUid)
	if err != nil {
		return nil, errors.Wrapf(err,
			"cannot generate split key for list with base key %s and start UID %d",
			hex.EncodeToString(baseKey), startUid)
	}
	kv := MarshalPostingList(plist, alloc)
	kv.Version = out.newMinTs + 1
	kv.Key = alloc.Copy(key)
	return kv, nil
}

// MarshalPostingList returns a KV with the marshalled posting list. The caller
// SHOULD SET the Key and Version for the returned KV.
func MarshalPostingList(plist *pb.PostingList, alloc *z.Allocator) *bpb.KV {
	x.VerifyPack(plist)
	kv := y.NewKV(alloc)
	if isPlistEmpty(plist) {
		kv.Value = nil
		kv.UserMeta = alloc.Copy([]byte{BitEmptyPosting})
		return kv
	}

	out := alloc.Allocate(plist.Size())
	n, err := plist.MarshalToSizedBuffer(out)
	x.Check(err)
	kv.Value = out[:n]
	kv.UserMeta = alloc.Copy([]byte{BitCompletePosting})
	return kv
}

type rollupOutput struct {
	plist    *pb.PostingList
	parts    map[uint64]*pb.PostingList
	newMinTs uint64
	sranges  map[uint64]uint64
}

// A range contains [start, end], both inclusive. So, no overlap should exist
// between ranges.
func (ro *rollupOutput) initRanges(split bool) {
	ro.sranges = make(map[uint64]uint64)
	splits := ro.plist.Splits
	if !split {
		splits = splits[:0]
	}
	for i := 0; i < len(splits); i++ {
		end := uint64(math.MaxUint64)
		if i < len(splits)-1 {
			end = splits[i+1] - 1
		}
		start := splits[i]
		ro.sranges[start] = end
	}
	if len(ro.sranges) == 0 {
		ro.sranges[1] = math.MaxUint64
	}
}

func (ro *rollupOutput) getRange(uid uint64) (uint64, uint64) {
	for start, end := range ro.sranges {
		if uid >= start && uid <= end {
			return start, end
		}
	}
	return 1, math.MaxUint64
}

func ShouldSplit(plist *pb.PostingList) bool {
	if plist.Size() >= maxListSize {
		r := sroar.FromBuffer(plist.Bitmap)
		return r.GetCardinality() > 1
	}
	return false
}

func (ro *rollupOutput) runSplits() error {
	if len(ro.parts) == 0 {
		ro.parts[1] = ro.plist
	}
	for startUid, pl := range ro.parts {
		if ShouldSplit(pl) {
			if err := ro.split(startUid); err != nil {
				return err
			}
		}
	}
	return nil
}

func (ro *rollupOutput) split(startUid uint64) error {
	pl := ro.parts[startUid]

	r := sroar.FromBuffer(pl.Bitmap)

	getPostings := func(startUid, endUid uint64) []*pb.Posting {
		startIdx := sort.Search(len(pl.Postings), func(i int) bool {
			return pl.Postings[i].Uid >= startUid
		})
		endIdx := sort.Search(len(pl.Postings), func(i int) bool {
			return pl.Postings[i].Uid > endUid
		})
		return pl.Postings[startIdx:endIdx]
	}

	f := func(start, end uint64) uint64 {
		posts := getPostings(start, end)
		if len(posts) == 0 {
			return 0
		}
		// Just approximate by taking first postings size and multiplying it.
		return uint64(posts[0].Size() * len(posts))
	}

	// Provide a 30% cushion, because Split doesn't do equal splitting based on maxListSize.
	bms := r.Split(f, uint64(0.7*float64(maxListSize)))

	for i, bm := range bms {
		c := bm.GetCardinality()
		if c == 0 {
			continue
		}
		start, err := bm.Select(0)
		x.Check(err)
		end, err := bm.Select(uint64(c) - 1)
		x.Check(err)

		newpl := &pb.PostingList{}
		newpl.Bitmap = bm.ToBuffer()
		postings := getPostings(start, end)
		newpl.Postings = postings

		// startUid = 1 is treated specially. ro.parts should always contain 1.
		if i == 0 && startUid == 1 {
			start = 1
		}
		ro.parts[start] = newpl
	}

	return nil
}

func (l *List) encode(out *rollupOutput, readTs uint64, split bool) error {
	bm, err := l.bitmap(ListOptions{ReadTs: readTs})
	if err != nil {
		return err
	}

	out.initRanges(split)
	// Pick up all the bitmaps first.
	for startUid, endUid := range out.sranges {
		r := bm.Clone()
		r.RemoveRange(0, startUid) // Excluding startUid.
		if endUid != math.MaxUint64 {
			codec.RemoveRange(r, endUid+1, math.MaxUint64) // Removes both.
		}

		plist := &pb.PostingList{}
		plist.Bitmap = r.ToBuffer()

		out.parts[startUid] = plist
	}

	// Now pick up all the postings.
	startUid, endUid := out.getRange(1)
	plist := out.parts[startUid]
	err = l.iterate(readTs, 0, func(p *pb.Posting) error {
		if p.Uid > endUid {
			startUid, endUid = out.getRange(p.Uid)
			plist = out.parts[startUid]
		}

		if p.PostingType != pb.Posting_REF {
			plist.Postings = append(plist.Postings, p)
		}
		return nil
	})
	// Finish  writing the last part of the list (or the whole list if not a multi-part list).
	return errors.Wrapf(err, "cannot iterate through the list")
}

// Merge all entries in mutation layer with commitTs <= l.commitTs into
// immutable layer. Note that readTs can be math.MaxUint64, so do NOT use it
// directly. It should only serve as the read timestamp for iteration.
func (l *List) rollup(readTs uint64, split bool) (*rollupOutput, error) {
	l.AssertRLock()

	// Pick all committed entries
	if l.minTs > readTs {
		// If we are already past the readTs, then skip the rollup.
		return nil, nil
	}

	out := &rollupOutput{
		plist: &pb.PostingList{
			Splits: l.plist.Splits,
		},
		parts: make(map[uint64]*pb.PostingList),
	}

	if len(out.plist.Splits) > 0 || len(l.mutationMap) > 0 {
		// In case there were splits, this would read all the splits from
		// Badger.
		if err := l.encode(out, readTs, split); err != nil {
			return nil, errors.Wrapf(err, "while encoding")
		}
	} else {
		// We already have a nicely packed posting list. Just use it.
		x.VerifyPack(l.plist)
		out.plist = proto.Clone(l.plist).(*pb.PostingList)
	}

	maxCommitTs := l.minTs
	{
		// We can't rely upon iterate to give us the max commit timestamp, because it can skip over
		// postings which had deletions to provide a sorted view of the list. Therefore, the safest
		// way to get the max commit timestamp is to pick all the relevant postings for the given
		// readTs and calculate the maxCommitTs.
		// If deleteBelowTs is greater than zero, there was a delete all marker. The list of
		// postings has been trimmed down.
		deleteBelowTs, mposts := l.pickPostings(readTs)
		maxCommitTs = x.Max(maxCommitTs, deleteBelowTs)
		for _, mp := range mposts {
			maxCommitTs = x.Max(maxCommitTs, mp.CommitTs)
		}
	}

	out.newMinTs = maxCommitTs
	if split {
		// Check if the list (or any of it's parts if it's been previously split) have
		// become too big. Split the list if that is the case.
		if err := out.runSplits(); err != nil {
			return nil, err
		}
	} else {
		out.plist.Splits = nil
	}
	out.finalize()
	return out, nil
}

func abs(a int) int {
	if a < 0 {
		return -a
	}
	return a
}

// Uids returns the UIDs given some query params.
// We have to apply the filtering before applying (offset, count).
// WARNING: Calling this function just to get UIDs is expensive
func (l *List) Uids(opt ListOptions) (*pb.List, error) {
	bm, err := l.Bitmap(opt)

	out := &pb.List{}
	if err != nil {
		return out, err
	}

	// TODO: Need to fix this. We shouldn't pick up too many uids.
	// Before this, we were only picking math.Int32 number of uids.
	// Now we're picking everything.
	if opt.First == 0 {
		out.Bitmap = bm.ToBufferWithCopy()
		// TODO: Not yet ready to use Bitmap for data transfer. We'd have to deal with all the
		// places where List.Uids is being called.
		// out.Bitmap = codec.ToBytes(bm)
		return out, nil
	}
	num := uint64(abs(opt.First))
	sz := uint64(bm.GetCardinality())
	if num < sz {
		if opt.First > 0 {
			x, err := bm.Select(num)
			if err != nil {
				return nil, errors.Wrap(err, "While selecting Uids")
			}
			codec.RemoveRange(bm, x, math.MaxUint64)
		} else {
			x, err := bm.Select(sz - num)
			if err != nil {
				return nil, errors.Wrap(err, "While selecting Uids")
			}
			codec.RemoveRange(bm, 0, x)
		}
	}
	return codec.ToList(bm), nil
}

// Postings calls postFn with the postings that are common with
// UIDs in the opt ListOptions.
func (l *List) Postings(opt ListOptions, postFn func(*pb.Posting) error) error {
	l.RLock()
	defer l.RUnlock()

	err := l.iterate(opt.ReadTs, opt.AfterUid, func(p *pb.Posting) error {
		if p.PostingType != pb.Posting_REF {
			return nil
		}
		return postFn(p)
	})
	return errors.Wrapf(err, "cannot retrieve postings from list with key %s",
		hex.EncodeToString(l.key))
}

// AllValues returns all the values in the posting list.
func (l *List) AllValues(readTs uint64) ([]types.Sval, error) {
	l.RLock()
	defer l.RUnlock()

	var vals []types.Sval
	err := l.iterate(readTs, 0, func(p *pb.Posting) error {
		vals = append(vals, types.Sval(p.Value))
		return nil
	})
	return vals, errors.Wrapf(err, "cannot retrieve all values from list with key %s",
		hex.EncodeToString(l.key))
}

// TODO(Lang): Remove this.
func (l *List) GetLangTags(readTs uint64) ([]string, error) {
	l.RLock()
	defer l.RUnlock()

	var tags []string
	err := l.iterate(readTs, 0, func(p *pb.Posting) error {
		tags = append(tags, "")
		return nil
	})
	return tags, errors.Wrapf(err, "cannot retrieve language tags from list with key %s",
		hex.EncodeToString(l.key))
}

// Value returns the default value from the posting list. The default value is
// defined as the value without a language tag.
func (l *List) Value(readTs uint64) (rval types.Sval, rerr error) {
	l.RLock()
	defer l.RUnlock()
	val, found, err := l.findValue(readTs, math.MaxUint64)
	if err != nil {
		return val, errors.Wrapf(err,
			"cannot retrieve default value from list with key %s", hex.EncodeToString(l.key))
	}
	if !found {
		return val, ErrNoValue
	}
	return val, nil
}

// ValueFor returns a value from posting list.
func (l *List) ValueFor(readTs uint64) (rval types.Sval, rerr error) {
	l.RLock() // All public methods should acquire locks, while private ones should assert them.
	defer l.RUnlock()
	p, err := l.postingFor(readTs)
	if err != nil {
		return rval, err
	}
	return types.Sval(p.Value), nil
}

// PostingFor returns the posting according to the preferred language list.
func (l *List) PostingFor(readTs uint64, langs []string) (p *pb.Posting, rerr error) {
	l.RLock()
	defer l.RUnlock()
	return l.postingFor(readTs)
}

func (l *List) postingFor(readTs uint64) (p *pb.Posting, rerr error) {
	l.AssertRLock() // Avoid recursive locking by asserting a lock here.

	_, pos, err := l.findPosting(readTs, math.MaxUint64)
	return pos, err
}

func (l *List) findValue(readTs, uid uint64) (rval types.Sval, found bool, err error) {
	l.AssertRLock()
	found, p, err := l.findPosting(readTs, uid)
	if !found {
		return rval, found, err
	}

	return types.Sval(p.Value), true, nil
}

func (l *List) findPosting(readTs uint64, uid uint64) (found bool, pos *pb.Posting, err error) {
	// Iterate starts iterating after the given argument, so we pass UID - 1
	err = l.iterate(readTs, uid-1, func(p *pb.Posting) error {
		if p.Uid == uid {
			pos = p
			found = true
		}
		return ErrStopIteration
	})

	return found, pos, errors.Wrapf(err,
		"cannot retrieve posting for UID %d from list with key %s", uid, hex.EncodeToString(l.key))
}

// readListPart reads one split of a posting list from Badger.
func (l *List) readListPart(startUid uint64) (*pb.PostingList, error) {
	key, err := x.SplitKey(l.key, startUid)
	if err != nil {
		return nil, errors.Wrapf(err,
			"cannot generate key for list with base key %s and start UID %d",
			hex.EncodeToString(l.key), startUid)
	}
	txn := pstore.NewTransactionAt(l.minTs, false)
	item, err := txn.Get(key)
	if err != nil {
		return nil, errors.Wrapf(err, "could not read list part with key %s",
			hex.EncodeToString(key))
	}
	part := &pb.PostingList{}
	if err := unmarshalOrCopy(part, item); err != nil {
		return nil, errors.Wrapf(err, "cannot unmarshal list part with key %s",
			hex.EncodeToString(key))
	}
	return part, nil
}

// Returns the sorted list of start UIDs based on the keys in out.parts.
// out.parts is considered the source of truth so this method is considered
// safer than using out.plist.Splits directly.
func (out *rollupOutput) updateSplits() {
	if out.plist == nil || len(out.parts) > 0 {
		out.plist = &pb.PostingList{}
	}

	var splits []uint64
	for startUid := range out.parts {
		splits = append(splits, startUid)
	}
	sort.Slice(splits, func(i, j int) bool {
		return splits[i] < splits[j]
	})
	out.plist.Splits = splits
}

// finalize updates the split list by removing empty posting lists' startUids. In case there is
// only part, then that part is set to main plist.
func (out *rollupOutput) finalize() {
	for startUid, plist := range out.parts {
		// Do not remove the first split for now, as every multi-part list should always
		// have a split starting with UID 1.
		if startUid == 1 {
			continue
		}

		if isPlistEmpty(plist) {
			delete(out.parts, startUid)
		}
	}

	if len(out.parts) == 1 && isPlistEmpty(out.parts[1]) {
		// Only the first split remains. If it's also empty, remove it as well.
		// This should mark the entire list for deletion. Please note that the
		// startUid of the first part is always one because a node can never have
		// its uid set to zero.
		delete(out.parts, 1)
	}

	// We only have one part. Move it to the main plist.
	if len(out.parts) == 1 {
		out.plist = out.parts[1]
		x.AssertTrue(out.plist != nil)
		out.parts = nil
	}
	out.updateSplits()
}

// isPlistEmpty returns true if the given plist is empty. Plists with splits are
// considered non-empty.
func isPlistEmpty(plist *pb.PostingList) bool {
	if len(plist.Splits) > 0 {
		return false
	}
	r := sroar.FromBuffer(plist.Bitmap)
	if r.IsEmpty() {
		return true
	}
	return false
}

// TODO: Remove this func.
// PartSplits returns an empty array if the list has not been split into multiple parts.
// Otherwise, it returns an array containing the start UID of each part.
func (l *List) PartSplits() []uint64 {
	splits := make([]uint64, len(l.plist.Splits))
	copy(splits, l.plist.Splits)
	return splits
}
