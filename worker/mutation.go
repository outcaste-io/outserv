// Portions Copyright 2016-2018 Dgraph Labs, Inc. are available under the Apache License v2.0.
// Portions Copyright 2022 Outcaste LLC are available under the Sustainable License v1.0.

package worker

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/outcaste-io/badger/v3/y"

	"github.com/golang/glog"
	"github.com/pkg/errors"
	otrace "go.opencensus.io/trace"

	"github.com/outcaste-io/badger/v3"
	"github.com/outcaste-io/outserv/conn"
	"github.com/outcaste-io/outserv/posting"
	"github.com/outcaste-io/outserv/protos/pb"
	"github.com/outcaste-io/outserv/schema"
	"github.com/outcaste-io/outserv/types"
	"github.com/outcaste-io/outserv/x"
	"github.com/outcaste-io/ristretto/z"
)

var (
	// ErrNonExistentTabletMessage is the error message sent when no tablet is serving a predicate.
	ErrNonExistentTabletMessage = "Requested predicate is not being served by any tablet"
	errNonExistentTablet        = errors.Errorf(ErrNonExistentTabletMessage)
	errUnservedTablet           = errors.Errorf("Tablet isn't being served by this instance")
)

// Default limit on number of simultaneous open files on unix systems
const DefaultMaxOpenFileLimit = 1024

func isDeletePredicate(edge *pb.Edge) bool {
	return len(edge.Subject) == 0 && x.IsStarAll(edge.ObjectValue)
}

// runMutation goes through all the edges and applies them.
func runMutation(ctx context.Context, edge *pb.Edge, txn *posting.Txn) error {
	ctx = schema.GetWriteContext(ctx)

	// We shouldn't check whether this Alpha serves this predicate or not. Membership information
	// isn't consistent across the entire cluster. We should just apply whatever is given to us.
	su, ok := schema.State().Get(ctx, edge.Predicate)
	if edge.Op == pb.Edge_SET {
		if !ok {
			return errors.Errorf("runMutation: Unable to find schema for %s", edge.Predicate)
		}
	}

	if isDeletePredicate(edge) {
		return errors.New("We should never reach here")
	}

	// Once mutation comes via raft we do best effort conversion
	// Type check is done before proposing mutation, in case schema is not
	// present, some invalid entries might be written initially
	if err := ValidateAndConvert(edge, &su); err != nil {
		return err
	}

	entity := x.FromHex(edge.Subject)
	key := x.DataKey(edge.Predicate, entity)
	// The following is a performance optimization which allows us to not read a posting list from
	// disk. We calculate this based on how AddMutationWithIndex works. The general idea is that if
	// we're not using the read posting list, we don't need to retrieve it. We need the posting list
	// if we're doing indexing or count index or enforcing single UID, etc. In other cases, we can
	// just create a posting list facade in memory and use it to store the delta in Badger. Later,
	// the rollup operation would consolidate all these deltas into a posting list.
	var getFn func(key []byte) (*posting.List, error)
	switch {
	case len(su.GetTokenizer()) > 0 || su.GetCount():
		// Any index or count index.
		getFn = txn.Get
	case su.GetValueType() == types.TypeUid.Int() && !su.GetList():
		// Single UID, not a list.
		getFn = txn.Get
	case edge.Op == pb.Edge_DEL:
		// Covers various delete cases to keep things simple.
		getFn = txn.Get
	default:
		// Reverse index doesn't need the posting list to be read. We already covered count index,
		// single uid and delete all above.
		// Values, whether single or list, don't need to be read.
		// Uid list doesn't need to be read.
		getFn = txn.GetFromDelta
	}

	plist, err := getFn(key)
	if err != nil {
		return err
	}
	return plist.AddMutationWithIndex(ctx, edge, txn)
}

func undoSchemaUpdate(predicate string) {
	maxRetries := 10
	loadErr := x.RetryUntilSuccess(maxRetries, 10*time.Millisecond, func() error {
		return schema.Load(predicate)
	})

	if loadErr != nil {
		glog.Fatalf("failed to load schema after %d retries: %v", maxRetries, loadErr)
	}
}

func runSchemaMutation(ctx context.Context, updates []*pb.SchemaUpdate, startTs uint64) error {
	if len(updates) == 0 {
		return nil
	}
	// Wait until schema modification for all predicates is complete. There cannot be two
	// background tasks running as this is a race condition. We typically won't propose an
	// index update if one is already going on. If that's not the case, then the receiver
	// of the update had probably finished the previous index update but some follower
	// (or perhaps leader) had not finished it.
	// In other words, the proposer checks whether there is another indexing in progress.
	// If that's the case, the alter request is rejected. Otherwise, the request is accepted.
	// Before reaching here, the proposer P would have checked that no indexing is in progress
	// (could also be because proposer was done earlier than others). If P was still indexing
	// when the req was received, it would have rejected the Alter request. Only if P is
	// not indexing, it would accept and propose the request.
	// It is possible that a receiver R of the proposal is still indexing. In that case, R would
	// block here and wait for indexing to be finished.
	gr.Node.waitForTask(opIndexing)

	// done is used to ensure that we only stop the indexing task once.
	var done uint32
	start := time.Now()
	stopIndexing := func(closer *z.Closer) {
		// runSchemaMutation can return. stopIndexing could be called by goroutines.
		if !schema.State().IndexingInProgress() {
			if atomic.CompareAndSwapUint32(&done, 0, 1) {
				closer.Done()
				// Time check is here so that we do not propose snapshot too frequently.
				if time.Since(start) < 10*time.Second || !gr.Node.AmLeader() {
					return
				}
				if err := gr.Node.proposeSnapshot(); err != nil {
					glog.Errorf("error in proposing snapshot: %v", err)
				}
			}
		}
	}

	buildIndexesHelper := func(update *pb.SchemaUpdate, rebuild posting.IndexRebuild) error {
		wrtCtx := schema.GetWriteContext(context.Background())
		if err := rebuild.BuildIndexes(wrtCtx); err != nil {
			return err
		}
		if err := updateSchema(update, rebuild.StartTs); err != nil {
			return err
		}

		glog.Infof("Done schema update %+v\n", update)
		return nil
	}

	// This wg allows waiting until setup for all the predicates is complete
	// before running buildIndexes for any of those predicates.
	var wg sync.WaitGroup
	wg.Add(1)
	defer wg.Done()
	// This throttle allows is used to limit the number of files which are opened simultaneously
	// by badger while building indexes for predicates in background.
	maxOpenFileLimit, err := x.QueryMaxOpenFiles()
	if err != nil {
		// Setting to default value on unix systems
		maxOpenFileLimit = 1024
	}
	glog.Infof("Max open files limit: %d", maxOpenFileLimit)
	// Badger opens around 8 files for indexing per predicate.
	// The throttle limit is set to maxOpenFileLimit/8 to ensure that indexing does not throw
	// "Too many open files" error.
	throttle := y.NewThrottle(maxOpenFileLimit / 8)

	buildIndexes := func(update *pb.SchemaUpdate, rebuild posting.IndexRebuild, c *z.Closer) {
		// In case background indexing is running, we should call it here again.
		defer stopIndexing(c)

		// We should only start building indexes once this function has returned.
		// This is in order to ensure that we do not call DropPrefix for one predicate
		// and write indexes for another predicate simultaneously. because that could
		// cause writes to badger to fail leading to undesired indexing failures.
		wg.Wait()

		x.Check(throttle.Do())
		// undo schema changes in case re-indexing fails.
		if err := buildIndexesHelper(update, rebuild); err != nil {
			glog.Errorf("error in building indexes, aborting :: %v\n", err)
			undoSchemaUpdate(update.Predicate)
		}
		throttle.Done(nil)
	}

	var closer *z.Closer
	for _, su := range updates {
		if tablet, err := groups().Tablet(su.Predicate); err != nil {
			return err
		} else if tablet.GetGroupId() != groups().groupId() {
			return errors.Errorf("Tablet isn't being served by this group. Tablet: %+v", tablet)
		}

		if err := checkSchema(su); err != nil {
			return err
		}

		old, ok := schema.State().Get(ctx, su.Predicate)
		rebuild := posting.IndexRebuild{
			Attr:          su.Predicate,
			StartTs:       startTs,
			OldSchema:     &old,
			CurrentSchema: su,
		}
		shouldRebuild := ok && rebuild.NeedIndexRebuild()

		// Start opIndexing task only if schema update needs to build the indexes.
		if shouldRebuild && !gr.Node.isRunningTask(opIndexing) {
			closer, err = gr.Node.startTaskAtTs(opIndexing, startTs)
			if err != nil {
				return err
			}
			defer stopIndexing(closer)
		}

		querySchema := rebuild.GetQuerySchema()
		// Sets the schema only in memory. The schema is written to
		// disk only after schema mutations are successful.
		schema.State().Set(su.Predicate, querySchema)
		schema.State().SetMutSchema(su.Predicate, su)

		// TODO(Aman): If we return an error, we may not have right schema reflected.
		setup := func() error {
			if !ok {
				return nil
			}
			if err := rebuild.DropIndexes(ctx); err != nil {
				return err
			}
			return rebuild.BuildData(ctx)
		}
		if err := setup(); err != nil {
			glog.Errorf("error in building indexes, aborting :: %v\n", err)
			undoSchemaUpdate(su.Predicate)
			return err
		}

		if shouldRebuild {
			go buildIndexes(su, rebuild, closer)
		} else if err := updateSchema(su, rebuild.StartTs); err != nil {
			return err
		}
	}

	return nil
}

// updateSchema commits the schema to disk in blocking way, should be ok because this happens
// only during schema mutations or we see a new predicate.
func updateSchema(s *pb.SchemaUpdate, ts uint64) error {
	schema.State().Set(s.Predicate, s)
	schema.State().DeleteMutSchema(s.Predicate)
	txn := pstore.NewTransactionAt(ts, true)
	defer txn.Discard()
	data, err := s.Marshal()
	x.Check(err)
	e := &badger.Entry{
		Key:      x.SchemaKey(s.Predicate),
		Value:    data,
		UserMeta: posting.BitSchemaPosting,
	}
	if err = txn.SetEntry(e.WithDiscard()); err != nil {
		return err
	}
	return txn.CommitAt(ts, nil)
}

func createSchema(attr string, typ types.TypeID, ts uint64) error {
	ctx := schema.GetWriteContext(context.Background())

	// Don't overwrite schema blindly, acl's might have been set even though
	// type is not present
	s, ok := schema.State().Get(ctx, attr)
	if ok {
		s.ValueType = typ.Int()
	} else {
		s = pb.SchemaUpdate{ValueType: typ.Int(), Predicate: attr}
		// For type TypeUid, set List to true. This is done because previously
		// all predicates of type TypeUid were implicitly considered lists.
		if typ == types.TypeUid {
			s.List = true
		}
	}
	if err := checkSchema(&s); err != nil {
		return err
	}
	return updateSchema(&s, ts)
}

func hasEdges(attr string, startTs uint64) bool {
	pk := x.ParsedKey{Attr: attr}
	iterOpt := badger.DefaultIteratorOptions
	iterOpt.PrefetchValues = false
	iterOpt.Prefix = pk.DataPrefix()

	txn := pstore.NewTransactionAt(startTs, false)
	defer txn.Discard()

	it := txn.NewIterator(iterOpt)
	defer it.Close()

	for it.Rewind(); it.Valid(); it.Next() {
		// NOTE: This is NOT correct.
		// An incorrect, but efficient way to quickly check if we have at least one non-empty
		// posting. This does NOT consider those posting lists which can have multiple deltas
		// summing up to an empty posting list. I'm leaving it as it is for now. But, this could
		// cause issues because of this inaccuracy.
		if it.Item().UserMeta()&posting.BitEmptyPosting == 0 {
			return true
		}
	}
	return false
}
func checkSchema(s *pb.SchemaUpdate) error {
	if s == nil {
		return errors.Errorf("Nil schema")
	}

	if x.ParseAttr(s.Predicate) == "" {
		return errors.Errorf("No predicate specified in schema mutation")
	}

	if x.IsInternalPredicate(s.Predicate) {
		return errors.Errorf("Cannot create user-defined predicate with internal name %s",
			x.ParseAttr(s.Predicate))
	}

	if s.Directive == pb.SchemaUpdate_INDEX && len(s.Tokenizer) == 0 {
		return errors.Errorf("Tokenizer must be specified while indexing a predicate: %+v", s)
	}

	if len(s.Tokenizer) > 0 && s.Directive != pb.SchemaUpdate_INDEX {
		return errors.Errorf("Directive must be SchemaUpdate_INDEX when a tokenizer is specified")
	}

	typ := types.TypeID(s.ValueType)
	if typ == types.TypeUid && s.Directive == pb.SchemaUpdate_INDEX {
		// index on uid type
		return errors.Errorf("Index not allowed on predicate of type uid on predicate %s",
			x.ParseAttr(s.Predicate))
	}

	// If schema update has upsert directive, it should have index directive.
	if s.Upsert && len(s.Tokenizer) == 0 {
		return errors.Errorf("Index tokenizer is mandatory for: [%s] when specifying @upsert directive",
			x.ParseAttr(s.Predicate))
	}

	t, err := schema.State().TypeOf(s.Predicate)
	if err != nil {
		// No schema previously defined, so no need to do checks about schema conversions.
		return nil
	}

	// schema was defined already
	switch {
	case t.IsScalar() && (t == types.TypePassword || s.ValueType == types.TypePassword.Int()):
		// can't change password -> x, x -> password
		if t.Int() != s.ValueType {
			return errors.Errorf("Schema change not allowed from %s to %s", t, typ)
		}

	case t.IsScalar() == typ.IsScalar():
		// If old type was list and new type is non-list, we don't allow it until user
		// has data.
		if schema.State().IsList(s.Predicate) && !s.List && hasEdges(s.Predicate, math.MaxUint64) {
			return errors.Errorf("Schema change not allowed from [%s] => %s without"+
				" deleting pred: %s", t, typ, x.ParseAttr(s.Predicate))
		}

	default:
		// uid => scalar or scalar => uid. Check that there shouldn't be any data.
		if hasEdges(s.Predicate, math.MaxUint64) {
			return errors.Errorf("Schema change not allowed from scalar to uid or vice versa"+
				" while there is data for pred: %s", x.ParseAttr(s.Predicate))
		}
	}
	return nil
}

// ValidateAndConvert checks compatibility or converts to the schema type if the storage type is
// specified. If no storage type is specified then it converts to the schema type.
func ValidateAndConvert(edge *pb.Edge, su *pb.SchemaUpdate) error {
	if isDeletePredicate(edge) {
		return nil
	}
	if edge.Op == pb.Edge_DEL && x.IsStarAll(edge.ObjectValue) {
		return nil
	}

	storageType := posting.TypeID(edge)
	schemaType := types.TypeID(su.ValueType)

	// type checks
	switch {
	case !schemaType.IsScalar() && !storageType.IsScalar():
		return nil

	case !schemaType.IsScalar() && storageType.IsScalar():
		return errors.Errorf("Schema type: %q and Storage type: %q"+
			" don't match for pred: %q edge: %v",
			schemaType, storageType, x.ParseAttr(edge.Predicate), edge)

	case schemaType.IsScalar() && !storageType.IsScalar():
		return errors.Errorf("Schema type: %q and Storage type: %q"+
			" don't match for pred: %q edge: %v",
			schemaType, storageType, x.ParseAttr(edge.Predicate), edge)

	// The suggested storage type matches the schema, OK!
	case storageType == schemaType && schemaType != types.TypeDefault:
		return nil

	// We accept the storage type iff we don't have a schema type and a storage type is specified.
	case schemaType == types.TypeDefault:
		schemaType = storageType
	}

	var (
		dst types.Val
		err error
	)

	src := types.Sval(edge.ObjectValue)
	// check compatibility of schema type and storage type
	if dst, err = types.Convert(src, schemaType); err != nil {
		return err
	}

	edge.ObjectValue, err = dst.Marshal()
	if err != nil {
		return err
	}

	if x.WorkerConfig.AclEnabled && x.ParseAttr(edge.GetPredicate()) == "dgraph.rule.permission" {
		perm, ok := dst.Value.(int64)
		if !ok {
			return errors.Errorf("Value for predicate <dgraph.rule.permission>" +
				" should be of type int")
		}
		if perm < 0 || perm > 7 {
			return errors.Errorf("Can't set <dgraph.rule.permission> to %d, Value for this"+
				" predicate should be between 0 and 7", perm)
		}
	}
	return nil
}

// proposeOrSend either proposes the mutation if the node serves the group gid or sends it to
// the leader of the group gid for proposing.
func proposeOrSend(ctx context.Context, gid uint32, m *pb.Mutations, chr chan res) {
	res := res{}
	if groups().ServesGroup(gid) {
		res.ctx = &pb.TxnContext{}
		res.err = (&grpcWorker{}).proposeAndWait(ctx, res.ctx, m)
		chr <- res
		return
	}

	pl := groups().Leader(gid)
	if pl == nil {
		res.err = conn.ErrNoConnection
		chr <- res
		return
	}

	var tc *pb.TxnContext
	c := pb.NewWorkerClient(pl.Get())

	ch := make(chan error, 1)
	go func() {
		var err error
		tc, err = c.Mutate(ctx, m)
		ch <- err
	}()

	select {
	case <-ctx.Done():
		res.err = ctx.Err()
		res.ctx = nil
	case err := <-ch:
		res.err = err
		res.ctx = tc
	}
	chr <- res
}

// populateMutationMap populates a map from group id to the mutation that
// should be sent to that group.
func populateMutationMap(src *pb.Mutations) (map[uint32]*pb.Mutations, error) {
	mm := make(map[uint32]*pb.Mutations)
	for _, nq := range src.Edges {
		gid, err := groups().BelongsTo(nq.Predicate)
		if err != nil {
			return nil, err
		}

		mu := mm[gid]
		if mu == nil {
			mu = &pb.Mutations{GroupId: gid}
			mm[gid] = mu
		}
		mu.Edges = append(mu.Edges, nq)
	}
	for _, obj := range src.NewObjects {
		if len(obj.Edges) == 0 {
			continue
		}
		// All edges for a type should belong to the same group. So, we can just
		// look at any one of them, and find the group it belongs to.
		edge := obj.Edges[0]
		gid, err := groups().BelongsTo(edge.Predicate)
		if err != nil {
			return nil, err
		}
		mu := mm[gid]
		if mu == nil {
			mu = &pb.Mutations{GroupId: gid}
			mm[gid] = mu
		}
		mu.NewObjects = append(mu.NewObjects, obj)
	}
	for _, schema := range src.Schema {
		gid, err := groups().BelongsTo(schema.Predicate)
		if err != nil {
			return nil, err
		}

		mu := mm[gid]
		if mu == nil {
			mu = &pb.Mutations{GroupId: gid}
			mm[gid] = mu
		}
		mu.Schema = append(mu.Schema, schema)
	}

	if src.DropOp > 0 {
		for _, gid := range KnownGroups() {
			mu := mm[gid]
			if mu == nil {
				mu = &pb.Mutations{GroupId: gid}
				mm[gid] = mu
			}
			mu.DropOp = src.DropOp
			mu.DropValue = src.DropValue
		}
	}

	return mm, nil
}

type res struct {
	err error
	ctx *pb.TxnContext
}

// MutateOverNetwork checks which group should be running the mutations
// according to the group config and sends it to that instance.
func MutateOverNetwork(ctx context.Context, m *pb.Mutations) (*pb.TxnContext, error) {
	ctx, span := otrace.StartSpan(ctx, "worker.MutateOverNetwork")
	defer span.End()

	mutationMap, err := populateMutationMap(m)
	if err != nil {
		return nil, err
	}
	span.Annotate(nil, "mutation map populated")

	resCh := make(chan res, len(mutationMap))
	for gid, mu := range mutationMap {
		if gid == 0 {
			span.Annotatef(nil, "Group id zero for mutation: %+v", mu)
			return nil, errNonExistentTablet
		}
		go proposeOrSend(ctx, gid, mu, resCh)
	}

	// Wait for all the goroutines to reply back.
	// We return if an error was returned or the parent called ctx.Done()
	var tctx *pb.TxnContext
	var e error
	for i := 0; i < len(mutationMap); i++ {
		res := <-resCh
		if res.err != nil {
			e = res.err
		} else {
			if tctx == nil {
				tctx = res.ctx
			} else {
				for k, v := range res.ctx.GetUids() {
					tctx.Uids[k] = v
				}
			}
		}
	}
	close(resCh)
	return tctx, e
}

func (w *grpcWorker) proposeAndWait(ctx context.Context, txnCtx *pb.TxnContext,
	m *pb.Mutations) error {
	if x.WorkerConfig.StrictMutations {
		for _, edge := range m.Edges {
			if _, err := schema.State().TypeOf(edge.Predicate); err != nil {
				return err
			}
		}
	}

	// We used to WaitForTs(ctx, m.StartTs) here. But, with concurrent mutation execution, we can do
	// the re-arranging of mutations post Raft proposals to ensure that they get run after server's
	// MaxAssignedTs >= m.StartTs.
	node := groups().Node
	data, err := node.proposeAndWait(ctx, &pb.Proposal{Mutations: m})
	if txn, ok := data.(*posting.Txn); ok {
		txn.FillContext(txnCtx)
	}
	return err
}

// Mutate is used to apply mutations over the network on other instances.
func (w *grpcWorker) Mutate(ctx context.Context, m *pb.Mutations) (*pb.TxnContext, error) {
	ctx, span := otrace.StartSpan(ctx, "worker.Mutate")
	defer span.End()

	txnCtx := &pb.TxnContext{}
	if ctx.Err() != nil {
		return txnCtx, ctx.Err()
	}
	if !groups().ServesGroup(m.GroupId) {
		return txnCtx, errors.Errorf("This server doesn't serve group id: %v", m.GroupId)
	}

	return txnCtx, w.proposeAndWait(ctx, txnCtx, m)
}
