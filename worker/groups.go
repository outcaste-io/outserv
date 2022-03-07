// Portions Copyright 2016-2018 Dgraph Labs, Inc. are available under the Apache 2.0 license.
// Portions Copyright 2022 Outcaste, Inc. are available under the Smart License.

package worker

import (
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/glog"
	"github.com/golang/protobuf/proto"
	badgerpb "github.com/outcaste-io/badger/v3/pb"
	"github.com/outcaste-io/outserv/conn"
	"github.com/outcaste-io/outserv/protos/pb"
	"github.com/outcaste-io/outserv/raftwal"
	"github.com/outcaste-io/outserv/schema"
	"github.com/outcaste-io/outserv/x"
	"github.com/outcaste-io/outserv/zero"
	"github.com/outcaste-io/ristretto/z"
	"github.com/pkg/errors"
	"golang.org/x/net/trace"
)

type groupi struct {
	x.SafeMutex
	Node         *node
	gid          uint32
	triggerCh    chan struct{} // Used to trigger membership sync
	blockDeletes *sync.Mutex   // Ensure that deletion won't happen when move is going on.
	closer       *z.Closer

	// Group checksum is used to determine if the tablets served by the groups have changed from
	// the membership information that the Alpha has. If so, Alpha cannot service a read.
	deltaChecksum      uint64 // Checksum received by OracleDelta.
	membershipChecksum uint64 // Checksum received by MembershipState.
}

var gr = &groupi{
	blockDeletes: new(sync.Mutex),
	closer:       z.NewCloser(2), // Match CLOSER:1 in this file.
}

func groups() *groupi {
	return gr
}

func newNode(store *raftwal.DiskStorage, gid uint32, id uint64, myAddr string) *node {
	glog.Infof("Node ID: %#x with GroupID: %d\n", id, gid)
	isLearner := x.WorkerConfig.Raft.GetBool("learner")
	rc := &pb.RaftContext{
		WhoIs:     "alpha",
		Addr:      myAddr,
		Group:     gid,
		Id:        id,
		IsLearner: isLearner,
	}
	glog.Infof("RaftContext: %+v\n", rc)
	m := conn.NewNode(rc, store, x.WorkerConfig.TLSClientConfig)

	n := &node{
		Node: m,
		ctx:  context.Background(),
		gid:  gid,
		// We need a generous size for applyCh, because raft.Tick happens every
		// 10ms. If we restrict the size here, then Raft goes into a loop trying
		// to maintain quorum health.
		applyCh:      make(chan []*pb.Proposal, 1000),
		concApplyCh:  make(chan *pb.Proposal, 100),
		drainApplyCh: make(chan struct{}),
		elog:         trace.NewEventLog("Dgraph", "ApplyCh"),
		closer:       z.NewCloser(4), // Matches CLOSER:1
		ops:          make(map[op]operation),
		cdcTracker:   newCDC(),
		keysWritten:  newKeysWritten(),
	}
	return n
}

// StartRaftNodes will read the WAL dir, create the RAFT groups,
// and either start or restart RAFT nodes.
// This function triggers RAFT nodes to be created, and is the entrance to the RAFT
// world from main.go.
func StartRaftNodes(walStore *raftwal.DiskStorage, bindall bool) {
	raftIdx := x.WorkerConfig.Raft.GetUint64("idx")
	groupIdx := x.WorkerConfig.Raft.GetUint32("group")
	x.AssertTruef(raftIdx > 0 && groupIdx > 0, "Raft ID and Group ID must be provided")
	glog.Infof("Current Raft Id: %#x Group ID: %#x\n", raftIdx, groupIdx)
	atomic.StoreUint32(&gr.gid, groupIdx)

	// Successfully connect with dgraphzero, before doing anything else.
	// Connect with Zero leader and figure out what group we should belong to.
	m := &pb.Member{
		Id:      raftIdx,
		GroupId: x.WorkerConfig.Raft.GetUint32("group"),
		Addr:    x.WorkerConfig.MyAddr,
		Learner: x.WorkerConfig.Raft.GetBool("learner"),
	}
	if m.GroupId > 0 {
		// TODO: Remove the concept of forced group id.
		m.ForceGroupId = true
	}
	glog.Infof("Sending member request to Zero: %+v\n", m)

	// TODO: The following should directly interact with the zero package.

	var connState *pb.MembershipState
	var err error
	for i := 0; i < 120; i++ {
		connState, err = zero.LatestMembershipState(gr.Ctx())
		if err == nil {
			break
		}
		time.Sleep(time.Second)
	}
	x.Checkf(err, "Unable to get latest membership state")
	if connState == nil {
		x.Fatalf("Unable to join cluster via dgraphzero")
	}

	gid := gr.groupId()
	gr.triggerCh = make(chan struct{}, 1)

	// Initialize DiskStorage and pass it along.
	walStore.SetUint(raftwal.RaftId, raftIdx)
	walStore.SetUint(raftwal.GroupId, uint64(gid))

	gr.Node = newNode(walStore, gid, raftIdx, x.WorkerConfig.MyAddr)

	// This timestamp would be used for reading during snapshot after bulk load.
	// The stream is async, we need this information before we start or else replica might
	// not get any data.
	gr.applyState(connState)

	x.Checkf(schema.LoadFromDb(), "Error while initializing schema")
	glog.Infof("Load schema from DB: OK")

	conn.UpdateNode("alpha", gr.Node.Node)

	gr.Node.InitAndStartNode()
	glog.Infof("Init and start Raft node: OK")

	go gr.sendMembershipUpdates()
	go gr.receiveMembershipUpdates()

	gr.informZeroAboutTablets()

	glog.Infof("Informed Zero about tablets I have: OK")
	gr.applyInitialSchema()
	gr.applyInitialTypes()
	glog.Infof("Upserted Schema and Types: OK")

	x.UpdateHealthStatus(true)
	glog.Infof("Server is ready: OK")
}

func (g *groupi) Ctx() context.Context {
	return g.closer.Ctx()
}

func (g *groupi) IsClosed() bool {
	return g.closer.Ctx().Err() != nil
}

func (g *groupi) informZeroAboutTablets() {
	// Before we start this Alpha, let's pick up all the predicates we have in our postings
	// directory, and ask Zero if we are allowed to serve it. Do this irrespective of whether
	// this node is the leader or the follower, because this early on, we might not have
	// figured that out.
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for range ticker.C {
		preds := schema.State().Predicates()
		glog.Infof("----> Going to inform about preds: %+v\n", preds)
		if _, err := g.Inform(preds); err != nil {
			glog.Errorf("Error while getting tablet for preds %v", err)
		} else {
			glog.V(1).Infof("Done informing Zero about the %d tablets I have", len(preds))
			return
		}
	}
}

func (g *groupi) applyInitialTypes() {
	initialTypes := schema.InitialTypes(x.GalaxyNamespace)
	for _, t := range initialTypes {
		if _, ok := schema.State().GetType(t.TypeName); ok {
			continue
		}
		// It is okay to write initial types at ts=1.
		if err := updateType(t.GetTypeName(), *t, 1); err != nil {
			glog.Errorf("Error while applying initial type: %s", err)
		}
	}
}

func (g *groupi) applyInitialSchema() {
	if g.groupId() != 1 {
		return
	}

	if _, err := zero.LatestMembershipState(g.Ctx()); err != nil {
		glog.Fatalf("While getting latest membership state: %v\n", err)
	}
	initialSchema := schema.InitialSchema(x.GalaxyNamespace)
	ctx := g.Ctx()

	apply := func(s *pb.SchemaUpdate) {
		// There are 2 cases: either the alpha is fresh or it restarted. If it is fresh cluster
		// then we can write the schema at ts=1. If alpha restarted, then we will already have the
		// schema at higher version and this operation will be a no-op.
		if err := applySchema(s, 1); err != nil {
			glog.Errorf("Error while applying initial schema: %s", err)
		}
	}

	for _, s := range initialSchema {
		if gid, err := g.BelongsToReadOnly(s.Predicate, 0); err != nil {
			glog.Errorf("Error getting tablet for predicate %s. Will force schema proposal.",
				s.Predicate)
			apply(s)
		} else if gid == 0 {
			// The tablet is not being served currently.
			apply(s)
		} else if curr, _ := schema.State().Get(ctx, s.Predicate); gid == g.groupId() &&
			!proto.Equal(s, &curr) {
			// If this tablet is served to the group, do not upsert the schema unless the
			// stored schema and the proposed one are different.
			apply(s)
		} else {
			// The schema for this predicate has already been proposed.
			glog.V(1).Infof("Schema found for predicate %s: %+v", s.Predicate, curr)
			continue
		}
	}
}

func applySchema(s *pb.SchemaUpdate, ts uint64) error {
	if err := updateSchema(s, ts); err != nil {
		return err
	}
	if servesTablet, err := groups().ServesTablet(s.Predicate); err != nil {
		return err
	} else if !servesTablet {
		return errors.Errorf("group 1 should always serve reserved predicate %s", s.Predicate)
	}
	return nil
}

// No locks are acquired while accessing this function.
// Don't acquire RW lock during this, otherwise we might deadlock.
func (g *groupi) groupId() uint32 {
	return atomic.LoadUint32(&g.gid)
}

// TODO: Remove this func.
// MaxLeaseId returns the maximum UID that has been leased.
func MaxLeaseId() uint64 {
	st := zero.MembershipState()
	return st.MaxUID
}

// // UpdateMembershipState contacts zero for an update on membership state.
// func UpdateMembershipState(ctx context.Context) error {
// 	st, err := zero.LatestMembershipState(gr.Ctx())
// 	if err != nil {
// 		return errors.Wrapf(err, "while retrieving membership")
// 	}
// 	gr.applyState(st)
// 	return nil
// }

func (g *groupi) applyState(state *pb.MembershipState) {
	myId := g.Node.Id
	x.AssertTrue(state != nil)
	g.Lock()
	defer g.Unlock()

	glog.Infof("Got membership state: %+v\n", state)
	if _, has := state.Members[myId]; !has {
		// I'm not part of this cluster. I should crash myself.
		glog.Fatalf("Unable to find myself [id:%d group:%d] in membership state: %+v. Goodbye!",
			myId, g.groupId(), state)
	}

	// Sometimes this can cause us to lose latest tablet info, but that shouldn't cause any issues.
	for _, member := range state.Members {
		conn.GetPools().Connect(member.Addr, x.WorkerConfig.TLSClientConfig)
		// if x.WorkerConfig.MyAddr != member.Addr {
		// 	conn.GetPools().Connect(member.Addr, x.WorkerConfig.TLSClientConfig)
		// }
	}

	// While restarting we fill Node information after retrieving initial state.
	if g.Node != nil {
		// Lets have this block before the one that adds the new members, else we may end up
		// removing a freshly added node.

		for _, removedId := range state.GetRemoved() {
			if !g.Node.AmLeader() {
				break
			}
			// Don't try to remove a member if it's already marked as removed in
			// the membership state and is not a current peer of the node.
			_, isPeer := g.Node.Peer(removedId)
			// isPeer should only be true if the removed node is not the same as this node.
			isPeer = isPeer && removedId != g.Node.RaftContext.Id
			if !isPeer {
				glog.Infof("%#x is not my peer. So, not removing.\n", removedId)
				continue
			}

			go func(rid uint64) {
				// Note: Not sure why we need to check here again.
				// for _, oldMember := range oldState.GetRemoved() {
				// 	if oldMember.GetId() == member.GetId() && !isPeer {
				// 		return
				// 	}
				// }
				glog.Infof("Proposing removal of peer %#x\n", rid)
				if err := g.Node.ProposePeerRemoval(g.Ctx(), rid); err != nil {
					glog.Errorf("Error while proposing node removal: %+v", err)
				}
			}(removedId)
		}
		conn.GetPools().RemoveInvalid(state)
	}
}

func (g *groupi) ServesGroup(gid uint32) bool {
	return g.groupId() == gid
}

func (g *groupi) ChecksumsMatch(ctx context.Context) error {
	return nil
	// TODO: Fix this up.

	if atomic.LoadUint64(&g.deltaChecksum) == atomic.LoadUint64(&g.membershipChecksum) {
		return nil
	}
	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			if atomic.LoadUint64(&g.deltaChecksum) == atomic.LoadUint64(&g.membershipChecksum) {
				return nil
			}
		case <-ctx.Done():
			return errors.Errorf("Group checksum mismatch for id: %d", g.groupId())
		}
	}
}

func (g *groupi) BelongsTo(key string) (uint32, error) {
	if tablet, err := g.Tablet(key); err != nil {
		return 0, err
	} else if tablet != nil {
		return tablet.GroupId, nil
	}
	return 0, nil
}

// BelongsToReadOnly acts like BelongsTo except it does not ask zero to serve
// the tablet for key if no group is currently serving it.
// The ts passed should be the start ts of the query, so this method can compare that against a
// tablet move timestamp. If the tablet was moved to this group after the start ts of the query, we
// should reject that query.
func (g *groupi) BelongsToReadOnly(key string, ts uint64) (uint32, error) {
	st := zero.MembershipState()
	tablet := st.Tablets[key]
	if tablet == nil {
		return 0, nil
	}
	if ts > 0 && ts < tablet.MoveTs {
		return 0, errors.Errorf("StartTs: %d is from before MoveTs: %d for pred: %q",
			ts, tablet.MoveTs, key)
	}
	return tablet.GetGroupId(), nil
}

func (g *groupi) ServesTablet(key string) (bool, error) {
	if tablet, err := g.Tablet(key); err != nil {
		return false, err
	} else if tablet != nil && tablet.GroupId == groups().groupId() {
		return true, nil
	}
	return false, nil
}

func (g *groupi) sendTablet(tablet *pb.Tablet) (*pb.Tablet, error) {
	return nil, nil
	// TODO: Fix up below.

	// pl := g.connToZeroLeader()
	// zc := pb.NewZeroClient(pl.Get())

	// out, err := zc.ShouldServe(g.Ctx(), tablet)
	// if err != nil {
	// 	glog.Errorf("Error while ShouldServe grpc call %v", err)
	// 	return nil, err
	// }

	// // Do not store tablets with group ID 0, as they are just dummy tablets for
	// // predicates that do no exist.
	// if out.GroupId > 0 {
	// 	g.Lock()
	// 	g.tablets[out.GetPredicate()] = out
	// 	g.Unlock()
	// }

	// if out.GroupId == groups().groupId() {
	// 	glog.Infof("Serving tablet for: %v\n", tablet.GetPredicate())
	// }
	// return out, nil
}

func (g *groupi) Inform(preds []string) ([]*pb.Tablet, error) {
	// Don't need to use its result. Just ensure that Zero's information is up
	// to date.
	st, err := zero.LatestMembershipState(g.Ctx())
	if err != nil {
		return nil, errors.Wrapf(err, "while getting latest membership state")
	}

	unknownPreds := make([]*pb.Tablet, 0)
	g.RLock()
	for _, p := range preds {
		if len(p) == 0 {
			continue
		}

		if _, ok := st.Tablets[p]; !ok {
			unknownPreds = append(unknownPreds, &pb.Tablet{GroupId: g.groupId(), Predicate: p})
		}
	}
	g.RUnlock()

	if len(unknownPreds) == 0 {
		return nil, nil
	}

	prop := &pb.ZeroProposal{
		Tablets: unknownPreds,
	}
	glog.Infof("Sending proposal: %+v\n", prop)
	if _, err := zero.ProposeAndWait(g.Ctx(), prop); err != nil {
		return nil, errors.Wrapf(err, "Unable to propose: %+v\n", prop)
	}

	st, err = zero.LatestMembershipState(g.Ctx())
	if err != nil {
		return nil, errors.Wrapf(err, "while getting latest membership state")
	}
	var res []*pb.Tablet
	for _, t := range st.Tablets {
		if t.GroupId == g.groupId() {
			glog.Infof("Serving tablet for: %v\n", t.GetPredicate())
			res = append(res, t)
		}
	}
	return res, nil
}

// Do not modify the returned Tablet
func (g *groupi) Tablet(key string) (*pb.Tablet, error) {
	// TODO: Remove all this later, create a membership state and apply it
	st := zero.MembershipState()
	tablet, ok := st.Tablets[key]
	if ok {
		return tablet, nil
	}

	// We don't know about this tablet.
	// Check with Zero if we can serve it.
	tablets, err := g.Inform([]string{key})
	if err != nil {
		return nil, errors.Wrapf(err, "while informing Zero")
	}
	return tablets[0], nil
}

func (g *groupi) ForceTablet(key string) (*pb.Tablet, error) {
	return g.sendTablet(&pb.Tablet{GroupId: g.groupId(), Predicate: key, Force: true})
}

func (g *groupi) HasMeInState() bool {
	st := zero.MembershipState()
	_, has := st.Members[g.Node.Id]
	return has
}

// Returns 0, 1, or 2 valid server addrs.
func (g *groupi) AnyTwoServers(gid uint32) []string {
	st := zero.MembershipState()
	var res []string
	for _, m := range st.Members {
		if m.GroupId != gid {
			continue
		}
		// map iteration gives us members in no particular order.
		res = append(res, m.Addr)
		if len(res) >= 2 {
			break
		}
	}
	return res
}

func (g *groupi) members(gid uint32) map[uint64]*pb.Member {
	st := zero.MembershipState()
	res := make(map[uint64]*pb.Member)
	for _, m := range st.Members {
		if m.GroupId != gid {
			continue
		}
		// map iteration gives us members in no particular order.
		res[m.Id] = m
	}
	return res
}

func (g *groupi) AnyServer(gid uint32) *conn.Pool {
	members := g.members(gid)
	for _, m := range members {
		pl, err := conn.GetPools().Get(m.Addr)
		if err == nil {
			return pl
		}
	}
	return nil
}

func (g *groupi) MyPeer() (uint64, bool) {
	members := g.members(g.groupId())
	for _, m := range members {
		if m.Id != g.Node.Id {
			return m.Id, true
		}
	}
	return 0, false
}

// Leader will try to return the leader of a given group, based on membership information.
// There is currently no guarantee that the returned server is the leader of the group.
func (g *groupi) Leader(gid uint32) *conn.Pool {
	members := g.members(gid)
	if members == nil {
		return nil
	}
	for _, m := range members {
		if m.Leader {
			if pl, err := conn.GetPools().Get(m.Addr); err == nil {
				return pl
			}
		}
	}
	return nil
}

func (g *groupi) KnownGroups() (gids []uint32) {
	st := zero.MembershipState()
	for gid := range st.Leaders {
		gids = append(gids, gid)
	}
	return
}

// KnownGroups returns the known groups using the global groupi instance.
func KnownGroups() []uint32 {
	return groups().KnownGroups()
}

// GroupId returns the group to which this worker belongs to.
func GroupId() uint32 {
	return groups().groupId()
}

// NodeId returns the raft id of the node.
func NodeId() uint64 {
	return groups().Node.Id
}

func (g *groupi) triggerMembershipSync() {
	// It's ok if we miss the trigger, periodic membership sync runs every minute.
	select {
	case g.triggerCh <- struct{}{}:
	// It's ok to ignore it, since we would be sending update of a later state
	default:
	}
}

func (g *groupi) doSendMembership(tablets map[string]*pb.Tablet) error {
	leader := g.Node.AmLeader()
	src, err := zero.LatestMembershipState(g.Ctx())
	if err != nil {
		return errors.Wrapf(err, "while getting latest membership state")
	}
	srcMember, ok := src.Members[g.Node.Id]
	if !ok {
		return errors.Wrapf(err, "Unknown member: %#x", g.Node.Id)
	}
	member := &pb.Member{
		Id:         g.Node.Id,
		GroupId:    g.groupId(),
		Addr:       x.WorkerConfig.MyAddr,
		Leader:     leader,
		LastUpdate: uint64(time.Now().Unix()),
	}
	if srcMember.Addr != member.Addr ||
		srcMember.Leader != member.Leader ||
		srcMember.GroupId != member.GroupId {

		prop := &pb.ZeroProposal{
			Member: member,
		}
		if _, err := zero.ProposeAndWait(g.Ctx(), prop); err != nil {
			return errors.Wrapf(err, "Proposal %+v failed", prop)
		}
		glog.Infof("Proposal applied: %+v\n", prop)
	}

	// Only send tablet info, if I'm the leader.
	if !leader {
		return nil
	}

	var changes []*pb.Tablet
	for key, dstTablet := range tablets {
		srcTablet, has := src.Tablets[key]
		if !has {
			continue
		}
		s := float64(srcTablet.OnDiskBytes)
		d := float64(dstTablet.OnDiskBytes)
		if dstTablet.Remove || (s == 0 && d > 0) || (s > 0 && math.Abs(d/s-1) > 0.1) {
			dstTablet.Force = false
			changes = append(changes, dstTablet)
		}
	}
	if len(changes) == 0 {
		return nil
	}
	prop := &pb.ZeroProposal{
		Tablets: changes,
	}
	if _, err := zero.ProposeAndWait(g.Ctx(), prop); err != nil {
		return errors.Wrapf(err, "Proposal %+v failed", prop)
	}
	glog.Infof("Proposal applied: %+v\n", prop)

	// TODO: Look into snapshot timestamps.
	// if snap, err := g.Node.Snapshot(); err == nil {
	// 	group.SnapshotTs = snap.BaseTs
	// }
	// group.CheckpointTs = atomic.LoadUint64(&g.Node.checkpointTs)

	// zero.ProposeAndWait(g.Ctx(),

	return nil
	// TODO: Fix up below.

	// pl := g.connToZeroLeader()
	// if pl == nil {
	// 	return errNoConnection
	// }
	// c := pb.NewZeroClient(pl.Get())
	// ctx, cancel := context.WithTimeout(g.Ctx(), 10*time.Second)
	// defer cancel()
	// reply, err := c.UpdateMembership(ctx, group)
	// if err != nil {
	// 	return err
	// }
	// if string(reply.GetData()) == "OK" {
	// 	return nil
	// }
	// return errors.Errorf(string(reply.GetData()))
}

// sendMembershipUpdates sends the membership update to Zero leader. If this Alpha is the leader, it
// would also calculate the tablet sizes and send them to Zero.
func (g *groupi) sendMembershipUpdates() {
	defer func() {
		glog.Infoln("Closing sendMembershipUpdates")
		g.closer.Done() // CLOSER:1
	}()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	consumeTriggers := func() {
		for {
			select {
			case <-g.triggerCh:
			default:
				return
			}
		}
	}

	g.triggerMembershipSync() // Ticker doesn't start immediately
	var lastSent time.Time
	for {
		select {
		case <-g.closer.HasBeenClosed():
			return
		case <-ticker.C:
			if time.Since(lastSent) > 10*time.Second {
				// On start of node if it becomes a leader, we would send tablets size for sure.
				g.triggerMembershipSync()
			}
		case <-g.triggerCh:
			// Let's send update even if not leader, zero will know that this node is still active.
			// We don't need to send tablet information everytime. So, let's only send it when we
			// calculate it.
			consumeTriggers()
			if err := g.doSendMembership(nil); err != nil {
				glog.Errorf("While sending membership update: %v", err)
			} else {
				lastSent = time.Now()
			}
		}
	}
}

// receiveMembershipUpdates receives membership updates from ANY Zero server. This is the main
// connection which tells Alpha about the state of the cluster, including the latest Zero leader.
// All the other connections to Zero, are only made only to the leader.
func (g *groupi) receiveMembershipUpdates() {
	defer func() {
		glog.Infoln("Closing receiveMembershipUpdates")
		g.closer.Done() // CLOSER:1
	}()

	ch := zero.Subscribe()
	for {
		select {
		case <-g.closer.HasBeenClosed():
			return
		case st := <-ch:
			g.applyState(st)
		}
	}
}

// SubscribeForUpdates will listen for updates for the given group.
func SubscribeForUpdates(prefixes [][]byte, ignore string, cb func(kvs *badgerpb.KVList),
	group uint32, closer *z.Closer) {

	x.BlockUntilHealthy() // No need to proceed if server is still coming up.

	var prefix []byte
	if len(prefixes) > 0 {
		prefix = prefixes[0]
	}
	defer func() {
		glog.Infof("SubscribeForUpdates closing for prefix: %q\n", prefix)
		closer.Done()
	}()

	listen := func() error {
		// Connect to any of the group 1 nodes.
		members := groups().AnyTwoServers(group)
		// There may be a lag while starting so keep retrying.
		if len(members) == 0 {
			return fmt.Errorf("Unable to find any servers for group: %d", group)
		}
		pool := conn.GetPools().Connect(members[0], x.WorkerConfig.TLSClientConfig)
		client := pb.NewWorkerClient(pool.Get())

		// Get Subscriber stream.
		stream, err := client.Subscribe(closer.Ctx(),
			&pb.SubscriptionRequest{Matches: x.PrefixesToMatches(prefixes, ignore)})
		if err != nil {
			return errors.Wrapf(err, "error from client.subscribe")
		}
		for {
			// Listen for updates.
			kvs, err := stream.Recv()
			if err != nil {
				return errors.Wrapf(err, "while receiving from stream")
			}
			cb(kvs)
		}
	}

	for {
		if err := listen(); err != nil {
			glog.Errorf("Error during SubscribeForUpdates for prefix %q: %v. closer err: %v\n",
				prefix, err, closer.Ctx().Err())
		}
		if closer.Ctx().Err() != nil {
			return
		}
		time.Sleep(time.Second)
	}
}
