// Portions Copyright 2017-2018 Dgraph Labs, Inc. are available under the Apache 2.0 license.
// Portions Copyright 2022 Outcaste, Inc. are available under the Smart license.

package zero

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/glog"
	"github.com/google/uuid"
	"github.com/outcaste-io/outserv/conn"
	"github.com/outcaste-io/outserv/protos/pb"
	"github.com/outcaste-io/outserv/x"
	"github.com/outcaste-io/ristretto/z"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
	ostats "go.opencensus.io/stats"
	otrace "go.opencensus.io/trace"
)

const (
	raftDefaults = "idx=1; learner=false;"
)

var proposalKey uint64

type node struct {
	*conn.Node
	state  *State
	ctx    context.Context
	closer *z.Closer // to stop Run.
	ch     chan *pb.MembershipState

	// The last timestamp when this Zero was able to reach quorum.
	mu sync.RWMutex
}

func (n *node) amLeader() bool {
	if n.Raft() == nil {
		return false
	}
	r := n.Raft()
	return r.Status().Lead == r.Status().ID
}

func (n *node) AmLeader() bool {
	return n.amLeader()

	// Note (Mar 8, 2022):
	// We used to check quorum here before. Because for transactions, Zero
	// leader used to hold state all of which was not circulated via Raft. Now,
	// all Zero operations achieve Raft quorum. So, we don't need to check
	// separately for quorum again.
	//
	// Old message:
	// This node must be the leader, but must also be an active member of
	// the cluster, and not hidden behind a partition. Basically, if this
	// node was the leader and goes behind a partition, it would still
	// think that it is indeed the leader for the duration mentioned below.
}

// {2 bytes Node ID} {4 bytes for random} {2 bytes zero}
func (n *node) initProposalKey(id uint64) error {
	x.AssertTrue(id != 0)
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return err
	}
	proposalKey = n.Id<<48 | binary.BigEndian.Uint64(b)<<16
	return nil
}

func (n *node) uniqueKey() uint64 {
	return atomic.AddUint64(&proposalKey, 1)
}

var errInternalRetry = errors.New("Retry Raft proposal internally")

func ProposeAndWait(ctx context.Context, proposal *pb.ZeroProposal) (*pb.MembershipState, error) {
	return inode.proposeAndWait(ctx, proposal)
}

// proposeAndWait makes a proposal to the quorum for Group Zero and waits for it to be accepted by
// the group before returning. It is safe to call concurrently.
func (n *node) proposeAndWait(ctx context.Context,
	proposal *pb.ZeroProposal) (*pb.MembershipState, error) {

	switch {
	case n.Raft() == nil:
		return nil, errors.Errorf("Raft isn't initialized yet.")
	case ctx.Err() != nil:
		return nil, ctx.Err()
	case !n.AmLeader():
		// Do this check upfront. Don't do this inside propose for reasons explained below.
		return nil, errors.Errorf("Not Zero leader. Aborting proposal: %+v", proposal)
	}

	// We could consider adding a wrapper around the user proposal, so we can access any key-values.
	// Something like this:
	// https://github.com/golang/go/commit/5d39260079b5170e6b4263adb4022cc4b54153c4
	span := otrace.FromContext(ctx)
	// Overwrite ctx, so we no longer enforce the timeouts or cancels from ctx.
	ctx = otrace.NewContext(context.Background(), span)

	stop := x.SpanTimer(span, "n.proposeAndWait")
	defer stop()

	// propose runs in a loop. So, we should not do any checks inside, including n.AmLeader. This is
	// to avoid the scenario where the first proposal times out and the second one gets returned
	// due to node no longer being the leader. In this scenario, the first proposal can still get
	// accepted by Raft, causing a txn violation later for us, because we assumed that the proposal
	// did not go through.
	propose := func(timeout time.Duration) (*pb.MembershipState, error) {
		cctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		resCh := make(chan conn.ProposalResult, 1)
		pctx := &conn.ProposalCtx{
			ResCh: resCh,
			// Don't use the original context, because that's not what we're passing to Raft.
			Ctx: cctx,
		}
		key := n.uniqueKey()
		// unique key is randomly generated key and could have collision.
		// This is to ensure that even if collision occurs, we retry.
		for !n.Proposals.Store(key, pctx) {
			glog.Warningf("Found existing proposal with key: [%v]", key)
			key = n.uniqueKey()
		}
		defer n.Proposals.Delete(key)
		span.Annotatef(nil, "Proposing with key: %d. Timeout: %v", key, timeout)

		data := make([]byte, 8+proposal.Size())
		binary.BigEndian.PutUint64(data[:8], key)
		sz, err := proposal.MarshalToSizedBuffer(data[8:])
		if err != nil {
			return nil, err
		}
		data = data[:8+sz]
		// Propose the change.
		if err := n.Raft().Propose(cctx, data); err != nil {
			span.Annotatef(nil, "Error while proposing via Raft: %v", err)
			return nil, errors.Wrapf(err, "While proposing")
		}

		// Wait for proposal to be applied or timeout.
		select {
		case res := <-resCh:
			// We arrived here by a call to n.props.Done().
			return res.Data.(*pb.MembershipState), res.Err
		case <-cctx.Done():
			span.Annotatef(nil, "Internal context timeout %s. Will retry...", timeout)
			return nil, errInternalRetry
		}
	}

	// Some proposals can be stuck if leader change happens. For e.g. MsgProp message from follower
	// to leader can be dropped/end up appearing with empty Data in CommittedEntries.
	// Having a timeout here prevents the mutation being stuck forever in case they don't have a
	// timeout. We should always try with a timeout and optionally retry.
	var st *pb.MembershipState
	err := errInternalRetry
	timeout := 4 * time.Second
	for err == errInternalRetry {
		st, err = propose(timeout)
		timeout *= 2 // Exponential backoff
		if timeout > time.Minute {
			timeout = 32 * time.Second
		}
	}
	return st, err
}

var (
	errInvalidProposal = errors.New("Invalid group proposal")
)

func (n *node) applyProposal(e raftpb.Entry) error {
	x.AssertTrue(len(e.Data) > 0)

	var p pb.ZeroProposal
	key := binary.BigEndian.Uint64(e.Data[:8])
	if err := p.Unmarshal(e.Data[8:]); err != nil {
		return err
	}
	span := otrace.FromContext(n.Proposals.Ctx(key))

	n.state.Lock()
	defer n.state.Unlock()

	dst := proto.Clone(n.state._state).(*pb.MembershipState)
	if len(p.Cid) > 0 {
		if len(dst.Cid) > 0 {
			return errInvalidProposal
		}
		dst.Cid = p.Cid
	}
	if p.Member != nil {
		if err := n.handleMemberProposal(dst, p.Member); err != nil {
			span.Annotatef(nil, "While applying membership proposal: %+v", err)
			glog.Errorf("While applying membership proposal: %+v", err)
			return err
		}
	}
	if len(p.Tablets) > 0 {
		glog.Infof("Applying proposal: %+v\n", p.Tablets)
		if err := n.handleTabletProposal(dst, p.Tablets); err != nil {
			span.Annotatef(nil, "While applying tablet proposal: %+v", err)
			glog.Errorf("While applying tablet proposal: %+v", err)
			return err
		}
		glog.Infof("Proposal applied: %+v\n", p.Tablets)
	}

	switch {
	case p.NumUids > 0:
		dst.MaxUID += uint64(p.NumUids)
	case p.NumNsids > 0:
		dst.MaxNsID += uint64(p.NumNsids)
	}

	// Now assign the new state back.
	glog.Infof("Updated state: %+v\n", dst)
	n.state._state = dst
	n.ch <- dst
	return nil
}

func (n *node) handleMemberProposal(dst *pb.MembershipState, member *pb.Member) error {
	n.state.AssertLock()

	m := n.state.member(member.Addr)
	// Ensures that different nodes don't have same address.
	if m != nil && m.Id != member.Id {
		return errors.Errorf("Found another member %d with same address: %v", m.Id, m.Addr)
	}

	m, has := dst.Members[member.Id]
	if member.AmDead {
		if has {
			delete(dst.Members, member.Id)
			dst.Removed = append(dst.Removed, m.Id)
		}
		return nil
	}

	// Create a connection to this server.
	go conn.GetPools().Connect(member.Addr, n.state.tlsClientConfig)

	dst.Members[member.Id] = member
	if member.Leader {
		// Unset leader flag for other nodes, there can be only one
		// leader at a time.
		for _, m := range dst.Members {
			if m.Id != member.Id {
				m.Leader = false
			}
		}
	}
	return nil
}

func (n *node) handleTabletProposal(dst *pb.MembershipState, tablets []*pb.Tablet) error {
	n.state.AssertLock()

	handleOne := func(tablet *pb.Tablet) error {
		if tablet.GroupId == 0 {
			return errors.Errorf("Tablet group id is zero: %+v", tablet)
		}
		if tablet.Remove {
			glog.Infof("Removing tablet for attr: [%v], gid: [%v]\n",
				tablet.Predicate, tablet.GroupId)
			delete(dst.Tablets, tablet.Predicate)
			return nil
		}

		// There's a edge case that we're handling.
		// Two servers ask to serve the same tablet, then we need to ensure that
		// only the first one succeeds.
		//
		// TODO: Do we need tablet.Force?
		if prev := dst.Tablets[tablet.Predicate]; prev != nil && !tablet.Force {
			return fmt.Errorf("Tablet %s is already served. Prev: %+v New: %+v\n",
				tablet.Predicate, prev, tablet)
		}
		tablet.Force = false
		dst.Tablets[tablet.Predicate] = tablet
		return nil
	}

	if dst.Tablets == nil {
		dst.Tablets = make(map[string]*pb.Tablet)
	}
	for _, tablet := range tablets {
		if err := handleOne(tablet); err != nil {
			return err
		}
	}
	return nil
}

func (n *node) ensureClusterID() {
	tick := time.NewTicker(time.Minute)
	defer tick.Stop()

	propose := func() {
		id := uuid.New().String()

		_, err := n.proposeAndWait(context.Background(), &pb.ZeroProposal{Cid: id})
		if err == nil {
			glog.Infof("CID set via proposal for cluster: %v", id)
			return
		}
		if err == errInvalidProposal {
			glog.Errorf("invalid proposal error while proposing cluster id")
			return
		}
		glog.Errorf("While proposing CID: %v", err)
	}

	for range tick.C {
		if cid := n.state.membership().GetCid(); len(cid) > 0 {
			glog.Infof("Found Cluster ID: %s\n", cid)
			return
		}
		if !n.AmLeader() {
			continue
		}
		propose()
	}
}

func (n *node) initAndStartNode() error {
	x.Check(n.initProposalKey(n.Id))
	_, restart, err := n.PastLife()
	x.Check(err)

	// TODO: Check other peers too.
	peer := x.WorkerConfig.PeerAddr[0]

	switch {
	case restart:
		glog.Infoln("Restarting node for dgraphzero")
		sp, err := n.Store.Snapshot()
		x.Checkf(err, "Unable to get existing snapshot")
		if !raft.IsEmptySnap(sp) {
			// It is important that we pick up the conf state here.
			n.SetConfState(&sp.Metadata.ConfState)

			var zs pb.ZeroSnapshot
			x.Check(zs.Unmarshal(sp.Data))
			n.state.SetMembershipState(zs.State)
			for _, id := range sp.Metadata.ConfState.Nodes {
				n.Connect(id, zs.State.Members[id].Addr)
			}
		}
		n.SetRaft(raft.RestartNode(n.Cfg))

	case len(peer) > 0:
		glog.Infoln("Connecting to an existing peer: %+v\n", peer)
		p := conn.GetPools().Connect(peer, x.WorkerConfig.TLSClientConfig)
		if p == nil {
			return errors.Errorf("Unhealthy connection to %v", peer)
		}

		timeout := 8 * time.Second
		for {
			c := pb.NewRaftClient(p.Get())
			ctx, cancel := context.WithTimeout(n.ctx, timeout)
			// JoinCluster can block indefinitely, raft ignores conf change proposal
			// if it has pending configuration.
			_, err := c.JoinCluster(ctx, n.RaftContext)
			if err == nil {
				cancel()
				break
			}
			if x.ShouldCrash(err) {
				cancel()
				log.Fatalf("Error while joining cluster: %v", err)
			}
			glog.Errorf("Error while joining cluster: %v\n", err)
			timeout *= 2
			if timeout > 32*time.Second {
				timeout = 32 * time.Second
			}
			time.Sleep(timeout) // This is useful because JoinCluster can exit immediately.
			cancel()
		}
		glog.Infof("[%#x] Starting node\n", n.Id)
		n.SetRaft(raft.StartNode(n.Cfg, nil))

	default:
		glog.Infof("Starting a brand new Zero node.")
		data, err := n.RaftContext.Marshal()
		x.Check(err)
		peers := []raft.Peer{{ID: n.Id, Context: data}}
		n.SetRaft(raft.StartNode(n.Cfg, peers))
	}

	go n.Run()
	go n.BatchAndSendMessages()
	go n.ReportRaftComms(n.closer)
	go n.ensureClusterID()
	return nil
}

const tickDur = 100 * time.Millisecond

func (n *node) Run() {
	ticker := time.NewTicker(tickDur)
	defer ticker.Stop()

	slowTicker := time.NewTicker(time.Minute)
	defer slowTicker.Stop()

	n.ch = make(chan *pb.MembershipState, 16)
	// snapshot can cause select loop to block while deleting entries, so run
	// it in goroutine
	closer := z.NewCloser(2)

	defer func() {
		closer.SignalAndWait()
		glog.Infof("Zero Node.Run finished.")
		n.closer.Done()
	}()

	go n.snapshotPeriodically(closer)
	if !x.WorkerConfig.HardSync {
		closer.AddRunning(1)
		go x.StoreSync(n.Store, closer)
	}

	// We only stop runReadIndexLoop after the for loop below has finished interacting with it.
	// That way we know sending to readStateCh will not deadlock.
	readStateCh := make(chan raft.ReadState, 100)
	go n.RunReadIndexLoop(closer, readStateCh)

	glog.Infof("Zero.Run")
	var leader bool
	var timer x.Timer
	for {
		select {
		case <-n.closer.HasBeenClosed():
			glog.Infof("Exiting Zero.Run")
			n.Raft().Stop()
			return

		case <-ticker.C:
			n.Raft().Tick()

		case <-slowTicker.C:
			// We should calculate the snapshot in the same loop, to ensure that
			// we have applied all the Raft committed entries to the state, and
			// that we're picking up the correct state.
			if err := n.calculateAndProposeSnapshot(); err != nil {
				glog.Errorf("While calculating snapshot: %v\n", err)
			}

		case rd := <-n.Raft().Ready():
			timer.Start()
			_, span := otrace.StartSpan(n.ctx, "Zero.RunLoop",
				otrace.WithSampler(otrace.ProbabilitySampler(0.001)))
			for _, rs := range rd.ReadStates {
				// No need to use select-case-default on pushing to readStateCh. It is typically
				// empty.
				readStateCh <- rs
			}
			if len(rd.ReadStates) > 0 {
				glog.V(2).Infof("Read states: %d\n", len(rd.ReadStates))
				span.Annotatef(nil, "Pushed %d readstates", len(rd.ReadStates))
			}

			if rd.SoftState != nil {
				leader = rd.RaftState == raft.StateLeader
				glog.Infof("Am I leader? %v\n", leader)
			}
			if leader {
				ostats.Record(n.ctx, x.RaftIsLeader.M(1))
				// Leader can send messages in parallel with writing to disk.
				for i := range rd.Messages {
					n.Send(&rd.Messages[i])
				}
			} else {
				ostats.Record(n.ctx, x.RaftIsLeader.M(0))
			}

			n.SaveToStorage(&rd.HardState, rd.Entries, &rd.Snapshot)
			timer.Record("disk")
			span.Annotatef(nil, "Saved to storage")
			for x.WorkerConfig.HardSync && rd.MustSync {
				if err := n.Store.Sync(); err != nil {
					glog.Errorf("Error while calling Store.Sync: %v", err)
					time.Sleep(10 * time.Millisecond)
					continue
				}
				timer.Record("sync")
				break
			}

			if !raft.IsEmptySnap(rd.Snapshot) {
				var zs pb.ZeroSnapshot
				x.Check(zs.Unmarshal(rd.Snapshot.Data))
				n.state.SetMembershipState(zs.State)
			}

			for _, entry := range rd.CommittedEntries {
				n.Applied.Begin(entry.Index)
				switch {
				case entry.Type == raftpb.EntryConfChange:
					n.applyConfChange(entry)
					glog.Infof("Done applying conf change at %#x", n.Id)

				case len(entry.Data) == 0:
					// Raft commits empty entry on becoming a leader.
					// Do nothing.

				case entry.Type == raftpb.EntryNormal:
					start := time.Now()
					key := binary.BigEndian.Uint64(entry.Data[:8])
					err := n.applyProposal(entry)
					if err != nil {
						glog.Errorf("While applying proposal: %v\n", err)
					}

					st := n.state.membership()
					n.Proposals.Done(key, conn.ProposalResult{err, st})

					if took := time.Since(start); took > time.Second {
						var p pb.ZeroProposal
						// Raft commits empty entry on becoming a leader.
						if err := p.Unmarshal(entry.Data[8:]); err == nil {
							glog.V(2).Infof("Proposal took %s to apply: %+v\n",
								took.Round(time.Second), p)
						}
					}

				default:
					glog.Infof("Unhandled entry: %+v\n", entry)
				}
				n.Applied.Done(entry.Index)
			}
			span.Annotatef(nil, "Applied %d CommittedEntries", len(rd.CommittedEntries))

			if !leader {
				// Followers should send messages later.
				for i := range rd.Messages {
					n.Send(&rd.Messages[i])
				}
			}
			span.Annotate(nil, "Sent messages")
			timer.Record("proposals")

			n.Raft().Advance()
			span.Annotate(nil, "Advanced Raft")
			timer.Record("advance")

			span.End()
			if timer.Total() > 5*tickDur {
				glog.Warningf(
					"Raft.Ready took too long to process: %s."+
						" Num entries: %d. Num committed entries: %d. MustSync: %v",
					timer.String(), len(rd.Entries), len(rd.CommittedEntries), rd.MustSync)
			}
		}
	}
}

func (n *node) snapshotPeriodically(closer *z.Closer) {
	defer closer.Done()
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := n.calculateAndProposeSnapshot(); err != nil {
				glog.Errorf("While calculateAndProposeSnapshot: %v", err)
			}

		case <-closer.HasBeenClosed():
			return
		}
	}
}

// It uses that information to calculate a snapshot, which it proposes to other
// Zeros. When the proposal arrives via Raft, all Zeros apply it to themselves
// via applySnapshot in raft.Ready.
func (n *node) calculateAndProposeSnapshot() error {
	// Only run this on the leader.
	if !n.AmLeader() {
		return nil
	}

	_, span := otrace.StartSpan(n.ctx, "Calculate.Snapshot",
		otrace.WithSampler(otrace.AlwaysSample()))
	defer span.End()

	first, err := n.Store.FirstIndex()
	if err != nil {
		span.Annotatef(nil, "FirstIndex error: %v", err)
		return err
	}
	last, err := n.Store.LastIndex()
	if err != nil {
		span.Annotatef(nil, "LastIndex error: %v", err)
		return err
	}

	// Let's keep the last 64 entries
	num := last - first
	span.Annotatef(nil, "First index: %d. Last index: %d. num: %d",
		first, last, num)
	if num < 64 {
		return nil
	}

	state := n.state.membershipCopy()
	zs := &pb.ZeroSnapshot{
		Index: last,
		State: state,
	}
	glog.V(2).Infof("Proposing snapshot at index: %d\n", zs.Index)
	zp := &pb.ZeroProposal{Snapshot: zs}
	if _, err = n.proposeAndWait(n.ctx, zp); err != nil {
		glog.Errorf("Error while proposing snapshot: %v\n", err)
		span.Annotatef(nil, "Error while proposing snapshot: %v", err)
		return err
	}
	span.Annotatef(nil, "Snapshot proposed: Done")
	return nil
}

func (n *node) applyConfChange(e raftpb.Entry) {
	var cc raftpb.ConfChange
	if err := cc.Unmarshal(e.Data); err != nil {
		glog.Errorf("While unmarshalling confchange: %+v", err)
	}

	if cc.Type == raftpb.ConfChangeRemoveNode {
		if cc.NodeID == n.Id {
			glog.Fatalf("I [id:%#x group:0] have been removed. Goodbye!", n.Id)
		}
		n.DeletePeer(cc.NodeID)
		n.state.RemoveMember(cc.NodeID)

	} else if len(cc.Context) > 0 {
		var rc pb.RaftContext
		x.Check(rc.Unmarshal(cc.Context))
		go n.Connect(rc.Id, rc.Addr)

		m := &pb.Member{
			Id:      rc.Id,
			Addr:    rc.Addr,
			GroupId: 0,
			Learner: rc.IsLearner,
		}
		for _, mid := range n.state.membership().Removed {
			// Reusing Raft IDs is not allowed.
			if m.Id == mid {
				err := errors.Errorf("REUSE_RAFTID: Reusing removed id: %d.\n", mid)
				n.DoneConfChange(cc.ID, err)
				// Cancel configuration change.
				cc.NodeID = raft.None
				n.Raft().ApplyConfChange(cc)
				return
			}
		}

		n.state.StoreMember(m)
	}

	cs := n.Raft().ApplyConfChange(cc)
	n.SetConfState(cs)
	n.DoneConfChange(cc.ID, nil)
}
