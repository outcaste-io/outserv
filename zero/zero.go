// Portions Copyright 2017-2018 Dgraph Labs, Inc. are available under the Apache License v2.0.
// Portions Copyright 2022 Outcaste LLC are available under the Sustainable License v1.0.

package zero

import (
	"context"
	"crypto/tls"
	"fmt"
	"math"
	"os"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/glog"
	"github.com/outcaste-io/outserv/billing"
	"github.com/outcaste-io/outserv/conn"
	"github.com/outcaste-io/outserv/protos/pb"
	"github.com/outcaste-io/outserv/raftwal"
	"github.com/outcaste-io/outserv/x"
	"github.com/outcaste-io/ristretto/z"
	"github.com/pkg/errors"
)

type State struct {
	x.SafeMutex

	_state *pb.MembershipState

	// nextUint is the uint64 which we can hand out next. See maxLease for the
	// max ID leased via Zero quorum.
	nextUint map[pb.NumLeaseType]uint64

	// groupMap    map[uint32]*Group
	closer *z.Closer // Used to tell stream to close.

	// tls client config used to connect with zero internally
	tlsClientConfig *tls.Config
}

// Init initializes the zero server.
func NewState() *State {
	s := &State{}
	s._state = &pb.MembershipState{
		Tablets: make(map[string]*pb.Tablet),
		Members: make(map[uint64]*pb.Member),
	}
	s.nextUint = make(map[pb.NumLeaseType]uint64)
	s.nextUint[pb.Num_UID] = 16 // We keep the first 16 for special purposes.
	s.nextUint[pb.Num_NS_ID] = 1
	s.closer = z.NewCloser(2) // grpc and http

	// TODO: add UidLeaseLimit
	// if opts.limiterConfig.UidLeaseLimit > 0 {
	// 	// rate limiting is not enabled when lease limit is set to zero.
	// 	s.rateLimiter = x.NewRateLimiter(int64(opts.limiterConfig.UidLeaseLimit),
	// 		opts.limiterConfig.RefillAfter, s.closer)
	// }

	// TODO: Add functionality to rebalance types.
	// go s.rebalanceTablets()
	return s
}

func (s *State) member(addr string) *pb.Member {
	s.AssertRLock()
	for _, m := range s._state.Members {
		if m.Addr == addr {
			return m
		}
	}
	return nil
}

// SetMembershipState updates the membership state to the given one.
func (s *State) SetMembershipState(state *pb.MembershipState) {
	s.Lock()
	defer s.Unlock()

	s._state = state

	if state.Members == nil {
		state.Members = make(map[uint64]*pb.Member)
	}
	if state.Tablets == nil {
		state.Tablets = make(map[string]*pb.Tablet)
	}

	// Create connections to all members.
	for _, m := range state.Members {
		conn.GetPools().Connect(m.Addr, s.tlsClientConfig)
	}
}

func (s *State) membership() *pb.MembershipState {
	s.Lock()
	defer s.Unlock()

	return s._state
}

func (s *State) StoreMember(m *pb.Member) {
	s.Lock()
	defer s.Unlock()

	st := proto.Clone(s._state).(*pb.MembershipState)
	if st.Members == nil {
		st.Members = make(map[uint64]*pb.Member)
	}
	st.Members[m.Id] = m
	s._state = st
}

func (s *State) RemoveMember(raftId uint64) {
	s.Lock()
	defer s.Unlock()

	st := proto.Clone(s._state).(*pb.MembershipState)
	if st.Members == nil {
		st.Members = make(map[uint64]*pb.Member)
	}
	delete(st.Members, raftId)

	var has bool
	for _, id := range st.Removed {
		if id == raftId {
			has = true
		}
	}
	if !has {
		st.Removed = append(st.Removed, raftId)
	}
	s._state = st
}

var inode *node

func MembershipState() *pb.MembershipState {
	ms := inode.state.membership()
	if ms == nil {
		return &pb.MembershipState{}
	}
	return ms
}

func LatestMembershipState(ctx context.Context) (*pb.MembershipState, error) {
	if err := inode.WaitLinearizableRead(ctx); err != nil {
		return nil, err
	}
	ms := inode.state.membership()
	if ms == nil {
		return &pb.MembershipState{}, nil
	}
	return ms, nil
}

func (n *node) periodicallyProposeUsage(closer *z.Closer) {
	defer closer.Done()

	tick := time.NewTicker(15 * billing.Minute)
	defer tick.Stop()

	charge := func() error {
		if !n.amLeader() {
			return nil
		}
		st, err := LatestMembershipState(closer.Ctx())
		if err != nil {
			return errors.Wrapf(err, "while getting latest membership state")
		}

		lastCharged := time.Unix(st.LastCharged, 0)
		if time.Since(lastCharged) < 10*24*time.Hour {
			// We charged 10 days ago. No need to do anything.
			return nil
		}

		now := time.Now()
		if now.Day() != 1 && now.Day() != 15 {
			// Charge only on the 1st and 15th of the month.
			return nil
		}
		if st.CpuHours < 20.0/billing.USDPerCpuHour {
			// Charge for at least $20.
			return nil
		}

		cpuHours := st.CpuHours
		for i := 0; i < 60; i++ {
			if !n.amLeader() {
				return nil
			}
			err = billing.Charge(cpuHours)
			if err == nil {
				break
			} else {
				glog.Errorf("While charging for usage: %v", err)
			}
			time.Sleep(30 * time.Second)
		}
		if err != nil {
			return fmt.Errorf("Unable to charge for usage: %v", err)
		}

		// We successfully charged for the usage. Reduce the core hours. Note
		// that we should no longer check if the node is a leader or not. We
		// charged for it, so we must account for it.
		err = x.RetryUntilSuccess(60, time.Minute, func() error {
			_, err := ProposeAndWait(closer.Ctx(), &pb.ZeroProposal{
				CpuHours:    -cpuHours,
				LastCharged: time.Now().Unix(),
			})
			return err
		})
		x.Checkf(err, "Usage was charged for. But, unable to account for it.")
		return nil
	}

	for {
		select {
		case <-tick.C: // Every 15 mins.
			cpuHours := billing.CPUHours()
			err := x.RetryUntilSuccess(16, time.Minute, func() error {
				_, err := ProposeAndWait(closer.Ctx(), &pb.ZeroProposal{
					CpuHours: cpuHours,
				})
				return err
			})
			x.Check(err)
			billing.AccountedFor(cpuHours)

			// See if we need to charge for the usage.
			if err := charge(); err != nil {
				glog.Errorf("Charge failed: %v\n", err)
			}

		case <-closer.HasBeenClosed():
			return
		}
	}
}

func Run(closer *z.Closer, bindall bool) {
	glog.Infof("Starting Zero...")

	wdir := x.WorkerConfig.Dir.ZeroRaftWal
	nodeId := x.WorkerConfig.Raft.GetUint64("idx")

	// Create and initialize write-ahead log.
	x.Checkf(os.MkdirAll(wdir, 0700), "Error while creating Zero Raft WAL dir.")
	store := raftwal.Init(wdir)
	store.SetUint(raftwal.RaftId, nodeId)
	store.SetUint(raftwal.GroupId, 0) // All zeros have group zero.

	rc := pb.RaftContext{
		WhoIs:     "zero",
		Id:        nodeId,
		Addr:      x.WorkerConfig.MyAddr,
		Group:     0,
		IsLearner: x.WorkerConfig.Raft.GetBool("learner"),
	}
	cn := conn.NewNode(&rc, store, x.WorkerConfig.TLSClientConfig)
	conn.UpdateNode(rc.WhoIs, cn)

	nodeCloser := z.NewCloser(3)
	go x.MonitorDiskMetrics("wal_fs", wdir, nodeCloser)
	go func() {
		defer closer.Done()
		<-closer.HasBeenClosed()

		nodeCloser.SignalAndWait()

		err := store.Close()
		glog.Infof("Zero Raft WAL closed with error: %v\n", err)
		glog.Info("Goodbye from Zero!")
	}()

	// This must be here. It does not work if placed before Grpc init.
	inode = &node{
		Node:   cn,
		closer: nodeCloser,
		ctx:    nodeCloser.Ctx(),
		state:  NewState(),
	}
	go inode.periodicallyProposeUsage(nodeCloser)
	x.Check(inode.initAndStartNode())
}

func AssignUids(ctx context.Context, num uint32) (*pb.AssignedIds, error) {
	// TODO: Make it so we don't have to propose to Zero for every request.
	prop := &pb.ZeroProposal{NumUids: num}
	st, err := ProposeAndWait(ctx, prop)
	if err != nil {
		return nil, errors.Wrapf(err, "while AssignUids")
	}

	end := st.MaxUID
	// Both the StartId and EndId are inclusive.
	return &pb.AssignedIds{StartId: end - uint64(num), EndId: end - 1}, nil
}

func BumpMaxUid(ctx context.Context, maxUid uint64) error {
	for {
		ms, err := LatestMembershipState(ctx)
		if err != nil {
			return errors.Wrapf(err, "while retrieving latest membership state")
		}
		if ms.MaxUID > maxUid {
			glog.Infof("ms.MaxUID: %#x > maxUid: %#x. Returning from BumpMaxUid",
				ms.MaxUID, maxUid)
			return nil
		}
		ask := uint32(maxUid + 1 - ms.MaxUID)
		if maxUid-ms.MaxUID >= math.MaxUint32 {
			ask = uint32(math.MaxUint32)
		}
		glog.Infof("ms.MaxUID: %#x maxUid: %#x Assigning %#x UIDs\n",
			ms.MaxUID, maxUid, ask)
		if _, err := AssignUids(ctx, ask); err != nil {
			return errors.Wrapf(err, "while assigning UIDS")
		}
	}
}

func AssignNsids(ctx context.Context, num uint32) (*pb.AssignedIds, error) {
	prop := &pb.ZeroProposal{NumNsids: num}
	st, err := ProposeAndWait(ctx, prop)
	if err != nil {
		return nil, errors.Wrapf(err, "while AssignUids")
	}

	end := st.MaxNsID
	// Both the StartId and EndId are inclusive.
	return &pb.AssignedIds{StartId: end - uint64(num), EndId: end - 1}, nil
}
