// Portions Copyright 2017-2018 Dgraph Labs, Inc. are available under the Apache 2.0 license.
// Portions Copyright 2022 Outcaste, Inc. are available under the Smart license.

package zero

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"os"
	"sync"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/glog"
	"github.com/outcaste-io/outserv/conn"
	"github.com/outcaste-io/outserv/protos/pb"
	"github.com/outcaste-io/outserv/raftwal"
	"github.com/outcaste-io/outserv/x"
	"github.com/outcaste-io/ristretto/z"
	"go.opencensus.io/plugin/ocgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type State struct {
	x.SafeMutex

	_state *pb.MembershipState

	// nextUint is the uint64 which we can hand out next. See maxLease for the
	// max ID leased via Zero quorum.
	nextUint  map[pb.NumLeaseType]uint64
	leaseLock sync.Mutex // protects nextUID, nextTxnTs, nextNsID and corresponding proposals.

	// TODO: Do we need RateLimiter?
	rateLimiter *x.RateLimiter

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
		Leaders: make(map[uint32]uint64),
	}
	s.nextUint = make(map[pb.NumLeaseType]uint64)
	s.nextUint[pb.Num_UID] = 1
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
	if state.Leaders == nil {
		state.Leaders = make(map[uint32]uint64)
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
func (s *State) membershipCopy() *pb.MembershipState {
	s.Lock()
	defer s.Unlock()

	return proto.Clone(s._state).(*pb.MembershipState)
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

func setupListener(addr string, port int, kind string) (listener net.Listener, err error) {
	laddr := fmt.Sprintf("%s:%d", addr, port)
	glog.Infof("Setting up %s listener at: %v\n", kind, laddr)
	return net.Listen("tcp", laddr)
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
	glog.Infof("Called LatestMembershipState")
	if err := inode.WaitLinearizableRead(ctx); err != nil {
		return nil, err
	}
	ms := inode.state.membership()
	if ms == nil {
		return &pb.MembershipState{}, nil
	}
	return ms, nil
}

func Subscribe() <-chan *pb.MembershipState {
	return inode.ch
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

	go x.MonitorDiskMetrics("wal_fs", wdir, closer)

	// x.RegisterExporters(Zero.Conf, "dgraph.zero")
	grpcOpts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(x.GrpcMaxSize),
		grpc.MaxSendMsgSize(x.GrpcMaxSize),
		grpc.MaxConcurrentStreams(1000),
		grpc.StatsHandler(&ocgrpc.ServerHandler{}),
	}

	if x.WorkerConfig.TLSServerConfig != nil {
		grpcOpts = append(grpcOpts, grpc.Creds(credentials.NewTLS(x.WorkerConfig.TLSServerConfig)))
	}
	gServer := grpc.NewServer(grpcOpts...)
	go func() {
		addr := "localhost"
		if bindall {
			addr = "0.0.0.0"
		}
		x.AssertTrue(len(x.WorkerConfig.MyAddr) > 0)
		grpcListener, err := setupListener(addr, x.PortZeroGrpc+x.Config.PortOffset, "grpc")
		x.Check(err)

		err = gServer.Serve(grpcListener)
		glog.Infof("gRPC server stopped : %v", err)
	}()

	rc := pb.RaftContext{
		Id:        nodeId,
		Addr:      x.WorkerConfig.MyAddr,
		Group:     0,
		IsLearner: x.WorkerConfig.Raft.GetBool("learner"),
	}
	cn := conn.NewNode(&rc, store, x.WorkerConfig.TLSClientConfig)
	raftServer := conn.NewRaftServer(cn)
	pb.RegisterRaftServer(gServer, raftServer)

	nodeCloser := z.NewCloser(1)
	go func() {
		defer closer.Done()
		<-closer.HasBeenClosed()

		nodeCloser.SignalAndWait()
		gServer.Stop()

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
	x.Check(inode.initAndStartNode())
}
