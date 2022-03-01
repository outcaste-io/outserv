package zero

import (
	"crypto/tls"
	"sync"

	"github.com/gogo/protobuf/proto"
	"github.com/outcaste-io/outserv/conn"
	"github.com/outcaste-io/outserv/protos/pb"
	"github.com/outcaste-io/outserv/x"
	"github.com/outcaste-io/ristretto/z"
)

type State struct {
	x.SafeMutex
	Node *node

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
func (s *State) Init() {
	s.Lock()
	defer s.Unlock()

	s._state = &pb.MembershipState{
		Groups: make(map[uint32]*pb.Group),
		Zeros:  make(map[uint64]*pb.Member),
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
}

func (s *State) member(addr string) *pb.Member {
	s.AssertRLock()
	for _, g := range s._state.Groups {
		for _, m := range g.Members {
			if m.Addr == addr {
				return m
			}
		}
	}
	return nil
}

// SetMembershipState updates the membership state to the given one.
func (s *State) SetMembershipState(state *pb.MembershipState) {
	s.Lock()
	defer s.Unlock()

	s._state = state

	if state.Zeros == nil {
		state.Zeros = make(map[uint64]*pb.Member)
	}
	if state.Groups == nil {
		state.Groups = make(map[uint32]*pb.Group)
	}

	// Create connections to all members.
	for _, g := range state.Groups {
		for _, m := range g.Members {
			conn.GetPools().Connect(m.Addr, s.tlsClientConfig)
		}

		if g.Tablets == nil {
			g.Tablets = make(map[string]*pb.Tablet)
		}
	}
}

func (s *State) Membership() *pb.MembershipState {
	s.Lock()
	defer s.Unlock()

	return s._state
}
func (s *State) MembershipCopy() *pb.MembershipState {
	s.Lock()
	defer s.Unlock()

	return proto.Clone(s._state).(*pb.MembershipState)
}

func (s *State) StoreZero(m *pb.Member) {
	s.Lock()
	defer s.Unlock()

	st := proto.Clone(s._state).(*pb.MembershipState)
	st.Zeros[m.Id] = m
	s._state = st
}

func Run(closer *z.Closer) {
}
