// Portions Copyright 2018 Dgraph Labs, Inc. are available under the Apache License v2.0.
// Portions Copyright 2022 Outcaste LLC are available under the Sustainable License v1.0.

package conn

import (
	"context"
	"encoding/binary"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/glog"
	"github.com/outcaste-io/outserv/protos/pb"
	"github.com/outcaste-io/outserv/x"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/raft/raftpb"
	otrace "go.opencensus.io/trace"
	"google.golang.org/grpc"
)

type sendmsg struct {
	to   uint64
	data []byte
}

type lockedSource struct {
	lk  sync.Mutex
	src rand.Source
}

func (r *lockedSource) Int63() int64 {
	r.lk.Lock()
	defer r.lk.Unlock()
	return r.src.Int63()
}

func (r *lockedSource) Seed(seed int64) {
	r.lk.Lock()
	defer r.lk.Unlock()
	r.src.Seed(seed)
}

type ProposalResult struct {
	Err  error
	Data interface{}
}

// ProposalCtx stores the context for a proposal with extra information.
type ProposalCtx struct {
	Found uint32
	ResCh chan ProposalResult
	Ctx   context.Context
}

type proposals struct {
	sync.RWMutex
	all map[uint64]*ProposalCtx
}

func (p *proposals) Store(key uint64, pctx *ProposalCtx) bool {
	if key == 0 {
		return false
	}
	p.Lock()
	defer p.Unlock()
	if p.all == nil {
		p.all = make(map[uint64]*ProposalCtx)
	}
	if _, has := p.all[key]; has {
		return false
	}
	p.all[key] = pctx
	return true
}

func (p *proposals) Ctx(key uint64) context.Context {
	if pctx := p.Get(key); pctx != nil {
		return pctx.Ctx
	}
	return context.Background()
}

func (p *proposals) Get(key uint64) *ProposalCtx {
	p.RLock()
	defer p.RUnlock()
	return p.all[key]
}

func (p *proposals) Delete(key uint64) {
	if key == 0 {
		return
	}
	p.Lock()
	defer p.Unlock()
	delete(p.all, key)
}

func (p *proposals) Done(key uint64, res ProposalResult) {
	if key == 0 {
		return
	}
	p.Lock()
	defer p.Unlock()
	pd, has := p.all[key]
	if !has {
		// If we assert here, there would be a race condition between a context
		// timing out, and a proposal getting applied immediately after. That
		// would cause assert to fail. So, don't assert.
		return
	}
	delete(p.all, key)
	pd.ResCh <- res
}

// RaftServer is a wrapper around node that implements the Raft service.
type RaftServer struct {
	m     sync.RWMutex
	nodes map[string]*Node
}

var rs *RaftServer

func init() {
	rs = &RaftServer{nodes: make(map[string]*Node)}
}

func Register(server *grpc.Server) {
	pb.RegisterRaftServer(server, rs)
}

// UpdateNode safely updates the node.
func UpdateNode(whoIs string, n *Node) {
	rs.m.Lock()
	defer rs.m.Unlock()
	rs.nodes[whoIs] = n
}

// GetNode safely retrieves the node.
func (rs *RaftServer) GetNode(whoIs string) *Node {
	rs.m.RLock()
	defer rs.m.RUnlock()
	return rs.nodes[whoIs]
}

// IsPeer checks whether this node is a peer of the node sending the request.
func (rs *RaftServer) IsPeer(ctx context.Context, rc *pb.RaftContext) (
	*pb.PeerResponse, error) {
	node := rs.GetNode(rc.WhoIs)
	if node == nil || node.Raft() == nil {
		return &pb.PeerResponse{}, ErrNoNode
	}

	confState := node.ConfState()

	if confState == nil {
		return &pb.PeerResponse{}, nil
	}

	for _, raftIdx := range confState.Nodes {
		if rc.Id == raftIdx {
			return &pb.PeerResponse{Status: true}, nil
		}
	}
	return &pb.PeerResponse{}, nil
}

// JoinCluster handles requests to join the cluster.
func (rs *RaftServer) JoinCluster(ctx context.Context,
	rc *pb.RaftContext) (*pb.Payload, error) {
	if ctx.Err() != nil {
		return &pb.Payload{}, ctx.Err()
	}

	node := rs.GetNode(rc.WhoIs)
	if node == nil || node.Raft() == nil {
		return nil, ErrNoNode
	}

	return node.joinCluster(ctx, rc)
}

// RaftMessage handles RAFT messages.
func (rs *RaftServer) RaftMessage(server pb.Raft_RaftMessageServer) error {
	ctx := server.Context()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	span := otrace.FromContext(ctx)
	done := make(map[uint64]bool)

	step := func(batch *pb.RaftBatch) error {
		rc := batch.GetContext()
		node := rs.GetNode(rc.WhoIs)
		if node == nil || node.Raft() == nil {
			glog.Warningf("No node found for %s\n", rc.WhoIs)
			return ErrNoNode
		}
		if !done[rc.Id] {
			node.Connect(rc.Id, rc.Addr)
			done[rc.Id] = true
		}

		span.Annotatef(nil, "Stream server is node %#x", node.Id)
		ctx, cancel := context.WithTimeout(ctx, time.Minute)
		defer cancel()

		raft := node.Raft()
		data := batch.Payload.GetData()
		for idx := 0; idx < len(data); {
			x.AssertTruef(len(data[idx:]) >= 4,
				"Slice left of size: %v. Expected at least 4.", len(data[idx:]))

			sz := int(binary.LittleEndian.Uint32(data[idx : idx+4]))
			idx += 4
			msg := raftpb.Message{}
			if idx+sz > len(data) {
				return errors.Errorf(
					"Invalid query. Specified size %v overflows slice [%v,%v)\n",
					sz, idx, len(data))
			}
			if err := msg.Unmarshal(data[idx : idx+sz]); err != nil {
				x.Check(err)
			}
			// This should be done in order, and not via a goroutine.
			// Step can block forever. See: https://github.com/etcd-io/etcd/issues/10585
			// So, add a context with timeout to allow it to get out of the blockage.
			if glog.V(2) {
				switch msg.Type {
				case raftpb.MsgHeartbeat, raftpb.MsgHeartbeatResp:
					atomic.AddInt64(&node.heartbeatsIn, 1)
				case raftpb.MsgReadIndex, raftpb.MsgReadIndexResp:
				case raftpb.MsgApp, raftpb.MsgAppResp:
				case raftpb.MsgProp:
				default:
					glog.Infof("RaftComm: [%#x] Received msg of type: %s from %#x",
						msg.To, msg.Type, msg.From)
				}
			}
			if err := raft.Step(ctx, msg); err != nil {
				glog.Warningf("Error while raft.Step from %#x: %v. Closing RaftMessage stream.",
					rc.GetId(), err)
				return errors.Wrapf(err, "error while raft.Step from %#x", rc.GetId())
			}
			idx += sz
		}
		return nil
	}

	for loop := 1; ; loop++ {
		batch, err := server.Recv()
		if err != nil {
			return err
		}
		if err := step(batch); err != nil {
			glog.Errorf("Error while stepping: %s\n", err)
			return err
		}
	}
}

// Heartbeat rpc call is used to check connection with other workers after worker
// tcp server for this instance starts.
func (rs *RaftServer) Heartbeat(_ *pb.Payload, stream pb.Raft_HeartbeatServer) error {
	ticker := time.NewTicker(echoDuration)
	defer ticker.Stop()

	// Hardcoding alpha here. We shouldn't need to switch between zero and
	// alpha.
	node := rs.GetNode("alpha")
	if node == nil {
		return ErrNoNode
	}
	// TODO(Aman): Send list of ongoing tasks as part of heartbeats.
	// Currently, there is a cyclic dependency of imports worker -> conn -> worker.
	info := pb.HealthInfo{
		Instance: "alpha",
		Address:  node.MyAddr,
		Group:    strconv.Itoa(int(node.RaftContext.GetGroup())),
		Version:  x.Version(),
		Uptime:   int64(time.Since(node.StartTime) / time.Second),
	}
	if info.Group == "0" {
		info.Instance = "zero"
	}

	ctx := stream.Context()

	for {
		info.Uptime = int64(time.Since(node.StartTime) / time.Second)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := stream.Send(&info); err != nil {
				return err
			}
		}
	}
}
