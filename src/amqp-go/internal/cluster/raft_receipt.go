package cluster

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/maxpert/amqp-go/internal/clusterraft"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

const (
	raftInboxBatches      = 64
	raftGroupInboxCount   = 1024
	raftNodeInboxCount    = 4096
	raftGroupInboxBytes   = maxWireBytes + (1 << 20)
	raftNodeInboxBytes    = 2 * raftGroupInboxBytes
	raftInboxMessageBytes = 256
	raftInboxEntryBytes   = 128
)

type raftReceipts struct {
	mu     sync.Mutex
	groups map[*clusterraft.Group]*raftInbox
	count  int
	bytes  uint64
}

type raftInbox struct {
	batches chan raftInboundBatch
	count   int
	bytes   uint64
}

type raftInboundBatch struct {
	messages []*pb.Message
	bytes    uint64
	done     chan error // Only a batch containing a snapshot waits for Step.
}

// Raft accepts a peer batch into the local group's bounded FIFO and takes
// ownership of the decoded messages. Snapshot receipts retain the durable
// completion required by the sender's ReportSnapshot path.
func (n *Node) Raft(ctx context.Context, id uint64, messages []*pb.Message) error {
	group := n.group(id)
	if group == nil {
		return ErrUnavailable
	}
	v := n.view()
	if !n.enrolled(v) {
		return ErrUnavailable
	}
	batch, err := n.prepareRaftBatch(v, group, messages)
	if err != nil {
		return err
	}
	if err := n.admitRaftBatch(ctx, id, group, batch); err != nil {
		return err
	}
	if batch.done == nil {
		return nil
	}
	select {
	case err := <-batch.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (n *Node) prepareRaftBatch(v view, group *clusterraft.Group, messages []*pb.Message) (raftInboundBatch, error) {
	if len(messages) == 0 || len(messages) > raftGroupInboxCount {
		return raftInboundBatch{}, errors.New("invalid Raft batch message count")
	}
	batch := raftInboundBatch{messages: messages}
	var encoded uint64 = 10
	for _, message := range messages {
		if message == nil || message.GetTo() != n.cfg.Peer.ID ||
			message.GetFrom() == message.GetTo() || address(v, message.GetFrom()) == "" {
			return raftInboundBatch{}, errors.New("invalid Raft sender or recipient")
		}
		typ := message.GetType()
		if !validRaftPeerType(typ) || len(message.GetResponses()) != 0 {
			return raftInboundBatch{}, errors.New("invalid Raft peer message type or storage response")
		}
		size := uint64(proto.Size(message))
		encoded += size + 4
		batch.bytes += size + raftInboxMessageBytes + uint64(len(message.GetEntries()))*raftInboxEntryBytes
		if encoded > maxWireBytes || batch.bytes > raftGroupInboxBytes {
			return raftInboundBatch{}, clusterraft.ErrTooLarge
		}
		if err := group.CheckPeerInputSize(message, size); err != nil {
			return raftInboundBatch{}, err
		}
		if typ == pb.MsgSnap && batch.done == nil {
			batch.done = make(chan error, 1)
		}
	}
	return batch, nil
}

func validRaftPeerType(kind pb.MessageType) bool {
	switch kind {
	case pb.MsgApp, pb.MsgAppResp, pb.MsgVote, pb.MsgVoteResp, pb.MsgPreVote, pb.MsgPreVoteResp,
		pb.MsgHeartbeat, pb.MsgHeartbeatResp, pb.MsgSnap, pb.MsgTimeoutNow:
		return true
	default:
		return false
	}
}

func (n *Node) admitRaftBatch(ctx context.Context, id uint64, group *clusterraft.Group, batch raftInboundBatch) error {
	r := &n.receipts
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if n.ctx.Err() != nil || n.group(id) != group {
		return ErrUnavailable
	}
	select {
	case <-group.Done():
		return ErrUnavailable
	default:
	}
	count := len(batch.messages)
	if r.count+count > raftNodeInboxCount || r.bytes+batch.bytes > raftNodeInboxBytes {
		return clusterraft.ErrBusy
	}
	inbox := r.groups[group]
	if inbox == nil {
		if r.groups == nil {
			r.groups = make(map[*clusterraft.Group]*raftInbox)
		}
		inbox = &raftInbox{batches: make(chan raftInboundBatch, raftInboxBatches)}
		r.groups[group] = inbox
		n.wg.Add(1)
		go n.runRaftInbox(group, inbox)
	}
	if inbox.count+count > raftGroupInboxCount || inbox.bytes+batch.bytes > raftGroupInboxBytes {
		return clusterraft.ErrBusy
	}
	select {
	case inbox.batches <- batch:
		inbox.count += count
		inbox.bytes += batch.bytes
		r.count += count
		r.bytes += batch.bytes
		return nil
	default:
		return clusterraft.ErrBusy
	}
}

func (n *Node) runRaftInbox(group *clusterraft.Group, inbox *raftInbox) {
	defer n.wg.Done()
	defer n.closeRaftInbox(group, inbox)
	for n.ctx.Err() == nil {
		select {
		case <-n.ctx.Done():
			return
		case <-group.Done():
			return
		case batch := <-inbox.batches:
			err := n.stepRaftBatch(group, batch)
			n.receipts.mu.Lock()
			n.receipts.release(inbox, batch)
			n.receipts.mu.Unlock()
			if batch.done != nil {
				batch.done <- err
			}
		}
	}
}

func (n *Node) closeRaftInbox(group *clusterraft.Group, inbox *raftInbox) {
	r := &n.receipts
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.groups, group)
	for len(inbox.batches) != 0 {
		batch := <-inbox.batches
		r.release(inbox, batch)
		if batch.done != nil {
			batch.done <- ErrUnavailable
		}
	}
}

func (r *raftReceipts) release(inbox *raftInbox, batch raftInboundBatch) {
	count := len(batch.messages)
	inbox.count -= count
	inbox.bytes -= batch.bytes
	r.count -= count
	r.bytes -= batch.bytes
}

func (n *Node) stepRaftBatch(group *clusterraft.Group, batch raftInboundBatch) error {
	ctx, cancel := context.WithTimeout(n.ctx, requestTimeout)
	defer cancel()
	for _, message := range batch.messages {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := group.Step(ctx, message)
		if errors.Is(err, clusterraft.ErrBusy) {
			err = retryRaftStep(ctx, group, message)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func retryRaftStep(ctx context.Context, group *clusterraft.Group, message *pb.Message) error {
	// Keep a busy message at the head while the Raft writer drains. A
	// prolonged stall drops the packet and lets Raft retransmit.
	retry := time.NewTicker(5 * time.Millisecond)
	defer retry.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-group.Done():
			return ErrUnavailable
		case <-retry.C:
			if err := group.Step(ctx, message); !errors.Is(err, clusterraft.ErrBusy) {
				return err
			}
		}
	}
}
