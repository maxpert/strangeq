package server

import (
	"bytes"
	"context"
	"encoding/base64"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/internal/cluster"
	"github.com/maxpert/amqp-go/internal/clusterqueue"
	"github.com/maxpert/amqp-go/internal/clusterraft"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
)

type selectedCountingMachine struct {
	*ClusterMachine
	selected atomic.Int32
	portable atomic.Int32
}

func (m *selectedCountingMachine) CaptureLocalAssignmentPage(request clusterqueue.AssignmentRequest, allow bool) (func() (*clusterqueue.BorrowedAssignmentPage, error), int, error) {
	m.selected.Add(1)
	return m.ClusterMachine.CaptureLocalAssignmentPage(request, allow)
}
func (m *selectedCountingMachine) CaptureAssignmentPage(request clusterqueue.AssignmentRequest) (func() ([]byte, error), int, error) {
	m.portable.Add(1)
	return m.ClusterMachine.CaptureAssignmentPage(request)
}

func TestSelectedAssignmentNormalEntryOneSelectionOnSaturationAndPromotion(t *testing.T) {
	cache := clusterqueue.NewShardedHotPayloadCache()
	machine := &selectedCountingMachine{ClusterMachine: NewClusterHotPayloadAssignmentMachine(cache.ForShard(1))}
	node, err := cluster.OpenLocal(cluster.Config{Directory: t.TempDir(), Shards: 1, ProcessDurability: true, ExperimentalAssignments: true, NewMachine: func(uint64) clusterraft.Machine { return machine }})
	require.NoError(t, err)
	defer node.Close()
	b := NewClusterBroker(node)
	const queue = "selected-once"
	_, err = b.DeclareQueue(queue, true, false, false, nil)
	require.NoError(t, err)
	body := bytes.Repeat([]byte("selected-body"), 6000)
	require.NoError(t, b.PublishMessage("", queue, &protocol.Message{Body: body}))
	consumer := &protocol.Consumer{ID: "selected", Tag: "selected", Queue: queue, PrefetchCount: 1, Messages: make(chan *protocol.Delivery, 1), Cancel: make(chan struct{})}
	require.NoError(t, b.registerConsumer(queue, consumer.ID, consumer))
	defer b.UnregisterConsumer(consumer.ID)
	b.mu.Lock()
	request := b.consumers[consumer.ID].session.assignmentRequest(0)
	b.mu.Unlock()
	bad := request
	bad.Owner += "-wrong"
	_, through, err := node.ReadAssignmentPage(t.Context(), bad)
	require.ErrorIs(t, err, clusterqueue.ErrSessionOwner)
	require.Equal(t, request.After, through)
	require.EqualValues(t, 1, machine.selected.Load())
	require.Zero(t, machine.portable.Load())
	var held [][]clusterqueue.Delivery
	var expected []byte
	for range 64 {
		prior := machine.selected.Load()
		items, through, err := node.ReadAssignmentPage(t.Context(), request)
		require.NoError(t, err)
		require.EqualValues(t, 1, through)
		require.Len(t, items, 1)
		require.NotNil(t, items[0].BodyLease)
		require.Equal(t, prior+1, machine.selected.Load(), "one page makes exactly one authorized selection")
		if expected == nil {
			expected = bytes.Clone(items[0].Body)
		}
		held = append(held, items)
	}
	defer func() {
		for _, items := range held {
			clusterqueue.ReleaseDeliveryBodies(items)
		}
	}()
	prior := machine.selected.Load()
	copied, through, err := node.ReadAssignmentPage(t.Context(), request)
	require.NoError(t, err)
	require.EqualValues(t, 1, through)
	require.Len(t, copied, 1)
	require.Nil(t, copied[0].BodyLease)
	require.Equal(t, expected, copied[0].Body)
	require.Equal(t, prior+1, machine.selected.Load(), "loan saturation must materialize the same selection, without a portable retry")
	require.Zero(t, machine.portable.Load())
	copied[0].Body[0] ^= 1
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := listener.Addr().String()
	require.NoError(t, listener.Close())
	require.NoError(t, node.ActivatePeers(cluster.Config{Peer: cluster.Peer{Address: address}, Token: base64.RawURLEncoding.EncodeToString(make([]byte, 32)), AllowInsecure: true}))
	require.Equal(t, expected, held[0][0].Body, "an outstanding body lease must survive the actual activation fence")
	require.NotNil(t, held[0][0].BodyLease)
	prior = machine.selected.Load()
	portable, through, err := node.ReadAssignmentPage(t.Context(), request)
	require.NoError(t, err)
	require.EqualValues(t, 1, through)
	require.Equal(t, expected, portable[0].Body)
	require.Nil(t, portable[0].BodyLease)
	require.Equal(t, prior, machine.selected.Load())
	require.EqualValues(t, 1, machine.portable.Load(), "promotion uses the ordinary one-fence portable route")
}

type busySelectedRuntime struct {
	calls    atomic.Int32
	requests chan clusterqueue.AttemptID
	body     []byte
}

func (p *busySelectedRuntime) Submit(context.Context, string, []byte) ([]byte, error) {
	return nil, clusterraft.ErrBusy
}
func (p *busySelectedRuntime) SupportsAssignments() bool { return true }
func (p *busySelectedRuntime) WaitAssignmentReady(ctx context.Context, _ clusterqueue.AssignmentRequest) error {
	<-ctx.Done()
	return ctx.Err()
}
func (p *busySelectedRuntime) ReadAssignmentPage(ctx context.Context, request clusterqueue.AssignmentRequest) ([]clusterqueue.Delivery, clusterqueue.AttemptID, error) {
	p.requests <- request.After
	if request.After >= 1 {
		<-ctx.Done()
		return nil, request.After, ctx.Err()
	}
	if p.calls.Add(1) == 1 {
		return nil, request.After, clusterraft.ErrBusy
	}
	return []clusterqueue.Delivery{{MessageID: 1, Attempt: 1, Body: p.body}}, 1, nil
}

func TestSelectedAssignmentDispatcherRetriesBusyWithoutCursorAdvanceForBigFirst(t *testing.T) {
	body := bytes.Repeat([]byte{0x71}, 2<<20)
	header, err := encodeClusterMessageHeader(&protocol.Message{Body: body})
	require.NoError(t, err)
	peer := &busySelectedRuntime{requests: make(chan clusterqueue.AttemptID, 8), body: append(header, body...)}
	b := NewClusterBroker(peer)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	session := &clusterSession{broker: b, ctx: ctx, cancel: cancel, queue: "q", incarnation: 1, id: 1, owner: "owner", autoAssign: true, creditWake: make(chan struct{}, 1)}
	consumer := &protocol.Consumer{ID: "big-first", Messages: make(chan *protocol.Delivery, 1), Cancel: make(chan struct{})}
	entry := &clusterConsumer{session: session, protocol: consumer}
	entry.credit.Store(1)
	b.consumers[consumer.ID] = entry
	done := make(chan struct{})
	go func() { b.consumeManualBatch(consumer.ID, entry); close(done) }()
	var delivery *protocol.Delivery
	select {
	case delivery = <-consumer.Messages:
	case <-time.After(5 * time.Second):
		t.Fatal("busy result stranded the oversized first delivery")
	}
	require.Len(t, delivery.Message.Body, len(body))
	require.Equal(t, body, delivery.Message.Body)
	require.Zero(t, <-peer.requests)
	require.Zero(t, <-peer.requests, "the busy capture must not advance the dispatcher cursor")
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("dispatcher did not cancel")
	}
	require.EqualValues(t, 1, session.assignmentCursor)
}
