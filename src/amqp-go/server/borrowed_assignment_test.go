package server

import (
	"bytes"
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/internal/cluster"
	"github.com/maxpert/amqp-go/internal/clusterqueue"
	"github.com/maxpert/amqp-go/internal/clusterraft"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type countedBorrowedLease struct{ calls atomic.Int32 }

func (l *countedBorrowedLease) Release() { l.calls.Add(1) }

func borrowedFixtureDelivery(body []byte, lease *countedBorrowedLease) *protocol.Delivery {
	message := &protocol.Message{Body: body}
	message.SetBodyRelease(lease.Release)
	return &protocol.Delivery{Message: message}
}

func TestBorrowedBodyForwarderStopReleasesHeldAndBufferedItems(t *testing.T) {
	for _, flow := range []bool{true, false} {
		name := "flow-park"
		if flow {
			name = "fan-in-park"
		}
		t.Run(name, func(t *testing.T) {
			channel := protocol.NewChannel(1, nil)
			channel.FlowActive.Store(flow)
			messages := make(chan *protocol.Delivery, 2)
			fanIn := make(chan *protocol.Delivery)
			stop, done := make(chan struct{}), make(chan struct{})
			first, second := &countedBorrowedLease{}, &countedBorrowedLease{}
			messages <- borrowedFixtureDelivery([]byte("first"), first)
			go func() { forwardConsumerMessages(channel, messages, fanIn, stop); close(done) }()
			require.Eventually(t, func() bool { return len(messages) == 0 }, time.Second, time.Millisecond)
			messages <- borrowedFixtureDelivery([]byte("buffered"), second)
			close(stop)
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("forwarder did not stop")
			}
			drainConsumerBodyLeases(messages)
			require.EqualValues(t, 1, first.calls.Load())
			require.EqualValues(t, 1, second.calls.Load())
		})
	}
}

func TestBorrowedBodySynchronousWriterAndUnsentExtraRelease(t *testing.T) {
	for _, fail := range []bool{false, true} {
		name := "success"
		if fail {
			name = "write-failure"
		}
		t.Run(name, func(t *testing.T) {
			client, socket := net.Pipe()
			defer client.Close()
			defer socket.Close()
			conn := protocol.NewConnection(socket)
			srv := &Server{Log: zap.NewNop(), Config: config.DefaultConfig()}
			lease := &countedBorrowedLease{}
			body := bytes.Repeat([]byte("borrowed-write-body"), 4096)
			delivery := &protocol.Delivery{Message: &protocol.Message{Body: body}, DeliveryTag: 1, ConsumerID: "reader"}
			delivery.Message.SetBodyRelease(lease.Release)
			if fail {
				require.NoError(t, client.Close())
			} else {
				_, _ = newRecordingReader(client)
			}
			err := srv.sendBatchedDeliveries(conn, 1, "reader", []*protocol.Delivery{delivery})
			if fail {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.EqualValues(t, 1, lease.calls.Load())
			require.Nil(t, delivery.Message.Body)
		})
	}
	srv := &Server{Log: zap.NewNop()}
	lease := &countedBorrowedLease{}
	extra := &protocol.Delivery{Message: &protocol.Message{Body: []byte("removed")}, ConsumerID: "removed"}
	extra.Message.SetBodyRelease(lease.Release)
	require.True(t, srv.sendExtraDeliveries(nil, map[string]*consumerInfo{}, []*protocol.Delivery{extra}))
	require.EqualValues(t, 1, lease.calls.Load())
}

type heldBorrowedPageRuntime struct {
	started chan struct{}
	release chan struct{}
	lease   *countedBorrowedLease
	body    []byte
}

func (p *heldBorrowedPageRuntime) Submit(context.Context, string, []byte) ([]byte, error) {
	return nil, errors.New("unused")
}
func (p *heldBorrowedPageRuntime) SupportsAssignments() bool { return true }
func (p *heldBorrowedPageRuntime) ReadAssignmentPage(context.Context, clusterqueue.AssignmentRequest) ([]clusterqueue.Delivery, clusterqueue.AttemptID, error) {
	close(p.started)
	<-p.release
	body := p.body
	if body == nil {
		body = []byte("loan")
	}
	return []clusterqueue.Delivery{{Attempt: 1, Body: body, BodyLease: p.lease}}, 1, nil
}
func (p *heldBorrowedPageRuntime) WaitAssignmentReady(context.Context, clusterqueue.AssignmentRequest) error {
	return nil
}

func TestBorrowedAssignmentPageSupersededAndCancelledBeforeHandoff(t *testing.T) {
	peer := &heldBorrowedPageRuntime{started: make(chan struct{}), release: make(chan struct{}), lease: &countedBorrowedLease{}}
	b := NewClusterBroker(peer)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	session := &clusterSession{broker: b, ctx: ctx, autoAssign: true, queue: "q", id: 1, incarnation: 1, owner: "owner"}
	entry := &clusterConsumer{session: session, protocol: &protocol.Consumer{Cancel: make(chan struct{})}}
	done := make(chan error, 1)
	go func() {
		items, _, err := entry.nextManualPage(context.Background(), 0)
		if err == nil && (len(items) != 1 || string(items[0].Body) != "portable") {
			err = errors.New("wrong replacement page")
		}
		done <- err
	}()
	<-peer.started
	b.mu.Lock()
	session.assignmentPage = bufferedAssignmentPage{through: 1, deliveries: []clusterqueue.Delivery{{Attempt: 1, Body: []byte("portable")}}}
	b.mu.Unlock()
	close(peer.release)
	require.NoError(t, <-done)
	require.EqualValues(t, 1, peer.lease.calls.Load())
	lease := &countedBorrowedLease{}
	cancel()
	b.mu.Lock()
	_, _, err := entry.acceptAssignmentPage(1, bufferedAssignmentPage{through: 2, deliveries: []clusterqueue.Delivery{{Attempt: 2, Body: []byte("cancelled"), BodyLease: lease}}})
	b.mu.Unlock()
	require.ErrorIs(t, err, context.Canceled)
	require.EqualValues(t, 1, lease.calls.Load())
	require.EqualValues(t, 1, session.assignmentCursor)
}

type observedBorrowedRuntime struct {
	*cluster.Node
	acquired atomic.Int32
	released atomic.Int32
}
type observedBorrowedLease struct {
	inner   clusterqueue.BodyLease
	runtime *observedBorrowedRuntime
	once    atomic.Bool
}

func (l *observedBorrowedLease) Release() {
	if l.once.CompareAndSwap(false, true) {
		l.inner.Release()
		l.runtime.released.Add(1)
	}
}
func (p *observedBorrowedRuntime) ReadAssignmentPage(ctx context.Context, request clusterqueue.AssignmentRequest) ([]clusterqueue.Delivery, clusterqueue.AttemptID, error) {
	items, through, err := p.Node.ReadAssignmentPage(ctx, request)
	if err == nil {
		for i := range items {
			if items[i].BodyLease != nil {
				items[i].BodyLease = &observedBorrowedLease{inner: items[i].BodyLease, runtime: p}
				p.acquired.Add(1)
			}
		}
	}
	return items, through, err
}

func TestBorrowedAssignmentNormalLocalDeliveryAndConnectionClose(t *testing.T) {
	for _, fail := range []bool{false, true} {
		name := "write-completes"
		if fail {
			name = "close-with-unsent-loans"
		}
		t.Run(name, func(t *testing.T) {
			caches := clusterqueue.NewShardedHotPayloadCache()
			node, err := cluster.OpenLocal(cluster.Config{Directory: t.TempDir(), Shards: 1, ProcessDurability: true, ExperimentalAssignments: true, NewMachine: func(id uint64) clusterraft.Machine { return NewClusterHotPayloadAssignmentMachine(caches.ForShard(id)) }})
			require.NoError(t, err)
			defer node.Close()
			peer := &observedBorrowedRuntime{Node: node}
			b := NewClusterBroker(peer)
			const queue = "borrowed-normal"
			_, err = b.DeclareQueue(queue, true, false, false, nil)
			require.NoError(t, err)
			count := 2
			if fail {
				count = 32
			}
			body := bytes.Repeat([]byte("typed-assignment-body"), 3200)
			for range count {
				require.NoError(t, b.PublishMessage("", queue, &protocol.Message{Body: body, DeliveryMode: 2}))
			}
			client, socket := net.Pipe()
			defer client.Close()
			defer socket.Close()
			conn := protocol.NewConnection(socket)
			channel := protocol.NewChannel(1, conn)
			consumer := &protocol.Consumer{ID: "borrowed-consumer", Tag: "borrowed", Queue: queue, Channel: channel, PrefetchCount: uint16(count), Messages: make(chan *protocol.Delivery, 100), Cancel: make(chan struct{})}
			channel.Consumers[consumer.Tag] = consumer
			conn.Channels.Store(uint16(1), channel)
			srv := &Server{Broker: b, Log: zap.NewNop(), Config: config.DefaultConfig()}
			if !fail {
				_, _ = newRecordingReader(client)
			}
			require.NoError(t, b.RegisterConsumer(queue, consumer.ID, consumer))
			conn.ConsumersDirty.Store(true)
			done := make(chan struct{})
			go func() { srv.consumerDeliveryLoop(srv.runtimeFor(conn)); close(done) }()
			require.Eventually(t, func() bool { return peer.acquired.Load() > 0 }, 5*time.Second, time.Millisecond)
			if fail {
				require.NoError(t, client.Close())
			} else {
				require.Eventually(t, func() bool { return peer.released.Load() == int32(count) }, 5*time.Second, time.Millisecond)
				conn.Closed.Store(true)
			}
			require.NoError(t, b.UnregisterConsumer(consumer.ID))
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("delivery loop/forwarders did not join")
			}
			require.Eventually(t, func() bool { return peer.released.Load() == peer.acquired.Load() }, 5*time.Second, time.Millisecond, "all pending/buffered/fan-in/write loans must release after close")
			require.Positive(t, peer.acquired.Load())
		})
	}
}

func TestBorrowedMessageOwnsCancelledPendingLeaseExactlyOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		body := []byte("pending-loan")
		header, err := encodeClusterMessageHeader(&protocol.Message{Body: body})
		require.NoError(t, err)
		peer := &heldBorrowedPageRuntime{started: make(chan struct{}), release: make(chan struct{}), lease: &countedBorrowedLease{}, body: append(header, body...)}
		b := NewClusterBroker(peer)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		session := &clusterSession{broker: b, ctx: ctx, cancel: cancel, queue: "q", incarnation: 1, id: 1, owner: "owner", autoAssign: true, creditWake: make(chan struct{}, 1)}
		consumer := &protocol.Consumer{ID: "pending", Messages: make(chan *protocol.Delivery, 1), Cancel: make(chan struct{})}
		consumer.Messages <- &protocol.Delivery{} // Force an unsent handoff retry.
		entry := &clusterConsumer{session: session, protocol: consumer}
		entry.credit.Store(1)
		b.consumers[consumer.ID] = entry
		done := make(chan struct{})
		go func() { b.consumeManualBatch(consumer.ID, entry); close(done) }()
		<-peer.started
		close(peer.release)
		synctest.Wait() // Decoder transferred its lease; handoff is parked.
		cancel()
		synctest.Wait()
		<-done
		require.EqualValues(t, 1, peer.lease.calls.Load(), "the pending item must no longer duplicate Message's callback ownership")
	})
}
