package broker

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
)

// tx_frontier_race_test.go — guard for the transactional publish sibling of the
// frontier strand (always-reserve-at-mint iteration).
//
// THE BUG (pre-fix): PublishMessageTx mints its delivery tag lock-free
// (b.globalDeliveryTag.Add(1), NOT through FrontierReserve). The tag is staged
// in a transaction buffer. After commit, a deferred closure makes the tag
// visible with the old 3-way check (FrontierPublishTransient if frontier-active,
// else Publish). A concurrent PublishMessage does FrontierReserve(H > L),
// stores, FrontierComplete(H, true) — head advances past L. The consumer claims
// L, GetMessage misses (the tx message is still in the buffer, not real storage),
// and gap-skips it. When the tx commits and the deferred closure runs
// FrontierPublishTransient(L), waiting.Add(1) fires for a tag the consumer's
// tail already passed — a permanent WaitingCount leak.
//
// THE FIX: PublishMessageTx now reserves the tag on the frontier at STAGE TIME
// via FrontierReserve (minting inside the callback, same as PublishMessage). The
// deferred closure calls FrontierComplete(tag, true) on commit (message is
// durable/stored) or FrontierComplete(tag, false) on abort (release the frontier
// slot, message never stored). This closes the race: a concurrent publish's
// FrontierComplete can never advance head past the tx's still-pending tag, so
// the consumer can never claim and gap-skip it before commit.

// TestFrontierFlip_TxSibling_NoStrand reproduces the tx-path race
// deterministically: a tx publish (staged via ExecuteAtomic's buffered
// txnStore, NOT yet in real storage) races a concurrent non-tx PublishMessage
// to the same queue with a live consumer. Pre-fix, the consumer gap-skips the
// tx tag (not yet committed) and the post-commit visibility closure strands it
// with a permanent WaitingCount leak. Post-fix, FrontierReserve at stage time
// holds head at the tx tag until commit, so the consumer never claims it
// early. After full drain, WaitingCount and InflightCount must both be zero.
//
// The reproduction is deterministic (no probabilistic race): the buffered
// txnStore keeps the tx message out of real storage until ExecuteAtomic
// commits, and the concurrent PublishMessage + consumer drain both run inside
// the callback — before the post-commit deferred closures fire.
func TestFrontierFlip_TxSibling_NoStrand(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	const qname = "q"
	_, err := b.DeclareQueue(qname, true, false, false, nil)
	require.NoError(t, err)
	qs := b.getOrCreateQueueState(qname)

	msgs := make(chan *protocol.Delivery, 256)
	var delivered atomic.Int64
	drainerDone := make(chan struct{})
	go func() {
		defer close(drainerDone)
		for range msgs {
			delivered.Add(1)
		}
	}()
	cons := &protocol.Consumer{Tag: "c", Queue: qname, NoAck: true, Messages: msgs}
	require.NoError(t, b.RegisterConsumer(qname, "c", cons))

	var deferred []func(bool)
	txMsg := &protocol.Message{RoutingKey: qname, Body: []byte("t"), DeliveryMode: 1}
	err = b.storage.ExecuteAtomic(func(txnStore interfaces.Storage) error {
		d, err := b.PublishMessageTx(txnStore, "", qname, txMsg)
		if err != nil {
			return err
		}
		deferred = d

		racingMsg := &protocol.Message{RoutingKey: qname, Body: []byte("x"), DeliveryMode: 1}
		if err := b.PublishMessage("", qname, racingMsg); err != nil {
			return err
		}

		require.Eventually(t, func() bool {
			return qs.tail.Load() >= qs.head.Load()
		}, 10*time.Second, 100*time.Microsecond, "consumer did not drain the visible range")

		return nil
	})
	require.NoError(t, err)

	for _, f := range deferred {
		f(true)
	}

	require.Eventually(t, func() bool {
		return qs.tail.Load() >= qs.head.Load()
	}, 10*time.Second, time.Millisecond, "consumer did not catch up after post-commit visibility")

	require.NoError(t, b.UnregisterConsumer("c"))
	close(msgs)
	<-drainerDone

	require.Zero(t, qs.WaitingCount(), "tx path must not leak WaitingCount (transient-vs-tx race)")
	require.Zero(t, qs.InflightCount(), "tx path must not leak InflightCount")
}

// TestFrontierFlip_TxAbort_ReleasesFrontierSlot verifies the abort path: when a
// transaction is rolled back after staging, the deferred closure must call
// FrontierComplete(tag, false) to release the frontier slot. Without this, the
// tag stays pending on the frontier forever, blocking all later publishes to the
// queue and leaking the slot. After abort, the consumer claims the released tag,
// GetMessage misses (rolled back, not in real storage), and gap-skips it — no
// leak.
func TestFrontierFlip_TxAbort_ReleasesFrontierSlot(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	const qname = "q"
	_, err := b.DeclareQueue(qname, true, false, false, nil)
	require.NoError(t, err)
	qs := b.getOrCreateQueueState(qname)

	msgs := make(chan *protocol.Delivery, 256)
	var delivered atomic.Int64
	drainerDone := make(chan struct{})
	go func() {
		defer close(drainerDone)
		for range msgs {
			delivered.Add(1)
		}
	}()
	cons := &protocol.Consumer{Tag: "c", Queue: qname, NoAck: true, Messages: msgs}
	require.NoError(t, b.RegisterConsumer(qname, "c", cons))

	var deferred []func(bool)
	txMsg := &protocol.Message{RoutingKey: qname, Body: []byte("t"), DeliveryMode: 1}
	err = b.storage.ExecuteAtomic(func(txnStore interfaces.Storage) error {
		d, err := b.PublishMessageTx(txnStore, "", qname, txMsg)
		if err != nil {
			return err
		}
		deferred = d
		return errors.New("test abort")
	})
	require.Error(t, err)

	for _, f := range deferred {
		f(false)
	}

	require.Eventually(t, func() bool {
		return qs.tail.Load() >= qs.head.Load()
	}, 10*time.Second, time.Millisecond, "consumer did not drain after tx abort")

	require.NoError(t, b.UnregisterConsumer("c"))
	close(msgs)
	<-drainerDone

	require.Zero(t, qs.WaitingCount(), "tx abort must not leak WaitingCount")
	require.Zero(t, qs.InflightCount(), "tx abort must not leak InflightCount")
}
