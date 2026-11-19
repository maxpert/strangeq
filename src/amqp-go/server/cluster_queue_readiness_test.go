package server

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/internal/clusterqueue"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
)

type queueReadinessEvent struct {
	kind        string
	queue       string
	incarnation clusterqueue.QueueID
}

type clusterQueueReadinessFixture struct {
	state          localClusterMachine
	events         chan queueReadinessEvent
	emptyReply     chan struct{}
	firstEmpty     atomic.Bool
	firstWaitReply chan error
	firstWait      atomic.Bool
	ready          chan struct{}
	readyOnce      sync.Once
}

func (f *clusterQueueReadinessFixture) Submit(ctx context.Context, queue string, request []byte) ([]byte, error) {
	wire, err := f.state.Submit(ctx, queue, request)
	if err != nil {
		return nil, err
	}
	command, err := clusterqueue.DecodeCommand(request)
	if err != nil {
		return nil, err
	}
	switch command.Kind {
	case clusterqueue.PublishMessage:
		result, err := clusterqueue.DecodeResponse(wire)
		if err != nil {
			return nil, err
		}
		if result.Err() == nil {
			f.readyOnce.Do(func() { close(f.ready) })
		}
	case clusterqueue.ClaimBatch:
		result, err := clusterqueue.DecodeResponse(wire)
		if err != nil {
			return nil, err
		}
		event := queueReadinessEvent{kind: "claim", queue: queue, incarnation: command.Incarnation}
		empty := errors.Is(result.Err(), clusterqueue.ErrEmpty)
		if empty {
			event.kind = "empty claim"
		}
		select {
		case f.events <- event:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		if empty && f.firstEmpty.CompareAndSwap(false, true) {
			select {
			case <-f.emptyReply:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}
	return wire, nil
}

// Model a durable ready condition that is visible even when the publish
// applied before this consumer began waiting.
func (f *clusterQueueReadinessFixture) WaitQueueReady(ctx context.Context, queue string, incarnation clusterqueue.QueueID) error {
	select {
	case f.events <- queueReadinessEvent{kind: "wait", queue: queue, incarnation: incarnation}:
	case <-ctx.Done():
		return ctx.Err()
	}
	if f.firstWaitReply != nil && f.firstWait.CompareAndSwap(false, true) {
		select {
		case err := <-f.firstWaitReply:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	select {
	case <-f.ready:
		return nil
	case <-ctx.Done():
		f.events <- queueReadinessEvent{kind: "wait canceled", queue: queue, incarnation: incarnation}
		return ctx.Err()
	}
}

func TestClusterManualConsumerWaitsForAppliedQueueReadiness(t *testing.T) {
	for _, tc := range []struct {
		name              string
		publishBeforeWait bool
		cancelWhileParked bool
		waitError         error
		retryIdleWait     bool
	}{
		{name: "published_after_wait"},
		{name: "published_before_wait", publishBeforeWait: true},
		{name: "cancel_wait", cancelWhileParked: true},
		{name: "unavailable_wait_falls_back_to_claim", waitError: errors.New("readiness request unavailable")},
		{name: "transport_timeout_falls_back_to_claim", waitError: context.DeadlineExceeded},
		{name: "healthy_idle_retries_wait", waitError: clusterqueue.ErrEmpty, retryIdleWait: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const queue = "jobs"
			fixture := &clusterQueueReadinessFixture{events: make(chan queueReadinessEvent, 8),
				emptyReply: make(chan struct{}), ready: make(chan struct{})}
			if tc.waitError != nil {
				fixture.firstWaitReply = make(chan error, 1)
			}
			var replyOnce sync.Once
			releaseEmpty := func() { replyOnce.Do(func() { close(fixture.emptyReply) }) }
			consumerBroker, publisher := NewClusterBroker(fixture), NewClusterBroker(fixture)
			declared, err := consumerBroker.DeclareQueue(queue, true, false, false, nil)
			require.NoError(t, err)
			incarnation := clusterqueue.QueueID(declared.Ordinal)
			consumer := &protocol.Consumer{ID: "reader", PrefetchCount: 1,
				Messages: make(chan *protocol.Delivery, 1), Cancel: make(chan struct{})}
			require.NoError(t, consumerBroker.RegisterConsumer(queue, consumer.ID, consumer))
			t.Cleanup(func() {
				releaseEmpty()
				require.NoError(t, consumerBroker.UnregisterConsumer(consumer.ID))
			})
			expectEvent := func(want string) {
				t.Helper()
				select {
				case event := <-fixture.events:
					require.Equal(t, queueReadinessEvent{kind: want, queue: queue, incarnation: incarnation}, event)
				case <-time.After(2 * time.Second):
					t.Fatalf("consumer did not report %q", want)
				}
			}
			publish := func() {
				t.Helper()
				require.NoError(t, publisher.PublishMessage("", queue, &protocol.Message{Body: []byte("ready")}))
			}

			expectEvent("empty claim")
			published := tc.publishBeforeWait
			if published {
				publish()
			}
			releaseEmpty()
			expectEvent("wait")
			if tc.cancelWhileParked {
				require.NoError(t, consumerBroker.UnregisterConsumer(consumer.ID))
				expectEvent("wait canceled")
				return
			}
			if tc.waitError != nil {
				if tc.retryIdleWait {
					fixture.firstWaitReply <- tc.waitError
					expectEvent("wait")
				} else {
					publish()
					published = true
					fixture.firstWaitReply <- tc.waitError
				}
			}
			if !published {
				publish()
			}
			expectEvent("claim")
			select {
			case delivery := <-consumer.Messages:
				require.Equal(t, "ready", string(delivery.Message.Body))
				require.NotZero(t, delivery.DeliveryTag, "the applied claim owns the message before handoff")
			case <-time.After(2 * time.Second):
				t.Fatal("consumer did not deliver after the applied readiness hint")
			}
		})
	}
}
