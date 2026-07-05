package broker

import (
	"fmt"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/maxpert/amqp-go/storage"
	"github.com/stretchr/testify/require"
)

// newBrokerWithPrefetchCap builds a test broker whose EngineConfig sets the
// prefetch-0 gate cap to capValue (0 leaves it unset, exercising the default
// fallback).
func newBrokerWithPrefetchCap(t testing.TB, capValue int) *StorageBroker {
	t.Helper()
	store, err := storage.NewDisruptorStorageWithDataDir(t.TempDir())
	require.NoError(t, err)
	return NewStorageBroker(store, interfaces.EngineConfig{
		RingBufferSize:          65536,
		SpillThresholdPercent:   80,
		ConsumerSelectTimeoutMS: 1,
		ConsumerMaxBatchSize:    100,
		UnlimitedPrefetchCap:    capValue,
	})
}

// TestUnlimitedPrefetchCapHonorsConfig pins §5.3: the prefetch-0 gate cap is a
// named, config-driven constant, and an unset (0) config falls back to the
// documented default rather than a hard-coded magic number.
func TestUnlimitedPrefetchCapHonorsConfig(t *testing.T) {
	cases := []struct {
		name     string
		capValue int
		wantLim  int64
	}{
		{"explicit-1500", 1500, 1500},
		{"explicit-1024", 1024, 1024},
		{"unset-uses-default", 0, defaultUnlimitedPrefetchCap},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			broker := newBrokerWithPrefetchCap(t, tc.capValue)
			broker.DeclareQueue("q", false, false, false, nil)

			consumer := &protocol.Consumer{
				Tag:           "c",
				Queue:         "q",
				NoAck:         false,
				Messages:      make(chan *protocol.Delivery, 8),
				PrefetchCount: 0, // "unlimited" per spec -> the finite cap applies
			}
			if err := broker.RegisterConsumer("q", "c", consumer); err != nil {
				t.Fatalf("RegisterConsumer: %v", err)
			}
			v, ok := broker.activeConsumers.Load("c")
			if !ok {
				t.Fatal("consumer not registered")
			}
			if got := v.(*ConsumerState).gate.limit.Load(); got != tc.wantLim {
				t.Fatalf("prefetch-0 gate limit = %d, want %d", got, tc.wantLim)
			}
		})
	}
}

// TestUnlimitedPrefetchCapBoundsOutstandingAllFlow verifies the Step-4 flow
// requirement: with a finite prefetch-0 cap, a manual-ack consumer whose
// multi-ack cadence is below the cap drains a total far exceeding the cap
// (the cap bounds the OUTSTANDING window, never the total), while the
// outstanding set never exceeds the cap. It runs two cadences — one well below
// the cap and one just below it — to cover "cap comfortably > cadence" and
// "cap only just > cadence"; both must flow. (A cadence >= cap would wedge,
// which is precisely why the cap is configurable: raise it above your coarsest
// ack cadence.)
func TestUnlimitedPrefetchCapBoundsOutstandingAllFlow(t *testing.T) {
	const capValue = 100
	for _, cadence := range []int{40, 80} {
		cadence := cadence
		t.Run(fmt.Sprintf("cadence-%d-under-cap-%d", cadence, capValue), func(t *testing.T) {
			broker := newBrokerWithPrefetchCap(t, capValue)
			qname := fmt.Sprintf("capq-%d", cadence)
			broker.DeclareQueue(qname, false, false, false, nil)

			const total = 900 // 9x the cap: only possible if the cap bounds outstanding, not total
			consumer := &protocol.Consumer{
				Tag:           "cap-consumer",
				Queue:         qname,
				NoAck:         false,
				Messages:      make(chan *protocol.Delivery, total),
				PrefetchCount: 0,
			}
			if err := broker.RegisterConsumer(qname, "cap-consumer", consumer); err != nil {
				t.Fatalf("RegisterConsumer: %v", err)
			}

			for i := 0; i < total; i++ {
				msg := &protocol.Message{Body: []byte(fmt.Sprintf("m-%d", i)), Exchange: "", RoutingKey: qname}
				if err := broker.PublishMessage("", qname, msg); err != nil {
					t.Fatalf("PublishMessage %d: %v", i, err)
				}
			}

			received := 0
			var lastTag uint64
			for received < total {
				select {
				case d := <-consumer.Messages:
					received++
					if d.DeliveryTag > lastTag {
						lastTag = d.DeliveryTag
					}
					if received%cadence == 0 {
						if err := broker.AcknowledgeMessage("cap-consumer", lastTag, true); err != nil {
							t.Fatalf("AcknowledgeMessage at %d: %v", received, err)
						}
						// Outstanding must stay within the configured cap (± the
						// poll loop's single pre-acquired credit).
						assertGateNoLeak(t, broker, qname, "cap-consumer", capValue)
					}
				case <-time.After(10 * time.Second):
					t.Fatalf("delivery stalled at received=%d/%d with cap=%d cadence=%d",
						received, total, capValue, cadence)
				}
			}

			// Drain any remaining (total not divisible by cadence) and confirm
			// the window fully reopened — the cap never capped the total.
			if received != total {
				t.Fatalf("received %d, want %d", received, total)
			}
			// After the final batch ack, the outstanding window must remain
			// within the cap (never capped the total, never exceeded the cap).
			if final := assertGateNoLeak(t, broker, qname, "cap-consumer", capValue); final > capValue {
				t.Fatalf("outstanding %d exceeds cap %d at end", final, capValue)
			}
		})
	}
}
