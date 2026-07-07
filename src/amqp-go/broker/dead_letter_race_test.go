package broker

import (
	"sync"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
)

// TestDeadLetter_ConcurrentRejects_NoRaceNoLoss hammers the single-reject DLX
// path from many goroutines at once (basic.get reject, requeue=false) into a
// shared dead-letter target. It is meant to be run under -race: it stresses the
// non-blocking republish, the shared routeScratch pool, and the global delivery
// tag under contention, and asserts every rejected message is dead-lettered
// exactly once (no loss, no duplication on the happy path).
func TestDeadLetter_ConcurrentRejects_NoRaceNoLoss(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	setupDLX(t, b, dlxOpts{
		exchange: "dlx", exchangeKind: "direct", targets: []string{"dlq"}, bindKey: "src",
		sourceArgs: map[string]interface{}{"x-dead-letter-exchange": "dlx"},
	})

	const n = 200
	for i := 0; i < n; i++ {
		publishToSource(t, b, "src", "m")
	}

	// Each worker pulls one basic.get delivery and rejects it (requeue=false).
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			msg, tag, _, err := b.GetMessageForGet("src", false)
			if err != nil || msg == nil {
				return
			}
			_ = b.RejectGetDelivery(tag, false)
		}()
	}
	wg.Wait()

	require.Equal(t, n, countInQueue(t, b, "dlq"),
		"every concurrently-rejected message must be dead-lettered exactly once")
}

// TestDeadLetter_ConcurrentMultiNack_NoRace stresses the batched multi-nack
// fan-out (discardNackedBatch spawns a goroutine per message) across several
// consumers on distinct queues concurrently. Run under -race to catch data
// races in the concurrent deadLetter republish.
func TestDeadLetter_ConcurrentMultiNack_NoRace(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	// Shared DLX + target.
	require.NoError(t, b.DeclareExchange("dlx", "fanout", false, false, false, nil))
	_, err := b.DeclareQueue("dlq", false, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, b.BindQueue("dlq", "dlx", "", nil))

	const (
		consumers   = 4
		perConsumer = 30
	)
	names := []string{"s0", "s1", "s2", "s3"}
	var wg sync.WaitGroup
	for ci := 0; ci < consumers; ci++ {
		q := names[ci]
		_, err := b.DeclareQueue(q, false, false, false, map[string]interface{}{
			"x-dead-letter-exchange": "dlx",
		})
		require.NoError(t, err)

		c := registerManualConsumer(t, b, q, "c-"+q, perConsumer, perConsumer)
		for i := 0; i < perConsumer; i++ {
			require.NoError(t, b.PublishMessage("", q, &protocol.Message{Exchange: "", RoutingKey: q, Body: []byte("m")}))
		}
		ds := recvDeliveries(t, c, perConsumer)
		var maxTag uint64
		for _, d := range ds {
			if d.DeliveryTag > maxTag {
				maxTag = d.DeliveryTag
			}
		}
		wg.Add(1)
		go func(tag uint64, tag2 string) {
			defer wg.Done()
			_ = b.NacknowledgeMessage("c-"+tag2, tag, true, false)
		}(maxTag, q)
	}

	doneCh := make(chan struct{})
	go func() { wg.Wait(); close(doneCh) }()
	select {
	case <-doneCh:
	case <-time.After(10 * time.Second):
		t.Fatal("concurrent multi-nack did not complete in time")
	}

	require.Equal(t, consumers*perConsumer, countInQueue(t, b, "dlq"),
		"all multi-nacked messages across consumers must be dead-lettered")
}
