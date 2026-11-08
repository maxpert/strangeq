package broker

import (
	"runtime"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/maxpert/amqp-go/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDepthHighWM_Respected(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := storage.NewDisruptorStorageWithDataDir(tmpDir)
	require.NoError(t, err)
	defer store.Close()

	broker := NewStorageBroker(store, interfaces.EngineConfig{
		DepthHighWM: 1000,
	})
	defer broker.Close()

	assert.Equal(t, uint64(1000), broker.computeDepthHighWM())
}

func TestDepthHighWM_ZeroFallsBack(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := storage.NewDisruptorStorageWithDataDir(tmpDir)
	require.NoError(t, err)
	defer store.Close()

	broker := NewStorageBroker(store, interfaces.EngineConfig{})
	defer broker.Close()

	assert.Equal(t, uint64(52428), broker.computeDepthHighWM())
}

// TestComputeDepthHighWM_ConfiguredAndZeroValue pins the resolver itself: the
// configured DepthHighWM is returned verbatim, and a zero-value EngineConfig
// resolves to interfaces.DefaultDepthHighWM (52428).
func TestComputeDepthHighWM_ConfiguredAndZeroValue(t *testing.T) {
	zeroStore, err := storage.NewDisruptorStorageWithDataDir(t.TempDir())
	require.NoError(t, err)
	defer zeroStore.Close()
	zero := NewStorageBroker(zeroStore, interfaces.EngineConfig{})
	defer zero.Close()
	assert.Equal(t, interfaces.DefaultDepthHighWM, zero.computeDepthHighWM())

	cfgStore, err := storage.NewDisruptorStorageWithDataDir(t.TempDir())
	require.NoError(t, err)
	defer cfgStore.Close()
	configured := NewStorageBroker(cfgStore, interfaces.EngineConfig{DepthHighWM: 12345})
	defer configured.Close()
	assert.Equal(t, uint64(12345), configured.computeDepthHighWM())
}

// TestDepthHighWM_GatesAtConfiguredDepth proves the configured depth is wired
// into publisher backpressure, rather than merely computed: HWM-1 is admitted,
// HWM is gated, and an ack releases the parked producer.
//
// Deliberate coverage removal: this no longer compares end-to-end p50 latency
// for shallow and deep backlogs. It therefore gives up the empirical claim
// "deeper backlog has higher p50", including that scenario's incidental timing
// sensitivity to FIFO/drain behavior. That relative-performance assertion was
// load-dependent and is not retained in the pass/fail suite.
func TestDepthHighWM_GatesAtConfiguredDepth(t *testing.T) {
	const (
		hwm                 = uint64(3)
		unreachableFallback = 10 * time.Minute
	)

	ec := interfaces.EngineConfig{
		DepthHighWM: hwm,
	}
	store, err := storage.NewDisruptorStorageWithEngineConfig(t.TempDir(), ec)
	require.NoError(t, err)
	defer store.Close()

	broker := NewStorageBroker(store, ec)
	defer broker.Close()
	_, err = broker.DeclareQueue("hwm-q", true, false, false, nil)
	require.NoError(t, err)
	qs := broker.getOrCreateQueueState("hwm-q")

	require.Equal(t, hwm, qs.DepthHighWM(),
		"DepthHighWM did not reach the queue's production predicate")
	qs.SetCapacityFallback(unreachableFallback)
	require.Equal(t, unreachableFallback, qs.capacityFallback(),
		"fixture premise broken: capacity fallback was not moved out of reach")

	stop, cancel := makeStop()
	defer cancel()
	addInflight := func() uint64 {
		tag := qs.FrontierReserve()
		qs.FrontierComplete(tag, true)
		timer := testTimer(qs)
		defer timer.Stop()
		claimed, _, ok := qs.Claim(stop, timer)
		require.True(t, ok, "fixture failed to claim reserved tag %d", tag)
		qs.ClaimInflight(claimed)
		return claimed
	}

	ackTag := addInflight()
	_ = addInflight()
	require.Equal(t, hwm-1, qs.Depth(),
		"fixture premise broken: expected depth HWM-1")
	require.False(t, qs.AtHighWaterMark(),
		"AtHighWaterMark must be false one message below the override")

	_ = addInflight()
	require.Equal(t, hwm, qs.Depth(),
		"fixture premise broken: expected depth HWM")
	require.True(t, qs.AtHighWaterMark(),
		"AtHighWaterMark must be true at the override")

	publishDone := make(chan error, 1)
	go func() {
		publishDone <- broker.PublishMessage("", "hwm-q", &protocol.Message{
			Body:         []byte("blocked-at-hwm"),
			RoutingKey:   "hwm-q",
			DeliveryMode: 1,
		})
	}()
	requireSynchronousPublishParked(t, qs, publishDone)

	qs.AckAdvance(ackTag)
	select {
	case err := <-publishDone:
		require.NoError(t, err, "synchronous publish was refused instead of completing after ack released capacity")
	case <-time.After(5 * time.Second):
		t.Fatal("ack never released the synchronous publish parked at DepthHighWM; the 10-minute fallback was unreachable")
	}
}

// requireSynchronousPublishParked proves the production PublishMessage path,
// not a direct QueueState call, reached WaitForCapacity and registered itself.
// Its clock is only a generous never-happened bound.
func requireSynchronousPublishParked(t *testing.T, qs *QueueState, completed <-chan error) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for qs.producerParked.Load() == 0 {
		select {
		case err := <-completed:
			t.Fatalf("synchronous PublishMessage returned (%v) without registering a producer park; production publish must traverse WaitForCapacity", err)
		case <-deadline.C:
			t.Fatal("synchronous PublishMessage never registered a producer park at DepthHighWM")
		default:
			runtime.Gosched()
		}
	}
}
