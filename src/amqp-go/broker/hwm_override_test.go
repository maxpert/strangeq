package broker

import (
	"encoding/binary"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	hdrhistogram "github.com/HdrHistogram/hdrhistogram-go"
	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/maxpert/amqp-go/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDepthHighWMOverride_Respected(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := storage.NewDisruptorStorageWithDataDir(tmpDir)
	require.NoError(t, err)
	defer store.Close()

	broker := NewStorageBroker(store, interfaces.EngineConfig{
		RingBufferSize:        65536,
		SpillThresholdPercent: 80,
		DepthHighWMOverride:   1000,
	})
	defer broker.Close()

	assert.Equal(t, uint64(1000), broker.computeDepthHighWM())
}

func TestDepthHighWMOverride_ZeroFallsBack(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := storage.NewDisruptorStorageWithDataDir(tmpDir)
	require.NoError(t, err)
	defer store.Close()

	broker := NewStorageBroker(store, interfaces.EngineConfig{
		RingBufferSize:        65536,
		SpillThresholdPercent: 80,
	})
	defer broker.Close()

	assert.Equal(t, uint64(52428), broker.computeDepthHighWM())
}

func TestDepthHighWMOverride_LatencyProportional(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping latency test in short mode")
	}

	const timestampedCount = 2000

	runScenario := func(hwmOverride uint64) time.Duration {
		tmpDir := t.TempDir()
		ec := interfaces.EngineConfig{
			RingBufferSize:        65536,
			SpillThresholdPercent: 80,
			DepthHighWMOverride:   hwmOverride,
			WALSyncDisabled:       true,
			WALBatchSize:          1000,
			WALBatchTimeoutMS:     5,
		}
		store, err := storage.NewDisruptorStorageWithEngineConfig(
			tmpDir, storage.DefaultCheckpointInterval, ec)
		require.NoError(t, err)
		defer store.Close()

		broker := NewStorageBroker(store, ec)
		defer broker.Close()

		_, err = broker.DeclareQueue("lat-q", true, false, false, nil)
		require.NoError(t, err)
		store.RegisterConsumerCursor("lat-q", "lat-cons")
		qs := broker.getOrCreateQueueState("lat-q")

		// Pre-fill the queue to HWM depth so timestamped messages sit
		// behind a backlog proportional to HWM. The 1-byte body lets the
		// consumer distinguish pre-fill from timestamped messages.
		fillBody := []byte{0}
		for i := uint64(0); i < hwmOverride; i++ {
			msg := &protocol.Message{
				Body:         fillBody,
				Exchange:     "",
				RoutingKey:   "lat-q",
				DeliveryMode: 2,
			}
			if err := broker.PublishMessage("", "lat-q", msg); err != nil {
				t.Fatalf("pre-fill publish %d: %v", i, err)
			}
		}

		stop, cancel := makeStop()
		defer cancel()

		hist := hdrhistogram.New(1, 30_000_000, 3)
		var processed atomic.Int64
		totalMsgs := int64(hwmOverride) + int64(timestampedCount)

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			timer := time.NewTimer(qs.parkTimeout)
			defer timer.Stop()
			for processed.Load() < totalMsgs {
				tag, _, ok := qs.Claim(stop, timer)
				if !ok {
					return
				}
				msg, err := store.GetMessage("lat-q", tag)
				if err != nil {
					qs.GapSkipAdvance(tag)
					processed.Add(1)
					continue
				}
				if len(msg.Body) >= 8 {
					publishNano := int64(binary.LittleEndian.Uint64(msg.Body[:8]))
					latency := time.Since(time.Unix(0, publishNano))
					if us := latency.Microseconds(); us > 0 {
						_ = hist.RecordValue(us)
					}
				}
				qs.ClaimInflight(tag)
				store.DeliverToConsumer("lat-q", "lat-cons", tag)
				store.DeleteMessage("lat-q", tag)
				store.AckFromConsumer("lat-q", "lat-cons", tag)
				qs.SetMinAckCursor(store.GetMinAckCursor("lat-q"))
				qs.AckAdvance(tag)
				processed.Add(1)
			}
		}()

		for i := 0; i < timestampedCount; i++ {
			body := make([]byte, 8)
			binary.LittleEndian.PutUint64(body, uint64(time.Now().UnixNano()))
			msg := &protocol.Message{
				Body:         body,
				Exchange:     "",
				RoutingKey:   "lat-q",
				DeliveryMode: 2,
			}
			if err := broker.PublishMessage("", "lat-q", msg); err != nil {
				cancel()
				t.Fatalf("timestamped publish %d: %v", i, err)
			}
		}

		wg.Wait()
		return time.Duration(hist.ValueAtQuantile(50)) * time.Microsecond
	}

	p50Low := runScenario(1000)
	p50High := runScenario(50000)

	t.Logf("p50 latency: HWM=1000 -> %v, HWM=50000 -> %v", p50Low, p50High)

	assert.Less(t, p50Low, p50High, "lower HWM should yield lower p50 latency")
}
