package storage

import (
	"testing"
	"time"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
)

// Regression tests for the group-commit lone-writer stall.
//
// batchWriterLoop used to hold a sub-BatchSize batch until the BatchTimeout
// ticker fired: a lone synchronous writer paid the full BatchTimeout on every
// WALManager.Write. With the default 10ms timeout that capped a single
// publisher at ~100 durable (or spilled-transient) writes per second, which
// presents as a publisher wedge once a queue ring crosses the spill threshold
// (see TestDisruptorStorage_SpillPathNotBatchTimeoutBound below).
//
// The tests use a deliberately huge BatchTimeout so that the pre-fix behavior
// trips the watchdog deterministically, while the fixed drain-then-flush group
// commit completes in milliseconds.

// TestWALWrite_LoneWriterNotBatchTimeoutBound asserts that a single
// synchronous writer is flushed as soon as the batch writer is idle, instead
// of waiting out the group-commit BatchTimeout ticker.
func TestWALWrite_LoneWriterNotBatchTimeoutBound(t *testing.T) {
	cfg := DefaultWALConfig()
	cfg.BatchTimeout = 2 * time.Second // pre-fix: EVERY lone write stalls this long

	wm, err := NewWALManagerWithConfig(t.TempDir(), cfg)
	if err != nil {
		t.Fatalf("NewWALManagerWithConfig: %v", err)
	}
	defer wm.Close()

	const writes = 3
	done := make(chan error, 1)
	start := time.Now()
	go func() {
		for i := 1; i <= writes; i++ {
			msg := &protocol.Message{
				Body:         []byte("payload"),
				RoutingKey:   "q",
				DeliveryMode: 2,
				DeliveryTag:  uint64(i),
			}
			if err := wm.Write("q", msg, uint64(i)); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("WAL write failed: %v", err)
		}
		if elapsed := time.Since(start); elapsed >= cfg.BatchTimeout {
			t.Fatalf("lone-writer WAL writes are batch-timeout bound: %d writes took %v (BatchTimeout=%v)",
				writes, elapsed, cfg.BatchTimeout)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("lone-writer WAL writes wedged: 3 sequential Write calls still not done after 3s " +
			"(each write is waiting out the group-commit BatchTimeout ticker)")
	}
}

// TestDisruptorStorage_SpillPathNotBatchTimeoutBound reproduces the
// publisher-wedge shape end to end at the storage level: a fast transient
// publisher whose queue ring crosses the spill threshold enters the WAL spill
// path (DisruptorStorage.StoreMessage -> WALManager.Write) and, pre-fix,
// stalls for a full BatchTimeout per message.
func TestDisruptorStorage_SpillPathNotBatchTimeoutBound(t *testing.T) {
	ec := interfaces.EngineConfig{
		RingBufferSize:        1024,
		SpillThresholdPercent: 80, // spill threshold = 819
		WALBatchSize:          1000,
		WALBatchTimeoutMS:     1000, // pre-fix: 1s per spilled message
	}
	ds, err := NewDisruptorStorageWithEngineConfig(t.TempDir(), DefaultCheckpointInterval, ec)
	if err != nil {
		t.Fatalf("NewDisruptorStorageWithEngineConfig: %v", err)
	}
	defer ds.Close()

	// 850 transient publishes with no consumer: the ring count crosses the
	// spill threshold at 820, so ~30 messages take the WAL spill path.
	// Pre-fix that is ~30s; the 5s watchdog trips long before that.
	const total = 850
	done := make(chan error, 1)
	go func() {
		for i := 1; i <= total; i++ {
			msg := &protocol.Message{
				Body:        []byte("transient payload"),
				RoutingKey:  "q",
				DeliveryTag: uint64(i),
				// DeliveryMode intentionally != 2: transient message.
			}
			if err := ds.StoreMessage("q", msg); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("StoreMessage failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("transient publisher wedged in the WAL spill path: 850 stores (~30 spilled) " +
			"still not done after 5s — each spill write is waiting out the group-commit BatchTimeout")
	}
}
