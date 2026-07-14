package storage

import (
	"encoding/binary"
	"hash/crc32"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAppendMessageRecord_BackpatchOffsets is STAGE A test A2. It packs several
// records into one shared buffer and then INDEPENDENTLY parses the [CRC][length]
// framing of a NON-first record at its own start offset, asserting the length
// equals the record's data span and the CRC recomputes over exactly that span.
// This is the surgical guard for the classic in-place hazard: backpatching the
// CRC/length at absolute offsets (buf[0:4]/buf[4:8], CRC over buf[4:]) instead of
// record-relative offsets (buf[recStart:...]) would leave every non-first
// record's framing pointing at the wrong bytes — invisible to a fresh-buffer
// test but fatal on the shared batch buffer.
func TestAppendMessageRecord_BackpatchOffsets(t *testing.T) {
	msgs := []*protocol.Message{
		{RoutingKey: "first", Body: []byte("alpha"), DeliveryMode: 2},
		{RoutingKey: "second", Exchange: "ex", Body: []byte("bravo-body-payload"), DeliveryMode: 2},
		{RoutingKey: "third", Body: []byte("charlie"), DeliveryMode: 2},
	}

	var buf []byte
	starts := make([]int, len(msgs))
	for i, m := range msgs {
		starts[i] = len(buf)
		var err error
		buf, err = appendMessageRecord(buf, "q", m, uint64(i+1), false)
		require.NoError(t, err)
	}

	// Validate every record, but the middle one is the point of the test.
	for i := range msgs {
		recStart := starts[i]
		end := len(buf)
		if i+1 < len(starts) {
			end = starts[i+1]
		}
		require.GreaterOrEqual(t, end-recStart, 8, "record %d must have room for CRC+length", i)

		gotCRC := binary.BigEndian.Uint32(buf[recStart : recStart+4])
		gotLen := binary.BigEndian.Uint32(buf[recStart+4 : recStart+8])

		// The length field counts everything after the CRC+length header.
		dataSpan := (end - recStart) - 8
		assert.Equal(t, uint32(dataSpan), gotLen, "record %d length field must equal its data span", i)

		// CRC covers [length][data] == buf[recStart+4 : end].
		wantCRC := crc32.ChecksumIEEE(buf[recStart+4 : end])
		assert.Equal(t, wantCRC, gotCRC, "record %d CRC must recompute over its own [length][data] span", i)
	}
}

// TestWriteAsync_DurabilityBarrierOrdering is STAGE A test A5. With the group-
// commit fsync gated open, it asserts that NO completion fires and NO record is
// readable until the barrier is released — i.e. the buffer-reuse refactor did not
// move completion/index-update ahead of the fsync — and that once released the
// completions fire in enqueue order. This is the ordering half of the A1 proof
// (persist ⇒ fsynced ⇒ observable), independent of the byte-identity half.
func TestWriteAsync_DurabilityBarrierOrdering(t *testing.T) {
	release := make(chan struct{})
	var releaseOnce sync.Once
	var fsyncCount atomic.Int64
	restore := SetWALGroupCommitFsyncForTest(func(f *os.File) error {
		fsyncCount.Add(1)
		<-release
		return f.Sync()
	})
	defer restore()
	defer releaseOnce.Do(func() { close(release) })

	wm, err := NewWALManagerWithConfig(t.TempDir(), DefaultWALConfig())
	require.NoError(t, err)
	defer wm.Close()

	const n = 16
	var order []uint64
	var orderMu sync.Mutex
	done := make(chan uint64, n)

	for i := 1; i <= n; i++ {
		off := uint64(i)
		body := make([]byte, 65536)
		for j := range body {
			body[j] = byte((int(off)*13 + j) & 0xff)
		}
		msg := &protocol.Message{Body: body, RoutingKey: "q", DeliveryMode: 2, DeliveryTag: off}
		wm.WriteAsync("q", msg, off, func(cerr error) {
			require.NoError(t, cerr)
			orderMu.Lock()
			order = append(order, off)
			orderMu.Unlock()
			done <- off
		})
	}

	// Wait until the writer is parked inside the gated fsync (barrier engaged).
	require.Eventually(t, func() bool { return fsyncCount.Load() >= 1 }, 5*time.Second, 5*time.Millisecond,
		"batch writer must reach the fsync barrier")

	// Barrier held: nothing may complete and nothing may be readable yet.
	orderMu.Lock()
	require.Empty(t, order, "no completion may fire before the fsync barrier releases")
	orderMu.Unlock()
	_, rerr := wm.Read("q", 1)
	assert.Error(t, rerr, "no record may be readable before its fsync completes (index not yet updated)")

	// Release the barrier and drain all completions.
	releaseOnce.Do(func() { close(release) })
	deadline := time.After(10 * time.Second)
	for i := 0; i < n; i++ {
		select {
		case <-done:
		case <-deadline:
			t.Fatalf("only %d/%d completions fired after barrier release", i, n)
		}
	}

	orderMu.Lock()
	defer orderMu.Unlock()
	require.Len(t, order, n)
	for i := 0; i < n; i++ {
		assert.Equal(t, uint64(i+1), order[i], "completions must fire in enqueue order")
	}
}

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
