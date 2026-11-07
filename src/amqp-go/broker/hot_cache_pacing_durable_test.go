package broker

// DURABLE-MODE INERTNESS for the hot-cache publisher pacing (design D-B).
//
// The pace gate keeps the delivered head in the per-queue hot cache by parking
// the publisher's connection reader at the cache's BYTE budget. That is a win
// when the WAL fsync barrier is DISABLED (fsync:false), where it recovers ~5.7%
// paired throughput and ~96% of deliveries become hot.
//
// It is a LOSS in DURABLE mode (fsync:true). There the WAL group-commit batch
// size IS the throughput limiter: the ~16 ms F_FULLFSYNC per batch is the
// critical path, so msgs/fsync is throughput. Parking the publisher's reader
// creates arrival gaps that halve the batch, and every halving costs a whole
// fsync. Measured (campaign ab-runC, F3 64 KiB 1p/1c durable+confirm):
//
//	main      wal_writes=854588 fsyncs=1711 -> 499 msgs/fsync, 28,732 msg/s
//	coldread  wal_writes=594563 fsyncs=2465 -> 241 msgs/fsync, 19,391 msg/s
//	combo     wal_writes=584507 fsyncs=2641 -> 221 msgs/fsync, 19,126 msg/s
//
// i.e. fsync CALLS rose ~50% while delivered volume fell ~33%. In durable mode
// the backlog is already naturally shallow because confirm-after-fsync
// rate-limits the publisher through round-trip latency, so the cold-read
// problem the gate targets barely arises. The gate is therefore NOT WIRED in
// durable mode; the publisher path is wired exactly as on main.

import (
	"bytes"
	"testing"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/storage"
	"github.com/stretchr/testify/require"
)

// TestDurableModeHotCachePaceGateInert is the durable-mode counterpart of
// TestPublisherPacedToHotCacheCapacity: the SAME byte-budget-exceeded condition
// that arms the gate and parks the publisher in fsync:false mode must leave the
// publisher UNPACED when the WAL fsync barrier is enabled.
//
// RED BEFORE THE FIX: NewStorageBroker resolved the storage's pace capability
// unconditionally, so exceeding the byte budget armed the gate and this test
// failed on the require.Nil(gate) assertion below.
func TestDurableModeHotCachePaceGateInert(t *testing.T) {
	const queue = "durable-inert-q"
	body := bytes.Repeat([]byte("d"), 256)
	ec := interfaces.EngineConfig{
		// The exact budget and body the fsync:false pacing test uses, so the
		// byte-budget-exceeded condition is reached identically.
		HotCacheQueueBytes:    3200,
		DepthHighWMOverride:   1_000_000,
		RingBufferSize:        65536,
		SpillThresholdPercent: 80,
		// DURABLE MODE: fsync enabled. WALSyncDisabled=false is fsync ON (the
		// shipped default); named explicitly so the mode under test is obvious.
		WALSyncDisabled: false,
	}
	store, err := storage.NewDisruptorStorageWithEngineConfig(t.TempDir(), ec)
	require.NoError(t, err)
	b := NewStorageBroker(store, ec)
	t.Cleanup(func() { b.Close(); _ = store.Close() })
	_, err = b.DeclareQueue(queue, true, false, false, nil)
	require.NoError(t, err)

	// Publish past the byte budget. Every publish must report NO backpressure
	// gate: the server therefore never sets the connection's depth gate and the
	// publisher reader is never parked.
	var charge int64
	for i := 0; i < 16; i++ {
		gate, _ := publishDurableAwait(t, b, queue, body)
		require.Nil(t, gate,
			"durable mode must never pace the publisher (publish %d): the group-commit "+
				"batch size is the throughput limiter, and parking starves it", i)
		if i == 0 {
			// The first publish is admitted: its resident bytes are one
			// message's exact charge.
			charge = store.HotCacheResidentBytes(queue)
		}
	}

	// Fixture premise (asserted LAST so a fast failure above is not masked):
	// the cache must now be within one message's charge of full. That is the
	// condition the byte gate tests (resident + charge > budget), so the gate
	// WOULD arm here in fsync:false mode — and the SAME condition is what
	// TestPublisherPacedToHotCacheCapacity reaches. The non-evicting admission
	// caps resident bytes at the budget, so "exceeded" means "no room for one
	// more".
	require.Greater(t, charge, int64(0), "fixture premise: the first publish must be hot-admitted")
	require.GreaterOrEqual(t, store.HotCacheResidentBytes(queue)+charge, ec.HotCacheQueueBytes,
		"fixture premise: the cache must be within one message of full — the condition that arms the pacing gate in fsync:false mode")
}

// TestHotCachePaceGateWiringIsDurableModeGated is the byte-level guard for the
// wiring itself: the resolved pacer is PRESENT when the WAL fsync barrier is
// disabled (pacing retained) and ABSENT when it is enabled (pacing inert). It
// asserts the structural property directly, independent of whether any gate
// happens to arm.
func TestHotCachePaceGateWiringIsDurableModeGated(t *testing.T) {
	newBroker := func(t *testing.T, syncDisabled bool) *StorageBroker {
		t.Helper()
		ec := interfaces.EngineConfig{
			HotCacheQueueBytes: 3200,
			// fsync ON (durable) is the zero value; syncDisabled=true is
			// fsync:false, the only mode in which pacing is wired.
			WALSyncDisabled: syncDisabled,
		}
		store, err := storage.NewDisruptorStorageWithEngineConfig(t.TempDir(), ec)
		require.NoError(t, err)
		b := NewStorageBroker(store, ec)
		t.Cleanup(func() { b.Close(); _ = store.Close() })
		return b
	}

	require.NotNil(t, newBroker(t, true).hotPacer,
		"fsync:false mode must wire the hot-cache pacer")
	require.Nil(t, newBroker(t, false).hotPacer,
		"durable (fsync:true) mode must leave the publisher path unpaced")
}
