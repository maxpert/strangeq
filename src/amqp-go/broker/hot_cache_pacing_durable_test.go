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

// TestBarrierModeHotCachePaceGateActive is the behavioural twin of
// TestForceFlushModeHotCachePaceGateInert: under crash_model: barrier (the
// shipped durable default) the SAME byte-budget-exceeded condition DOES arm the
// gate, because the ~71 µs ordering fence is not the throughput limiter and cold
// WAL reads are. Guarded on darwin: on Linux and other platforms the barrier IS
// a full drive-cache flush, so pacing stays off exactly as under force_flush.
func TestBarrierModeHotCachePaceGateActive(t *testing.T) {
	if !storage.ConfirmFenceOrdersOnly {
		t.Skip("crash_model: barrier is a full flush on this platform; pacing stays off")
	}
	const queue = "barrier-pace-q"
	body := bytes.Repeat([]byte("b"), 256)
	ec := interfaces.EngineConfig{
		HotCacheQueueBytes:    3200,
		DepthHighWMOverride:   1_000_000,
		RingBufferSize:        65536,
		SpillThresholdPercent: 80,
		WALSyncDisabled:       false,
		CrashModel:            interfaces.CrashModelBarrier,
	}
	store, err := storage.NewDisruptorStorageWithEngineConfig(t.TempDir(), ec)
	require.NoError(t, err)
	b := NewStorageBroker(store, ec)
	t.Cleanup(func() { b.Close(); _ = store.Close() })
	_, err = b.DeclareQueue(queue, true, false, false, nil)
	require.NoError(t, err)

	armed := 0
	for i := 0; i < 16; i++ {
		gate, _ := publishDurableAwait(t, b, queue, body)
		if gate != nil {
			armed++
		}
	}
	require.Greater(t, armed, 0,
		"durable + crash_model barrier must arm the hot-cache pace gate once the byte budget is "+
			"full: the ordering fence is not the throughput limiter")
}

// TestForceFlushModeHotCachePaceGateInert is the durable-mode counterpart of
// TestPublisherPacedToHotCacheCapacity: the SAME byte-budget-exceeded condition
// that arms the gate and parks the publisher in fsync:false mode must leave the
// publisher UNPACED under crash_model: force_flush, where the confirm fence is
// the ~16 ms full drive-cache flush and batch size IS throughput.
//
// RED BEFORE THE FIX: NewStorageBroker resolved the storage's pace capability
// unconditionally, so exceeding the byte budget armed the gate and this test
// failed on the require.Nil(gate) assertion below.
func TestForceFlushModeHotCachePaceGateInert(t *testing.T) {
	const queue = "durable-inert-q"
	body := bytes.Repeat([]byte("d"), 256)
	ec := interfaces.EngineConfig{
		// The exact budget and body the fsync:false pacing test uses, so the
		// byte-budget-exceeded condition is reached identically.
		HotCacheQueueBytes:    3200,
		DepthHighWMOverride:   1_000_000,
		RingBufferSize:        65536,
		SpillThresholdPercent: 80,
		// DURABLE MODE with the full drive-cache flush, where pacing stays off
		// (the 52e0a47 wiring).
		WALSyncDisabled: false,
		CrashModel:      interfaces.CrashModelForceFlush,
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
			"force_flush mode must never pace the publisher (publish %d): the full drive-cache "+
				"flush makes the group-commit batch size the throughput limiter, and parking starves it", i)
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

// TestHotCachePaceGateWiringByCrashModel is the byte-level guard for the wiring
// itself: the resolved pacer is PRESENT when the WAL fsync barrier is disabled,
// PRESENT in durable mode when the confirm fence is an ordering-only barrier
// (crash_model: barrier on darwin — the fence stops being the throughput
// limiter, so the gate's cold-read win dominates), and ABSENT under
// crash_model: force_flush (and under barrier on a platform where the barrier
// IS a full flush) — where the ~16 ms flush makes batch size throughput. It
// asserts the structural property directly, independent of whether any gate
// happens to arm.
func TestHotCachePaceGateWiringByCrashModel(t *testing.T) {
	newBroker := func(t *testing.T, syncDisabled bool, crashModel string) *StorageBroker {
		t.Helper()
		ec := interfaces.EngineConfig{
			HotCacheQueueBytes: 3200,
			// fsync ON (durable) is the zero value; syncDisabled=true is
			// fsync:false, which always wires pacing.
			WALSyncDisabled: syncDisabled,
			CrashModel:      crashModel,
		}
		store, err := storage.NewDisruptorStorageWithEngineConfig(t.TempDir(), ec)
		require.NoError(t, err)
		b := NewStorageBroker(store, ec)
		t.Cleanup(func() { b.Close(); _ = store.Close() })
		return b
	}

	require.NotNil(t, newBroker(t, true, interfaces.CrashModelBarrier).hotPacer,
		"fsync:false mode must wire the hot-cache pacer")

	require.Nil(t, newBroker(t, false, interfaces.CrashModelForceFlush).hotPacer,
		"durable + crash_model force_flush must leave the publisher path unpaced: the full flush "+
			"makes the group-commit batch size the throughput limiter")

	if storage.ConfirmFenceOrdersOnly {
		// darwin: crash_model barrier is the cheap ordering fence.
		require.NotNil(t, newBroker(t, false, interfaces.CrashModelBarrier).hotPacer,
			"durable + crash_model barrier on a platform with an ordering-only fence must wire "+
				"the pacer: cold WAL reads, not batch size, are the limiter")
	} else {
		// Linux and every other platform: the barrier IS fdatasync (a full
		// flush), so the 52e0a47 rationale still holds and pacing stays off.
		require.Nil(t, newBroker(t, false, interfaces.CrashModelBarrier).hotPacer,
			"durable + crash_model barrier on a platform whose barrier is a full flush must "+
				"leave the publisher path unpaced")
	}
}
