package storage

// FIX 2 — THE BARRIER-MODE PACING EXEMPTION (2026-11-07).
//
// In crash_model: barrier every admitted body sits in the hot cache for its
// durability wait (batch fill + fence + completion) before it can be delivered.
// Charging those NOT-YET-DURABLE bytes against the 64 MiB pace budget made the
// publisher park on bytes the deliverable backlog could have used (measured:
// 6.9-8.9 s of park per 30 s window vs 3.6-4.8 s at fsync:false).
//
// The fix exempts, in barrier mode ONLY, up to queueBytes/8 of a queue's
// not-yet-durable bytes from BOTH the admission budget and the pace gate. With
// storage.fsync disabled (no barrier at all) and under crash_model: force_flush
// (the pace gate is not wired there) the exemption is OFF and the charging is
// byte-identical to before.
//
// The RED assertions are the ones that change: while every resident byte is
// not-yet-durable and the resident set is within one message of the budget, the
// gate must NOT arm -- before the fix it armed, because the raw resident bytes
// already exceeded budget-charge.

import (
	"bytes"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
)

// exemptMsg is one admitted body with a fixed shape, so every message's cache
// charge is identical and the arithmetic below is exact.
func exemptMsg(queue string, tag uint64) *protocol.Message {
	return &protocol.Message{
		Exchange:     "amq.direct",
		RoutingKey:   queue,
		Body:         bytes.Repeat([]byte("e"), 4096),
		DeliveryMode: 2,
		DeliveryTag:  tag,
	}
}

func newPaceExemptStorage(t *testing.T, budget int64, syncDisabled bool, crashModel string) *StorageEngine {
	t.Helper()
	ds, err := NewStorageEngineWithConfig(t.TempDir(), interfaces.EngineConfig{
		HotCacheQueueBytes: budget,
		WALSyncDisabled:    syncDisabled,
		CrashModel:         crashModel,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds.Close() })
	return ds
}

// TestBarrierPaceExemption_NotYetDurableBytesAreNotCharged is the deterministic
// arithmetic: with a budget of exactly ten messages' charge and a cap of
// budget/8, ten NOT-YET-DURABLE bodies fill the budget yet the gate stays nil
// (RED before the fix: resident+charge already exceeds the budget), the eleventh
// is still admitted (the exemption lets the cache hold budget + up to cap), and
// the gate arms only when the exempt pending bytes exceed the cap. Marking the
// entries durable (the fence returning) removes the exemption and the gate arms
// at once.
func TestBarrierPaceExemption_NotYetDurableBytesAreNotCharged(t *testing.T) {
	const queue = "pex-q"
	charge := cacheCharge(queue, exemptMsg(queue, 1))
	require.Greater(t, charge, int64(0))
	const fill = 10

	ds := newPaceExemptStorage(t, fill*charge, false, interfaces.CrashModelBarrier)
	require.True(t, ds.hot.exemptPendingDurable,
		"premise: crash_model barrier with the fence on must enable the exemption")

	entries := make([]*hotCacheEntry, 0, fill+1)
	for i := 1; i <= fill; i++ {
		entry, ok := ds.hot.PutBestEffortEntry(queue, exemptMsg(queue, uint64(i)), true, false)
		require.True(t, ok, "non-durable admission %d must fit within budget + cap", i)
		entries = append(entries, entry)
	}

	require.Equal(t, int64(fill*charge), ds.HotCacheResidentBytes(queue),
		"the ten non-durable bodies fill the budget exactly")
	capBytes := (fill * charge) / 8
	require.Equal(t, int64(fill*charge)-capBytes, ds.hot.paceEffectiveResident(queue),
		"the gate must see resident minus the capped pending-durable exemption")

	// RED BEFORE THE FIX: raw resident + charge already exceeds the budget, so
	// the old gate armed here. With the exemption it must NOT.
	require.Nil(t, ds.HotCachePaceGate(queue, exemptMsg(queue, 999)),
		"the pace gate must not arm while every resident body is still awaiting its fence")

	// The eleventh body is still admitted: resident may rise to budget + up to
	// cap (bounded extra memory), which is what restores the deliverable
	// backlog the barrier-mode residency wait was eating.
	entry, ok := ds.hot.PutBestEffortEntry(queue, exemptMsg(queue, uint64(fill+1)), true, false)
	require.True(t, ok, "the eleventh body is admitted while the exemption still has room")
	entries = append(entries, entry)
	require.Greater(t, ds.HotCacheResidentBytes(queue), int64(fill*charge),
		"the exemption lets the resident set rise above the raw budget")

	// The gate ARMS once the pending-durable bytes exceed the cap (the excess
	// counts again, bounding the extra memory and the exposure to bytes that
	// will land cold).
	require.NotNil(t, ds.HotCachePaceGate(queue, exemptMsg(queue, 1000)),
		"once pending-durable bytes exceed the cap the gate must arm")

	// The twelfth admission is refused (budget + cap is the hard ceiling).
	_, ok = ds.hot.PutBestEffortEntry(queue, exemptMsg(queue, uint64(fill+2)), true, false)
	require.False(t, ok, "an admission beyond budget + cap must be refused")

	// The fence returns: every entry becomes durable and the exemption drops.
	for _, e := range entries {
		require.True(t, ds.hot.MarkDurableEntry(e))
	}
	q := ds.hot.queueStateIfPresent(queue)
	require.NotNil(t, q)
	require.Zero(t, q.pendingDurable.Load(), "the pending-durable total must drain to zero")
	require.Equal(t, ds.HotCacheResidentBytes(queue), ds.hot.paceEffectiveResident(queue),
		"with nothing pending the effective resident is the raw resident again")
	require.NotNil(t, ds.HotCachePaceGate(queue, exemptMsg(queue, 1001)),
		"once the bodies are durable the gate must arm on the raw resident bytes")
}

// TestBarrierPaceExemption_DurableAdmissionDoesNotEvictPending pins that the
// EVICTING admission (Put) honours the same exemption: a queue that is over the
// raw budget only because of its not-yet-durable bodies must not evict a
// DELIVERABLE entry to make room for a durable admission. This matters on the
// shared-body fan-out path (StoreSharedAsync's completion admits with Put), where
// the raw rule would evict roughly cap/charge deliverable bodies on every
// completion. RED before the fix: the raw condition evicts the oldest resident.
func TestBarrierPaceExemption_DurableAdmissionDoesNotEvictPending(t *testing.T) {
	const queue = "pex-put-q"
	charge := cacheCharge(queue, exemptMsg(queue, 1))
	const fill = 10

	ds := newPaceExemptStorage(t, fill*charge, false, interfaces.CrashModelBarrier)

	tags := make([]uint64, 0, fill)
	for i := 1; i <= fill; i++ {
		_, ok := ds.hot.PutBestEffortEntry(queue, exemptMsg(queue, uint64(i)), true, false)
		require.True(t, ok)
		tags = append(tags, uint64(i))
	}
	require.Equal(t, int64(fill*charge), ds.HotCacheResidentBytes(queue))

	// A durable admission of one more message: the exemption leaves room for it
	// (budget - cap + charge <= budget), so it must NOT evict anything.
	require.True(t, ds.hot.Put(queue, exemptMsg(queue, 100), true),
		"the durable admission must succeed")
	for _, tag := range tags {
		_, _, resident := ds.hot.GetResident(queue, tag)
		require.True(t, resident,
			"the durable admission must not have evicted the not-yet-durable entry for tag %d", tag)
	}
	require.Equal(t, int64((fill+1)*charge), ds.HotCacheResidentBytes(queue),
		"nothing was evicted: the durable admission added exactly its own charge")
}

// write-side admission: with the group-commit fence held open, a single batch
// fills the budget with entries whose fence has not returned, and the pace gate
// the publisher would consult must stay clear. Releasing the fence makes the
// same entries durable and the gate arms.
func TestBarrierPaceExemption_RealWritePathAdmitsNonDurable(t *testing.T) {
	const queue = "pex-real-q"
	const fill = 10
	charge := cacheCharge(queue, exemptMsg(queue, 1))

	ds := newPaceExemptStorage(t, fill*charge, false, interfaces.CrashModelBarrier)

	fsyncEntered := make(chan struct{})
	fsyncRelease := make(chan struct{})
	var fsyncOnce, fsyncRelOnce sync.Once
	doRelease := func() { fsyncRelOnce.Do(func() { close(fsyncRelease) }) }
	restoreFsync := SetWALGroupCommitFsyncForTest(func(f *os.File) error {
		fsyncOnce.Do(func() { close(fsyncEntered) })
		<-fsyncRelease
		return fdatasyncFile(f)
	})
	t.Cleanup(func() { doRelease(); restoreFsync() })

	// Pin first-batch membership: hold the stager on its first staged record
	// until all `fill` requests are queued, so they commit in ONE batch.
	stageRelease := make(chan struct{})
	var stageOnce, stageRelOnce sync.Once
	doStageRelease := func() { stageRelOnce.Do(func() { close(stageRelease) }) }
	restoreStage := SetWALStageHookForTest(func(staged int) {
		stageOnce.Do(func() { <-stageRelease })
	})
	t.Cleanup(func() { doStageRelease(); restoreStage() })

	var written atomic.Int64
	var durable atomic.Int64
	for i := 1; i <= fill; i++ {
		require.NoError(t, ds.StoreMessageAsync(queue, exemptMsg(queue, uint64(i)),
			func() { written.Add(1) }, func(error) { durable.Add(1) }))
	}
	doStageRelease()

	require.Eventually(t, func() bool { return written.Load() == fill }, 5*time.Second, time.Millisecond,
		"the whole batch must have been written (its bodies admitted)")

	select {
	case <-fsyncEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("the group-commit fence was never entered")
	}
	require.Equal(t, int64(0), durable.Load(), "premise: no entry may be durable while the fence is held")
	require.Equal(t, int64(fill*charge), ds.HotCacheResidentBytes(queue),
		"the batch's bodies must all be resident and NOT yet durable")

	// RED BEFORE THE FIX: resident+charge exceeds the budget, so the old gate
	// armed even though every byte is still awaiting its fence.
	require.Nil(t, ds.HotCachePaceGate(queue, exemptMsg(queue, 500)),
		"a publisher must not be parked on bodies that are not yet durable")

	// The fence returns: the entries are durable and the gate arms.
	doRelease()
	require.Eventually(t, func() bool { return durable.Load() == fill }, 5*time.Second, time.Millisecond,
		"every entry must become durable once the fence releases")
	require.NotNil(t, ds.HotCachePaceGate(queue, exemptMsg(queue, 501)),
		"once the entries are durable the gate arms on the raw resident bytes")
}

// TestPaceChargingUnchanged_ForceFlush pins that the exemption is OFF under
// crash_model: force_flush: the same non-durable admission arms the gate
// exactly where it did before.
func TestPaceChargingUnchanged_ForceFlush(t *testing.T) {
	const queue = "pex-ff-q"
	charge := cacheCharge(queue, exemptMsg(queue, 1))
	const fill = 10

	ds := newPaceExemptStorage(t, fill*charge, false, interfaces.CrashModelForceFlush)
	require.False(t, ds.hot.exemptPendingDurable,
		"force_flush must keep the exemption OFF")

	for i := 1; i <= fill; i++ {
		_, ok := ds.hot.PutBestEffortEntry(queue, exemptMsg(queue, uint64(i)), true, false)
		require.True(t, ok)
	}
	require.Equal(t, int64(fill*charge), ds.HotCacheResidentBytes(queue))
	require.Equal(t, int64(fill*charge), ds.hot.paceEffectiveResident(queue),
		"with the exemption off the effective resident is the raw resident")
	require.NotNil(t, ds.HotCachePaceGate(queue, exemptMsg(queue, 999)),
		"force_flush charging must be byte-identical: the gate arms at the budget as before")
}

// TestPaceChargingUnchanged_SyncDisabled pins that the exemption is OFF with
// storage.fsync disabled (no WAL barrier at all), so the nofsync pace charging
// is byte-identical to before.
func TestPaceChargingUnchanged_SyncDisabled(t *testing.T) {
	const queue = "pex-nofsync-q"
	charge := cacheCharge(queue, exemptMsg(queue, 1))
	const fill = 10

	ds := newPaceExemptStorage(t, fill*charge, true, interfaces.CrashModelBarrier)
	require.False(t, ds.hot.exemptPendingDurable,
		"storage.fsync disabled must keep the exemption OFF")

	for i := 1; i <= fill; i++ {
		_, ok := ds.hot.PutBestEffortEntry(queue, exemptMsg(queue, uint64(i)), true, false)
		require.True(t, ok)
	}
	require.Equal(t, int64(fill*charge), ds.hot.paceEffectiveResident(queue))
	require.NotNil(t, ds.HotCachePaceGate(queue, exemptMsg(queue, 999)),
		"nofsync charging must be byte-identical: the gate arms at the budget as before")
}
