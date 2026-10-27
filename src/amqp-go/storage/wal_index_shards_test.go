package storage

// THE SHARDED WAL INDEX -- the tests.
//
// WHAT THIS FILE PINS.
//
//   - the acknowledgement path takes ONLY its own shard's write lock, so the
//     acknowledgements of different tags do not exclude each other. That is
//     the whole point of the shard: at F2 the acknowledgement path runs about
//     100,000 times per second, and ONE global exclusive lock saturated it
//     (96% of the F2 mutex profile's contention was
//     WALManager.Acknowledge -> RWMutex.Unlock);
//   - every physical copy of one (queue, tag) lives in the SAME shard, so a
//     copy list -- which is ONE slice -- never spans two shards;
//   - the recovery scan still returns every unacknowledged record EXACTLY
//     once and never serves an acknowledged one, with the sharded index;
//   - the acknowledgement path, the committer and the readers can run at the
//     same time without a data race and without a deadlock.

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
)

// walIndexEntry returns the newest copy of one key and whether the key is
// indexed at all. It reads the key's OWN shard, under that shard's read
// lock.
func walIndexEntry(qw *QueueWAL, key recordKey) (*segmentRecord, bool) {
	sh := qw.shardForKey(key)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	rec, ok := sh.offset[key]
	return rec, ok
}

// walIndexCopies returns every physical copy of one key, read under that
// key's own shard's read lock. The slice is COPIED, so the caller may keep
// it after the lock is released.
func walIndexCopies(qw *QueueWAL, key recordKey) []*segmentRecord {
	sh := qw.shardForKey(key)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	copies := sh.copies[key]
	out := make([]*segmentRecord, len(copies))
	copy(out, copies)
	return out
}

// walIndexHasCopy reports whether any physical copy of one key is indexed.
func walIndexHasCopy(qw *QueueWAL, key recordKey) bool {
	return len(walIndexCopies(qw, key)) > 0
}

// walIndexCopiesForTag returns every indexed physical copy of one delivery
// tag, across EVERY shard. It is the test-side view of the whole index: the
// index is sharded, so a tag's copies are looked up shard by shard.
func walIndexCopiesForTag(qw *QueueWAL, tag uint64) []*segmentRecord {
	var out []*segmentRecord
	for i := range qw.indexShards {
		sh := &qw.indexShards[i]
		sh.mu.RLock()
		for key, copies := range sh.copies {
			if key.deliveryTag != tag {
				continue
			}
			out = append(out, copies...)
		}
		sh.mu.RUnlock()
	}
	return out
}

// TestWALIndexShards_AcknowledgeTakesOnlyItsOwnShardsLock pins THE property
// the shard exists for: an acknowledgement excludes the acknowledgements of
// the OTHER shards, and nothing else.
//
// The test holds one shard's write lock and asserts that an acknowledgement
// of a tag in ANOTHER shard still completes -- it must not be excluded by a
// lock that has nothing to do with it -- while an acknowledgement of a tag in
// the HELD shard does not, because it takes that shard's own write lock.
func TestWALIndexShards_AcknowledgeTakesOnlyItsOwnShardsLock(t *testing.T) {
	dir := t.TempDir()
	wm, err := NewWALManagerWithConfig(dir, walIndexBoundConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = wm.Close() })
	qw := wm.sharedWAL

	const queue = "shard-lock-q"
	const tagA = uint64(11)
	const tagB = uint64(12)
	require.NotEqual(t, walIndexShardOf(tagA), walIndexShardOf(tagB),
		"PREMISE BROKEN: the fixture needs two tags in two different shards")

	for _, tag := range []uint64{tagA, tagB} {
		require.NoError(t, wm.Write(queue, &protocol.Message{
			RoutingKey: queue, Body: []byte("payload"), DeliveryMode: 2, DeliveryTag: tag,
		}, tag))
	}
	_, indexedA := walIndexEntry(qw, recordKey{queueName: queue, deliveryTag: tagA})
	require.True(t, indexedA, "PREMISE BROKEN: the first record was never indexed")

	shardA := qw.shardForTag(tagA)

	// HOLD SHARD A's WRITE LOCK.
	shardA.mu.Lock()

	// An acknowledgement of a tag in ANOTHER shard must not be excluded.
	otherDone := make(chan struct{})
	go func() {
		defer close(otherDone)
		wm.Acknowledge(queue, tagB)
	}()
	select {
	case <-otherDone:
	case <-time.After(10 * time.Second):
		t.Fatal("an acknowledgement of another shard's tag was excluded by this shard's write lock: " +
			"the acknowledgement path must take ONLY its own shard's lock")
	}

	// An acknowledgement of a tag in the HELD shard must be excluded.
	ownDone := make(chan struct{})
	go func() {
		defer close(ownDone)
		wm.Acknowledge(queue, tagA)
	}()
	select {
	case <-ownDone:
		t.Fatal("an acknowledgement of this shard's tag did not take this shard's write lock")
	case <-time.After(250 * time.Millisecond):
	}

	shardA.mu.Unlock()

	select {
	case <-ownDone:
	case <-time.After(10 * time.Second):
		t.Fatal("the acknowledgement did not proceed after the shard's write lock was released")
	}
	require.True(t, wm.IsAcknowledged(queue, tagA))
	require.True(t, wm.IsAcknowledged(queue, tagB))
}

// TestWALIndexShards_EveryPhysicalCopyOfATagLivesInOneShard pins the
// invariant that makes the shard safe: a key's copy list is ONE slice, so
// every physical copy of one (queue, tag) must live in the SAME shard.
//
// The fixture publishes a MIXED set of tags, and then publishes ONE tag TWICE,
// into two different WAL files, and COMPACTS one of them -- so that tag's
// copy in that file MOVES to a new position and is re-published under the
// same key. The tag's copies are then spread over two files and two
// positions, and they must still be in ONE shard.
func TestWALIndexShards_EveryPhysicalCopyOfATagLivesInOneShard(t *testing.T) {
	dir := t.TempDir()
	cfg := walIndexBoundConfig()
	// A tiny threshold so the acknowledged garbage makes the sealed file
	// eligible for the rewrite.
	cfg.CompactionThreshold = 0.001
	wm, err := NewWALManagerWithConfig(dir, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = wm.Close() })

	const queueA = "mix-a"
	const queueB = "mix-b"
	const mixed = uint64(40)
	for i := uint64(1); i <= mixed; i++ {
		require.NoError(t, wm.Write(queueA, &protocol.Message{
			RoutingKey: queueA, Body: []byte("a"), DeliveryMode: 2, DeliveryTag: i,
		}, i))
		require.NoError(t, wm.Write(queueB, &protocol.Message{
			RoutingKey: queueB, Body: []byte("b"), DeliveryMode: 2, DeliveryTag: 1000 + i,
		}, 1000+i))
	}

	// THE TAG WHOSE COPIES ARE MOVED BY A COMPACTION.
	const queue = "one-shard-q"
	const tag = uint64(4242)
	key := recordKey{queueName: queue, deliveryTag: tag}

	// COPY A, in file 1.
	require.NoError(t, wm.Write(queue, &protocol.Message{
		RoutingKey: queue, Body: []byte("copy-a"), DeliveryMode: 2, DeliveryTag: tag,
	}, tag))
	// GARBAGE in file 1: acknowledged, so the file becomes compaction-eligible.
	require.NoError(t, wm.Write("garbage-q", &protocol.Message{
		RoutingKey: "garbage-q", Body: []byte(strings.Repeat("G", 4096)), DeliveryMode: 2, DeliveryTag: 1,
	}, 1))
	fileA, _ := rollSegstoreReclaimWAL(t, wm)
	wm.Acknowledge("garbage-q", 1)

	// COPY B, in file 2.
	require.NoError(t, wm.Write(queue, &protocol.Message{
		RoutingKey: queue, Body: []byte("copy-b"), DeliveryMode: 2, DeliveryTag: tag,
	}, tag))
	fileB, _ := rollSegstoreReclaimWAL(t, wm)

	qw := wm.sharedWAL
	require.Len(t, walIndexCopies(qw, key), 2,
		"PREMISE BROKEN: the fixture must hold two physical copies of the tag")

	// THE COMPACTION: it rewrites file A, so the tag's copy in file A moves
	// to a new position and is re-published under the same key.
	qw.tryCompactOldFiles()

	// THE INVARIANT, over the WHOLE index: every key's copy list lives in
	// the shard its delivery tag selects, and every copy in the list carries
	// that key.
	for i := range qw.indexShards {
		sh := &qw.indexShards[i]
		sh.mu.RLock()
		for k, copies := range sh.copies {
			require.Equal(t, walIndexShardOf(k.deliveryTag), i,
				"a key's copy list must live in the shard its delivery tag selects")
			for _, c := range copies {
				require.Equal(t, k.deliveryTag, c.deliveryTag,
					"a copy list must hold copies of its own key")
				require.Equal(t, k.queueName, c.queueName,
					"a copy list must hold copies of its own key")
			}
		}
		sh.mu.RUnlock()
	}

	// AND THE COMPACTED TAG SPECIFICALLY: exactly ONE shard holds it, and
	// that shard holds EVERY physical copy of it.
	holders := 0
	for i := range qw.indexShards {
		sh := &qw.indexShards[i]
		sh.mu.RLock()
		if len(sh.copies[key]) > 0 {
			holders++
			require.Equal(t, walIndexShardOf(tag), i,
				"the compacted tag's copies must live in the shard the delivery tag selects")
		}
		sh.mu.RUnlock()
	}
	require.Equal(t, 1, holders, "a tag's copies must never be spread over two shards")

	// The two copies are the compacted one (at its NEW position) and the
	// untouched one, and the compacted position really moved.
	posA := walCopyPositionsInInventory(qw.oldFiles[fileA].inv, tag)
	posB := walCopyPositionsInInventory(qw.oldFiles[fileB].inv, tag)
	require.Len(t, posA, 1, "PREMISE BROKEN: the compacted file must hold the tag's copy")
	require.Len(t, posB, 1, "PREMISE BROKEN: the second file must hold the tag's copy")
	require.NotEqual(t, posA[0], posB[0], "PREMISE BROKEN: the two copies must be at different positions")

	copies := walIndexCopies(qw, key)
	require.Len(t, copies, 2, "both physical copies must still be in the one shard")
	positions := make([]recordPos, 0, len(copies))
	for _, c := range copies {
		positions = append(positions, c.pos)
	}
	require.Contains(t, positions, posA[0], "the compacted copy must be in the list, at its new position")
	require.Contains(t, positions, posB[0], "the untouched copy must be in the list")
}

// TestWALIndexShards_RecoverFromWALReturnsEachRecordOnceAndHidesAcknowledgedTags
// pins the recovery scan against the sharded index.
//
// THE TWO PROPERTIES.
//
//   - every record that is NOT acknowledged is recovered EXACTLY once, even
//     though its key lives in ONE shard and the scan walks EVERY shard;
//   - an acknowledged tag is NOT served, even though its bytes are still on
//     disk: the eager settling removed it from the index and the volatile
//     read gate says it was acknowledged.
func TestWALIndexShards_RecoverFromWALReturnsEachRecordOnceAndHidesAcknowledgedTags(t *testing.T) {
	dir := t.TempDir()
	wm, err := NewWALManagerWithConfig(dir, walIndexBoundConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = wm.Close() })

	const queue = "recover-shard-q"
	const total = uint64(150)

	for i := uint64(1); i <= total; i++ {
		require.NoError(t, wm.Write(queue, &protocol.Message{
			RoutingKey: queue, Body: []byte("payload"), DeliveryMode: 2, DeliveryTag: i,
		}, i))
	}
	// Every third tag is published a SECOND time, so it has TWO physical
	// copies: an older one and a newer one. The older copy is what a
	// re-pointing removal would revive.
	for i := uint64(3); i <= total; i += 3 {
		require.NoError(t, wm.Write(queue, &protocol.Message{
			RoutingKey: queue, Body: []byte("payload-again"), DeliveryMode: 2, DeliveryTag: i,
		}, i))
	}
	// Every third tag is acknowledged.
	acked := make(map[uint64]bool)
	for i := uint64(1); i <= total; i++ {
		if i%3 == 0 {
			wm.Acknowledge(queue, i)
			acked[i] = true
		}
	}
	// The records are in a SEALED file, so the scan reads them from disk.
	rollSegstoreReclaimWAL(t, wm)

	recovered, err := wm.RecoverFromWAL()
	require.NoError(t, err)

	seen := make(map[uint64]int, total)
	for _, rm := range recovered {
		require.Equal(t, queue, rm.QueueName)
		seen[rm.Offset]++
	}
	for i := uint64(1); i <= total; i++ {
		if acked[i] {
			require.Zero(t, seen[i], "an acknowledged tag must not be served by the recovery scan")
			continue
		}
		require.Equal(t, 1, seen[i], "every unacknowledged record must be recovered EXACTLY once")
	}
	require.Len(t, seen, int(total)-len(acked))
}

// TestWALIndexShards_AcknowledgeRacesWithTheCommitterAndTheReaders runs the
// three paths that touch the index AT THE SAME TIME: the acknowledgement path
// (which takes a shard's write lock), the committer (which publishes into a
// shard's write lock) and the readers (which take a shard's read lock
// through the ReadAt).
//
// Under -race this asserts that the sharded index has NO data race between
// them, and the timeout asserts that the lock order admits NO DEADLOCK: the
// committer and the acknowledger take ONE shard at a time, and the cold
// paths that take every shard take them in ascending order.
func TestWALIndexShards_AcknowledgeRacesWithTheCommitterAndTheReaders(t *testing.T) {
	dir := t.TempDir()
	wm, err := NewWALManagerWithConfig(dir, walIndexBoundConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = wm.Close() })
	qw := wm.sharedWAL

	const queue = "race-q"
	const records = 400

	var wg sync.WaitGroup

	// THE PUBLISHER. The writes are asynchronous, so the committer batches
	// them and the acknowledgements below overlap the NEXT batch's commit.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := uint64(1); i <= records; i++ {
			msg := &protocol.Message{
				RoutingKey: queue, Body: []byte("payload"), DeliveryMode: 2, DeliveryTag: i,
			}
			wm.WriteAsync(queue, msg, i, func(error) {})
		}
	}()

	// THE ACKNOWLEDGER. It waits for the record to be INDEXED -- which is
	// what the completion fires after -- and then acknowledges it, so the
	// acknowledgement runs while the committer is already committing the
	// NEXT batch. That is the overlap the global index lock used to
	// serialize.
	//
	// IT IS NOT DONE IN THE COMPLETION CALLBACK ON PURPOSE: the completion
	// runs on the committer's goroutine, and Close() holds the manager's
	// lock while it waits for that goroutine, so a callback that called back
	// into the manager would deadlock the shutdown.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := uint64(1); i <= records; i++ {
			key := recordKey{queueName: queue, deliveryTag: i}
			for {
				if _, ok := walIndexEntry(qw, key); ok {
					break
				}
				time.Sleep(time.Millisecond)
			}
			wm.Acknowledge(queue, i)
		}
	}()

	// THE READERS.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := uint64(1); i <= records; i++ {
			_, _ = wm.Read(queue, i)
		}
	}()

	// THE RECOVERY SCAN, which walks EVERY shard.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 4; i++ {
			_, _ = wm.RecoverFromWAL()
		}
	}()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(120 * time.Second):
		t.Fatal("the acknowledgement, the committer and the readers deadlocked")
	}

	// Every record was acknowledged, so the index must hold none of them:
	// the bound the eager settling exists for survives the sharding.
	require.Eventually(t, func() bool {
		indexed, copies := walIndexCounts(qw)
		return indexed == 0 && copies == 0
	}, 30*time.Second, 10*time.Millisecond,
		"every acknowledged record must have left the sharded index")
}

// TestWALIndexShards_ShardSelectionIsStableAndSpread asserts the shard
// function's two properties: it is a FUNCTION of the delivery tag alone (so
// every copy of a tag lands in the same shard), and it SPREADS the tags over
// the shards (so the contention is actually spread).
func TestWALIndexShards_ShardSelectionIsStableAndSpread(t *testing.T) {
	for _, tag := range []uint64{0, 1, 7, 1 << 20, 1 << 44, 1<<64 - 1} {
		require.Equal(t, walIndexShardOf(tag), walIndexShardOf(tag),
			"the shard of a tag must be a function of the tag alone")
		require.GreaterOrEqual(t, walIndexShardOf(tag), 0)
		require.Less(t, walIndexShardOf(tag), walIndexShardCount)
	}

	// SPREAD: the broker's tags are dense per queue, so a raw tag % N would
	// crowd them into a few shards. The finalizer must spread them.
	counts := make([]int, walIndexShardCount)
	const n = 4096
	for i := uint64(0); i < n; i++ {
		counts[walIndexShardOf(i)]++
	}
	for i, c := range counts {
		require.Greater(t, c, 0,
			fmt.Sprintf("shard %d holds none of %d dense tags: the shard function does not spread the tags", i, n))
	}
}
