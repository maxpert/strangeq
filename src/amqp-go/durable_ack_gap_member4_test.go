package main

// R1 — gap member 4: durable records in a NON-DURABLE queue.
//
// Loop 3 Step 0 recorded this as DERIVED, not executed, and as the cheapest
// door in the Door-1 gap:
//
//	DeclareQueue persists a metadata record for EVERY queue (the durability
//	check only gates updateDurableMetadata), so a durable=false queue leaves a
//	permanent record carrying an allocated Ordinal N. DeliveryMode==2 publishes
//	to it take the DURABLE WAL branch with no spill threshold involved, and
//	their acks enter the ack set. At restart, recoverPersistentMessages resolves
//	hasRecord=true / durable=false, raises a BenignFault, REAPS, and never calls
//	RecoverSeq — so no span is widened and nextSeq stays at its zero value. The
//	redeclare reclaims ordinal N from its own surviving metadata record and
//	re-mints (N<<44)|0,1,2,... straight into the tag band the previous
//	incarnation already used.
//
// WHAT IS AND IS NOT CLAIMED HERE, because it decides how this test is written.
//
// On the CURRENT tree acknowledgement is not durable at all, so no inherited
// ack set exists and nothing is destroyed today. Member 4 is a hazard OF THE
// PROPOSED FIX, not a live data-loss defect. What IS true of the current tree,
// and what this test establishes as an executed fact, is member 4's
// PRECONDITION — the thing that makes any tag-keyed durable ack set unsafe:
//
//	after a restart and redeclare of a non-durable queue, the broker re-mints
//	delivery tags that are ALREADY CARRIED by physically-present records of the
//	previous incarnation, which were delivered and acknowledged before the
//	restart.
//
// Two physically distinct records, different MessageIDs, same delivery tag,
// both on disk at once. That is the whole of the hazard: any ack set keyed by
// tag value cannot tell them apart, so an ack recorded against the first
// silently cancels the second.
//
// The destruction is then demonstrated against a SIMULATED tag-keyed ack set
// (the exact artifact the three withdrawn cycles built) rather than by shipping
// the buggy code — see the destroyed/surviving partition at the end.
//
// Five boots: broker boot 1, a storage-level reader, broker boot 2, a second
// reader, broker boot 3. The three-boot minimum in
// .notes/loop-2/deferred-ack-durability.md §4.6 exists because cycle 2's
// end-to-end test read back within ONE incarnation and was structurally blind
// to the stale-snapshot door.

import (
	"strconv"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"

	"github.com/maxpert/amqp-go/storage"
)

const (
	member4Queue = "m4.transient.queue"

	// Boot 1 publishes ids [0, member4FirstBatch).
	member4FirstBatch = 200

	// Boot 2 publishes ids [member4SecondBase, member4SecondBase+member4SecondBatch).
	// The second batch is deliberately LARGER than the first so that the
	// re-minted tag band overlaps the inherited one only in part: the overlap
	// is the destroyed set, the tail beyond it is the surviving set, and §4.6
	// requires both to be non-empty and disjoint.
	member4SecondBase  = 10_000
	member4SecondBatch = 300
)

// publishConfirmedDurableToTransientQueue declares `queue` as durable=FALSE and
// publishes ids [from,to) with DeliveryMode=2 under confirm.select, failing
// unless every one is POSITIVELY confirmed.
//
// The mismatch — a non-durable queue carrying persistent messages — is the
// whole point and is entirely legal AMQP: queue durability and message delivery
// mode are independent knobs, and StoreMessage dispatches on the MESSAGE's
// DeliveryMode, not on the queue's Durable flag.
func publishConfirmedDurableToTransientQueue(t *testing.T, uri, queue string, from, to int) {
	t.Helper()

	conn, err := amqp.Dial(uri)
	require.NoError(t, err, "dial %s", uri)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)

	_, err = ch.QueueDeclare(queue, false /*durable=FALSE*/, false, false, false, nil)
	require.NoError(t, err, "declaring NON-DURABLE queue %s", queue)
	require.NoError(t, ch.Confirm(false), "confirm.select")

	n := to - from
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, n+1))

	acked := make(chan int, 1)
	go func() {
		count := 0
		for i := 0; i < n; i++ {
			c, ok := <-confirms
			if !ok {
				break
			}
			if c.Ack {
				count++
			}
		}
		acked <- count
	}()

	body := make([]byte, restartTestBodySize)
	for i := from; i < to; i++ {
		require.NoError(t, ch.Publish("", queue, false, false, amqp.Publishing{
			DeliveryMode: amqp.Persistent, // DeliveryMode == 2
			MessageId:    strconv.Itoa(i),
			Body:         body,
		}), "publish %d", i)
	}

	select {
	case got := <-acked:
		require.Equal(t, n, got,
			"every publish must be positively confirmed before this test asserts anything; "+
				"got %d acks for %d publishes", got, n)
	case <-time.After(5 * time.Minute):
		t.Fatalf("timed out waiting for %d publisher confirms on %s", n, queue)
	}
	t.Logf("CONFIRMED %d DeliveryMode=2 publishes to NON-DURABLE queue %s (ids %d..%d)",
		n, queue, from, to-1)
}

// durableRecord is one physically-present record, reduced to the two facts this
// test reasons about: the delivery tag it carries, and which incarnation wrote
// it (recovered from the MessageID the publisher stamped).
type durableRecord struct {
	tag       uint64
	messageID int
}

// readDurableRecords returns every physically-present record for `queue` from
// BOTH durable tiers.
//
// Both tiers are consulted because a record migrates between them: the WAL file
// a previous incarnation left behind is registered into oldFiles at the next
// boot, and performCheckpoint (which runs on Close) moves its unacknowledged
// records into segments and removes the file. A reader that looked only at the
// WAL would report records "gone" that had merely moved, which is
// indistinguishable from the destruction this test is trying to measure.
//
// NOTE, stated because it is a side effect and not an accident: opening a WAL
// manager over the directory is itself a boot, and closing it runs a final
// checkpoint. That is why the reads below happen at chosen points rather than
// only at the end — it keeps each incarnation's records in a DIFFERENT tier and
// so keeps two same-tag records from ever having to coexist in one.
func readDurableRecords(t *testing.T, dir, queue string) []durableRecord {
	t.Helper()

	walCfg := storage.DefaultWALConfig()
	walCfg.CleanupInterval = time.Hour
	walCfg.CompactionInterval = time.Hour

	segCfg := storage.DefaultSegmentConfig()
	segCfg.CompactionInterval = time.Hour
	segCfg.CheckpointInterval = time.Hour

	wal, err := storage.NewWALManagerWithConfig(dir, walCfg)
	require.NoError(t, err)
	seg, err := storage.NewSegmentManagerWithConfig(dir, segCfg)
	require.NoError(t, err)

	var out []durableRecord
	add := func(rm *storage.RecoveryMessage) {
		require.NotNil(t, rm.Message,
			"recovered record at tag %d carries no message, so it cannot be attributed to an "+
				"incarnation", rm.Offset)
		id, cerr := strconv.Atoi(rm.Message.MessageID)
		require.NoError(t, cerr,
			"every message this test publishes carries an integer MessageID; got %q at tag %d "+
				"— the records being read are not the ones this test wrote",
			rm.Message.MessageID, rm.Offset)
		out = append(out, durableRecord{tag: rm.Offset, messageID: id})
	}

	fromWAL, err := wal.RecoverFromWAL()
	require.NoError(t, err)
	for _, rm := range fromWAL {
		if rm.QueueName == queue {
			add(rm)
		}
	}

	fromSeg, err := seg.RecoverFromSegments()
	require.NoError(t, err)
	for _, rm := range fromSeg[queue] {
		add(rm)
	}

	require.NoError(t, wal.Close())
	require.NoError(t, seg.Close())
	return out
}

// ---------------------------------------------------------------------------
// the reproduction
// ---------------------------------------------------------------------------

// TestDurableAckGap_Member4_TransientQueueRemintsAnAcknowledgedTagBand is the
// executed form of gap member 4.
//
// Contract it establishes as a FACT about the current tree: a non-durable queue
// carrying DeliveryMode==2 messages re-mints, after a restart and redeclare,
// delivery tags that are still carried by physically-present records which were
// acknowledged before that restart.
//
// It is written as a positive assertion about tag REUSE rather than about data
// loss, because on this tree there is no durable ack set and therefore no loss
// to measure. The loss is what the reuse COSTS the moment such a set exists,
// and the final partition quantifies it against a simulated one.
func TestDurableAckGap_Member4_TransientQueueRemintsAnAcknowledgedTagBand(t *testing.T) {
	b := newRestartableBroker(t, nil) // shipped defaults; no knob is involved
	dir := b.dir

	// ---- BOOT 1: publish durable messages to a NON-DURABLE queue, ack them --
	publishConfirmedDurableToTransientQueue(t, b.uri, member4Queue, 0, member4FirstBatch)

	acked := drainIDs(t, b.uri, member4Queue, member4FirstBatch, false, 10*time.Second, 2*time.Minute)
	missing, _ := missingIDs(acked, member4FirstBatch)
	require.Zero(t, missing,
		"PREMISE BROKEN: only %d of %d messages were delivered and acknowledged in boot 1, so "+
			"this test cannot say anything about an acknowledged tag band",
		member4FirstBatch-missing, member4FirstBatch)

	requireQueueSettlesEmpty(t, b.uri, member4Queue, 60*time.Second)
	b.stop()

	// ---- READER 1: what did boot 1 leave physically on disk? ----------------
	firstIncarnation := readDurableRecords(t, dir, member4Queue)
	require.NotEmpty(t, firstIncarnation,
		"PREMISE BROKEN: boot 1 left NO physically-present durable records for a non-durable "+
			"queue. Member 4 depends on those records surviving the restart (recovery REAPS "+
			"them — it does not delete them), so if they are already gone there is no "+
			"inherited tag band and nothing to collide with")

	firstTags := make(map[uint64]int, len(firstIncarnation)) // tag -> messageID
	for _, r := range firstIncarnation {
		require.Less(t, r.messageID, member4SecondBase,
			"PREMISE BROKEN: record at tag %d carries MessageID %d, which is not from boot 1",
			r.tag, r.messageID)
		firstTags[r.tag] = r.messageID
	}
	t.Logf("PREMISE: boot 1 left %d physically-present DeliveryMode=2 records (%d distinct tags) "+
		"for NON-DURABLE queue %q, all of them delivered AND acknowledged before the restart",
		len(firstIncarnation), len(firstTags), member4Queue)

	// ---- BOOT 2: redeclare the same non-durable queue, publish afresh -------
	b.start()
	publishConfirmedDurableToTransientQueue(t, b.uri, member4Queue,
		member4SecondBase, member4SecondBase+member4SecondBatch)
	b.stop()

	// ---- READER 2: both incarnations' records, on disk together ------------
	all := readDurableRecords(t, dir, member4Queue)

	secondTags := make(map[uint64]int)
	firstStillPresent := 0
	for _, r := range all {
		if r.messageID >= member4SecondBase {
			secondTags[r.tag] = r.messageID
		} else {
			firstStillPresent++
		}
	}
	require.NotEmpty(t, secondTags,
		"PREMISE BROKEN: boot 2's republished records are not physically present, so no "+
			"collision with boot 1's band could be observed either way")

	// THE FINDING: tags carried by a boot-2 record that a boot-1 record also
	// carries. Two distinct records, two distinct MessageIDs, one tag.
	var collisions []uint64
	for tag := range secondTags {
		if _, ok := firstTags[tag]; ok {
			collisions = append(collisions, tag)
		}
	}

	t.Logf("RESULT: boot 1 records still on disk=%d; boot 2 records on disk=%d; "+
		"TAGS RE-MINTED INTO BOOT 1'S ACKNOWLEDGED BAND = %d",
		firstStillPresent, len(secondTags), len(collisions))
	if len(collisions) > 0 {
		sample := collisions[0]
		t.Logf("RESULT: e.g. delivery tag %d is carried by BOTH boot 1's MessageID %d "+
			"(delivered and ACKNOWLEDGED before the restart) and boot 2's MessageID %d "+
			"(confirmed durable after it)", sample, firstTags[sample], secondTags[sample])
	}

	require.NotEmpty(t, collisions,
		"MEMBER 4 DID NOT REPRODUCE: boot 2 re-minted no tag that boot 1 had already used and "+
			"acknowledged. Step 0's derivation of gap member 4 is then wrong on this tree and "+
			"the design step must be told before anything is built on it")

	// ---- the cost, against a SIMULATED tag-keyed ack set --------------------
	//
	// firstTags is exactly what the three withdrawn cycles persisted: the set of
	// delivery-tag VALUES acknowledged by a previous incarnation. Partition boot
	// 2's confirmed durable messages by whether such a set would suppress them.
	destroyed := make(map[int]bool)
	surviving := make(map[int]bool)
	for tag, id := range secondTags {
		if _, inherited := firstTags[tag]; inherited {
			destroyed[id] = true
		} else {
			surviving[id] = true
		}
	}

	t.Logf("RESULT: against an inherited tag-keyed ack set, %d of %d confirmed durable messages "+
		"published in boot 2 would be silently destroyed; %d would survive",
		len(destroyed), len(secondTags), len(surviving))

	require.NotEmpty(t, destroyed,
		"the destroyed set must be non-empty (§4.6): with an empty destroyed set this fixture "+
			"demonstrates no hazard")
	require.NotEmpty(t, surviving,
		"the surviving set must be non-empty (§4.6): a fixture in which EVERYTHING is destroyed "+
			"cannot distinguish targeted destruction from a broker that lost the whole queue, "+
			"which is a different defect with a different fix")
	for id := range destroyed {
		require.False(t, surviving[id],
			"the destroyed and surviving sets must be disjoint (§4.6); MessageID %d is in both", id)
	}

	// ---- BOOT 3: the state persists into a third incarnation ----------------
	//
	// Cycle 2's end-to-end test read back within ONE incarnation and was
	// structurally blind to the stale-snapshot door. Boot 3 exists so that this
	// fixture cannot inherit that blindness.
	b.start()
	after := drainIDs(t, b.uri, member4Queue, 0, false, 5*time.Second, 30*time.Second)
	t.Logf("RESULT (boot 3): %d message(s) delivered from the redeclared non-durable queue — "+
		"AMQP 0-9-1 §1.7.2.1 requires a non-durable queue not to survive a restart, so an "+
		"empty result here is CORRECT and is not evidence against the finding above",
		len(after))

	stillPresent := readDurableRecords(t, dir, member4Queue)
	t.Logf("RESULT (boot 3): %d record(s) for %q are STILL physically present on disk across "+
		"three broker boots; the reaped records are never deleted, which is why the inherited "+
		"tag band outlives every incarnation that could have explained it",
		len(stillPresent), member4Queue)
	require.NotEmpty(t, stillPresent,
		"PREMISE BROKEN: no records remain after boot 3, so the inherited band does not in "+
			"fact outlive the incarnations and member 4's reachability claim needs revisiting")
}
