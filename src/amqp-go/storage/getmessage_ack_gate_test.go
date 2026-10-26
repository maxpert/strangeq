package storage

// Commit 2's fixtures — the TWO acknowledgement guards in
// DisruptorStorage.GetMessage, each attributed to its OWN observable.
//
// WHY THIS FILE EXISTS. segment_ack_gate_test.go covers the pair jointly and
// cannot tell them apart: deleting either guard alone leaves it green, and only
// deleting both reddens it (at segment_ack_gate_test.go:212). That is the
// signature of one observable written twice — "the same mutation as the line
// above", tests.md §2 — and it left both guards unattributed.
//
// They are NOT one observable. They guard different windows and the difference
// is directly observable; the segment fixture is blind to it only because it
// acknowledges BEFORE it probes, which is the one ordering in which the two
// guards agree.
//
//	pre-read gate   a short-circuit. Its observable is that NO COLD TIER IS
//	                CONSULTED. It is not load-bearing for correctness on any tree
//	                — the re-validate catches everything it catches — but it is
//	                what keeps queue_reaper.go reapTTLSweep, which acknowledges a
//	                tag and re-probes it every sweep, from walking readAhead ->
//	                wal.ReadBatch -> wal.Read -> segments.Read (up to three disk
//	                reads) only to return the same error.
//
//	re-validate     the CORRECTNESS guard. Its observable is an acknowledgement
//	                that lands WHILE a cold read is in flight. The pre-read gate
//	                has already run and correctly seen nothing, so it is the only
//	                guard that can refuse.
//
// THE RIG, and why it is deterministic rather than raced. Both fixtures hold the
// SEGMENT TIER HOSTAGE: QueueSegments.readMessage takes qs.mutex as its very
// first act (segment_manager.go), so a test holding qs.mutex can park a reader
// inside the segment tier and acknowledge into that window with no race and no
// deadlock.
//
// THE ACKNOWLEDGEMENT IS ISSUED IN THE WAL TIER ONLY. Both gates consult
// WALManager.IsAcknowledged, whose bitmap is the WAL's. Acknowledging the
// SEGMENT tier as well would let the segment tier itself stop answering (its
// compaction may withdraw the record), and the read would then fail for a
// reason that is NOT the guard under test — the fixture would stay green with
// the re-validate deleted. With the segment tier left live, the record is still
// served by the tier and the ONLY thing that can refuse it is the gate.
//
// ONE CONSTRAINT ON THE RIG, stated because it is invisible and a future edit
// would walk straight into it: SegmentManager.Acknowledge's non-blocking send
// has a fallback. If ackChan is FULL it calls applyAck INLINE, which reaches
// acknowledgeInSegment and takes qs.mutex — the lock these fixtures are holding.
// That would deadlock the test against itself. It cannot happen here because
// these fixtures never acknowledge the segment tier at all. Anyone adding such
// an acknowledgement must keep that true, or acknowledge before taking
// qs.mutex.
//
// The segment tier is not an arbitrary choice: it is the ONLY UNGATED COLD TIER
// (that is F2), so it is the only arm that can hand an acknowledged record back
// to GetMessage and thereby reach the re-validate at all. The WAL arms cannot —
// readMessageBatch filters and QueueWAL.readMessage has its own gate — which is
// exactly why a fixture parked on QueueWAL.fileMutex proves nothing about
// either guard.
//
// SEGSTORE V1: WHERE THE RECORDS COME FROM. The records under test used to
// reach the segment tier because the checkpoint copied WAL records into a
// segment. Checkpoint copying is gone — the WAL tier owns durability and
// nothing moves records out of it — so the fixtures write the records as
// LEGACY segment records instead: the shape a broker finds on disk when it
// upgrades from the checkpointing build. They are never written to the WAL, so
// the WAL genuinely cannot answer for them, and they are never published, so
// the hot cache genuinely does not hold them. The segment tier is therefore
// still the ONLY arm that can answer — the property both guards rest on.
//
// NO ASSERTION IN THIS FILE IS A THRESHOLD. Every verdict comes from a POSITIVE
// observation: either the reader was seen parked inside QueueSegments.readMessage
// (via a runtime.Stack dump the fixture captures itself), or it was seen to
// return. The one duration here is a backstop for the case where NEITHER is
// observed, and it yields SKIP — never a pass and never a failure — because a
// run that could not establish its own precondition proves nothing (tests.md §3).

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/maxpert/amqp-go/protocol"
)

const (
	ackGateQueue = "getmessage.ackgate.q"
	// How many never-acknowledged controls each fixture serves. More than one on
	// purpose: the paired-control rule requires the observation count to be
	// asserted at runtime so an over-suppression control cannot silently degrade
	// to a single sample.
	ackGateControls = 3
	// ackGateTagBase is the first delivery tag the fixture uses. The shape of
	// the tags does not matter to the fixture — the records are never published
	// and never enter the WAL, so nothing can collide with them — but they are
	// spread out so that a tag can never be confused with a neighbouring
	// fixture's.
	ackGateTagBase = uint64(1) << 44
	// ackGateObserveBudget bounds only the search for a positive observation. It
	// is NOT a threshold on either verdict: both outcomes below are reached by
	// observing something, and exhausting this budget produces a SKIP.
	ackGateObserveBudget = 10 * time.Second
)

// ackGateFixture is the arranged state both fixtures share: a record that lives
// in a SEGMENT, misses the hot cache, and cannot be answered for by the WAL — so
// the segment tier is the only arm that can serve it, and holding qs.mutex
// parks any reader of it at a known point.
type ackGateFixture struct {
	ds       *DisruptorStorage
	qs       *QueueSegments
	victim   uint64
	controls []uint64
}

// legacySegmentRecords builds the legacy (pre-upgrade) segment records for
// tags.
//
// The segment tier keys a record by its OFFSET, and GetMessage looks a record
// up by its DELIVERY TAG, so the two must be the same value — which is how the
// broker minted delivery tags.
func legacySegmentRecords(queue string, tags []uint64) []*RecoveryMessage {
	msgs := make([]*RecoveryMessage, 0, len(tags))
	for _, tag := range tags {
		msgs = append(msgs, &RecoveryMessage{
			QueueName: queue,
			Offset:    tag,
			Message: &protocol.Message{
				Exchange:     "amq.direct",
				RoutingKey:   queue,
				Body:         []byte(fmt.Sprintf("ackgate-body-%d", tag)),
				DeliveryMode: 2,
				DeliveryTag:  tag,
				MessageID:    fmt.Sprintf("ackgate-%d", tag),
			},
		})
	}
	return msgs
}

// arrangeAckGate builds that state and asserts every premise it rests on. A
// green on a broken premise here is vacuous, and this exact blindness — a
// fixture whose records never reached a segment — is what let a data-loss
// defect certify green three times.
//
// SEGSTORE V1. The records used to reach the segment tier because the
// checkpoint copied WAL records into a segment. The checkpoint is gone, so they
// are written as LEGACY segment records — the shape a broker finds on disk
// when it upgrades from the checkpointing build. They are never written to the
// WAL and never published, which is what makes "the WAL cannot answer" and "the
// hot cache does not hold them" PROVEN rather than assumed.
func arrangeAckGate(t *testing.T) *ackGateFixture {
	t.Helper()

	dir := t.TempDir()

	tags := make([]uint64, 0, ackGateControls+1)
	tags = append(tags, ackGateTagBase)
	for i := 1; i <= ackGateControls; i++ {
		tags = append(tags, ackGateTagBase+uint64(i))
	}

	writeLegacyFixture(t, dir, ackGateQueue, legacySegmentRecords(ackGateQueue, tags)...)

	ds, err := NewDisruptorStorageWithDataDir(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds.Close() })

	// The segment tier must KNOW the queue: a legacy queue's directory is
	// attributed and opened by the recovery pass, exactly as at boot. Without it
	// the segment tier answers for nothing and the fixture would be vacuous.
	recovered, err := ds.segments.RecoverFromSegments()
	require.NoError(t, err)
	require.Len(t, recovered[ackGateQueue], len(tags),
		"PREMISE BROKEN: the segment tier recovered %d record(s) for %s, need %d — the legacy "+
			"records did not reach the segment tier",
		len(recovered[ackGateQueue]), ackGateQueue, len(tags))

	// The queue must exist in the hot cache: GetMessage refuses an unknown
	// queue outright, and every verdict below would then be vacuous.
	ds.getOrCreateQueueHot(ackGateQueue)
	require.NotNil(t, ds.getQueueHot(ackGateQueue),
		"PREMISE BROKEN: no hot-cache entry for %s", ackGateQueue)

	f := &ackGateFixture{ds: ds, victim: tags[0], controls: tags[1:]}

	// PREMISE: every tag really is in a segment and readable there BEFORE any
	// acknowledgement. Without this the segment tier is an assumed answering arm
	// rather than a demonstrated one.
	for _, tag := range tags {
		segMsg, segErr := ds.segments.Read(ackGateQueue, tag)
		require.NoError(t, segErr,
			"PREMISE BROKEN: tag %d is not readable from the segment tier, so the segment tier cannot "+
				"answer for it and this fixture would park no reader and gate nothing", tag)
		require.NotNil(t, segMsg, "PREMISE BROKEN: segment read for tag %d returned nil, no error", tag)
	}

	// PREMISE: the WAL can no longer answer. This eliminates every other cold
	// arm, so a served record is attributable to the segment tier alone — and it
	// is what makes qs.mutex a park point the reader must actually reach.
	for _, tag := range tags {
		_, walErr := ds.wal.Read(ackGateQueue, tag)
		require.Error(t, walErr,
			"PREMISE BROKEN: the WAL still answers for tag %d. The reader would then be served before "+
				"it ever reached the segment tier, and would never park", tag)
	}

	// PREMISE: nothing is served from the tier that sits ABOVE the segment tier.
	// A hot-cache hit is answered before either guard runs.
	for _, tag := range tags {
		_, _, hot := ds.hot.Get(ackGateQueue, tag)
		require.False(t, hot,
			"PREMISE BROKEN: tag %d is resident in the hot cache, so GetMessage answers from the hot "+
				"path and reaches neither guard", tag)
	}

	// GUARD THE GUARD. The per-tag "is cold" assertions above are only
	// meaningful if the hot cache is working at all — if it were silently
	// empty, every tag would read as cold and the checks would pass while
	// proving nothing. A freshly published message on the SAME queue is the
	// positive control for that.
	probeTag := ackGateTagBase + 1000
	require.NoError(t, ds.StoreMessage(ackGateQueue, &protocol.Message{
		Exchange: "amq.direct", RoutingKey: ackGateQueue,
		Body:         []byte("ackgate-hot-cache-positive-control"),
		DeliveryMode: 2, DeliveryTag: probeTag,
	}))
	_, _, probeHot := ds.hot.Get(ackGateQueue, probeTag)
	require.True(t, probeHot,
		"PREMISE BROKEN: a freshly published message is not in the hot cache, so the hot cache is not "+
			"behaving as the 'cold' assertions above assume and they would pass vacuously")

	val, ok := ds.segments.queueSegments.Load(ackGateQueue)
	require.True(t, ok, "PREMISE BROKEN: no QueueSegments for %s", ackGateQueue)
	f.qs = val.(*QueueSegments)

	t.Logf("PREMISE: %d legacy segment record(s); victim tag=%d, %d control tag(s)=%v; all "+
		"segment-resident, all cold, WAL cannot answer", len(tags), f.victim, len(f.controls), f.controls)

	return f
}

// acknowledgeThroughWAL acknowledges the victim's tag in the WAL tier — the tier
// whose bitmap BOTH acknowledgement gates consult.
//
// It deliberately does NOT acknowledge the segment tier. A segment
// acknowledgement can make the SEGMENT TIER itself stop answering (its
// compaction may withdraw the record), and the read would then fail for a
// reason that is not the guard under test: the fixture would stay green with
// the re-validate deleted. With the segment tier left live, the record is still
// served by the tier and the ONLY thing that can refuse it is the gate.
func (f *ackGateFixture) acknowledgeThroughWAL(t *testing.T) {
	t.Helper()
	f.ds.wal.Acknowledge(ackGateQueue, f.victim)
	require.True(t, f.ds.wal.IsAcknowledged(ackGateQueue, f.victim),
		"PREMISE BROKEN: the acknowledgement was issued but the oracle the gates consult does not "+
			"know about tag %d, so neither gate would have anything to see", f.victim)
}

// parkedInSegmentRead reports whether some goroutine in dump is blocked on
// qs.mutex INSIDE QueueSegments.readMessage.
//
// It matches per goroutine BLOCK rather than by substring over the whole dump:
// batchAckLoop parks in acknowledgeInSegment on the very same mutex while a
// fixture holds it, and a whole-dump substring search would read that as the
// reader having arrived. Requiring both frames in one block cannot.
func parkedInSegmentRead(dump string) bool {
	for _, g := range strings.Split(dump, "\n\n") {
		if strings.Contains(g, "storage.(*QueueSegments).readMessage") &&
			strings.Contains(g, "sync.(*Mutex).Lock") {
			return true
		}
	}
	return false
}

type ackGateResult struct {
	msg *protocol.Message
	err error
}

// serveControls drives the over-suppression arm and ASSERTS THE OBSERVATION
// COUNT. Every historical critical in this subsystem was over-suppression, and
// "an acknowledged record is not served" passes HARDER the more a gate
// over-suppresses — it is structurally blind to the failure that actually
// shipped. This is the arm that is not, and counting the observations is what
// stops it degrading to a single sample.
func serveControls(t *testing.T, f *ackGateFixture) {
	t.Helper()

	// The observation count. Narrow but not dead: arrangeAckGate already asserts
	// require.Len(tags, ackGateControls+1), so the mutation this catches and that
	// one does not is an edit to the `controls: tags[1:]` slice expression.
	// Asserting the count AFTER the loop instead would be dead outright —
	// require.NoError aborts on the first refusal, so a post-loop count can only
	// ever equal len(f.controls) (tests.md §2, the invariant-outcome trap).
	require.Len(t, f.controls, ackGateControls,
		"the over-suppression control would observe %d serve(s), not %d — a control that shrinks "+
			"toward one sample is a liveness check wearing a safety check's name",
		len(f.controls), ackGateControls)

	for _, tag := range f.controls {
		msg, err := f.ds.GetMessage(ackGateQueue, tag)
		require.NoError(t, err,
			"OVER-SUPPRESSION: tag %d was never acknowledged and is segment-resident, so GetMessage "+
				"must still serve it. Refusing it loses confirmed durable data silently, which is the "+
				"direction every withdrawn ack-durability cycle failed in and is strictly worse than "+
				"the bug under repair", tag)
		require.NotNil(t, msg, "control tag %d returned no error and no message either", tag)
	}
}

// TestGetMessage_ReValidateRefusesAnAckThatLandsDuringAColdRead attributes the
// SECOND guard — the re-validate after the tier read.
//
// CLASSIFICATION: SAFETY. It asserts a state that must never hold — an
// acknowledged record handed to a caller — and observes it once, deliberately,
// at a forced interleaving rather than by sampling for it.
//
// MUTATION THAT REDDENS IT: delete the re-validate (the second
// `ds.wal.IsAcknowledged` block, the one after the tier reads) from
// DisruptorStorage.GetMessage. The segment tier hands the record back and the
// require.Error below fires. Deleting the PRE-READ gate instead leaves this
// fixture GREEN, correctly: the acknowledgement had not happened when that gate
// ran, so it never had anything to catch.
func TestGetMessage_ReValidateRefusesAnAckThatLandsDuringAColdRead(t *testing.T) {
	f := arrangeAckGate(t)

	// Park the reader inside the segment tier.
	f.qs.mutex.Lock()
	unlocked := false
	unlock := func() {
		if !unlocked {
			unlocked = true
			f.qs.mutex.Unlock()
		}
	}
	defer unlock()

	done := make(chan ackGateResult, 1)
	go func() {
		msg, err := f.ds.GetMessage(ackGateQueue, f.victim)
		done <- ackGateResult{msg: msg, err: err}
	}()

	// DISCRIMINATOR: prove by POSITIVE OBSERVATION that the reader is inside the
	// tier read before the acknowledgement is issued. If it is not, the ack would
	// land before the pre-read gate, that gate would catch it, and this run would
	// be measuring the wrong guard.
	parked := false
	deadline := time.Now().Add(ackGateObserveBudget)
	for time.Now().Before(deadline) {
		if parkedInSegmentRead(captureAllStacks()) {
			parked = true
			break
		}
		select {
		case r := <-done:
			unlock()
			t.Skipf("INCONCLUSIVE: the reader returned (err=%v) before it reached the segment tier, "+
				"so no acknowledgement could land mid-read and the re-validate was never the guard "+
				"under test. This run proves nothing either way", r.err)
		default:
		}
		time.Sleep(time.Millisecond)
	}
	if !parked {
		unlock()
		<-done
		t.Skip("INCONCLUSIVE: the reader was never observed parked inside QueueSegments.readMessage, " +
			"so the mid-read window was never entered and this run measures nothing")
	}

	// The acknowledgement lands WHILE the read is in flight. WALManager.Acknowledge
	// records the bitmap synchronously, so the re-validate observes it with no
	// ticker in the way.
	f.acknowledgeThroughWAL(t)

	unlock()
	got := <-done

	require.Error(t, got.err,
		"ACKNOWLEDGED RECORD SERVED AFTER A MID-READ ACK: tag %d was acknowledged while a cold read "+
			"of it was parked inside QueueSegments.readMessage. The pre-read gate ran BEFORE that "+
			"acknowledgement and correctly saw nothing, so only the re-validate after the tier read can "+
			"refuse it. Serving it re-delivers a record the broker has already acknowledged", f.victim)
	require.Nil(t, got.msg, "an error was returned but a message came with it")

	// ATTRIBUTION. The segment tier still serves the record, so the refusal
	// above came from the RE-VALIDATE and not from the tier having withdrawn
	// the record. Without this the fixture would go green even with the
	// re-validate deleted: the read would fail in the tier and the error would
	// look exactly the same.
	segMsg, segErr := f.ds.segments.Read(ackGateQueue, f.victim)
	require.NoError(t, segErr,
		"ATTRIBUTION BROKEN: the segment tier no longer serves tag %d, so the refusal above may not "+
			"have come from the re-validate at all", f.victim)
	require.NotNil(t, segMsg)

	serveControls(t, f)
}

// TestGetMessage_PreReadGateAnswersWithoutConsultingAnyColdTier attributes the
// FIRST guard — the short-circuit below the ring read.
//
// It is the mirror of the fixture above and shares its rig. Here the
// acknowledgement lands BEFORE GetMessage is called, so BOTH guards would refuse
// and the returned error attributes neither. The observable that separates them
// is not the answer but the WORK: with the pre-read gate present the call never
// touches a cold tier, so it completes while this fixture is holding the segment
// tier hostage. Without it, the call walks into QueueSegments.readMessage and
// parks there.
//
// CLASSIFICATION: SAFETY on the short-circuit. The verdict is a positive
// observation in BOTH directions — the reader is seen to return, or it is seen
// parked in the segment tier — so there is no threshold. The budget is a
// backstop that yields SKIP.
//
// MUTATION THAT REDDENS IT: delete the pre-read gate (the first
// `ds.wal.IsAcknowledged` block, the one above the read-ahead lookup) from
// DisruptorStorage.GetMessage. The reader is then observed parked and the
// t.Fatalf below fires. Deleting the RE-VALIDATE instead leaves this fixture
// GREEN, correctly: it does not care which guard produces the error, only that
// no cold tier was consulted to produce it.
func TestGetMessage_PreReadGateAnswersWithoutConsultingAnyColdTier(t *testing.T) {
	f := arrangeAckGate(t)

	// Acknowledge FIRST — before the probe, which is the ordering the pre-read
	// gate exists for and the one the reaper produces every sweep.
	f.acknowledgeThroughWAL(t)

	// Hold the segment tier hostage. Any reader that consults it parks here.
	f.qs.mutex.Lock()
	unlocked := false
	unlock := func() {
		if !unlocked {
			unlocked = true
			f.qs.mutex.Unlock()
		}
	}
	defer unlock()

	done := make(chan ackGateResult, 1)
	go func() {
		msg, err := f.ds.GetMessage(ackGateQueue, f.victim)
		done <- ackGateResult{msg: msg, err: err}
	}()

	deadline := time.Now().Add(ackGateObserveBudget)
	for {
		select {
		case got := <-done:
			// OBSERVED: answered while the segment tier was held. It cannot have
			// consulted it.
			unlock()
			require.Error(t, got.err,
				"tag %d was acknowledged before this probe, so GetMessage must refuse it", f.victim)
			require.Nil(t, got.msg, "an error was returned but a message came with it")
			serveControls(t, f)
			return
		default:
		}

		if parkedInSegmentRead(captureAllStacks()) {
			// OBSERVED: it went to the segment tier for a tag it already knew was
			// acknowledged. Release and drain before failing, or the deferred
			// Close blocks and takes later fixtures with it.
			unlock()
			<-done
			t.Fatalf("NO SHORT-CIRCUIT: GetMessage walked into QueueSegments.readMessage for tag %d, "+
				"which was acknowledged before the call. The pre-read gate is what stops that. "+
				"broker/queue_reaper.go reapTTLSweep acknowledges a tag and re-probes it inside the "+
				"same sweep, so without this gate every sweep pays readAhead -> wal.ReadBatch -> "+
				"wal.Read -> segments.Read, up to three disk reads, to return the error the bitmap "+
				"already had", f.victim)
			return
		}

		if !time.Now().Before(deadline) {
			unlock()
			<-done
			t.Skip("INCONCLUSIVE: the reader neither returned nor was observed parked inside " +
				"QueueSegments.readMessage, so neither guard was observed and this run measures nothing")
			return
		}
		time.Sleep(time.Millisecond)
	}
}

// ---------------------------------------------------------------------------
// CROSS-QUEUE OVER-SUPPRESSION
// ---------------------------------------------------------------------------

const (
	xqQueueA = "getmessage.ackgate.a"
	xqQueueB = "getmessage.ackgate.b"
	// Two DIFFERENT queue ordinals. The whole point of this fixture is that the
	// two queues carry the same per-queue sequence numbers and are told apart by
	// the ordinal alone.
	xqOrdinalA = 1
	xqOrdinalB = 2
	// xqOrdinalShift mirrors broker.OrdinalShift. It is duplicated rather than
	// imported because broker imports storage, so importing it back here would
	// be an import cycle. If the packing layout ever changes, the disjointness
	// premise below fails loudly rather than this constant drifting in silence.
	xqOrdinalShift = 44
)

func xqTag(ordinal, seq uint64) uint64 { return ordinal<<xqOrdinalShift | seq }

// TestGetMessage_AckOnOneQueueDoesNotSuppressAnother is the CROSS-QUEUE
// over-suppression control, and nothing else in this change set covers the axis
// it covers.
//
// WHY IT EXISTS. Every cold tier DisruptorStorage.GetMessage consults is gated
// on WALManager.IsAcknowledged, so an acknowledgement on one queue must never
// suppress a record on another. Two properties make that true, and this
// fixture pins the BEHAVIOUR both of them produce: the acknowledgement bitmap
// is queue-scoped, and the two queues' delivery tags are distinct (tags are
// packed `ordinal << 44 | seq`, and the ordinal allocator never reuses an
// ordinal).
//
// The fixture acknowledges a tag on queue A and requires that the record on
// queue B carrying THE SAME PER-QUEUE SEQUENCE is still served. That pairing is
// the point: the two tags differ only in their ordinal bits, so if the gate
// ever stopped distinguishing them — a narrowed oracle, a masked tag, a global
// tag space reintroduced — this is the arm that sees it.
//
// CLASSIFICATION: SAFETY, on the over-suppression side. Every historical
// critical in this subsystem was over-suppression, and it is the direction that
// loses confirmed durable data silently rather than merely redelivering.
//
// MUTATION THAT REDDENS IT: make the acknowledgement lookup queue-INDEPENDENT
// (one global bitmap) AND key it by the per-queue sequence rather than by the
// full tag — `offset & ((1<<44)-1)`. Queue A's suppression assertion still
// passes; the queue B assertions below fail, because B's same-seq record is
// refused.
func TestGetMessage_AckOnOneQueueDoesNotSuppressAnother(t *testing.T) {
	dir := t.TempDir()

	// The same per-queue sequence range on both queues. The two tags differ
	// only in their ordinal bits, which is the only thing that tells the two
	// queues' records apart.
	queues := []struct {
		name    string
		ordinal uint64
	}{{xqQueueA, xqOrdinalA}, {xqQueueB, xqOrdinalB}}

	seqs := make([]uint64, 0, ackGateControls+1)
	for seq := uint64(1); seq <= uint64(ackGateControls+1); seq++ {
		seqs = append(seqs, seq)
	}
	require.Len(t, seqs, ackGateControls+1,
		"PREMISE BROKEN: %d sequence(s) selected, need %d — one to acknowledge on queue A "+
			"plus %d cross-queue controls on queue B",
		len(seqs), ackGateControls+1, ackGateControls)

	// The SAME-TAG probe record: queue B carries a record whose delivery tag is
	// the VICTIM's tag on queue A. The broker's tag space is global, so two
	// queues never carry the same delivery tag; this is therefore a state only
	// a fault in the acknowledgement oracle could produce — which is exactly
	// why it is here. See the probe assertion below.
	sameTagOnB := xqTag(xqOrdinalA, seqs[0])

	// SEGSTORE V1. The records are written as LEGACY segment records (the
	// checkpoint that used to copy WAL records into a segment is gone), so the
	// segment tier is the arm under test and the WAL holds nothing at all.
	for _, q := range queues {
		tags := make([]uint64, 0, len(seqs)+1)
		for _, seq := range seqs {
			tags = append(tags, xqTag(q.ordinal, seq))
		}
		if q.name == xqQueueB {
			tags = append(tags, sameTagOnB)
		}
		writeLegacyFixture(t, dir, q.name, legacySegmentRecords(q.name, tags)...)
	}

	ds, err := NewDisruptorStorageWithDataDir(dir)
	require.NoError(t, err)
	defer func() { _ = ds.Close() }()

	// The segment tier must KNOW both queues: a legacy queue's directory is
	// attributed and opened by the recovery pass, exactly as at boot.
	recovered, err := ds.segments.RecoverFromSegments()
	require.NoError(t, err)
	for _, q := range queues {
		want := len(seqs)
		if q.name == xqQueueB {
			want++ // the same-tag probe record
		}
		require.Len(t, recovered[q.name], want,
			"PREMISE BROKEN: the segment tier recovered %d record(s) for %s, need %d — the legacy "+
				"records did not reach the segment tier",
			len(recovered[q.name]), q.name, want)
	}

	for _, q := range queues {
		ds.getOrCreateQueueHot(q.name)
		require.NotNil(t, ds.getQueueHot(q.name),
			"PREMISE BROKEN: no hot-cache entry for %s", q.name)
	}

	// PREMISE: THIS IS ACTUALLY A CROSS-QUEUE TEST. Two tags that differ only in
	// their sequence bits are one queue's band, and a fixture built on them is an
	// expensive same-queue test wearing a cross-queue name. What must differ is
	// the ORDINAL — that is the only thing distinguishing the two queues in a tag.
	//
	// This is the guard that fires if someone sets the two ordinals equal.
	require.NotEqual(t, uint64(xqOrdinalA), uint64(xqOrdinalB),
		"PREMISE BROKEN: both queues use ordinal %d, so every tag below belongs to ONE queue's "+
			"band and nothing here is cross-queue", uint64(xqOrdinalA))
	for _, seq := range seqs {
		a, b := xqTag(xqOrdinalA, seq), xqTag(xqOrdinalB, seq)
		require.NotEqual(t, a>>xqOrdinalShift, b>>xqOrdinalShift,
			"PREMISE BROKEN: tags %d and %d carry the SAME ordinal bits, so they are the same "+
				"queue's band at sequence %d and this is not a cross-queue probe", a, b, seq)
		require.NotEqual(t, a, b,
			"PREMISE BROKEN: queues A and B produce the SAME delivery tag for sequence %d, so the "+
				"two queues' records are indistinguishable and the probe below is not cross-queue", seq)
	}

	// PREMISE: every tag under test is segment-resident and COLD on its own
	// queue. COLD means the tier above the segment: the hot cache, which sits
	// above the acknowledgement gate and would answer before it is ever
	// consulted.
	for _, seq := range seqs {
		for _, q := range queues {
			tag := xqTag(q.ordinal, seq)
			segMsg, segErr := ds.segments.Read(q.name, tag)
			require.NoError(t, segErr,
				"PREMISE BROKEN: tag %d (queue %s, seq %d) is not readable from the segment tier, so "+
					"this probe would not exercise the gated cold path", tag, q.name, seq)
			require.NotNil(t, segMsg, "PREMISE BROKEN: segment read for tag %d returned nil, no error", tag)

			_, walErr := ds.wal.Read(q.name, tag)
			require.Error(t, walErr,
				"PREMISE BROKEN: the WAL still answers for tag %d (queue %s), so this probe would be "+
					"served from the WAL and never reach the gated cold path", tag, q.name)

			_, _, hot := ds.hot.Get(q.name, tag)
			require.False(t, hot,
				"PREMISE BROKEN: tag %d is resident in %s's hot cache, so GetMessage answers from "+
					"the hot path and never consults the acknowledgement gate", tag, q.name)
		}
	}

	// GUARD THE GUARD. The "is cold" assertions above are only meaningful if the
	// hot cache is working at all: if it were silently empty, every tag would
	// read as cold and the checks would pass while proving nothing. A freshly
	// published message on EACH queue is the positive control.
	for _, q := range queues {
		probeTag := xqTag(q.ordinal, 1000)
		require.NoError(t, ds.StoreMessage(q.name, &protocol.Message{
			Exchange: "amq.direct", RoutingKey: q.name,
			Body: []byte("xq-hot-cache-positive-control"), DeliveryMode: 2, DeliveryTag: probeTag,
		}))
		_, _, probeHot := ds.hot.Get(q.name, probeTag)
		require.True(t, probeHot,
			"PREMISE BROKEN: a freshly published message is not in %s's hot cache, so the hot cache "+
				"is not behaving as the 'cold' assertions above assume", q.name)
	}

	victimSeq, controlSeqs := seqs[0], seqs[1:]
	victimA := xqTag(xqOrdinalA, victimSeq)
	twinB := xqTag(xqOrdinalB, victimSeq)

	t.Logf("PREMISE: acking queue A tag=%d (seq %d); twin on queue B=%d; %d cross-queue control "+
		"seq(s)=%v", victimA, victimSeq, twinB, len(controlSeqs), controlSeqs)

	// ACT: acknowledge on queue A only, in the WAL tier — the tier whose bitmap
	// the gates consult.
	ds.wal.Acknowledge(xqQueueA, victimA)
	require.True(t, ds.wal.IsAcknowledged(xqQueueA, victimA),
		"PREMISE BROKEN: the acknowledgement was issued but the oracle does not know tag %d was "+
			"acked, so neither half of this fixture is under test", victimA)
	require.False(t, ds.wal.IsAcknowledged(xqQueueB, victimA),
		"PREMISE BROKEN: the oracle does not discriminate the queue: acknowledging queue A already "+
			"reads as an acknowledgement on queue B")

	// SUPPRESSION HALF — the ack must take effect on its OWN queue. Without this
	// the fixture would pass on a gate that suppresses nothing at all.
	gotA, errA := ds.GetMessage(xqQueueA, victimA)
	require.Error(t, errA,
		"tag %d was acknowledged on queue %s and must not be served", victimA, xqQueueA)
	require.Nil(t, gotA, "an error was returned but a message came with it")

	// CROSS-QUEUE CONTROL — the twin, same per-queue sequence, different queue.
	gotB, errB := ds.GetMessage(xqQueueB, twinB)
	require.NoError(t, errB,
		"CROSS-QUEUE OVER-SUPPRESSION: tag %d on queue %s was NEVER acknowledged, but it was "+
			"refused after tag %d — the same per-queue sequence %d on queue %s — was acknowledged. "+
			"An acknowledgement on one queue must never suppress another queue's record. Refusing a "+
			"live durable record loses confirmed data silently, which is the direction every "+
			"withdrawn ack-durability cycle failed in",
		twinB, xqQueueB, victimA, victimSeq, xqQueueA)
	require.NotNil(t, gotB, "the cross-queue twin returned no error and no message either")

	// THE SAME-TAG PROBE. The same delivery tag on another queue — a state the
	// broker's global tag space makes impossible, and therefore a state only a
	// fault in the oracle could produce. It is here because it makes the guard
	// LOAD-BEARING: an oracle that stopped discriminating the QUEUE (one global
	// bitmap) would let queue A's acknowledgement suppress queue B's record,
	// and the same-seq twin above would NOT catch that, because the two queues'
	// tags differ in their ordinal bits.
	gotSame, errSame := ds.GetMessage(xqQueueB, sameTagOnB)
	require.NoError(t, errSame,
		"CROSS-QUEUE OVER-SUPPRESSION: tag %d on queue %s was NEVER acknowledged, but it was refused "+
			"after the same tag was acknowledged on queue %s. The acknowledgement oracle does not "+
			"discriminate the queue", sameTagOnB, xqQueueB, xqQueueA)
	require.NotNil(t, gotSame, "the same-tag cross-queue probe returned no error and no message either")

	// And the counted controls, so this arm cannot degrade to the single twin.
	require.Len(t, controlSeqs, ackGateControls,
		"the cross-queue control would observe %d serve(s), not %d — a control that shrinks toward "+
			"one sample cannot see over-suppression", len(controlSeqs), ackGateControls)
	for _, seq := range controlSeqs {
		tag := xqTag(xqOrdinalB, seq)
		msg, err := ds.GetMessage(xqQueueB, tag)
		require.NoError(t, err,
			"CROSS-QUEUE OVER-SUPPRESSION: tag %d on queue %s was never acknowledged and must "+
				"still be served after an unrelated acknowledgement on queue %s", tag, xqQueueB, xqQueueA)
		require.NotNil(t, msg, "cross-queue control tag %d returned no error and no message", tag)
	}
}
