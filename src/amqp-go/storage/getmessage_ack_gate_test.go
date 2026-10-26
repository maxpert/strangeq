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
// first act (segment_manager.go), and DisruptorStorage.DeleteMessage never takes
// that lock — SegmentManager.Acknowledge is a non-blocking ackChan send — so a
// test holding qs.mutex can park a reader inside the segment tier and
// acknowledge into that window with no race and no deadlock.
//
// ONE CONSTRAINT ON THE RIG, stated because it is invisible and a future edit
// would walk straight into it: SegmentManager.Acknowledge's non-blocking send
// has a fallback. If ackChan is FULL it calls applyAck INLINE, which reaches
// acknowledgeInSegment and takes qs.mutex — the lock these fixtures are holding.
// That would deadlock the test against itself. It cannot happen here because
// each fixture issues exactly ONE acknowledgement against a freshly built
// storage with an empty channel. Anyone adding acknowledgements to these
// fixtures must keep that true, or acknowledge before taking qs.mutex.
//
// The segment tier is not an arbitrary choice: it is the ONLY UNGATED COLD TIER
// (that is F2), so it is the only arm that can hand an acknowledged record back
// to GetMessage and thereby reach the re-validate at all. The WAL arms cannot —
// readMessageBatch filters and QueueWAL.readMessage has its own gate — which is
// exactly why a fixture parked on QueueWAL.fileMutex proves nothing about
// either guard.
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

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
)

const (
	ackGateQueue = "getmessage.ackgate.q"
	// Small enough that a few dozen half-kilobyte records roll the file several
	// times. performCheckpoint iterates oldFiles and nothing else, so without a
	// roll no record ever reaches a segment and every fixture here is vacuous.
	ackGateWALFileSize = 8 * 1024
	ackGateRecords     = 64
	ackGateBodySize    = 512
	// A tiny ring is load-bearing. GetMessage reads the ring FIRST and UNGATED,
	// so a record still resident there never reaches either guard. The default
	// ring holds 256K entries and would keep every record in this fixture hot.
	//
	// WHICH records go cold is the opposite of the intuition, and getting it
	// backwards is what the premise assertions below caught: AtomicRing.Store
	// places at `deliveryTag & mask` with CompareAndSwap(nil, msg) and NEVER
	// overwrites an occupied slot — it reports the store as spilled instead. So
	// with consecutive tags the FIRST ackGateRingSize records own the ring
	// permanently and every later one is cold. Candidates are therefore taken
	// from beyond that prefix, and both halves of that model are asserted.
	ackGateRingSize = 8
	// How many never-acknowledged controls each fixture serves. More than one on
	// purpose: the paired-control rule requires the observation count to be
	// asserted at runtime so an over-suppression control cannot silently degrade
	// to a single sample.
	ackGateControls = 3
	// ackGateObserveBudget bounds only the search for a positive observation. It
	// is NOT a threshold on either verdict: both outcomes below are reached by
	// observing something, and exhausting this budget produces a SKIP.
	ackGateObserveBudget = 10 * time.Second
)

// ackGateFixture is the arranged state both fixtures share: a record that lives
// in a SEGMENT, misses the ring, and cannot be answered for by the WAL — so the
// segment tier is the only arm that can serve it, and holding qs.mutex parks any
// reader of it at a known point.
type ackGateFixture struct {
	ds       *DisruptorStorage
	qs       *QueueSegments
	victim   uint64
	controls []uint64
}

// arrangeAckGate builds that state and asserts every premise it rests on. A
// green on a broken premise here is vacuous, and this exact blindness — a
// fixture at DefaultWALFileSize that never rolls — let a data-loss defect
// certify green three times.
func arrangeAckGate(t *testing.T) *ackGateFixture {
	t.Helper()

	ds, err := NewDisruptorStorageWithEngineConfig(t.TempDir(), interfaces.EngineConfig{
		WALFileSize:    ackGateWALFileSize,
		RingBufferSize: ackGateRingSize,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds.Close() })

	body := make([]byte, ackGateBodySize)
	for i := range body {
		body[i] = byte('a' + i%26)
	}
	base := uint64(1) << 44
	for i := 0; i < ackGateRecords; i++ {
		require.NoError(t, ds.StoreMessage(ackGateQueue, &protocol.Message{
			RoutingKey:   ackGateQueue,
			Body:         body,
			DeliveryMode: 2, // durable: takes the WAL branch unconditionally
			DeliveryTag:  base + uint64(i),
			MessageID:    fmt.Sprintf("ackgate-%d", i),
		}))
	}

	sw := ds.wal.sharedWAL
	require.NotNil(t, sw, "PREMISE BROKEN: no shared WAL was constructed")

	// PREMISE: the WAL rolled, and we can name enough offsets inside rolled
	// files. performCheckpoint only ever moves records out of rolled files.
	//
	// Offsets below base+ackGateRingSize are skipped because those are the tags
	// that own the ring (see ackGateRingSize). The skip is a SELECTION rule, not
	// the premise — the premise is that the selected tags are actually cold, and
	// that is asserted against the ring itself further down.
	coldFrom := base + ackGateRingSize
	sw.oldFilesMutex.RLock()
	rolledFiles := len(sw.oldFiles)
	var tags []uint64
	for _, info := range sw.oldFiles {
		if info.offsets == nil {
			continue
		}
		it := info.offsets.Iterator()
		for it.HasNext() && len(tags) < ackGateControls+1 {
			if off := it.Next(); off >= coldFrom {
				tags = append(tags, off)
			}
		}
		if len(tags) == ackGateControls+1 {
			break
		}
	}
	sw.oldFilesMutex.RUnlock()

	require.NotZero(t, rolledFiles,
		"PREMISE BROKEN: the WAL never rolled at FileSize=%d after %d records of %d bytes, so "+
			"qw.oldFiles is empty and performCheckpoint cannot move any record into a segment. "+
			"The segment tier would then never answer and neither guard is under test",
		ackGateWALFileSize, ackGateRecords, ackGateBodySize)
	require.Len(t, tags, ackGateControls+1,
		"PREMISE BROKEN: found only %d distinct offsets in %d rolled file(s), need %d — one victim "+
			"plus %d over-suppression controls at the same altitude",
		len(tags), rolledFiles, ackGateControls+1, ackGateControls)

	// ACT: move them WAL -> segment. This is the shipped function the checkpoint
	// ticker calls; invoking it directly rather than sleeping out the ticker
	// manufactures no state.
	sw.performCheckpoint()

	f := &ackGateFixture{ds: ds, victim: tags[0], controls: tags[1:]}

	// PREMISE: every tag really is in a segment and readable there BEFORE any
	// acknowledgement. Without this the segment tier is an assumed answering arm
	// rather than a demonstrated one.
	for _, tag := range tags {
		segMsg, segErr := ds.segments.Read(ackGateQueue, tag)
		require.NoError(t, segErr,
			"PREMISE BROKEN: tag %d did not reach a segment, so the segment tier cannot answer for "+
				"it and this fixture would park no reader and gate nothing", tag)
		require.NotNil(t, segMsg, "PREMISE BROKEN: segment read for tag %d returned nil, no error", tag)
	}

	// PREMISE: the WAL can no longer answer. This eliminates every other cold
	// arm, so a served record is attributable to the segment tier alone — and it
	// is what makes qs.mutex a park point the reader must actually reach.
	_, walErr := ds.wal.Read(ackGateQueue, f.victim)
	require.Error(t, walErr,
		"PREMISE BROKEN: the WAL still answers for tag %d after performCheckpoint removed its file "+
			"and index entry. The reader would then be served before it ever reached the segment "+
			"tier, and would never park", f.victim)

	// PREMISE: nothing is served from the two tiers that sit ABOVE the segment
	// tier. A ring hit is answered before either guard runs; a read-ahead hit is
	// answered before the tier read and so parks nothing.
	ring := ds.getQueueHot(ackGateQueue)
	require.NotNil(t, ring, "PREMISE BROKEN: no queue ring for %s", ackGateQueue)

	// GUARD THE GUARD. The per-tag "is cold" assertions below are only meaningful
	// if the ring is populated at all — if RingBufferSize were silently ignored,
	// or the ring were emptied by something, every tag would read as cold and the
	// checks would pass while proving nothing. base is the first tag stored and
	// owns its slot for the life of the fixture, so its presence is the positive
	// control for the placement model the selection rule above relies on.
	_, _, ringPopulated := ds.hot.Get(ackGateQueue, base)
	require.True(t, ringPopulated,
		"PREMISE BROKEN: tag %d — the FIRST record stored — is not in the ring, so the ring is not "+
			"behaving as AtomicRing.Store describes and the 'cold' assertions below would pass "+
			"vacuously for every tag", base)

	for _, tag := range tags {
		ds.hot.Delete(ackGateQueue, tag)
		_, _, hot := ds.hot.Get(ackGateQueue, tag)
		require.False(t, hot,
			"PREMISE BROKEN: tag %d is still resident in the ring at RingBufferSize=%d, so "+
				"GetMessage answers from the hot path and reaches neither guard", tag, ackGateRingSize)
	}

	val, ok := ds.segments.queueSegments.Load(ackGateQueue)
	require.True(t, ok, "PREMISE BROKEN: no QueueSegments for %s", ackGateQueue)
	f.qs = val.(*QueueSegments)

	t.Logf("PREMISE: %d rolled WAL file(s); victim tag=%d, %d control tag(s)=%v; all segment-resident, "+
		"all evicted from the ring, WAL cannot answer", rolledFiles, f.victim, len(f.controls), f.controls)

	return f
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
	require.NoError(t, f.ds.DeleteMessage(ackGateQueue, f.victim))
	require.True(t, f.ds.wal.IsAcknowledged(ackGateQueue, f.victim),
		"PREMISE BROKEN: DeleteMessage returned but the acknowledgement oracle does not know about "+
			"tag %d, so the re-validate below would have nothing to see", f.victim)

	unlock()
	got := <-done

	require.Error(t, got.err,
		"ACKNOWLEDGED RECORD SERVED AFTER A MID-READ ACK: tag %d was acknowledged through "+
			"DisruptorStorage.DeleteMessage while a cold read of it was parked inside "+
			"QueueSegments.readMessage. The pre-read gate ran BEFORE that acknowledgement and "+
			"correctly saw nothing, so only the re-validate after the tier read can refuse it. "+
			"Serving it re-delivers a record the broker has already acknowledged", f.victim)
	require.Nil(t, got.msg, "an error was returned but a message came with it")

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
	require.NoError(t, f.ds.DeleteMessage(ackGateQueue, f.victim))
	require.True(t, f.ds.wal.IsAcknowledged(ackGateQueue, f.victim),
		"PREMISE BROKEN: DeleteMessage returned but the acknowledgement oracle does not know about "+
			"tag %d, so the pre-read gate would have nothing to short-circuit on", f.victim)

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
// WHY IT EXISTS. WALManager.IsAcknowledged takes an offset and NO QUEUE
// DISCRIMINATOR — it is one global bitmap lookup against the shared WAL, and
// DisruptorStorage.GetMessage gates every cold tier on it. The entire safety of
// that design rests on delivery tags being globally unique across queues. That
// invariant is real (tags are packed `ordinal << 44 | seq`, and the ordinal
// allocator never reuses an ordinal), but it is enforced in the broker, one
// layer above the gate that depends on it, and no fixture asserted it from
// here.
//
// This fixture acknowledges a tag on queue A and requires that the record on
// queue B carrying THE SAME PER-QUEUE SEQUENCE is still served. That pairing is
// the point: the two tags differ only in their ordinal bits, so if the gate
// ever stopped distinguishing them — a narrowed oracle, a masked tag, a
// per-queue tag space reintroduced — this is the arm that sees it.
//
// CLASSIFICATION: SAFETY, on the over-suppression side. Every historical
// critical in this subsystem was over-suppression, and it is the direction that
// loses confirmed durable data silently rather than merely redelivering.
//
// MUTATION THAT REDDENS IT: mask the ordinal out of the oracle — make
// WALManager.IsAcknowledged consult `offset & ((1<<44)-1)`, i.e. the per-queue
// sequence only. Queue A's suppression assertion still passes; the queue B
// assertions below fail, because B's same-seq record is refused.
func TestGetMessage_AckOnOneQueueDoesNotSuppressAnother(t *testing.T) {
	ds, err := NewDisruptorStorageWithEngineConfig(t.TempDir(), interfaces.EngineConfig{
		WALFileSize:    ackGateWALFileSize,
		RingBufferSize: ackGateRingSize,
	})
	require.NoError(t, err)
	defer func() { _ = ds.Close() }()

	body := make([]byte, ackGateBodySize)
	for i := range body {
		body[i] = byte('a' + i%26)
	}

	// Same sequence range on both queues, interleaved so they share rolled files.
	for seq := 0; seq < ackGateRecords; seq++ {
		for _, q := range []struct {
			name    string
			ordinal uint64
		}{{xqQueueA, xqOrdinalA}, {xqQueueB, xqOrdinalB}} {
			require.NoError(t, ds.StoreMessage(q.name, &protocol.Message{
				RoutingKey:   q.name,
				Body:         body,
				DeliveryMode: 2,
				DeliveryTag:  xqTag(q.ordinal, uint64(seq)),
				MessageID:    fmt.Sprintf("%s-%d", q.name, seq),
			}))
		}
	}

	sw := ds.wal.sharedWAL
	require.NotNil(t, sw, "PREMISE BROKEN: no shared WAL was constructed")

	// Candidate sequences: those whose tags on BOTH queues sit in a rolled file,
	// skipping the ring-resident prefix (see ackGateRingSize). Selection is on
	// "was in a rolled file"; the assertions below are on "reached a segment"
	// and "misses the ring", which are different predicates — so they stay live.
	sw.oldFilesMutex.RLock()
	rolledFiles := len(sw.oldFiles)
	rolled := make(map[uint64]bool)
	for _, info := range sw.oldFiles {
		if info.offsets == nil {
			continue
		}
		it := info.offsets.Iterator()
		for it.HasNext() {
			rolled[it.Next()] = true
		}
	}
	sw.oldFilesMutex.RUnlock()

	var seqs []uint64
	for seq := uint64(ackGateRingSize); seq < ackGateRecords && len(seqs) < ackGateControls+1; seq++ {
		if rolled[xqTag(xqOrdinalA, seq)] && rolled[xqTag(xqOrdinalB, seq)] {
			seqs = append(seqs, seq)
		}
	}

	require.NotZero(t, rolledFiles,
		"PREMISE BROKEN: the WAL never rolled at FileSize=%d, so performCheckpoint moves nothing "+
			"into a segment and no cold read is under test", ackGateWALFileSize)
	require.Len(t, seqs, ackGateControls+1,
		"PREMISE BROKEN: found only %d sequence(s) present in a rolled file on BOTH queues, need "+
			"%d — one to acknowledge on queue A plus %d cross-queue controls on queue B",
		len(seqs), ackGateControls+1, ackGateControls)

	sw.performCheckpoint()

	// PREMISE: THIS IS ACTUALLY A CROSS-QUEUE TEST. Two tags that differ only in
	// their sequence bits are one queue's band, and a fixture built on them is an
	// expensive same-queue test wearing a cross-queue name. What must differ is
	// the ORDINAL — that is the only thing distinguishing the two queues in a tag,
	// and the only reason the global oracle is safe.
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
			"PREMISE BROKEN: queues A and B produce the SAME delivery tag for sequence %d. "+
				"WALManager.IsAcknowledged has no queue discriminator, so a shared tag space means "+
				"an ack on one queue necessarily suppresses the other", seq)
	}

	// PREMISE: every tag under test is segment-resident and COLD on its own
	// queue. COLD means BOTH tiers above the segment: the ring, which sits above
	// the acknowledgement gate and would answer before it is ever consulted, and
	// the read-ahead buffer, which sits below the gate but above the tier read
	// and would answer without the segment tier being touched — leaving the
	// cross-queue control served from a different altitude than the subject.
	for _, seq := range seqs {
		for _, q := range []struct {
			name    string
			ordinal uint64
		}{{xqQueueA, xqOrdinalA}, {xqQueueB, xqOrdinalB}} {
			tag := xqTag(q.ordinal, seq)
			ds.hot.Delete(q.name, tag)
			segMsg, segErr := ds.segments.Read(q.name, tag)
			require.NoError(t, segErr,
				"PREMISE BROKEN: tag %d (queue %s, seq %d) did not reach a segment, so this probe "+
					"would not exercise the gated cold path", tag, q.name, seq)
			require.NotNil(t, segMsg, "PREMISE BROKEN: segment read for tag %d returned nil, no error", tag)

			ring := ds.getQueueHot(q.name)
			require.NotNil(t, ring, "PREMISE BROKEN: no queue ring for %s", q.name)
			_, _, hot := ds.hot.Get(q.name, tag)
			require.False(t, hot,
				"PREMISE BROKEN: tag %d is still resident in %s's ring, so GetMessage answers from "+
					"the hot path and never consults the acknowledgement gate", tag, q.name)

		}
	}

	victimSeq, controlSeqs := seqs[0], seqs[1:]
	victimA := xqTag(xqOrdinalA, victimSeq)
	twinB := xqTag(xqOrdinalB, victimSeq)

	t.Logf("PREMISE: %d rolled WAL file(s); acking queue A tag=%d (seq %d); twin on queue B=%d; "+
		"%d cross-queue control seq(s)=%v", rolledFiles, victimA, victimSeq, twinB, len(controlSeqs), controlSeqs)

	// ACT: acknowledge on queue A only.
	require.NoError(t, ds.DeleteMessage(xqQueueA, victimA))
	require.True(t, ds.wal.IsAcknowledged(xqQueueA, victimA),
		"PREMISE BROKEN: DeleteMessage returned but the oracle does not know tag %d was acked, so "+
			"neither half of this fixture is under test", victimA)

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
			"WALManager.IsAcknowledged takes no queue discriminator, so it is only correct while "+
			"delivery tags are globally unique. Refusing a live durable record loses confirmed data "+
			"silently, which is the direction every withdrawn ack-durability cycle failed in",
		twinB, xqQueueB, victimA, victimSeq, xqQueueA)
	require.NotNil(t, gotB, "the cross-queue twin returned no error and no message either")

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
