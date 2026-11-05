package main

// Loop 3 Step 2 — does the stale-read chain CLOSE?
//
// Three fragments were separately EXECUTED by earlier steps and never joined in
// one execution:
//
//	(1) PRODUCER (gap member 4, R1). A durable=false queue leaves a permanent
//	    metadata record holding ordinal N; after restart+redeclare the broker
//	    re-mints tags into a band still carried by physically-present records of
//	    the previous incarnation.
//	(2) READ (D2b + the reachability run). WALManager.Acknowledge deletes the
//	    tag's offsetIndex entry, after which readMessage falls back to
//	    readMessageSequential, which searches oldFiles UNCONDITIONALLY BEFORE the
//	    current file and returns a stale record as a live message.
//	(3) CONSUMER (DERIVED only). broker/queue_reaper.go reapTTLSweep probes every
//	    tag in its window with b.storage.GetMessage and its own comment documents
//	    the expectation that an acked tag comes back empty. If a stale record
//	    answers instead it can be dead-lettered as expired, and the tail-advance
//	    loop breaks on a non-nil message so the tail stops advancing.
//
// THE COUNTER-ARGUMENT the previous step could not settle: the reaper only
// sweeps a CONSUMER-LESS queue, and the WAL read is only reached for a tag that
// is OUT OF THE RING. Nobody had shown those two conditions co-occur.
//
// These two tests are written to make BOTH answers reachable, and to keep them
// attributable to different mechanisms:
//
//	S1 — ONE incarnation. No member 4, no prior-incarnation record, no oldFiles.
//	     It asks only whether the reaper's own removal of a message from the ring
//	     puts that tag back on the WAL read path. S1 is the ATTRIBUTION CONTROL
//	     for S2: if S1 already amplifies, then an amplified S2 says nothing about
//	     member 4 on its own and only the boot-1 MessageIDs do.
//
//	S2 — TWO incarnations, the mandated chain end to end: non-durable queue with
//	     x-message-ttl + a DLX, boot 1 publishes DeliveryMode==2 and acks, restart,
//	     redeclare, republish, left CONSUMER-LESS so the reaper sweeps. A
//	     dead-lettered message carrying a BOOT-1 MessageID is a prior
//	     incarnation's record resurrected through the production read path, which
//	     is the whole chain in one execution.
//
// Both tests measure through the shipped AMQP surface only: publish with
// confirms, consume the dead-letter queue. Nothing reaches into storage.

import (
	"path/filepath"
	"strconv"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
)

// countSharedWALFiles returns how many physical WAL files exist under a broker's
// data directory.
//
// It is a premise instrument, not decoration. readMessageSequential searches
// qw.oldFiles BEFORE the current file, and oldFiles is populated only by
// rollFile (which fires at WALFileSize, 512 MB) and by rebuildBootState (which
// registers files inherited from a PREVIOUS boot). In a single-boot fixture that
// writes a few kilobytes, neither happens — so if exactly one WAL file exists,
// oldFiles was empty for the whole run and the oldFiles arm of the read path
// CANNOT have answered any probe. That is what makes S1's arm attributable by
// elimination rather than by assumption.
func countSharedWALFiles(t *testing.T, dir string) int {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, "wal", "shared", "*.wal"))
	require.NoError(t, err, "globbing WAL files under %s", dir)
	return len(matches)
}

const (
	// staleTTLMillis is the queue-level x-message-ttl. It has to be long enough
	// that boot 1's 300 messages are delivered and acked BEFORE the
	// delivery-time head-check would expire them (an expiry during boot 1 would
	// put boot-1 MessageIDs in the dead-letter queue legitimately and destroy
	// S2's discriminator), and short enough that boot 2's reaper fires inside
	// the test's observation window. The premise that boot 1 expired nothing is
	// asserted at runtime rather than assumed — see requireDLQDrainsEmpty.
	staleTTLMillis = 4000

	// staleFirstBatch / staleSecondBatch: boot 2 republishes FEWER messages than
	// boot 1 left behind, deliberately.
	//
	//   - Every tag boot 2 re-mints is inside boot 1's band, so any stale record
	//     that answers a probe is a boot-1 record.
	//   - Boot 1's WAL file is therefore never fully acknowledged in boot 2
	//     (200 of its 300 offsets at most), so tryDeleteOldFiles cannot reclaim
	//     it mid-test and quietly remove the evidence.
	//
	// staleSecondBatch also exceeds storage.readAheadMaxEntries (64): the
	// per-queue read-ahead buffer caches at most 64 messages from one batch read
	// and is never invalidated, so a smaller second batch could be answered
	// entirely out of that cache and would mask the WAL path under test.
	staleFirstBatch  = 300
	staleSecondBatch = 200
	staleSecondBase  = 10_000

	// staleSingleBatch is S1's one-incarnation batch, sized past the same
	// read-ahead bound for the same reason.
	staleSingleBatch = 100
)

// declareStaleFixture declares the dead-letter exchange, the dead-letter queue,
// its binding, and the NON-DURABLE source queue carrying x-message-ttl and
// x-dead-letter-exchange. It is called in every boot, because a non-durable
// queue does not survive a restart and must be redeclared — which is exactly the
// redeclare that re-mints the tag band (gap member 4).
func declareStaleFixture(t *testing.T, ch *amqp.Channel, src, dlx, dlq string) {
	t.Helper()

	require.NoError(t, ch.ExchangeDeclare(dlx, "fanout", true, false, false, false, nil),
		"declaring dead-letter exchange %s", dlx)
	_, err := ch.QueueDeclare(dlq, true /*durable*/, false, false, false, nil)
	require.NoError(t, err, "declaring dead-letter queue %s", dlq)
	require.NoError(t, ch.QueueBind(dlq, "", dlx, false, nil),
		"binding %s to %s", dlq, dlx)

	_, err = ch.QueueDeclare(src, false /*durable=FALSE*/, false, false, false, amqp.Table{
		"x-message-ttl":             int32(staleTTLMillis),
		"x-dead-letter-exchange":    dlx,
		"x-dead-letter-routing-key": dlq,
	})
	require.NoError(t, err, "declaring NON-DURABLE source queue %s with x-message-ttl", src)
}

// publishConfirmedDurableToStaleSource publishes ids [from,to) as DeliveryMode=2
// under confirm.select and fails unless every one is POSITIVELY confirmed. A
// message the broker never promised must not reach an assertion.
//
// The body carries the id too, so a dead-lettered message can be attributed to
// its incarnation from the body alone if MessageId is ever lost in transit.
func publishConfirmedDurableToStaleSource(t *testing.T, uri, src, dlx, dlq string, from, to int) {
	t.Helper()

	conn, err := amqp.Dial(uri)
	require.NoError(t, err, "dial %s", uri)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	declareStaleFixture(t, ch, src, dlx, dlq)
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

	for i := from; i < to; i++ {
		require.NoError(t, ch.Publish("", src, false, false, amqp.Publishing{
			DeliveryMode: amqp.Persistent, // DeliveryMode == 2
			MessageId:    strconv.Itoa(i),
			Body:         []byte(strconv.Itoa(i)),
		}), "publish %d", i)
	}

	select {
	case got := <-acked:
		require.Equal(t, n, got,
			"every publish must be positively confirmed before this test asserts anything; "+
				"got %d acks for %d publishes", got, n)
	case <-time.After(2 * time.Minute):
		t.Fatalf("timed out waiting for %d publisher confirms on %s", n, src)
	}
	t.Logf("CONFIRMED %d DeliveryMode=2 publishes to NON-DURABLE queue %q (ids %d..%d)",
		n, src, from, to-1)
}

// dlqArrival is one dead-letter delivery reduced to what these tests reason
// about: which incarnation published the original.
type dlqArrival struct {
	messageID int
}

// observeDLQ consumes the dead-letter queue with autoAck for a FIXED WALL-CLOCK
// WINDOW and returns every arrival, repeats included.
//
// The window is fixed rather than "until N messages" on purpose: the hypothesis
// under test is that the reaper re-delivers the SAME message on every sweep
// forever, so a loop that stopped at a target count could not distinguish
// "delivered once each" from "delivered without bound" — it would just stop
// earlier. A fixed window makes the repeat count the observable.
//
// This is not a rate assertion (canon rule 9): no threshold in either test is a
// throughput, and the window only bounds how long the test is willing to watch.
func observeDLQ(t *testing.T, uri, dlq string, window time.Duration) []dlqArrival {
	t.Helper()

	conn, err := amqp.Dial(uri)
	require.NoError(t, err, "dial %s", uri)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	require.NoError(t, ch.Qos(500, 0, false))

	deliveries, err := ch.Consume(dlq, "", true /*autoAck*/, false, false, false, nil)
	require.NoError(t, err, "consuming %s", dlq)

	deadline := time.NewTimer(window)
	defer deadline.Stop()

	var out []dlqArrival
	for {
		select {
		case d, ok := <-deliveries:
			if !ok {
				return out
			}
			id, convErr := strconv.Atoi(d.MessageId)
			require.NoError(t, convErr,
				"every message this test publishes carries an integer MessageId; got %q on the "+
					"dead-letter queue — the fixture is not observing what it published", d.MessageId)
			out = append(out, dlqArrival{messageID: id})
		case <-deadline.C:
			return out
		}
	}
}

// summarizeDLQ splits arrivals into distinct ids and total deliveries, and
// reports the id with the most repeats.
func summarizeDLQ(arrivals []dlqArrival) (distinct int, total int, hottestID int, hottestCount int) {
	counts := make(map[int]int, len(arrivals))
	for _, a := range arrivals {
		counts[a.messageID]++
	}
	hottestID = -1
	for id, c := range counts {
		if c > hottestCount {
			hottestID, hottestCount = id, c
		}
	}
	return len(counts), len(arrivals), hottestID, hottestCount
}

// requireDLQDrainsEmpty establishes, rather than hopes, that the dead-letter
// queue holds nothing before the phase that matters begins. S2's entire
// discriminator is "a boot-1 MessageID appears in the dead-letter queue during
// boot 2", which is worthless if boot 1 left one there.
func requireDLQDrainsEmpty(t *testing.T, uri, dlq string, budget time.Duration) int {
	t.Helper()

	conn, err := amqp.Dial(uri)
	require.NoError(t, err, "dial %s", uri)
	defer conn.Close()
	ch, err := conn.Channel()
	require.NoError(t, err)

	const consecutiveEmptyRequired = 3
	deadline := time.Now().Add(budget)
	emptyInARow, drained := 0, 0
	for time.Now().Before(deadline) {
		d, ok, getErr := ch.Get(dlq, false)
		require.NoError(t, getErr, "basic.get on %s", dlq)
		if ok {
			require.NoError(t, d.Ack(false))
			drained++
			emptyInARow = 0
			continue
		}
		emptyInARow++
		if emptyInARow >= consecutiveEmptyRequired {
			return drained
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("PREMISE BROKEN: dead-letter queue %q never reported empty %d times running within %s "+
		"(%d drained so far)", dlq, consecutiveEmptyRequired, budget, drained)
	return drained
}

// ---------------------------------------------------------------------------
// S1 — one incarnation. The attribution control.
// ---------------------------------------------------------------------------

// TestReaperStaleRead_S1_SingleIncarnationDurableTTLQueue asks the narrow
// question that decides how S2 must be read:
//
//	when the consumer-less reaper removes an expired DURABLE message from the
//	ring and acknowledges it, does the NEXT probe of that same tag come back
//	empty (the expectation reapTTLSweep's own comment documents), or does the WAL
//	answer it?
//
// There is no second incarnation here, no member 4, and no rolled WAL file — the
// only records on disk are this boot's own, in the CURRENT file. So a repeat
// dead-letter here is attributable to the reaper putting its own just-acked tag
// back on the WAL read path, and to nothing else.
//
// PASS means each message is dead-lettered exactly once and the reaper's
// documented expectation holds. FAIL means it does not.
func TestReaperStaleRead_S1_SingleIncarnationDurableTTLQueue(t *testing.T) {
	const (
		src = "s1.stale.src"
		dlx = "s1.stale.dlx"
		dlq = "s1.stale.dlq"
	)

	b := newRestartableBroker(t, nil) // shipped defaults; no knob is involved

	publishConfirmedDurableToStaleSource(t, b.uri, src, dlx, dlq, 0, staleSingleBatch)

	// The queue is left CONSUMER-LESS for the whole window, which is the
	// condition reapTTLSweep requires. Watch long enough to cover the TTL plus
	// several reaper sweeps (reaperMaxInterval is 250ms).
	window := time.Duration(staleTTLMillis)*time.Millisecond + 6*time.Second
	arrivals := observeDLQ(t, b.uri, dlq, window)

	distinct, total, hottestID, hottestCount := summarizeDLQ(arrivals)
	walFiles := countSharedWALFiles(t, b.dir)

	// THE LOAD-BEARING OBSERVABLE, emitted as a stable machine-readable token so
	// a repeat run can be scored k/N without re-reading prose. The raw counts on
	// the RESULT line are timing-dependent and incidental; `amplified` is not.
	t.Logf("S1 OBSERVABLE: amplified=%t total=%d distinct=%d walFiles=%d",
		total > distinct, total, distinct, walFiles)
	t.Logf("S1 RESULT: %d dead-letter deliveries over %s, %d distinct MessageIDs; "+
		"most-repeated id=%d delivered %d time(s)", total, window, distinct, hottestID, hottestCount)

	// Arm elimination, asserted at runtime rather than assumed. With exactly one
	// physical WAL file there is no rolled file, so qw.oldFiles was empty and
	// readMessageSequential's oldFiles loop had nothing to iterate; and
	// performCheckpoint (which only ever moves oldFiles into segments) cannot
	// have run, so the segment tier is empty too. Whatever answered the reaper's
	// re-probe came from the CURRENT file or from the read-ahead buffer, and
	// wal_ack_read_arm_test.go discriminates between those two.
	require.Equal(t, 1, walFiles,
		"PREMISE BROKEN: expected exactly one physical WAL file in a single-boot fixture that "+
			"writes a few kilobytes; found %d. With a rolled file present, oldFiles is non-empty "+
			"and this test can no longer attribute the read to the current-file arm by "+
			"elimination", walFiles)

	require.NotZero(t, total,
		"PREMISE BROKEN: the consumer-less reaper dead-lettered nothing in %s for a queue with "+
			"x-message-ttl=%dms and a DLX. Without a single expiry this fixture exercises no "+
			"read-after-ack at all and can conclude nothing either way", window, staleTTLMillis)
	require.Equal(t, staleSingleBatch, distinct,
		"PREMISE BROKEN: expected all %d published messages to expire and be dead-lettered; "+
			"only %d distinct ids arrived", staleSingleBatch, distinct)

	require.Equal(t, staleSingleBatch, total,
		"REAPER RESURRECTS ITS OWN ACKNOWLEDGED RECORD: %d messages were published and expired, "+
			"but %d dead-letter deliveries arrived (id %d alone arrived %d times). reapTTLSweep "+
			"removes an expired message from the ring and acknowledges it, then probes the same "+
			"tag again with storage.GetMessage; its own comment expects that probe to come back "+
			"empty (\"gap (acked/reaped/never-present tag)\"). A repeat delivery means the WAL "+
			"answered it instead, so the message is re-expired and re-dead-lettered on every "+
			"sweep and the tail-advance loop, which breaks on any non-nil message, cannot move "+
			"past it", staleSingleBatch, total, hottestID, hottestCount)
}

// ---------------------------------------------------------------------------
// S2 — two incarnations. The mandated chain, end to end.
// ---------------------------------------------------------------------------

// TestReaperStaleRead_S2_CrossIncarnationResurrectionThroughTheReaper joins all
// three fragments in ONE execution on the shipping tree:
//
//	boot 1 publishes DeliveryMode==2 to a NON-DURABLE queue carrying
//	x-message-ttl and a DLX, delivers and acknowledges every message, and the
//	broker restarts. Recovery REAPS those records (BenignFault) rather than
//	deleting them, so they stay physically on disk, and the redeclare reclaims
//	the same ordinal with nextSeq back at zero — so boot 2's publishes land on
//	tags boot 1's records still carry (member 4). Boot 2 is left CONSUMER-LESS so
//	the reaper sweeps.
//
// THE OBSERVABLE, and it is unambiguous: a dead-letter delivery carrying a
// BOOT-1 MessageID. Those messages were acknowledged before the restart and
// their queue is forbidden by AMQP 0-9-1 §1.7.2.1 from surviving it. Their only
// route to the dead-letter queue is the reaper probing a re-minted tag, the WAL
// answering with the previous incarnation's record, and that record being
// evaluated against its own dead TTL and republished as expired.
//
// PASS means no boot-1 record was resurrected — the chain DOES NOT close through
// this path. FAIL names the resurrected ids.
func TestReaperStaleRead_S2_CrossIncarnationResurrectionThroughTheReaper(t *testing.T) {
	const (
		src = "s2.stale.src"
		dlx = "s2.stale.dlx"
		dlq = "s2.stale.dlq"
	)

	b := newRestartableBroker(t, nil) // shipped defaults; no knob is involved

	// ---- BOOT 1: publish durable, deliver and acknowledge every one ---------
	publishConfirmedDurableToStaleSource(t, b.uri, src, dlx, dlq, 0, staleFirstBatch)

	acked := drainIDs(t, b.uri, src, staleFirstBatch, false, 10*time.Second, 2*time.Minute)
	missing, first := missingIDs(acked, staleFirstBatch)
	require.Zero(t, missing,
		"PREMISE BROKEN: only %d of %d messages were delivered and acknowledged in boot 1 "+
			"(first missing id %d), so there is no acknowledged tag band to collide with",
		staleFirstBatch-missing, staleFirstBatch, first)
	requireQueueSettlesEmpty(t, b.uri, src, 60*time.Second)

	// The discriminator only works if boot 1 dead-lettered nothing: a boot-1 id
	// that expired here would be indistinguishable from one resurrected in boot 2.
	strays := requireDLQDrainsEmpty(t, b.uri, dlq, 30*time.Second)
	require.Zero(t, strays,
		"PREMISE BROKEN: boot 1 dead-lettered %d message(s) before they could be acknowledged, "+
			"so a boot-1 MessageID in the dead-letter queue is no longer evidence of "+
			"resurrection. The x-message-ttl (%dms) is too short for this fixture's drain — "+
			"raise it; do not weaken the assertion below",
		strays, staleTTLMillis)
	t.Logf("PREMISE: boot 1 delivered and acknowledged all %d messages and dead-lettered none",
		staleFirstBatch)

	// ---- BOOT 2: redeclare the same non-durable queue, republish -----------
	b.stop()
	b.start()
	publishConfirmedDurableToStaleSource(t, b.uri, src, dlx, dlq,
		staleSecondBase, staleSecondBase+staleSecondBatch)

	// CONSUMER-LESS from here: nothing consumes src, so reapTTLSweep owns it.
	window := time.Duration(staleTTLMillis)*time.Millisecond + 6*time.Second
	arrivals := observeDLQ(t, b.uri, dlq, window)

	distinct, total, hottestID, hottestCount := summarizeDLQ(arrivals)

	resurrected := make(map[int]int)
	secondIncarnation := 0
	for _, a := range arrivals {
		if a.messageID < staleSecondBase {
			resurrected[a.messageID]++
		} else {
			secondIncarnation++
		}
	}

	// THE LOAD-BEARING OBSERVABLE, and the internal control this fixture got for
	// free. Boot 1 published ids 0..staleFirstBatch-1 onto per-queue sequences
	// 0..staleFirstBatch-1; boot 2 re-minted only sequences
	// 0..staleSecondBatch-1. So the tags boot 2 re-minted are carried by boot-1
	// records whose MessageIDs are EXACTLY [0, staleSecondBatch) — and boot 1's
	// remaining ids, [staleSecondBatch, staleFirstBatch), sit on tags boot 2
	// never re-minted.
	//
	// If resurrection tracked the re-minted band one-for-one, only the first
	// group can appear. If instead recovery had simply loaded the whole
	// non-durable queue — the competing hypothesis, and the one that would make
	// this finding uninteresting — the second group would appear too. So
	// `bandExact` discriminates between the two hypotheses; the delivery counts
	// do not.
	insideBand, outsideBand := 0, 0
	for id := range resurrected {
		if id < staleSecondBatch {
			insideBand++
		} else {
			outsideBand++
		}
	}
	bandExact := insideBand == staleSecondBatch && outsideBand == 0
	t.Logf("S2 OBSERVABLE: bandExact=%t resurrectedInBand=%d/%d outsideBand=%d "+
		"expectedBand=[0,%d) boot1Published=%d",
		bandExact, insideBand, staleSecondBatch, outsideBand, staleSecondBatch, staleFirstBatch)
	t.Logf("S2 RESULT: %d dead-letter deliveries over %s, %d distinct MessageIDs; "+
		"boot-2 deliveries=%d; BOOT-1 RECORDS RESURRECTED = %d distinct id(s); "+
		"most-repeated id=%d delivered %d time(s)",
		total, window, distinct, secondIncarnation, len(resurrected), hottestID, hottestCount)

	require.NotZero(t, total,
		"PREMISE BROKEN: the consumer-less reaper dead-lettered nothing in boot 2, so no probe "+
			"of a re-minted tag ever happened and this fixture cannot conclude anything")

	if len(resurrected) > 0 {
		var sample, samples int
		for id, c := range resurrected {
			if samples < 5 {
				t.Logf("S2 RESULT: boot-1 MessageID %d — acknowledged before the restart, on a "+
					"NON-DURABLE queue — was dead-lettered %d time(s) in boot 2", id, c)
				samples++
			}
			sample = id
		}
		_ = sample
	}

	require.Empty(t, resurrected,
		"CHAIN CLOSES: %d message(s) published in BOOT 1, delivered and acknowledged before the "+
			"restart, on a NON-DURABLE queue that AMQP 0-9-1 §1.7.2.1 forbids from surviving a "+
			"restart, were republished into the dead-letter exchange by boot 2's TTL reaper. "+
			"The route is the one Step 1 could only derive: the redeclare re-mints boot 1's tag "+
			"band (member 4); the reaper's own removal of the boot-2 message from the ring plus "+
			"WALManager.Acknowledge's deletion of the offsetIndex entry puts the tag on "+
			"readMessageSequential, which searches oldFiles BEFORE the current file; and "+
			"reapTTLSweep evaluates the returned dead incarnation's TTL and dead-letters it. "+
			"resurrected ids and their repeat counts: %v", len(resurrected), resurrected)
}
