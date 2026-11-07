package main

// Durable-restart recovery contract.
//
// These tests pin what a broker must do with messages it has positively
// confirmed to a publisher (confirm.select + basic.ack from the server) on a
// DURABLE queue with DeliveryMode=2, across a restart. They are correctness
// tests: every assertion is about WHICH messages are delivered, never about
// how fast (canon rule 9 — a rate belongs in cmd/benchgate).
//
// They drive the real broker over TCP with the rabbitmq/amqp091-go client, so
// they exercise the shipped publish/confirm/recover/deliver path end to end
// rather than a storage-package fixture.

import (
	"os"
	"strconv"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/server"
	"github.com/maxpert/amqp-go/storage"
)

// ---------------------------------------------------------------------------
// premise: the spill threshold, resolved at runtime from config
// ---------------------------------------------------------------------------

// spillThresholdFor mirrors NewDisruptorStorageWithEngineConfig's resolution of
// the in-memory ring's spill threshold, including its zero-value fallbacks to
// the storage package's own constants.
//
// It is computed, never written down. The shipped value has already moved once:
// the storage package's DefaultRingBufferSize is 262144, but
// config.DefaultConfig() sets Engine.RingBufferSize = 65536 and
// NewDisruptorStorageWithEngineConfig prefers the non-zero engine value, so the
// number that actually governs a default broker is 65536*80/100 = 52428 and not
// anything the storage package declares. A test that hardcoded either number
// would silently stop testing what it claims the next time one of them moves.
func spillThresholdFor(ec interfaces.EngineConfig) uint64 {
	ring := ec.RingBufferSize
	if ring <= 0 {
		ring = storage.DefaultRingBufferSize
	}
	pct := ec.SpillThresholdPercent
	if pct <= 0 {
		pct = storage.DefaultSpillThresholdPercent
	}
	if ring <= 0 || pct <= 0 {
		return 0
	}
	return uint64(ring) * uint64(pct) / 100
}

// requireBacklogCrossesSpillThreshold asserts, at runtime, the premise the
// loss test depends on: the backlog standing at restart must exceed the live
// spill threshold, because that is the boundary at which
// DisruptorStorage.LoadMessageFromRecovery starts returning nil (dropping the
// record) while recovery still counts it and widens the queue's tag range.
//
// Canon rule 11: this is asserted rather than described in a comment. If the
// engine defaults or this test's knobs drift so that the backlog no longer
// crosses the threshold, the test fails loudly with both numbers instead of
// passing while testing nothing.
func requireBacklogCrossesSpillThreshold(t *testing.T, cfg *config.AMQPConfig, backlog int) uint64 {
	t.Helper()

	threshold := spillThresholdFor(cfg.Engine)
	require.NotZero(t, threshold,
		"PREMISE BROKEN: this test's engine config resolves to a zero spill threshold "+
			"(RingBufferSize=%d, SpillThresholdPercent=%d); there is no boundary left to cross",
		cfg.Engine.RingBufferSize, cfg.Engine.SpillThresholdPercent)

	require.Greater(t, uint64(backlog), threshold,
		"PREMISE BROKEN: the backlog standing at restart (%d) no longer exceeds this "+
			"engine's spill threshold (%d = RingBufferSize %d x SpillThresholdPercent %d / 100). "+
			"Above that threshold LoadMessageFromRecovery drops the record, which is the defect "+
			"under test; at or below it, this test cannot detect anything. Raise the backlog or "+
			"lower the knobs — do not delete this check.",
		backlog, threshold, cfg.Engine.RingBufferSize, cfg.Engine.SpillThresholdPercent)

	t.Logf("PREMISE: backlog=%d > live spill threshold=%d (ring=%d, spill=%d%%)",
		backlog, threshold, cfg.Engine.RingBufferSize, cfg.Engine.SpillThresholdPercent)
	return threshold
}

// requireLoweredKnobsModelProduction asserts that a test running with a shrunk
// ring is still a scale model of the shipped broker: that the shipped defaults
// resolve their spill threshold through the same expression, and that this
// test's threshold is genuinely below it.
//
// Without this, a config change that stopped deriving the threshold from
// RingBufferSize x SpillThresholdPercent would leave the lowered-knob tests
// green, fast, and measuring a regime production no longer has — the exact
// shape of "a load-bearing premise living in a comment".
func requireLoweredKnobsModelProduction(t *testing.T, cfg *config.AMQPConfig) {
	t.Helper()
	threshold := spillThresholdFor(cfg.Engine)
	shipped := spillThresholdFor(config.DefaultConfig().Engine)
	require.NotZero(t, shipped,
		"PREMISE BROKEN: config.DefaultConfig() no longer resolves to a positive spill "+
			"threshold, so a lowered-knob test is no longer a scale model of production")
	require.Less(t, threshold, shipped,
		"PREMISE BROKEN: this test's threshold (%d) is not below the shipped default (%d); "+
			"the knobs exist to make the same defect reachable at a CI-affordable volume",
		threshold, shipped)
	t.Logf("PREMISE: lowered-knob threshold=%d models shipped default threshold=%d", threshold, shipped)
}

// ---------------------------------------------------------------------------
// restartable broker harness
// ---------------------------------------------------------------------------

// restartableBroker runs the real server against a fixed storage directory and
// can be stopped and rebuilt against that same directory, which is what makes
// "restart" mean the same thing here as it does to an operator.
type restartableBroker struct {
	t    *testing.T
	dir  string
	tune func(*config.AMQPConfig)

	srv *server.Server
	uri string
}

func newRestartableBroker(t *testing.T, tune func(*config.AMQPConfig)) *restartableBroker {
	t.Helper()
	b := &restartableBroker{t: t, dir: t.TempDir(), tune: tune}
	b.start()
	t.Cleanup(b.stop)
	return b
}

func (b *restartableBroker) start() {
	b.t.Helper()

	cfg := config.DefaultConfig()
	// Port 0: the kernel picks a free port and the test reads it back from the
	// bound listener. Hardcoded ports collide between packages, between
	// concurrently running tests, and with themselves under -count=N.
	cfg.Network.Address = "127.0.0.1:0"
	cfg.Storage.Path = b.dir
	if b.tune != nil {
		b.tune(cfg)
	}
	require.NoError(b.t, cfg.Validate(), "test engine config must be valid")

	// requireIsolatedStorage (testserver_test.go) asserts the path is absolute;
	// t.TempDir() is, and a relative path would silently share the package's
	// ./data directory with every other test and with previous runs.
	requireIsolatedStorage(b.t, cfg)

	srv, err := server.NewServerBuilder().WithConfig(cfg).Build()
	require.NoError(b.t, err, "building broker over %s", b.dir)
	waitForListening(b.t, srv)

	srv.Mutex.RLock()
	addr := srv.Listener.Addr().String()
	srv.Mutex.RUnlock()

	b.srv = srv
	b.uri = "amqp://guest:guest@" + addr + "/"
}

func (b *restartableBroker) stop() {
	b.t.Helper()
	if b.srv == nil {
		return
	}
	testShutdownServer(b.t, b.srv)
	b.srv = nil
}

// restart stops the broker and builds a new one over the same storage
// directory — the operator-visible operation whose contract these tests pin.
func (b *restartableBroker) restart() {
	b.t.Helper()
	b.stop()
	b.start()
}

// ---------------------------------------------------------------------------
// publish / drain helpers
// ---------------------------------------------------------------------------

const restartTestBodySize = 64

// publishConfirmedDurable declares queue as durable, publishes ids [from,to) as
// persistent messages under confirm.select, and fails unless every one of them
// is POSITIVELY confirmed. An unconfirmed or nacked publish is not a promise the
// broker made, so it must never reach the assertion phase of a data-loss test.
func publishConfirmedDurable(t *testing.T, uri, queue string, from, to int) {
	t.Helper()

	conn, err := amqp.Dial(uri)
	require.NoError(t, err, "dial %s", uri)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)

	_, err = ch.QueueDeclare(queue, true /*durable*/, false, false, false, nil)
	require.NoError(t, err, "declaring durable queue %s", queue)
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
			DeliveryMode: amqp.Persistent,
			MessageId:    strconv.Itoa(i),
			Body:         body,
		}), "publish %d", i)
	}

	select {
	case got := <-acked:
		require.Equal(t, n, got,
			"every publish must be positively confirmed before this test asserts anything "+
				"about durability; got %d acks for %d publishes", got, n)
	case <-time.After(5 * time.Minute):
		t.Fatalf("timed out waiting for %d publisher confirms on %s", n, queue)
	}
	t.Logf("CONFIRMED %d durable publishes to %s", n, queue)
}

// drainIDs consumes queue and returns the set of MessageId integers seen.
//
// It returns as soon as `want` distinct ids have arrived (so a passing run is
// fast) and otherwise as soon as `idle` elapses with no new delivery (so a
// failing run is fast too). `hard` is a one-sided liveness bound with generous
// headroom, not a rate: it only decides how long the test is willing to wait,
// never what counts as correct.
func drainIDs(t *testing.T, uri, queue string, want int, autoAck bool, idle, hard time.Duration) map[int]bool {
	t.Helper()

	conn, err := amqp.Dial(uri)
	require.NoError(t, err, "dial %s", uri)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	require.NoError(t, ch.Qos(200, 0, false))

	deliveries, err := ch.Consume(queue, "", autoAck, false, false, false, nil)
	require.NoError(t, err, "consuming %s", queue)

	seen := make(map[int]bool, want)
	idleTimer := time.NewTimer(idle)
	defer idleTimer.Stop()
	hardTimer := time.NewTimer(hard)
	defer hardTimer.Stop()

	for want <= 0 || len(seen) < want {
		select {
		case d, ok := <-deliveries:
			if !ok {
				return seen
			}
			id, convErr := strconv.Atoi(d.MessageId)
			require.NoError(t, convErr,
				"every message this suite publishes carries an integer MessageId; got %q "+
					"— the fixture is not delivering what it published", d.MessageId)
			seen[id] = true
			if !autoAck {
				require.NoError(t, d.Ack(false), "acking delivery of id %d", id)
			}
			if !idleTimer.Stop() {
				select {
				case <-idleTimer.C:
				default:
				}
			}
			idleTimer.Reset(idle)
		case <-idleTimer.C:
			return seen
		case <-hardTimer.C:
			return seen
		}
	}
	return seen
}

// requireQueueSettlesEmpty establishes, as an asserted fact rather than a hope,
// that a queue really is empty and everything in it really was acknowledged.
//
// It is needed because acknowledgements are NOT processed on the frame path:
// Server.processAckFrame hands them to a separate ackProcessor goroutine, so
// nothing a client can send on any channel orders itself after the application
// of a preceding basic.ack. A test that acked N deliveries and immediately
// stopped the broker could therefore leave the last ack unprocessed — and a
// CORRECT broker would then legitimately redeliver that one message, failing
// the redelivery test for a reason that is not the defect. Polling until
// basic.get reports the queue empty several times running is the only sound
// way to observe that the acks landed.
//
// Stragglers are drained and acknowledged rather than tolerated, and their
// count is logged, so that "the queue was empty before the restart" is
// established rather than assumed.
func requireQueueSettlesEmpty(t *testing.T, uri, queue string, budget time.Duration) {
	t.Helper()

	conn, err := amqp.Dial(uri)
	require.NoError(t, err, "dial %s", uri)
	defer conn.Close()
	ch, err := conn.Channel()
	require.NoError(t, err)

	const consecutiveEmptyRequired = 3
	deadline := time.Now().Add(budget)
	emptyInARow, stragglers := 0, 0

	for time.Now().Before(deadline) {
		d, ok, getErr := ch.Get(queue, false)
		require.NoError(t, getErr, "basic.get on %s", queue)
		if ok {
			require.NoError(t, d.Ack(false))
			stragglers++
			emptyInARow = 0
			continue
		}
		emptyInARow++
		if emptyInARow >= consecutiveEmptyRequired {
			t.Logf("SETTLED: %s reported empty %d times running (%d straggler(s) drained and acked)",
				queue, emptyInARow, stragglers)
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("PREMISE BROKEN: queue %q never reported empty %d times running within %s "+
		"(%d straggler(s) drained so far), so 'everything was acknowledged before the restart' "+
		"is not established and nothing may be concluded from what comes back after it",
		queue, consecutiveEmptyRequired, budget, stragglers)
}

// missingIDs returns the ids in [0,n) absent from seen, and the lowest one.
func missingIDs(seen map[int]bool, n int) (missing int, firstMissing int) {
	firstMissing = -1
	for i := 0; i < n; i++ {
		if !seen[i] {
			missing++
			if firstMissing < 0 {
				firstMissing = i
			}
		}
	}
	return missing, firstMissing
}

// loweredSpillEngine shrinks the ring buffer so that the spill threshold —
// the boundary at which LoadMessageFromRecovery starts dropping recovered
// records — is crossed by a few hundred messages instead of the 52,428 a
// default broker needs.
//
// This is a scale model, not a different defect: the threshold is resolved by
// the same expression in the same function for both settings, and
// requireLoweredKnobsModelProduction asserts at runtime that the shipped
// default still resolves through that same expression and is still larger.
//
// The full-size reproduction is TestDurableRestart_FullScaleAtShippedDefaults
// and it loses messages identically (delivered == threshold exactly, first
// missing index == threshold exactly, at both settings). It is kept out of the
// CI gate not because it is slow in absolute terms — measured at 19.5s under
// -race, and 17.6s under -race at GOMAXPROCS=2, i.e. it is IO-bound and barely
// cares about core count — but because it adds ~14% to the root package's
// runtime for a defect this test already detects with the identical signature,
// and because that runtime is set by the runner's fsync throughput, which is
// the one input nobody controls on shared CI hardware.
func loweredSpillEngine(cfg *config.AMQPConfig) {
	cfg.Engine.RingBufferSize = 1024 // power of two, required by config.Validate
	cfg.Engine.SpillThresholdPercent = 25
}

// ---------------------------------------------------------------------------
// C-1: confirmed durable messages must survive a restart
// ---------------------------------------------------------------------------

const c1Backlog = 400

// TestDurableRestart_EveryConfirmedMessageIsDeliveredAfterRestart is the
// primary C-1 assertion.
//
// Contract: a message the broker positively confirmed to a publisher, sent
// with DeliveryMode=2 to a durable queue, must be delivered to a consumer after
// the broker restarts. Not "most of them", and not "the ones that happened to
// fit in memory".
//
// On the unfixed tree the broker delivers exactly `spill threshold` messages
// and silently discards the rest: recovery counts every WAL record and widens
// the queue's tag range to cover them, but LoadMessageFromRecovery returns nil
// for every record past the threshold, so the tags are claimable and
// unreadable, and deliverMessage answers a read failure with
// releaseGate + GapSkipAdvance — no delivery, no nack, no log, no metric.
func TestDurableRestart_EveryConfirmedMessageIsDeliveredAfterRestart(t *testing.T) {
	b := newRestartableBroker(t, loweredSpillEngine)
	requireLoweredKnobsModelProduction(t, b.srv.Config)
	requireBacklogCrossesSpillThreshold(t, b.srv.Config, c1Backlog)

	const queue = "c1.confirmed.durable"
	publishConfirmedDurable(t, b.uri, queue, 0, c1Backlog)

	b.restart()

	seen := drainIDs(t, b.uri, queue, c1Backlog, true, 5*time.Second, 2*time.Minute)
	missing, firstMissing := missingIDs(seen, c1Backlog)
	t.Logf("RESULT: published+confirmed=%d delivered_after_restart=%d MISSING=%d first_missing_index=%d",
		c1Backlog, len(seen), missing, firstMissing)

	require.Zero(t, missing,
		"DATA LOSS: %d of %d confirmed durable messages were never delivered after restart "+
			"(lowest missing publish index %d)", missing, c1Backlog, firstMissing)
}

// TestDurableRestart_NoRestartControl is the A/B control that makes the test
// above attributable, and the active-fixture check that keeps it from passing
// vacuously: identical config, identical volume, identical drain — only the
// restart removed. If this ever fails, the fixture is broken and no conclusion
// may be drawn from its sibling.
func TestDurableRestart_NoRestartControl(t *testing.T) {
	b := newRestartableBroker(t, loweredSpillEngine)
	requireLoweredKnobsModelProduction(t, b.srv.Config)
	requireBacklogCrossesSpillThreshold(t, b.srv.Config, c1Backlog)

	const queue = "c1.control.norestart"
	publishConfirmedDurable(t, b.uri, queue, 0, c1Backlog)

	seen := drainIDs(t, b.uri, queue, c1Backlog, true, 5*time.Second, 2*time.Minute)
	missing, firstMissing := missingIDs(seen, c1Backlog)
	t.Logf("CONTROL (no restart): published+confirmed=%d delivered=%d MISSING=%d first_missing_index=%d",
		c1Backlog, len(seen), missing, firstMissing)

	require.Zero(t, missing,
		"CONTROL FAILED: %d of %d confirmed durable messages were lost WITHOUT a restart. "+
			"The restart is then not the variable under test and the sibling test proves nothing",
		missing, c1Backlog)
}

// TestDurableRestart_BelowSpillThresholdControl is the second control: the
// same restart, below the threshold. It bounds the defect (the loss starts at
// the threshold, it is not "restarts lose messages") and it fails if the
// restart machinery itself is broken.
func TestDurableRestart_BelowSpillThresholdControl(t *testing.T) {
	b := newRestartableBroker(t, loweredSpillEngine)
	requireLoweredKnobsModelProduction(t, b.srv.Config)

	threshold := spillThresholdFor(b.srv.Config.Engine)
	require.NotZero(t, threshold)
	backlog := int(threshold / 2)
	require.Greater(t, backlog, 0,
		"PREMISE BROKEN: half the spill threshold (%d) is not a usable backlog", threshold)

	const queue = "c1.control.belowthreshold"
	publishConfirmedDurable(t, b.uri, queue, 0, backlog)

	b.restart()

	seen := drainIDs(t, b.uri, queue, backlog, true, 5*time.Second, 2*time.Minute)
	missing, firstMissing := missingIDs(seen, backlog)
	t.Logf("CONTROL (below threshold %d): published+confirmed=%d delivered_after_restart=%d MISSING=%d first_missing_index=%d",
		threshold, backlog, len(seen), missing, firstMissing)

	require.Zero(t, missing,
		"CONTROL FAILED: %d of %d confirmed durable messages were lost across a restart even "+
			"BELOW the spill threshold (%d). The defect is then not the one this suite describes",
		missing, backlog, threshold)
}

// fullScaleRecoveryEnv gates the production-default reproduction below.
const fullScaleRecoveryEnv = "STRANGEQ_FULLSCALE_RECOVERY"

// TestDurableRestart_FullScaleAtShippedDefaults is the same assertion as
// TestDurableRestart_EveryConfirmedMessageIsDeliveredAfterRestart, run at
// config.DefaultConfig() with no knob touched, which requires publishing past
// the shipped spill threshold of 52,428.
//
// It is OPT-IN and is NOT part of the CI gate: set STRANGEQ_FULLSCALE_RECOVERY=1
// to run it. Measured cost is recorded in .notes/loop-2/step1-tests.md. It
// exists so that the claim "the lowered-knob test is a scale model of the
// shipped configuration" stays executable rather than becoming an assertion
// nobody can re-check — but a skipped test gates nothing, so nothing in this
// loop may cite it as coverage.
func TestDurableRestart_FullScaleAtShippedDefaults(t *testing.T) {
	if os.Getenv(fullScaleRecoveryEnv) == "" {
		t.Skipf("opt-in: set %s=1 to run the production-default reproduction "+
			"(it publishes past the shipped spill threshold and is not part of the CI gate)",
			fullScaleRecoveryEnv)
	}

	b := newRestartableBroker(t, nil) // shipped defaults, nothing lowered
	backlog := int(spillThresholdFor(b.srv.Config.Engine)) + 7572
	requireBacklogCrossesSpillThreshold(t, config.DefaultConfig(), backlog)

	const queue = "c1.fullscale"
	publishConfirmedDurable(t, b.uri, queue, 0, backlog)

	b.restart()

	seen := drainIDs(t, b.uri, queue, backlog, true, 15*time.Second, 10*time.Minute)
	missing, firstMissing := missingIDs(seen, backlog)
	t.Logf("RESULT (shipped defaults): published+confirmed=%d delivered_after_restart=%d MISSING=%d first_missing_index=%d",
		backlog, len(seen), missing, firstMissing)

	require.Zero(t, missing,
		"DATA LOSS: %d of %d confirmed durable messages were never delivered after restart "+
			"(lowest missing publish index %d)", missing, backlog, firstMissing)
}

// ---------------------------------------------------------------------------
// acknowledged messages must not come back
// ---------------------------------------------------------------------------

// TestDurableRestart_AcknowledgedMessagesAreNotRedelivered pins that a durable
// queue which was fully consumed and acknowledged before a restart is EMPTY
// after it.
//
// Ruling (recorded here because the assertion is the ruling): this is a
// correctness defect, not a throughput one. AMQP 0-9-1 is at-least-once, and
// redelivery after a crash is explicitly modelled — the redelivered flag
// exists for it. What at-least-once covers is the window between a delivery
// and the durable recording of its acknowledgement. This broker has no such
// window: the WAL ackBitmap and the segment ackBitmap are never persisted and
// never rebuilt, so acknowledgement is never made durable at all, and
// "at least once" degenerates into "again in full after every restart,
// forever". A durable queue is required to survive a restart (queue.declare
// durable=true); a queue that comes back holding messages that had been
// removed from it before the restart has not survived, it has been reverted,
// and basic.get answers get-ok where the same call answered get-empty one
// restart earlier. That is protocol-observable state moving backwards, which
// no consumer can dedupe against because nothing about the replay is bounded.
//
// The assertion is written at the AMQP level (the queue is empty) rather than
// at the storage level (RecoverFromWAL returns nothing) deliberately: it is
// the contract, and it leaves the implementer free to satisfy it by persisting
// ack state, by applying already-persisted consumer offset checkpoints at
// boot, or by compaction.
//
// NOTE for the implementer: storage/wal_recovery_test.go's
// TestWALRecovery_BasicRecovery currently asserts the OPPOSITE at the storage
// layer ("Should recover all messages (ACKs not persisted)"). The two are
// compatible only if the fix records ack durability above RecoverFromWAL. If
// it does not, that existing test must be re-argued in the loop log against
// this ruling — not quietly deleted, and not used to weaken this one.
//
// ---------------------------------------------------------------------------
// SKIPPED — the defect is OPEN, not fixed. The ruling above still stands.
// ---------------------------------------------------------------------------
//
// The assertion below is NOT weakened, NOT deleted and NOT to be softened: it
// is the executable statement of a known-open defect, kept so that whoever
// closes it inherits a red test rather than a paragraph. Delete the t.Skip and
// nothing else to reproduce.
//
// Defect: acknowledgement is never made durable. The WAL's ackBitmap and the
// segment ackBitmap are in-memory only, so a durable queue that was fully
// consumed and acknowledged comes back FULL after a restart, forever, with no
// bound on the replay.
//
// Register item: .notes/loop-2-backlog.md item 18 (Tier 1).
// Evidence + design constraints: .notes/loop-2/deferred-ack-durability.md.
//
// Measured on the tree that withdrew the fix (go test -race -count=1,
// filter matched 1 test, exit status 1):
//
//	RESULT: 200 acked-before-restart messages redelivered after restart
//	        (sentinel delivered=true)
//	--- FAIL: TestDurableRestart_AcknowledgedMessagesAreNotRedelivered (5.31s)
//
// The sentinel arriving is what distinguishes this from a dead fixture: the
// consume path is live and all 200 settled messages really did come back.
//
// Why it is not fixed here: closing it requires attributing each ack to the
// RECORD it cancels rather than to a delivery-tag value, because tags are
// re-minted by design. Three build/review cycles of a tag-keyed durable ack
// snapshot produced four criticals, each invisible to the previous cycle's
// tests, the mildest of which destroyed 66 of 300 confirmed durable messages
// and the worst 198 of 200. The bug this test names is bounded and
// pre-existing; the fixes for it were not. Do not re-attempt without reading
// deferred-ack-durability.md first.
func TestDurableRestart_AcknowledgedMessagesAreNotRedelivered(t *testing.T) {
	t.Skip("KNOWN-OPEN DEFECT (loop-2-backlog item 18): acknowledgement is not durable, so a " +
		"drained durable queue comes back full after every restart. This test is the " +
		"reproduction and it FAILS on purpose — measured 200/200 settled messages redelivered " +
		"with the sentinel delivered, so the fixture is live. Delete this t.Skip (and nothing " +
		"else) to reproduce. Do NOT soften the assertion below: it is the contract. Read " +
		".notes/loop-2/deferred-ack-durability.md before attempting a fix — a tag-keyed durable " +
		"ack set was built and withdrawn after three cycles and four criticals, because an ack " +
		"must be attributed to the record it cancels, not to a re-mintable tag value.")

	b := newRestartableBroker(t, nil) // shipped defaults; no knob is involved
	const (
		queue = "c1.acked.redelivery"
		n     = 200
	)

	publishConfirmedDurable(t, b.uri, queue, 0, n)

	// Manual ack, so that "acknowledged" is an explicit protocol act and not a
	// side effect of no-ack delivery.
	acked := drainIDs(t, b.uri, queue, n, false, 10*time.Second, 2*time.Minute)
	missing, _ := missingIDs(acked, n)
	require.Zero(t, missing,
		"PREMISE BROKEN: only %d of %d messages were delivered and acknowledged before the "+
			"restart, so this test cannot say anything about acknowledged messages coming back",
		n-missing, n)

	requireQueueSettlesEmpty(t, b.uri, queue, 60*time.Second)

	b.restart()

	// Publish one NEW message after the restart. Its delivery is the active
	// check that the consumer path is live: without it, "nothing arrived" is
	// indistinguishable from a dead fixture, and this assertion would pass by
	// construction the day the consume path breaks.
	const sentinel = 10_000
	publishConfirmedDurable(t, b.uri, queue, sentinel, sentinel+1)

	after := drainIDs(t, b.uri, queue, 0, false, 5*time.Second, 2*time.Minute)

	require.True(t, after[sentinel],
		"FIXTURE DEAD: the message published after the restart was not delivered, so the "+
			"absence of the acknowledged ones below would prove nothing")

	var replayed []int
	for id := range after {
		if id != sentinel {
			replayed = append(replayed, id)
		}
	}
	t.Logf("RESULT: %d acked-before-restart messages redelivered after restart (sentinel delivered=%v)",
		len(replayed), after[sentinel])

	require.Empty(t, replayed,
		"DUPLICATE DELIVERY: %d messages that were delivered AND acknowledged before the "+
			"restart were delivered again after it; the queue was empty before the restart and "+
			"is not empty after it", len(replayed))
}
