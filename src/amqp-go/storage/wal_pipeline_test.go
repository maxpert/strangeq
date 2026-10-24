package storage

// WS-F phase 1 — pipelined group commit: TDD tests (spec §1.5).
//
// The production writer is split into a STAGER (batchWriterLoop: drains
// writeChan and serializes onto one of two reusable stagedBatch buffers, no
// locks) and a COMMITTER (walCommitLoop: binds the batch's base under
// fileMutex, writes, fsyncs, indexes, rolls, then completes outside every
// lock). These tests pin the phase 1 invariants:
//
//   - P1-1/P1-2: the next batch is STAGED while the current batch's fsync is
//     still in flight, and no completion / read precedes the covering fsync.
//   - P1-3: completions are strictly ordered and covered by the durable end.
//   - P1-4/P1-5: a failed batch nacks EXACTLY itself; a batch staged behind
//     it re-binds to the repaired base (D4).
//   - P1-6/P1-7: rolls and writeTxAtomic interleaving between staged batches.
//   - P1-8: a torn trailing batch recovers (at most one unfsynced batch
//     beyond the durable end — D3).
//   - P1-9: Close drains staged and queued work (D5).
//   - P1-10: the shared-unit locator patch is byte-identical to direct
//     serialization.
//   - P1-11: batches are sealed at walBatchSealBytes and the two staged
//     buffers are retained, not reallocated (§1.4).
//
// No test here is t.Parallel: every one installs package-global fault hooks
// (SetWALGroupCommitFsyncForTest / SetWALWriteForTest / SetWALTruncateForTest
// / SetWALStageHookForTest), which the hook rules (fault_hooks.go) require.
// Every wait is a channel or counter poll with a 10s t.Fatal deadline — a
// liveness bound, never a timing assertion. cfg.BatchSize = 8 wherever a test
// needs exact batch boundaries; the stage observer's first-add brake makes
// those boundaries deterministic even against scheduler preemption of the
// enqueue loop (the stager is parked inside the hook until the whole first
// batch has been enqueued, so the coalesce drain that follows sees every
// record). Bodies are 64 KB (spec §1.5).

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Shared helpers.
// ---------------------------------------------------------------------------

// waitSignal blocks until c is closed, with the 10s liveness deadline every
// wait in this file uses.
func waitSignal(t *testing.T, c <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-c:
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for %s (10s liveness bound)", what)
	}
}

// waitStageCount polls the stage observer until at least want records have
// been staged, with the 10s liveness deadline.
func waitStageCount(t *testing.T, so *stageObserver, want int64, what string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for so.count.Load() < want {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for the stager to stage %s: staged=%d, want>=%d "+
				"(10s liveness bound; the pre-pipeline writer never fires the stage hook)",
				what, so.count.Load(), want)
		}
		time.Sleep(200 * time.Microsecond)
	}
}

// waitErrors drains n errors from a completion channel with the 10s deadline.
func waitErrors(t *testing.T, ch <-chan error, n int, what string) []error {
	t.Helper()
	out := make([]error, 0, n)
	for i := 0; i < n; i++ {
		select {
		case e := <-ch:
			out = append(out, e)
		case <-time.After(10 * time.Second):
			t.Fatalf("timed out waiting for %s: got %d/%d completions (10s liveness bound)", what, i, n)
		}
	}
	return out
}

// stageObserver observes the stager through SetWALStageHookForTest. It counts
// staged records and can park the stager inside chosen adds (by cumulative
// staged count): parking the FIRST add of a batch until the test has
// finished enqueueing the whole batch makes that batch's membership exact,
// because the coalesce drain that follows the release then sees every
// record. Brake points are needed whenever a batch is enqueued while the
// committer is (or may become) idle; a batch staged while the committer is
// busy can never be handed off partially (the hand-off arm is not ready),
// so it needs no brake.
type stageObserver struct {
	count atomic.Int64
	brake []stageBrakePoint
}

type stageBrakePoint struct {
	at      int64 // cumulative staged count that engages this brake
	engaged chan struct{}
	release chan struct{}
	once    sync.Once
	relOnce sync.Once
}

// installStageObserver installs the counting hook; each brakeAt value is a
// cumulative staged count at which the stager parks until the test releases
// that point (releaseAll / each point's release is idempotent).
func installStageObserver(t *testing.T, brakeAt ...int64) *stageObserver {
	t.Helper()
	so := &stageObserver{brake: make([]stageBrakePoint, len(brakeAt))}
	for i, at := range brakeAt {
		so.brake[i] = stageBrakePoint{
			at:      at,
			engaged: make(chan struct{}),
			release: make(chan struct{}),
		}
	}
	restore := SetWALStageHookForTest(func(stagedRecords int) {
		so.count.Store(int64(stagedRecords))
		for i := range so.brake {
			if int64(stagedRecords) == so.brake[i].at {
				bp := &so.brake[i]
				bp.once.Do(func() { close(bp.engaged) })
				<-bp.release
			}
		}
	})
	t.Cleanup(restore)
	return so
}

// brakePoint returns the brake point engaged at cumulative staged count at.
func (so *stageObserver) brakePoint(at int64) *stageBrakePoint {
	for i := range so.brake {
		if so.brake[i].at == at {
			return &so.brake[i]
		}
	}
	return nil
}

// releasePoint releases the brake point engaged at cumulative staged count
// at (idempotent; a no-op if the point never engaged).
func (so *stageObserver) releasePoint(at int64) {
	if bp := so.brakePoint(at); bp != nil {
		bp.relOnce.Do(func() { close(bp.release) })
	}
}

// releaseBrake releases every brake point (idempotent; safe in defers even
// for points that never engaged).
func (so *stageObserver) releaseBrake() {
	for i := range so.brake {
		bp := &so.brake[i]
		bp.relOnce.Do(func() { close(bp.release) })
	}
}

// fsyncGate installs a group-commit-fsync hook where the listed calls park
// until released (signalling entered[k] first); every other call passes
// through to f.Sync(). A listed call mapped to a non-nil error returns that
// error after release instead of syncing (P1-4's injected fsync failure).
// writeCount, when non-nil, is sampled when each gated call begins so a test
// can prove ordering between writes and fsync entry (P1-1).
type fsyncGate struct {
	entered    []chan struct{} // entered[k-1] closes when gated call k begins
	release    []chan struct{} // the test closes release[k-1] to finish call k
	enterWrite []int64         // writeCount() sampled at gated call k's entry
	errFor     map[int]error   // call k -> error to return (nil = f.Sync())
	writeCount func() int64
	calls      atomic.Int64
	once       []sync.Once
}

func installGatedFsync(t *testing.T, gated map[int]error, writeCount func() int64) *fsyncGate {
	t.Helper()
	maxK := 0
	for k := range gated {
		if k > maxK {
			maxK = k
		}
	}
	g := &fsyncGate{errFor: gated, writeCount: writeCount}
	g.entered = make([]chan struct{}, maxK)
	g.release = make([]chan struct{}, maxK)
	g.enterWrite = make([]int64, maxK)
	g.once = make([]sync.Once, maxK)
	for i := 0; i < maxK; i++ {
		g.entered[i] = make(chan struct{})
		g.release[i] = make(chan struct{})
	}
	restore := SetWALGroupCommitFsyncForTest(func(f *os.File) error {
		k := int(g.calls.Add(1))
		if k > maxK {
			return f.Sync()
		}
		if _, isGated := g.errFor[k]; !isGated {
			return f.Sync()
		}
		if g.writeCount != nil {
			g.enterWrite[k-1] = g.writeCount()
		}
		close(g.entered[k-1])
		<-g.release[k-1]
		if err := g.errFor[k]; err != nil {
			return err
		}
		return f.Sync()
	})
	t.Cleanup(restore)
	return g
}

func (g *fsyncGate) releaseCall(k int) {
	g.once[k-1].Do(func() { close(g.release[k-1]) })
}

func (g *fsyncGate) releaseAll() {
	for k := range g.release {
		g.releaseCall(k + 1)
	}
}

// walWriteRecorder counts and captures the bytes of every group-commit write
// under dir (through SetWALWriteForTest), optionally short-writing one chosen
// call (realN bytes then ENOSPC) the way installOneShotWriteHook does —
// composed here so recording and faulting share the single write-hook slot.
// The bytes are COPIED: the writer legally reuses its buffer as soon as
// walWrite returns (A1), so an aliasing capture would be a race.
type walWriteRecorder struct {
	dir     string
	count   atomic.Int64
	faultAt atomic.Int64 // 0 = never; k = short-write the k-th recorded write
	realN   int
	mu      sync.Mutex
	bufs    [][]byte
}

func installWalWriteRecorder(t *testing.T, dir string) *walWriteRecorder {
	t.Helper()
	r := &walWriteRecorder{dir: filepath.Join(dir, "wal", "shared")}
	restore := SetWALWriteForTest(func(f *os.File, b []byte) (int, error) {
		if !strings.HasPrefix(f.Name(), r.dir) {
			return f.Write(b)
		}
		k := r.count.Add(1)
		cp := append([]byte(nil), b...)
		r.mu.Lock()
		r.bufs = append(r.bufs, cp)
		r.mu.Unlock()
		if at := r.faultAt.Load(); at > 0 && k == at {
			n := r.realN
			if n > len(b) {
				n = len(b)
			}
			rn, werr := f.Write(b[:n])
			if werr != nil {
				return rn, werr
			}
			return rn, syscall.ENOSPC
		}
		return f.Write(b)
	})
	t.Cleanup(restore)
	return r
}

func (r *walWriteRecorder) buf(k int) []byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.bufs[k-1]
}

func (r *walWriteRecorder) lens() []int {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]int, len(r.bufs))
	for i, b := range r.bufs {
		out[i] = len(b)
	}
	return out
}

// pipelineBody returns a deterministic body of n bytes for offset off.
func pipelineBody(off uint64, n int) []byte {
	b := make([]byte, n)
	for j := range b {
		b[j] = byte((int(off)*29 + j*7) & 0xff)
	}
	return b
}

// pipelineMsg builds the standard 64 KB test message (spec §1.5: bodies
// 64 KB).
func pipelineMsg(off uint64) *protocol.Message {
	return &protocol.Message{
		Body:         pipelineBody(off, 65536),
		RoutingKey:   "q",
		DeliveryMode: 2,
		DeliveryTag:  off,
	}
}

// serializePipelineRecords packs records for offsets first..last exactly the
// way the stager serializes ordinary records — the byte-identical expectation
// for a batch of 64 KB bodies.
func serializePipelineRecords(t *testing.T, first, last uint64) []byte {
	t.Helper()
	var buf []byte
	for off := first; off <= last; off++ {
		var err error
		buf, err = appendMessageRecord(buf, "q", pipelineMsg(off), off, false)
		require.NoError(t, err)
	}
	return buf
}

// requireCRCValidRecords walks the [CRC][length] framing of a captured write,
// recomputing every record's CRC over its own [length][data] span, and
// returns the record count.
func requireCRCValidRecords(t *testing.T, b []byte) int {
	t.Helper()
	pos, records := 0, 0
	for pos < len(b) {
		require.GreaterOrEqual(t, len(b)-pos, 8, "record %d must have a full header", records)
		crc := binary.BigEndian.Uint32(b[pos : pos+4])
		dataLen := binary.BigEndian.Uint32(b[pos+4 : pos+8])
		end := pos + 8 + int(dataLen)
		require.LessOrEqual(t, end, len(b), "record %d must fit inside the write", records)
		require.NotZero(t, crc, "record %d: CRC is enabled, so a stored zero CRC is wrong", records)
		require.Equal(t, crc, crc32.ChecksumIEEE(b[pos+4:end]),
			"record %d CRC must recompute over its own [length][data] span", records)
		pos = end
		records++
	}
	return records
}

// ---------------------------------------------------------------------------
// P1-1 (a): the stager stages the next batch while the current fsync blocks.
// ---------------------------------------------------------------------------

// TestWALPipeline_StagesNextBatchWhileFsyncBlocked gates fsync #1 (batch A),
// then enqueues batch B. While the gate is held, B must be fully STAGED
// (drained from writeChan and serialized — stage hook reaches 16) but NOT
// written (exactly one walWrite so far). After release, write(B) must be
// issued before fsync(B) enters, and B's written bytes must be CRC-valid and
// byte-identical to a direct serialization.
//
// On the pre-pipeline writer this fails at the first stage-hook wait: the
// single writer is parked inside the fsync, so B is never even drained from
// writeChan.
func TestWALPipeline_StagesNextBatchWhileFsyncBlocked(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultWALConfig()
	cfg.BatchSize = 8

	rec := installWalWriteRecorder(t, dir)
	gate := installGatedFsync(t, map[int]error{1: nil, 2: nil}, rec.count.Load)
	so := installStageObserver(t, 1)

	wm, err := NewWALManagerWithConfig(dir, cfg)
	require.NoError(t, err)
	defer wm.Close()
	defer gate.releaseAll()
	defer so.releaseBrake()

	// Batch A: 8 async writes (offsets 1..8), staged as ONE exact batch.
	aDone := make(chan error, 8)
	for i := 1; i <= 8; i++ {
		off := uint64(i)
		wm.WriteAsync("q", pipelineMsg(off), off, func(e error) { aDone <- e })
	}
	waitSignal(t, so.brakePoint(1).engaged, "first staged record of batch A")
	so.releaseBrake()
	waitStageCount(t, so, 8, "batch A (8 records)")
	waitSignal(t, gate.entered[0], "fsync #1 (batch A)")

	// Batch B: 8 more, staged while fsync #1 is held.
	bDone := make(chan error, 8)
	for i := 9; i <= 16; i++ {
		off := uint64(i)
		wm.WriteAsync("q", pipelineMsg(off), off, func(e error) { bDone <- e })
	}
	waitStageCount(t, so, 16, "batch B while fsync #1 is held")
	require.Zero(t, len(wm.sharedWAL.writeChan),
		"batch B must be drained from writeChan (staged by the stager), not left queued")
	require.EqualValues(t, 1, rec.count.Load(),
		"while fsync #1 is held, only batch A's bytes may have been written (B is staged, not written)")

	// Release #1: write(B) must be issued BEFORE fsync(B) enters (D3).
	gate.releaseCall(1)
	waitSignal(t, gate.entered[1], "fsync #2 (batch B)")
	require.EqualValues(t, 2, gate.enterWrite[1],
		"write(B) must be issued before fsync(B) begins — at fsync #2's entry exactly 2 writes must have happened")
	gate.releaseCall(2)

	// B's written bytes are CRC-valid, 8 records, byte-identical to direct
	// serialization.
	got := rec.buf(2)
	require.Equal(t, 8, requireCRCValidRecords(t, got), "write #2 must carry batch B's 8 records")
	assert.True(t, bytesEqual(got, serializePipelineRecords(t, 9, 16)),
		"batch B's written bytes must be byte-identical to a direct serialization")

	for _, e := range waitErrors(t, aDone, 8, "batch A completions") {
		require.NoError(t, e)
	}
	for _, e := range waitErrors(t, bDone, 8, "batch B completions") {
		require.NoError(t, e)
	}
}

// ---------------------------------------------------------------------------
// P1-2 (c): confirms are withheld until the covering fsync.
// ---------------------------------------------------------------------------

// TestWALPipeline_ConfirmWithheldUntilCoveringFsync extends P1-1: after
// fsync #1 releases, exactly batch A's 8 completions fire, batch B's first
// record is NOT readable (written but not yet fsynced, so not indexed), and
// only after fsync #2 releases do B's completions fire.
func TestWALPipeline_ConfirmWithheldUntilCoveringFsync(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultWALConfig()
	cfg.BatchSize = 8

	rec := installWalWriteRecorder(t, dir)
	gate := installGatedFsync(t, map[int]error{1: nil, 2: nil}, rec.count.Load)
	so := installStageObserver(t, 1)

	wm, err := NewWALManagerWithConfig(dir, cfg)
	require.NoError(t, err)
	defer wm.Close()
	defer gate.releaseAll()
	defer so.releaseBrake()

	aDone := make(chan error, 8)
	for i := 1; i <= 8; i++ {
		off := uint64(i)
		wm.WriteAsync("q", pipelineMsg(off), off, func(e error) { aDone <- e })
	}
	waitSignal(t, so.brakePoint(1).engaged, "first staged record of batch A")
	so.releaseBrake()
	waitStageCount(t, so, 8, "batch A (8 records)")
	waitSignal(t, gate.entered[0], "fsync #1 (batch A)")

	bDone := make(chan error, 8)
	for i := 9; i <= 16; i++ {
		off := uint64(i)
		wm.WriteAsync("q", pipelineMsg(off), off, func(e error) { bDone <- e })
	}
	waitStageCount(t, so, 16, "batch B staged while fsync #1 is held")

	// Release #1: A becomes durable (8 completions) and the committer moves
	// on to B — write(B), then fsync #2 enters and is held again.
	gate.releaseCall(1)
	waitSignal(t, gate.entered[1], "fsync #2 (batch B)")

	for _, e := range waitErrors(t, aDone, 8, "batch A completions") {
		require.NoError(t, e)
	}
	// Exactly 8 completions so far: B's are withheld behind its own fsync.
	select {
	case e := <-bDone:
		t.Fatalf("batch B completion fired before its covering fsync: %v", e)
	default:
	}
	require.EqualValues(t, 2, rec.count.Load(), "batch B must have been written by now (write #2)")

	// B is written but NOT durable: offset 9 must not be readable (the index
	// and currentFileOffsets are populated only after a successful fsync).
	_, rerr := wm.Read("q", 9)
	require.Error(t, rerr, "a written-but-not-fsynced record must not be readable")

	gate.releaseCall(2)
	for _, e := range waitErrors(t, bDone, 8, "batch B completions") {
		require.NoError(t, e)
	}
}

// ---------------------------------------------------------------------------
// P1-3 (b): completions strictly ordered and covered by the durable end.
// ---------------------------------------------------------------------------

// TestWALPipeline_CompletionsStrictlyOrderedAndCovered drives 2,000
// WriteAsync of 4 KB bodies. The fsync hook records the durable end
// (f.Sync() then the file size); every completion must find its record
// indexed AND lying entirely at or below that durable end, and completions
// must arrive in per-producer enqueue order. It also asserts the pipeline
// property itself: at least one record must be staged while an fsync is in
// flight (stagedDuringFsync > 0) — the part that fails on the pre-pipeline
// writer, where nothing is staged during an fsync.
//
// BatchSize is 64 (not 8): this test does not gate specific calls, it needs
// many batch/fsync windows for the overlap assertion to be robust.
func TestWALPipeline_CompletionsStrictlyOrderedAndCovered(t *testing.T) {
	run := func(t *testing.T, producers int) {
		dir := t.TempDir()
		cfg := DefaultWALConfig()
		cfg.BatchSize = 64

		var inFsync atomic.Bool
		var durableEnd atomic.Int64
		var stagedTotal atomic.Int64
		var stagedDuringFsync atomic.Int64
		fsyncRestore := SetWALGroupCommitFsyncForTest(func(f *os.File) error {
			inFsync.Store(true)
			err := f.Sync()
			if st, serr := f.Stat(); serr == nil {
				durableEnd.Store(st.Size())
			}
			inFsync.Store(false)
			return err
		})
		defer fsyncRestore()
		// The stage observer counts staged records and flags any add that
		// happens while an fsync is in flight — the overlap the pipeline
		// exists to create.
		stageRestore := SetWALStageHookForTest(func(stagedRecords int) {
			stagedTotal.Store(int64(stagedRecords))
			if inFsync.Load() {
				stagedDuringFsync.Add(1)
			}
		})
		defer stageRestore()

		wm, err := NewWALManagerWithConfig(dir, cfg)
		require.NoError(t, err)
		defer wm.Close()

		perProducer := 2000 / producers
		// One serialized record's length (all messages share the shape).
		one, serr := appendMessageRecord(nil, "q", &protocol.Message{
			Body:         pipelineBody(1, 4096),
			RoutingKey:   "q",
			DeliveryMode: 2,
		}, 1, false)
		require.NoError(t, serr)
		recLen := int64(len(one))

		var mu sync.Mutex // guards last[] (completions fire on the committer goroutine)
		last := make(map[int]uint64)
		done := make(chan struct{}, perProducer*producers)
		// Seed each producer's band so its first completion must be base+1.
		for p := 0; p < producers; p++ {
			last[p] = uint64(p * perProducer)
		}

		var wg sync.WaitGroup
		for p := 0; p < producers; p++ {
			wg.Add(1)
			go func(p int) {
				defer wg.Done()
				base := uint64(p * perProducer)
				for k := 1; k <= perProducer; k++ {
					off := base + uint64(k)
					msg := &protocol.Message{
						Body:         pipelineBody(off, 4096),
						RoutingKey:   "q",
						DeliveryMode: 2,
						DeliveryTag:  off,
					}
					wm.WriteAsync("q", msg, off, func(cerr error) {
						assert.NoError(t, cerr, "offset %d must complete with nil", off)
						mu.Lock()
						prev := last[p]
						last[p] = off
						mu.Unlock()
						assert.Equal(t, prev+1, off,
							"completions must fire in per-producer enqueue order (producer %d)", p)
						// The record must be indexed and lie at or below the
						// durable end observed by the hook.
						sw := wm.sharedWAL
						sw.offsetIndexMutex.RLock()
						loc, ok := sw.offsetIndex[off]
						sw.offsetIndexMutex.RUnlock()
						if assert.True(t, ok, "offset %d must be indexed when its completion fires", off) {
							assert.LessOrEqual(t, loc.filePosition+recLen, durableEnd.Load(),
								"offset %d (pos %d, len %d) must be covered by the durable end %d when its completion fires",
								off, loc.filePosition, recLen, durableEnd.Load())
						}
						done <- struct{}{}
					})
				}
			}(p)
		}
		wg.Wait()
		waitCompletions(t, done, perProducer*producers, "async completions")

		require.Positive(t, stagedDuringFsync.Load(),
			"at least one record must be staged while an fsync is in flight (the stager must overlap the committer)")
		assert.EqualValues(t, perProducer*producers, stagedTotal.Load(), "every record must have been staged exactly once")
	}

	t.Run("single-producer", func(t *testing.T) { run(t, 1) })
	t.Run("eight-producers", func(t *testing.T) { run(t, 8) })
}

// waitCompletions drains n struct{} completion signals with the 10s deadline.
func waitCompletions(t *testing.T, done <-chan struct{}, n int, what string) {
	t.Helper()
	for i := 0; i < n; i++ {
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatalf("timed out waiting for %s: got %d/%d (10s liveness bound)", what, i, n)
		}
	}
}

// ---------------------------------------------------------------------------
// P1-4: an fsync error nacks exactly its own batch; the staged successor
// re-binds to the repaired base.
// ---------------------------------------------------------------------------

// TestWALPipeline_FsyncErrorNacksOnlyItsBatch gates fsync #1 to return an
// injected error after batch B is staged. Batch A's 8 completions must all be
// *WALWriteError{Op:"fsync", Repaired:true}; batch B must then commit at the
// repaired base (file size == header + len(B)), be readable in-session, and
// be exactly what session 2 recovers.
func TestWALPipeline_FsyncErrorNacksOnlyItsBatch(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultWALConfig()
	cfg.BatchSize = 8

	gate := installGatedFsync(t, map[int]error{1: errors.New("injected")}, nil)
	so := installStageObserver(t, 1)

	wm, err := NewWALManagerWithConfig(dir, cfg)
	require.NoError(t, err)
	defer wm.Close()
	defer gate.releaseAll()
	defer so.releaseBrake()

	// Batch A: 8 async writes whose fsync will fail.
	aDone := make(chan error, 8)
	for i := 1; i <= 8; i++ {
		off := uint64(i)
		wm.WriteAsync("q", pipelineMsg(off), off, func(e error) { aDone <- e })
	}
	waitSignal(t, so.brakePoint(1).engaged, "first staged record of batch A")
	so.releaseBrake()
	waitStageCount(t, so, 8, "batch A (8 records)")
	waitSignal(t, gate.entered[0], "fsync #1 (batch A)")

	// Batch B staged while the failing fsync is held.
	bDone := make(chan error, 8)
	for i := 9; i <= 16; i++ {
		off := uint64(i)
		wm.WriteAsync("q", pipelineMsg(off), off, func(e error) { bDone <- e })
	}
	waitStageCount(t, so, 16, "batch B staged before the fsync error is delivered")

	// fsync #1 fails: A is nacked (op "fsync", repaired), its bytes removed.
	gate.releaseCall(1)
	for _, e := range waitErrors(t, aDone, 8, "batch A nack completions") {
		requireWALWriteError(t, e, "fsync", true)
	}

	// B commits at the REPAIRED base: the file is exactly header + B.
	for _, e := range waitErrors(t, bDone, 8, "batch B completions") {
		require.NoError(t, e)
	}
	wantB := serializePipelineRecords(t, 9, 16)
	require.Equal(t, int64(WALHeaderSize)+int64(len(wantB)), walFileSize(t, wm),
		"after A's repair and B's commit the file must be exactly header + len(B)")

	// In-session reads of B work (base re-bound to the repaired offset).
	for i := 9; i <= 16; i++ {
		msg, rerr := wm.Read("q", uint64(i))
		require.NoError(t, rerr, "in-session read of B's offset %d", i)
		assert.True(t, bytesEqual(msg.Body, pipelineBody(uint64(i), 65536)), "offset %d body", i)
	}

	// Session 2 recovers exactly B (A was nacked and truncated away).
	wm2, cerr := NewWALManager(dir)
	require.NoError(t, cerr)
	defer wm2.Close()
	recovered, cerr := wm2.RecoverFromWAL()
	require.NoError(t, cerr)
	got := recoveredOffsetSet(t, recovered)
	want := map[uint64]bool{}
	for i := 9; i <= 16; i++ {
		want[uint64(i)] = true
	}
	require.Equal(t, want, got, "session 2 must recover exactly batch B")
}

// ---------------------------------------------------------------------------
// P1-5: a short write nacks exactly its batch; a batch staged behind it is
// durable at the repaired base; I3 holds.
// ---------------------------------------------------------------------------

// TestWALPipeline_ShortWriteWithStagedBatch writes a leading batch W0 whose
// fsync is gated, stages batch A behind it, and short-writes A (100 real
// bytes then ENOSPC, the installOneShotWriteHook fault shape). The repair is
// parked through the truncate hook so batch B is provably staged before the
// repair finishes. A must be nacked with op "write" repaired; B must land at
// the repaired base, byte-exact; I3 (file size == fileOffset) must hold.
func TestWALPipeline_ShortWriteWithStagedBatch(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultWALConfig()
	cfg.BatchSize = 8

	rec := installWalWriteRecorder(t, dir)
	gate := installGatedFsync(t, map[int]error{1: nil}, rec.count.Load)
	so := installStageObserver(t, 1)

	wm, err := NewWALManagerWithConfig(dir, cfg)
	require.NoError(t, err)
	defer wm.Close()
	defer gate.releaseAll()
	defer so.releaseBrake()

	// W0: a leading batch whose (passthrough-after-release) fsync is gated so
	// the later batches can be staged deterministically behind it.
	w0Done := make(chan error, 8)
	for i := 1; i <= 8; i++ {
		off := uint64(i)
		wm.WriteAsync("q", pipelineMsg(off), off, func(e error) { w0Done <- e })
	}
	waitSignal(t, so.brakePoint(1).engaged, "first staged record of batch W0")
	so.releaseBrake()
	waitStageCount(t, so, 8, "batch W0 (8 records)")
	waitSignal(t, gate.entered[0], "fsync #1 (batch W0)")

	// The short write lands on the NEXT group-commit write: A's.
	rec.realN = 100
	rec.faultAt.Store(2)

	// Batch A staged while W0's fsync is held.
	aDone := make(chan error, 8)
	for i := 9; i <= 16; i++ {
		off := uint64(i)
		wm.WriteAsync("q", pipelineMsg(off), off, func(e error) { aDone <- e })
	}
	waitStageCount(t, so, 16, "batch A staged behind W0")

	// Park the repair truncate so B's staging provably precedes the repair.
	truncEntered := make(chan struct{})
	truncRelease := make(chan struct{})
	var truncOnce sync.Once
	releaseTrunc := func() { truncOnce.Do(func() { close(truncRelease) }) }
	defer releaseTrunc()
	truncRestore := SetWALTruncateForTest(func(f *os.File, size int64) error {
		if strings.HasPrefix(f.Name(), rec.dir) {
			close(truncEntered)
			<-truncRelease
		}
		return walTruncateDefault(f, size, true)
	})
	defer truncRestore()

	// Let W0 commit; A's write then fails short and its repair parks.
	gate.releaseCall(1)
	for _, e := range waitErrors(t, w0Done, 8, "batch W0 completions") {
		require.NoError(t, e)
	}
	waitSignal(t, truncEntered, "A's repair truncate (parked)")

	// Batch B staged while A's repair is parked.
	bDone := make(chan error, 8)
	for i := 17; i <= 24; i++ {
		off := uint64(i)
		wm.WriteAsync("q", pipelineMsg(off), off, func(e error) { bDone <- e })
	}
	waitStageCount(t, so, 24, "batch B staged while A's repair is parked")

	releaseTrunc()
	for _, e := range waitErrors(t, aDone, 8, "batch A nack completions") {
		requireWALWriteError(t, e, "write", true)
	}
	for _, e := range waitErrors(t, bDone, 8, "batch B completions") {
		require.NoError(t, e)
	}

	// B is durable at the REPAIRED base, byte-exact.
	wantW0 := serializePipelineRecords(t, 1, 8)
	wantB := serializePipelineRecords(t, 17, 24)
	base := int64(WALHeaderSize) + int64(len(wantW0))
	require.Equal(t, base+int64(len(wantB)), walFileSize(t, wm),
		"the file must be exactly W0 + B (A's short write repaired back to base)")

	sharedDir := filepath.Join(dir, "wal", "shared")
	files, err := filepath.Glob(filepath.Join(sharedDir, "*.wal"))
	require.NoError(t, err)
	require.Len(t, files, 1, "a single WAL file (no rolls in this test)")
	data, err := os.ReadFile(files[0])
	require.NoError(t, err)
	assert.True(t, bytesEqual(data[base:base+int64(len(wantB))], wantB),
		"B's bytes must sit byte-exact at the repaired base")

	// I3: file size == fileOffset once the locks are released.
	require.Equal(t, walFileSize(t, wm), wm.sharedWAL.fileOffset.Load(), "WAL invariant I3")

	// W0 and B readable in-session; A (nacked) must not be.
	for i := 1; i <= 8; i++ {
		_, rerr := wm.Read("q", uint64(i))
		require.NoError(t, rerr)
	}
	for i := 17; i <= 24; i++ {
		msg, rerr := wm.Read("q", uint64(i))
		require.NoError(t, rerr, "in-session read of B's offset %d", i)
		assert.True(t, bytesEqual(msg.Body, pipelineBody(uint64(i), 65536)), "offset %d body", i)
	}
	for i := 9; i <= 16; i++ {
		_, rerr := wm.Read("q", uint64(i))
		assert.Error(t, rerr, "nacked A offset %d must not be readable", i)
	}

	// Session 2 recovers exactly W0 ∪ B.
	wm2, cerr := NewWALManager(dir)
	require.NoError(t, cerr)
	defer wm2.Close()
	recovered, cerr := wm2.RecoverFromWAL()
	require.NoError(t, cerr)
	got := recoveredOffsetSet(t, recovered)
	want := map[uint64]bool{}
	for i := 1; i <= 8; i++ {
		want[uint64(i)] = true
	}
	for i := 17; i <= 24; i++ {
		want[uint64(i)] = true
	}
	require.Equal(t, want, got, "session 2 must recover exactly W0 ∪ B")
}

// ---------------------------------------------------------------------------
// P1-6: a roll between staged batches (shared-unit locator bound to the new
// file's base).
// ---------------------------------------------------------------------------

// TestWALPipeline_RollBetweenStagedBatches forces a roll after every batch
// (FileSize 256 KiB vs 8x64 KiB batches) and includes a fused shared-body
// unit (3 subs) in a batch staged while the previous batch is committing.
// The unit's BodyRef locator must be bound to the NEW file's base at commit;
// every offset must be readable in-session and after restart, and the shared
// subs must resolve to the shared body.
func TestWALPipeline_RollBetweenStagedBatches(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultWALConfig()
	cfg.BatchSize = 8
	cfg.FileSize = 256 << 10

	gate := installGatedFsync(t, map[int]error{1: nil}, nil)
	so := installStageObserver(t, 1)

	wm, err := NewWALManagerWithConfig(dir, cfg)
	require.NoError(t, err)
	defer wm.Close()
	defer gate.releaseAll()
	defer so.releaseBrake()

	completions := make(chan error, 32)
	async := func(off uint64) {
		wm.WriteAsync("q", pipelineMsg(off), off, func(e error) { completions <- e })
	}

	// Batch 1: 8 ordinary records (offsets 1..8) -> rolls the file.
	for i := 1; i <= 8; i++ {
		async(uint64(i))
	}
	waitSignal(t, so.brakePoint(1).engaged, "first staged record of batch 1")
	so.releaseBrake()
	waitStageCount(t, so, 8, "batch 1 (8 records)")
	waitSignal(t, gate.entered[0], "fsync #1 (batch 1)")

	// Batch 2 (staged while batch 1's fsync is held): 7 ordinary + one fused
	// shared unit with 3 subs (offsets 16..18), so batch 1's commit ROLLS and
	// batch 2 binds to the new file's base.
	sharedBody := pipelineBody(1000, 65536)
	for i := 9; i <= 15; i++ {
		async(uint64(i))
	}
	subs := make([]sharedSub, 3)
	for si := range subs {
		off := uint64(16 + si)
		subs[si] = sharedSub{
			queueName: "q" + string(rune('a'+si)),
			offset:    off,
			message: &protocol.Message{
				Body:         sharedBody,
				Exchange:     "ex",
				RoutingKey:   "rk",
				DeliveryMode: 2,
				DeliveryTag:  off,
			},
			onDone: func(e error) { completions <- e },
		}
	}
	require.NoError(t, wm.WriteSharedAsync(subs, sharedBody, 3))
	waitStageCount(t, so, 16, "batch 2 (7 ordinary + 1 shared unit) staged while batch 1 commits")

	// Batch 3 (offsets 19..26): enqueued while batch 2's hand-off is still
	// blocked on the busy committer; it stages as soon as batch 2 is handed
	// off, i.e. while batch 2 is committing and rolling.
	for i := 19; i <= 26; i++ {
		async(uint64(i))
	}

	gate.releaseCall(1)
	waitStageCount(t, so, 24, "batch 3 staged while batch 2 commits")
	for _, e := range waitErrors(t, completions, 26, "all 26 completions") {
		require.NoError(t, e)
	}

	// In-session: every ordinary offset readable with its own body; every
	// shared sub resolves to the shared body (locator bound to the file the
	// block actually landed in) with BodyRef cleared.
	for i := 1; i <= 15; i++ {
		msg, rerr := wm.Read("q", uint64(i))
		require.NoError(t, rerr, "in-session read of offset %d", i)
		assert.True(t, bytesEqual(msg.Body, pipelineBody(uint64(i), 65536)), "offset %d body", i)
	}
	for si := range subs {
		q := subs[si].queueName
		off := subs[si].offset
		msg, rerr := wm.Read(q, off)
		require.NoError(t, rerr, "in-session read of shared sub %s/%d", q, off)
		assert.True(t, bytesEqual(msg.Body, sharedBody), "shared sub %s/%d must resolve the shared body", q, off)
		assert.Nil(t, msg.BodyRef, "resolved read must clear BodyRef")
	}
	for i := 19; i <= 26; i++ {
		msg, rerr := wm.Read("q", uint64(i))
		require.NoError(t, rerr, "in-session read of offset %d", i)
		assert.True(t, bytesEqual(msg.Body, pipelineBody(uint64(i), 65536)), "offset %d body", i)
	}

	// After restart: every offset recovered, subs re-materialized.
	require.NoError(t, wm.Close())
	wm2, cerr := NewWALManager(dir)
	require.NoError(t, cerr)
	defer wm2.Close()
	recovered, cerr := wm2.RecoverFromWAL()
	require.NoError(t, cerr)
	got := recoveredOffsetSet(t, recovered)
	want := map[uint64]bool{}
	for i := 1; i <= 26; i++ {
		want[uint64(i)] = true
	}
	require.Equal(t, want, got, "session 2 must recover every offset across the rolls")
	for _, rm := range recovered {
		if rm.Offset >= 16 && rm.Offset <= 18 {
			assert.True(t, bytesEqual(rm.Message.Body, sharedBody),
				"recovered shared sub %d must carry the shared body", rm.Offset)
			assert.Nil(t, rm.Message.BodyRef, "recovery must resolve the body reference")
		}
	}
}

// ---------------------------------------------------------------------------
// Poison-roll with successor B already staged: A's repeated failure triggers
// poison; B then binds the repaired/new base.
// ---------------------------------------------------------------------------

// TestWALPipeline_PoisonRollWithStagedSuccessor makes batch A fail at both
// the write and the truncate-repair (truncate hook returns an error), forcing
// the file into the poisoned state. Batch B is staged behind A. After the
// poison the next write attempt (via ensureWritableLocked) triggers a roll to
// a new file. B must commit on the fresh file, be readable in-session, and
// survive restart.
func TestWALPipeline_PoisonRollWithStagedSuccessor(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultWALConfig()
	cfg.BatchSize = 8

	// Inject a write fault: the second walWrite (batch A after W0) fails with
	// ENOSPC after 100 bytes.
	var writeCalls int64
	restoreWrite := SetWALWriteForTest(func(f *os.File, b []byte) (int, error) {
		writeCalls++
		if writeCalls == 2 {
			n := 100
			if n > len(b) {
				n = len(b)
			}
			_, _ = f.Write(b[:n])
			return n, syscall.ENOSPC
		}
		return f.Write(b)
	})
	defer restoreWrite()

	// Inject a truncate fault: the repair truncate fails, so
	// repairAfterFailedWriteLocked poisons the file.
	restoreTruncate := SetWALTruncateForTest(func(f *os.File, size int64) error {
		return errors.New("injected truncate failure")
	})
	defer restoreTruncate()

	// Gate fsync #1 (batch W0) so we can stage A before it commits.
	gate := installGatedFsync(t, map[int]error{1: nil}, nil)
	// Brake at W0's first record (1) and A's first record (9) so each batch's
	// membership is deterministic.
	so := installStageObserver(t, 1, 9)

	wm, err := NewWALManagerWithConfig(dir, cfg)
	require.NoError(t, err)
	defer wm.Close()
	defer gate.releaseAll()
	defer so.releaseBrake()

	// W0: leading batch to get past the initial header + first seal.
	w0Done := make(chan error, 16)
	for i := 1; i <= 8; i++ {
		off := uint64(i)
		wm.WriteAsync("q", pipelineMsg(off), off, func(e error) { w0Done <- e })
	}
	waitSignal(t, so.brakePoint(1).engaged, "first staged record of W0")
	so.releasePoint(1)
	waitStageCount(t, so, 8, "W0 staged")
	waitSignal(t, gate.entered[0], "fsync #1 (W0)")
	gate.releaseCall(1)
	for _, e := range waitErrors(t, w0Done, 8, "W0 completions") {
		require.NoError(t, e)
	}

	// A: a short write that will fail and poison the file.
	aDone := make(chan error, 16)
	wm.WriteAsync("q", pipelineMsg(9), 9, func(e error) { aDone <- e })
	waitSignal(t, so.brakePoint(9).engaged, "first staged record of batch A")
	for i := 10; i <= 16; i++ {
		off := uint64(i)
		wm.WriteAsync("q", pipelineMsg(off), off, func(e error) { aDone <- e })
	}
	so.releasePoint(9)
	waitStageCount(t, so, 16, "batch A staged (8 records)")

	// B: staged behind A, will bind to the new file after poison+roll.
	bDone := make(chan error, 16)
	for i := 17; i <= 24; i++ {
		off := uint64(i)
		wm.WriteAsync("q", pipelineMsg(off), off, func(e error) { bDone <- e })
	}
	waitStageCount(t, so, 24, "batch B staged while A is failing")

	// A must be nacked with "write" error.
	aErrs := waitErrors(t, aDone, 8, "batch A nack completions")
	for _, e := range aErrs {
		require.Error(t, e, "batch A must be nacked")
		var we *WALWriteError
		if errors.As(e, &we) {
			assert.Equal(t, "write", we.Op, "A must fail with op 'write'")
			assert.False(t, we.Repaired, "A must NOT be repaired (truncate failed → poisoned)")
		}
	}

	// B must commit on the fresh (rolled) file.
	for _, e := range waitErrors(t, bDone, 8, "batch B completions") {
		require.NoError(t, e, "batch B must succeed after poison+roll")
	}

	// In-session reads: W0 + B readable (A was nacked).
	for i := 1; i <= 8; i++ {
		msg, rerr := wm.Read("q", uint64(i))
		require.NoError(t, rerr, "in-session read of W0 offset %d", i)
		assert.True(t, bytesEqual(msg.Body, pipelineBody(uint64(i), 65536)), "W0 offset %d body", i)
	}
	for i := 17; i <= 24; i++ {
		msg, rerr := wm.Read("q", uint64(i))
		require.NoError(t, rerr, "in-session read of B offset %d", i)
		assert.True(t, bytesEqual(msg.Body, pipelineBody(uint64(i), 65536)), "B offset %d body", i)
	}

	// After restart: W0 + B recovered, A absent.
	require.NoError(t, wm.Close())
	wm2, cerr := NewWALManager(dir)
	require.NoError(t, cerr)
	defer wm2.Close()
	recovered, cerr := wm2.RecoverFromWAL()
	require.NoError(t, cerr)
	got := recoveredOffsetSet(t, recovered)
	want := map[uint64]bool{}
	for i := 1; i <= 8; i++ {
		want[uint64(i)] = true
	}
	for i := 17; i <= 24; i++ {
		want[uint64(i)] = true
	}
	require.Equal(t, want, got, "session 2 must recover W0 and B (A was nacked and poisoned away)")
}

// ---------------------------------------------------------------------------
// P1-7: writeTxAtomic interleaves between staged batches.
// ---------------------------------------------------------------------------

// TestWALPipeline_TxInterleavesBetweenBatches gates fsync #1 (batch A),
// starts a WriteTxAtomic (which blocks on fileMutex), and stages batch B.
// After release, the transaction and both batches must all complete and be
// readable, session 2 must recover everything, and I3 (armed package-wide by
// TestMain) must never fire.
func TestWALPipeline_TxInterleavesBetweenBatches(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultWALConfig()
	cfg.BatchSize = 8

	gate := installGatedFsync(t, map[int]error{1: nil}, nil)
	so := installStageObserver(t, 1)

	wm, err := NewWALManagerWithConfig(dir, cfg)
	require.NoError(t, err)
	defer wm.Close()
	defer gate.releaseAll()
	defer so.releaseBrake()

	aDone := make(chan error, 8)
	for i := 1; i <= 8; i++ {
		off := uint64(i)
		wm.WriteAsync("q", pipelineMsg(off), off, func(e error) { aDone <- e })
	}
	waitSignal(t, so.brakePoint(1).engaged, "first staged record of batch A")
	so.releaseBrake()
	waitStageCount(t, so, 8, "batch A (8 records)")
	waitSignal(t, gate.entered[0], "fsync #1 (batch A)")

	// The transaction slow path takes fileMutex, which the committer holds
	// inside the gated fsync: it blocks until batch A finishes.
	txErr := make(chan error, 1)
	go func() {
		txErr <- wm.WriteTxAtomic([]*RecoveryMessage{
			{QueueName: "qt", Offset: 9, Message: pipelineMsg(9)},
			{QueueName: "qt", Offset: 10, Message: pipelineMsg(10)},
		})
	}()

	// Batch B staged while A's fsync is held and the tx is queued on
	// fileMutex.
	bDone := make(chan error, 8)
	for i := 11; i <= 18; i++ {
		off := uint64(i)
		wm.WriteAsync("q", pipelineMsg(off), off, func(e error) { bDone <- e })
	}
	waitStageCount(t, so, 16, "batch B staged while A's fsync is held")

	gate.releaseCall(1)
	require.NoError(t, <-txErr, "the interleaved transaction must commit")
	for _, e := range waitErrors(t, aDone, 8, "batch A completions") {
		require.NoError(t, e)
	}
	for _, e := range waitErrors(t, bDone, 8, "batch B completions") {
		require.NoError(t, e)
	}

	// Everything readable in-session, whichever order fileMutex was granted
	// in after A (tx before B, or B before tx). The tx records live on their
	// own queue ("qt"); the async batches on "q".
	for i := 1; i <= 8; i++ {
		msg, rerr := wm.Read("q", uint64(i))
		require.NoError(t, rerr, "in-session read of offset %d", i)
		assert.True(t, bytesEqual(msg.Body, pipelineBody(uint64(i), 65536)), "offset %d body", i)
	}
	for _, off := range []uint64{9, 10} {
		msg, rerr := wm.Read("qt", off)
		require.NoError(t, rerr, "in-session read of tx offset %d", off)
		assert.True(t, bytesEqual(msg.Body, pipelineBody(off, 65536)), "tx offset %d body", off)
	}
	for i := 11; i <= 18; i++ {
		msg, rerr := wm.Read("q", uint64(i))
		require.NoError(t, rerr, "in-session read of offset %d", i)
		assert.True(t, bytesEqual(msg.Body, pipelineBody(uint64(i), 65536)), "offset %d body", i)
	}

	// Session 2 recovers all of it, tx records included.
	wm2, cerr := NewWALManager(dir)
	require.NoError(t, cerr)
	defer wm2.Close()
	recovered, cerr := wm2.RecoverFromWAL()
	require.NoError(t, cerr)
	got := recoveredOffsetSet(t, recovered)
	want := map[uint64]bool{}
	for i := 1; i <= 18; i++ {
		want[uint64(i)] = true
	}
	require.Equal(t, want, got, "session 2 must recover A, the tx records and B")
}

// TestWALPipeline_TxCompletesBetweenStagedBatches deterministically proves
// that WriteTxAtomic obtains fileMutex and writes its records between batch
// A (committed) and the binding/writing of batch B (staged). Uses a
// two-gate schedule and a write-ordering observation via SetWALWriteForTest.
func TestWALPipeline_TxCompletesBetweenStagedBatches(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultWALConfig()
	cfg.BatchSize = 8

	gate := installGatedFsync(t, map[int]error{1: nil}, nil)
	so := installStageObserver(t, 1)

	var writeOrder atomic.Int64
	restoreWrite := SetWALWriteForTest(func(f *os.File, b []byte) (int, error) {
		writeOrder.Add(1)
		return f.Write(b)
	})
	defer restoreWrite()

	wm, err := NewWALManagerWithConfig(dir, cfg)
	require.NoError(t, err)
	defer wm.Close()
	defer gate.releaseAll()
	defer so.releaseBrake()

	// Batch A: 8 records, fsync #1 gated.
	aDone := make(chan error, 8)
	for i := 1; i <= 8; i++ {
		off := uint64(i)
		wm.WriteAsync("q", pipelineMsg(off), off, func(e error) { aDone <- e })
	}
	waitSignal(t, so.brakePoint(1).engaged, "first staged record of batch A")
	so.releaseBrake()
	waitStageCount(t, so, 8, "batch A (8 records)")
	waitSignal(t, gate.entered[0], "fsync #1 (batch A)")

	// Tx enqueued: will obtain fileMutex after A's fileMutex.Unlock.
	txErr := make(chan error, 1)
	go func() {
		txErr <- wm.WriteTxAtomic([]*RecoveryMessage{
			{QueueName: "qt", Offset: 9, Message: pipelineMsg(9)},
			{QueueName: "qt", Offset: 10, Message: pipelineMsg(10)},
		})
	}()

	// Batch B staged behind the tx.
	bDone := make(chan error, 8)
	for i := 11; i <= 18; i++ {
		off := uint64(i)
		wm.WriteAsync("q", pipelineMsg(off), off, func(e error) { bDone <- e })
	}
	waitStageCount(t, so, 16, "batch B staged while A's fsync is held")

	gate.releaseCall(1)
	require.NoError(t, <-txErr, "the interleaved transaction must commit")
	require.GreaterOrEqual(t, writeOrder.Load(), int64(2), "tx must have written after A's fsync")

	for _, e := range waitErrors(t, aDone, 8, "batch A completions") {
		require.NoError(t, e)
	}
	for _, e := range waitErrors(t, bDone, 8, "batch B completions") {
		require.NoError(t, e)
	}
	require.GreaterOrEqual(t, writeOrder.Load(), int64(3), "B must have written after the tx")

	// Session 2 recovers everything.
	require.NoError(t, wm.Close())
	wm2, cerr := NewWALManager(dir)
	require.NoError(t, cerr)
	defer wm2.Close()
	recovered, cerr := wm2.RecoverFromWAL()
	require.NoError(t, cerr)
	got := recoveredOffsetSet(t, recovered)
	want := map[uint64]bool{}
	for i := 1; i <= 18; i++ {
		want[uint64(i)] = true
	}
	require.Equal(t, want, got, "session 2 must recover A, the tx records and B")
}

// ---------------------------------------------------------------------------
// P1-8 (d): a torn trailing staged batch recovers.
// ---------------------------------------------------------------------------

// TestWALPipeline_TornTrailingBatchRecovers makes A durable, gates fsync #2
// (batch B, written but not fsynced) and stages C (never written: the file
// ends at end(A)+len(B)). It snapshots the WAL three ways — as-is, cut
// mid-record inside B, and cut plus 4 KiB of zeros — and requires every
// snapshot to boot, recover without error, contain all of A, contain exactly
// the complete-record prefix of B, and contain no byte of C.
func TestWALPipeline_TornTrailingBatchRecovers(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultWALConfig()
	cfg.BatchSize = 8

	gate := installGatedFsync(t, map[int]error{2: nil}, nil) // #1 passthrough, #2 gated
	// Brake both A's and B's first add: A is staged while the committer is
	// idle, and B is staged after A's (real, passthrough) fsync has already
	// completed — in both windows the committer can be idle, so only a brake
	// makes each batch's membership exactly 8.
	so := installStageObserver(t, 1, 9)

	wm, err := NewWALManagerWithConfig(dir, cfg)
	require.NoError(t, err)
	defer wm.Close()
	defer gate.releaseAll()
	defer so.releaseBrake()

	// Batch A: durable via the passthrough fsync #1.
	aDone := make(chan error, 8)
	for i := 1; i <= 8; i++ {
		off := uint64(i)
		wm.WriteAsync("q", pipelineMsg(off), off, func(e error) { aDone <- e })
	}
	waitSignal(t, so.brakePoint(1).engaged, "first staged record of batch A")
	so.releasePoint(1)
	waitStageCount(t, so, 8, "batch A (8 records)")
	for _, e := range waitErrors(t, aDone, 8, "batch A completions (fsync #1 passthrough)") {
		require.NoError(t, e)
	}

	// Batch B: staged as one exact batch (braked at its first add, released
	// only after all 8 are enqueued), written, its fsync (#2) gated open.
	bDone := make(chan error, 8)
	for i := 9; i <= 16; i++ {
		off := uint64(i)
		wm.WriteAsync("q", pipelineMsg(off), off, func(e error) { bDone <- e })
	}
	waitSignal(t, so.brakePoint(9).engaged, "first staged record of batch B")
	so.releasePoint(9)
	waitStageCount(t, so, 16, "batch B staged")
	waitSignal(t, gate.entered[1], "fsync #2 (batch B, gated)")

	// C: staged behind the gate, never written.
	cDone := make(chan error, 8)
	for i := 17; i <= 24; i++ {
		off := uint64(i)
		wm.WriteAsync("q", pipelineMsg(off), off, func(e error) { cDone <- e })
	}
	waitStageCount(t, so, 24, "batch C staged while fsync #2 is held")

	// The snapshot: one file, ending exactly at end(A)+len(B) — no C byte.
	sharedDir := filepath.Join(dir, "wal", "shared")
	files, err := filepath.Glob(filepath.Join(sharedDir, "*.wal"))
	require.NoError(t, err)
	require.Len(t, files, 1, "a single WAL file (no rolls in this test)")
	name := filepath.Base(files[0])
	data, err := os.ReadFile(files[0])
	require.NoError(t, err)
	wantA := serializePipelineRecords(t, 1, 8)
	wantB := serializePipelineRecords(t, 9, 16)
	endA := int64(WALHeaderSize) + int64(len(wantA))
	require.Equal(t, endA+int64(len(wantB)), int64(len(data)),
		"the file must end at end(A)+len(B): batch C is staged but not written (D3)")
	require.True(t, bytesEqual(data[WALHeaderSize:endA], wantA), "A's bytes")
	require.True(t, bytesEqual(data[endA:], wantB), "B's bytes")

	// Cut mid-record inside B: B's first record complete, the second torn
	// (100 bytes into it).
	cut := endA + int64(pipelineRecordLen(t, 9)) + 100

	variants := []struct {
		name     string
		data     []byte
		wantBOff map[uint64]bool
	}{
		{"asis", data, offsets(9, 16)},
		{"torn", data[:cut], map[uint64]bool{9: true}},
		{"tornzeros", append(append([]byte(nil), data[:cut]...), make([]byte, 4096)...), map[uint64]bool{9: true}},
	}
	for _, v := range variants {
		t.Run(v.name, func(t *testing.T) {
			vdir := t.TempDir()
			require.NoError(t, os.MkdirAll(filepath.Join(vdir, "wal", "shared"), 0o755))
			require.NoError(t, os.WriteFile(filepath.Join(vdir, "wal", "shared", name), v.data, 0o644))

			wm2, cerr := NewWALManager(vdir)
			require.NoError(t, cerr, "boot must succeed on the snapshot")
			defer wm2.Close()
			recovered, cerr := wm2.RecoverFromWAL()
			require.NoError(t, cerr, "recovery must not error on the torn tail")

			got := recoveredOffsetSet(t, recovered)
			for off := uint64(1); off <= 8; off++ {
				require.True(t, got[off], "variant %s: A's offset %d must be recovered", v.name, off)
			}
			for off := uint64(9); off <= 16; off++ {
				require.Equal(t, v.wantBOff[off], got[off],
					"variant %s: B's offset %d must match the complete-record prefix of B", v.name, off)
			}
			for off := uint64(17); off <= 24; off++ {
				require.False(t, got[off], "variant %s: no byte of C (offset %d) may be present", v.name, off)
			}
		})
	}

	// Let session 1 finish normally.
	gate.releaseCall(2)
	for _, e := range waitErrors(t, bDone, 8, "batch B completions") {
		require.NoError(t, e)
	}
	for _, e := range waitErrors(t, cDone, 8, "batch C completions") {
		require.NoError(t, e)
	}
}

// pipelineRecordLen returns the serialized length of one 64 KB test record
// for offset off.
func pipelineRecordLen(t *testing.T, off uint64) int {
	t.Helper()
	one, err := appendMessageRecord(nil, "q", pipelineMsg(off), off, false)
	require.NoError(t, err)
	return len(one)
}

// offsets builds the inclusive set first..last.
func offsets(first, last uint64) map[uint64]bool {
	out := map[uint64]bool{}
	for off := first; off <= last; off++ {
		out[off] = true
	}
	return out
}

// ---------------------------------------------------------------------------
// P1-9 (e): Close drains staged and queued work.
// ---------------------------------------------------------------------------

// TestWALPipeline_CloseDrainsStagedAndQueued gates fsync #1 (A), stages B
// (sealed, its hand-off blocked on the busy committer), and enqueues C (8
// records that must sit in writeChan because B is sealed). Close must not
// return while the gate is held; after release every request must complete
// exactly once with nil, Close must return, session 2 must recover
// A ∪ B ∪ C, and a post-close WriteAsync must fail with ErrWALClosed.
func TestWALPipeline_CloseDrainsStagedAndQueued(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultWALConfig()
	cfg.BatchSize = 8

	gate := installGatedFsync(t, map[int]error{1: nil}, nil)
	so := installStageObserver(t, 1)

	wm, err := NewWALManagerWithConfig(dir, cfg)
	require.NoError(t, err)
	defer wm.Close()
	defer gate.releaseAll()
	defer so.releaseBrake()

	// Every completion counted per offset: exactly once, nil, for all 24.
	const total = 24
	var fired []atomic.Int32
	fired = make([]atomic.Int32, total+1)
	var errCount atomic.Int32
	mk := func(off uint64) func(error) {
		return func(e error) {
			if e != nil {
				errCount.Add(1)
			}
			fired[off].Add(1)
		}
	}

	// Batch A.
	for i := 1; i <= 8; i++ {
		wm.WriteAsync("q", pipelineMsg(uint64(i)), uint64(i), mk(uint64(i)))
	}
	waitSignal(t, so.brakePoint(1).engaged, "first staged record of batch A")
	so.releaseBrake()
	waitStageCount(t, so, 8, "batch A (8 records)")
	waitSignal(t, gate.entered[0], "fsync #1 (batch A)")

	// Batch B staged; C must stay in writeChan (B is sealed, the stager is
	// blocked handing it to the busy committer).
	for i := 9; i <= 16; i++ {
		wm.WriteAsync("q", pipelineMsg(uint64(i)), uint64(i), mk(uint64(i)))
	}
	waitStageCount(t, so, 16, "batch B staged while fsync #1 is held")
	for i := 17; i <= 24; i++ {
		wm.WriteAsync("q", pipelineMsg(uint64(i)), uint64(i), mk(uint64(i)))
	}
	require.Len(t, wm.sharedWAL.writeChan, 8,
		"batch C (8 records) must sit in writeChan: B is sealed and the stager is blocked on its hand-off")

	// Close must not return while fsync #1 is held.
	closed := make(chan struct{})
	go func() {
		_ = wm.Close()
		close(closed)
	}()
	select {
	case <-closed:
		t.Fatal("Close returned while fsync #1 was still held")
	case <-time.After(300 * time.Millisecond):
	}

	// Release: Close drains staged B and queued C and then returns.
	gate.releaseCall(1)
	waitSignal(t, closed, "Close to return after the gate is released")

	require.Zero(t, errCount.Load(), "no completion may carry an error")
	for off := 1; off <= total; off++ {
		require.EqualValues(t, 1, fired[off].Load(), "offset %d must complete exactly once", off)
	}

	// Session 2 recovers A ∪ B ∪ C.
	wm2, cerr := NewWALManager(dir)
	require.NoError(t, cerr)
	defer wm2.Close()
	recovered, cerr := wm2.RecoverFromWAL()
	require.NoError(t, cerr)
	require.Equal(t, offsets(1, 24), recoveredOffsetSet(t, recovered),
		"session 2 must recover A ∪ B ∪ C")

	// Post-close writes fail fast with ErrWALClosed.
	postDone := make(chan error, 1)
	wm.WriteAsync("q", pipelineMsg(25), 25, func(e error) { postDone <- e })
	select {
	case e := <-postDone:
		require.ErrorIs(t, e, ErrWALClosed, "post-close WriteAsync must fail with ErrWALClosed")
	case <-time.After(10 * time.Second):
		t.Fatal("post-close WriteAsync completion never fired")
	}
}

// ---------------------------------------------------------------------------
// P1-10: the shared-unit locator patch equals direct serialization.
// ---------------------------------------------------------------------------

// TestSharedUnit_LocatorPatchEqualsDirectSerialize pins the two pieces of
// shared-unit staging math: bodyRefLocatorOffset (where the 8 locator bytes
// sit inside a reference record) and the committer's patch (write the
// absolute block position over the staged relative one, then recompute the
// record CRC over its own [length][data] span). For several queue/exchange/
// routing-key/property shapes, a ref record staged with a relative locator
// and then patched must be byte-identical to the same record serialized
// directly with the final locator.
func TestSharedUnit_LocatorPatchEqualsDirectSerialize(t *testing.T) {
	const finalLoc = uint64(0x1122334455667788)
	const stagedRel = 777

	shapes := []struct {
		queue string
		msg   *protocol.Message
	}{
		{"q", &protocol.Message{Exchange: "", RoutingKey: "rk", Body: []byte("shared"), DeliveryMode: 2}},
		{"queue-with-a-longer-name", &protocol.Message{Exchange: "amq.direct", RoutingKey: "a.b.c", Body: []byte("shared"), DeliveryMode: 2, ContentType: "application/json", Priority: 3}},
		// Headers is a SINGLE-entry map on purpose: the field-table encoder
		// iterates the map unordered (protocol/methods.go serializeFieldTable),
		// so a multi-key table makes the two serializations' bytes legitimately
		// differ. One entry still exercises the field-table arm that follows
		// the locator (the CRC span the patch recomputes).
		{"q", &protocol.Message{Exchange: "ex", RoutingKey: "", Body: []byte("shared"), DeliveryMode: 1, Headers: map[string]interface{}{"x": int32(7)}}},
		{"q", &protocol.Message{Exchange: "ex", RoutingKey: "rk", Body: []byte("shared"), DeliveryMode: 2, Timestamp: 1730000000000, Expiration: "60000", MessageID: "m-1", AppID: "app"}},
		{"q", &protocol.Message{Exchange: "e", RoutingKey: "r", Body: []byte("shared"), DeliveryMode: 2, ContentType: "text/plain", ContentEncoding: "gzip", CorrelationID: "c", ReplyTo: "reply", Type: "T", UserID: "u", ClusterID: "cl"}},
	}

	for _, shape := range shapes {
		// Direct: serialize with the FINAL locator already in place.
		directMsg := *shape.msg
		var loc [8]byte
		binary.BigEndian.PutUint64(loc[:], finalLoc)
		directMsg.BodyRef = loc[:]
		direct, err := appendMessageRecord(nil, shape.queue, &directMsg, 42, false)
		require.NoError(t, err)

		// Staged: serialize with the RELATIVE block position, then patch.
		stagedMsg := *shape.msg
		var rel [8]byte
		binary.BigEndian.PutUint64(rel[:], stagedRel)
		stagedMsg.BodyRef = rel[:]
		staged, err := appendMessageRecord(nil, shape.queue, &stagedMsg, 42, false)
		require.NoError(t, err)

		locAt := bodyRefLocatorOffset(shape.queue, &stagedMsg)
		require.Greater(t, locAt, 0,
			"bodyRefLocatorOffset must return the locator's offset from the record start")
		require.LessOrEqual(t, locAt+8, len(staged), "the locator must sit fully inside the record")

		binary.BigEndian.PutUint64(staged[locAt:locAt+8], finalLoc)
		crc := crc32.ChecksumIEEE(staged[4:])
		binary.BigEndian.PutUint32(staged[0:4], crc)

		assert.True(t, bytesEqual(direct, staged),
			"queue=%q exch=%q rk=%q: patched staged record must equal the direct serialization",
			shape.queue, shape.msg.Exchange, shape.msg.RoutingKey)
	}
}

// ---------------------------------------------------------------------------
// Memory retention: stagedBatch.reset() clears pointer-bearing slices so the
// backing array's old message bodies, callbacks, subs, and errors are
// unreachable across bursts.
// ---------------------------------------------------------------------------

// TestWALPipeline_StagedBatchResetClearsReferences verifies that reset() does
// not retain message bodies or callbacks via the reqs and serErr backing
// arrays. It drives writes through the production add/reset path directly (not
// through a WAL manager) so there is no concurrent goroutine.
func TestWALPipeline_StagedBatchResetClearsReferences(t *testing.T) {
	body := sharedTestBody(4096)
	msg := &protocol.Message{Exchange: "ex", RoutingKey: "rk", Body: body, DeliveryMode: 2}

	sb := &stagedBatch{
		reqs:   make([]*writeRequest, 0, 8),
		buf:    make([]byte, 0, 4096),
		relPos: make([]int64, 0, 8),
	}

	for i := 0; i < 4; i++ {
		req := &writeRequest{queueName: "q", message: msg, offset: uint64(i + 1), onDone: func(err error) {}}
		sb.add(req, false)
	}
	require.Len(t, sb.reqs, 4, "reqs must have 4 entries before reset")
	require.Nil(t, sb.serErr, "serErr must be nil on success path")

	sb.reset()

	require.Lenf(t, sb.reqs, 0, "reqs must be empty after reset")
	require.Nil(t, sb.serErr, "serErr must be nil after reset")

	// Every backing slot must be nil (cleared by clear(sb.reqs)).
	if cap(sb.reqs) > 0 {
		backing := sb.reqs[:cap(sb.reqs):cap(sb.reqs)]
		for j := 0; j < cap(sb.reqs); j++ {
			require.Nilf(t, backing[j],
				"reqs backing slot %d must be nil after reset (cleared)", j)
		}
	}

	// After reset, add works.
	req := &writeRequest{queueName: "q", message: msg, offset: 42}
	sb.add(req, false)
	require.Len(t, sb.reqs, 1, "add must work after reset")
}

// ---------------------------------------------------------------------------
// P1-11: batches are sealed at walBatchSealBytes; the two buffers are
// retained, not reallocated. Uses non-rooting handoffSnapshot metadata.
// ---------------------------------------------------------------------------

// TestWALPipeline_BatchBytesSealedAndBufferRetained shrinks walBatchSealBytes
// to 1 MiB (restored in Cleanup), enqueues 64x64 KB (four sealed batches of
// ~1 MiB), and asserts every write is within the seal bound (+ one record of
// slack), that BOTH staged buffers exist with capacity within
// maxRetainedWALBatchBytes, and that their base addresses stay stable over 10
// further equal batches.
//
// Uses the stage brake/observer so that each retention round's batch
// membership is deterministic. Verifies the handoff generation advances
// every round.
//
// On the pre-pipeline writer this fails twice: one ~4 MiB write (no byte
// seal) and no retained staged buffers to observe.
func TestWALPipeline_BatchBytesSealedAndBufferRetained(t *testing.T) {
	prevSeal := walBatchSealBytes
	walBatchSealBytes = 1 << 20
	t.Cleanup(func() { walBatchSealBytes = prevSeal })

	dir := t.TempDir()
	cfg := DefaultWALConfig() // BatchSize 1000: the BYTE seal is the binding bound

	rec := installWalWriteRecorder(t, dir)
	gate := installGatedFsync(t, map[int]error{1: nil}, rec.count.Load)

	// Brake at the first staged record of batch 1, so its membership is
	// deterministic: the test finishes enqueuing all 64 records before the
	// coalesce drain starts.
	so := installStageObserver(t, 1)

	wm, err := NewWALManagerWithConfig(dir, cfg)
	require.NoError(t, err)
	defer wm.Close()
	defer gate.releaseAll()
	defer so.releaseBrake()

	// 64 x 64 KB in one burst; the stager seals at ~1 MiB (16 records).
	fired := make(chan error, 64)
	for i := 1; i <= 64; i++ {
		off := uint64(i)
		wm.WriteAsync("q", pipelineMsg(off), off, func(e error) { fired <- e })
	}
	waitSignal(t, so.brakePoint(1).engaged, "first staged record of batch 1")
	so.releaseBrake()
	waitSignal(t, gate.entered[0], "fsync #1 (first sealed batch)")
	gate.releaseCall(1)
	for _, e := range waitErrors(t, fired, 64, "initial drain completions") {
		require.NoError(t, e)
	}

	// Every write must respect the seal: 1 MiB + one record of slack.
	bound := 1<<20 + 70<<10
	for k, n := range rec.lens() {
		require.LessOrEqual(t, n, bound, "walWrite #%d wrote %d bytes, over the 1 MiB seal bound", k+1, n)
	}

	// Both staged buffers exist, retained within maxRetainedWALBatchBytes.
	snaps := wm.sharedWAL.pipelineBufsForTest()
	require.Len(t, snaps, 2, "the pipeline must own exactly two staged batch buffers")
	for i, s := range snaps {
		require.NotZero(t, s.HandoffBase, "staged buffer %d must have a nonzero base address", i)
		require.Greater(t, s.HandoffLen, 0, "staged buffer %d must have a non-empty last hand-off snapshot", i)
		require.LessOrEqual(t, s.HandoffCap, maxRetainedWALBatchBytes,
			"staged buffer %d capacity %d exceeds maxRetainedWALBatchBytes", i, s.HandoffCap)
		require.Greater(t, s.HandoffGen, int64(0), "staged buffer %d must have a positive generation", i)
	}

	// Record initial buffer identities for reuse verification.
	type bufID struct {
		base uintptr
		cap  int
		gen  int64
	}
	ids := make([]bufID, len(snaps))
	for i, s := range snaps {
		ids[i] = bufID{base: s.HandoffBase, cap: s.HandoffCap, gen: s.HandoffGen}
	}

	// 10 further equal batches: the buffers must be REUSED (same base, same
	// cap) and the generation must advance every round.
	for round := 0; round < 10; round++ {
		next := make(chan error, 16)
		first := uint64(65 + round*16)
		for i := 0; i < 16; i++ {
			off := first + uint64(i)
			wm.WriteAsync("q", pipelineMsg(off), off, func(e error) { next <- e })
		}
		for _, e := range waitErrors(t, next, 16, "equal-batch completions") {
			require.NoError(t, e)
		}

		snaps2 := wm.sharedWAL.pipelineBufsForTest()
		require.Len(t, snaps2, 2)
		for i, s := range snaps2 {
			require.NotZero(t, s.HandoffBase, "buffer %d base must be nonzero in round %d", i, round)
			require.Greater(t, s.HandoffGen, ids[i].gen,
				"buffer %d generation must advance in round %d: %d ≤ %d", i, round, s.HandoffGen, ids[i].gen)
		}
	}

	// Final check: base addresses and capacities must be stable (reused).
	snaps2 := wm.sharedWAL.pipelineBufsForTest()
	require.Len(t, snaps2, 2)
	got := map[uintptr]int{}
	for _, s := range snaps2 {
		got[s.HandoffBase] = s.HandoffCap
	}
	for _, id := range ids {
		c, ok := got[id.base]
		require.True(t, ok, "staged buffer base 0x%x must be stable across 10 further equal batches", id.base)
		require.Equal(t, id.cap, c, "staged buffer base 0x%x must keep its capacity", id.base)
	}
}

// TestWALPipeline_OversizedBufferRelease verifies that reset() releases a buf
// whose capacity exceeds maxRetainedWALBatchBytes, and that the staged batch
// stays usable afterwards.
func TestWALPipeline_OversizedBufferRelease(t *testing.T) {
	// Direct test: simulate a buf with cap just over maxRetainedWALBatchBytes.
	overSizedCap := maxRetainedWALBatchBytes + 1
	sb := &stagedBatch{
		reqs:   make([]*writeRequest, 0, 8),
		buf:    make([]byte, 1024, overSizedCap),
		relPos: make([]int64, 0, 8),
	}
	sb.reset()
	require.Nil(t, sb.buf, "buf must be nil after reset when cap > maxRetainedWALBatchBytes")

	// The object is still usable: add records.
	body := sharedTestBody(4096)
	msg := &protocol.Message{Exchange: "ex", RoutingKey: "rk", Body: body, DeliveryMode: 2}
	for i := 0; i < 4; i++ {
		req := &writeRequest{queueName: "q", message: msg, offset: uint64(i + 1)}
		sb.add(req, false)
	}
	require.Len(t, sb.reqs, 4, "add must work after reset on a released buf")
	require.Greater(t, len(sb.buf), 0, "buf grows as records are added")
}
