package storage

// Commit 1's fixtures — the old-file handle cache stops handing its lock to its
// callers, and the platform guarantee that makes that safe is asserted rather
// than documented.
//
// EVERY FIXTURE IN THIS FILE NEEDS A ROLLED WAL FILE. getOldFileHandle is
// reached only from the `else` arm of `location.fileNum == currentFileNum`; the
// current file is served from currentReadFile and takes no cache lock at all. At
// DefaultWALFileSize (512 MB) nothing here rolls and every one of these fixtures
// would pass while executing none of the code it names, which is the exact
// vacuous-premise shape that let a data-loss defect certify green three times.
// newRolledWAL overrides FileSize and ASSERTS the roll.

import (
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/maxpert/amqp-go/protocol"
)

const (
	oldHandleQueue = "old.handle.q"
	// Small enough that a couple of hundred half-kilobyte records roll the file
	// many times. rollFile is the only producer of qw.oldFiles in a single-boot
	// fixture.
	oldHandleWALFileSize = 4 * 1024
	oldHandleRecords     = 200
	oldHandleBodySize    = 256
)

// rolledWAL is the arranged state every fixture in this file shares: a
// WALManager whose WAL has rolled, and one record that lives in a ROLLED file,
// located through the same offsetIndex the production read path uses.
type rolledWAL struct {
	wm       *WALManager
	sw       *QueueWAL
	fileNum  uint64
	path     string
	offset   uint64
	position int64

	// skipClose suppresses the deferred WALManager.Close. It exists for exactly
	// one situation and it is cleanup, not assertion: if a fixture observes that
	// oldFileCacheMutex is still held when it should not be, Close cannot
	// return — qw.close() takes that same mutex — and because Close holds wm.mu
	// while blocking, every later WALManager call in this test binary blocks
	// with it (open-items item 10). Leaking one WAL's goroutines on a tree that
	// has already failed is strictly better than wedging every fixture after it.
	skipClose atomic.Bool
}

func newRolledWAL(t *testing.T) *rolledWAL {
	t.Helper()

	cfg := DefaultWALConfig()
	cfg.FileSize = oldHandleWALFileSize
	// These fixtures are single-goroutine and deterministic by construction.
	// Parking the background loops keeps a reclaim or a checkpoint from moving
	// the file out from under an assertion and turning a real red into a
	// scheduling story.
	cfg.CleanupInterval = time.Hour
	cfg.CheckpointInterval = time.Hour
	cfg.RetentionPeriod = 0

	r := &rolledWAL{}
	wm, err := NewWALManagerWithConfig(t.TempDir(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		if r.skipClose.Load() {
			t.Log("SKIPPING WALManager.Close(): oldFileCacheMutex is still held, so Close would " +
				"block forever inside qw.close() while holding wm.mu")
			return
		}
		_ = wm.Close()
	})

	body := make([]byte, oldHandleBodySize)
	for i := range body {
		body[i] = byte('a' + i%26)
	}
	base := uint64(1) << 44
	for i := 0; i < oldHandleRecords; i++ {
		require.NoError(t, wm.Write(oldHandleQueue, &protocol.Message{
			RoutingKey:   oldHandleQueue,
			Body:         body,
			DeliveryMode: 2,
			DeliveryTag:  base + uint64(i),
			MessageID:    "oh-" + string(rune('a'+i%26)),
		}, base+uint64(i)))
	}

	sw := wm.sharedWAL
	require.NotNil(t, sw, "PREMISE BROKEN: no shared WAL was constructed")

	sw.oldFilesMutex.RLock()
	rolled := len(sw.oldFiles)
	var fileNum, offset uint64
	var path string
	var found bool
	for fn, info := range sw.oldFiles {
		if info.offsets == nil {
			continue
		}
		it := info.offsets.Iterator()
		if it.HasNext() {
			fileNum, path, offset, found = fn, info.path, it.Next(), true
			break
		}
	}
	sw.oldFilesMutex.RUnlock()

	require.NotZero(t, rolled,
		"PREMISE BROKEN: the WAL never rolled at FileSize=%d after %d records of %d bytes, so "+
			"qw.oldFiles is empty and getOldFileHandle is unreachable. Every fixture in this file "+
			"would pass without executing the code it names", oldHandleWALFileSize, oldHandleRecords, oldHandleBodySize)
	require.True(t, found,
		"PREMISE BROKEN: %d rolled file(s) but none carries a known offset, so there is no record "+
			"to read back through the old-file path", rolled)

	sw.offsetIndexMutex.RLock()
	loc, ok := sw.offsetIndex[recordKey{queueName: oldHandleQueue, deliveryTag: offset}]
	sw.offsetIndexMutex.RUnlock()
	require.True(t, ok,
		"PREMISE BROKEN: offset %d has no offsetIndex entry, so the production read path could not "+
			"reach it either", offset)
	require.Equal(t, fileNum, loc.pos.segmentID,
		"PREMISE BROKEN: the offsetIndex places offset %d in file %d, not the rolled file %d it was "+
			"selected from", offset, loc.pos.segmentID, fileNum)

	r.wm, r.sw, r.fileNum, r.path, r.offset, r.position = wm, sw, fileNum, path, offset, loc.pos.position
	return r
}

// TestGetOldFileHandle_ReleasesTheCacheLockBeforeReturning is Commit 1's gate.
//
// CLASSIFICATION: SAFETY. It asserts a state that must NEVER hold — the caller
// still holding oldFileCacheMutex after getOldFileHandle returns — and observes
// it directly instead of racing for its consequence. There is no clock in it,
// no goroutine, and nothing to tune.
//
// MUTATION THAT REDDENS IT: delete the `qw.oldFileCacheMutex.RUnlock()` from
// getOldFileHandle's cache-HIT path so the hit returns with the read lock still
// held. Fires at the TryLock assertion.
func TestGetOldFileHandle_ReleasesTheCacheLockBeforeReturning(t *testing.T) {
	r := newRolledWAL(t)

	// Prime, so the call under test takes the cache-HIT path. The MISS path
	// returns holding nothing whatever the signature is, so a fixture that
	// silently took it would assert a property that holds trivially.
	first, err := r.sw.getOldFileHandle(r.fileNum)
	require.NoError(t, err)
	require.NotNil(t, first)

	// THE PREMISE, ASSERTED BETWEEN THE PRIME AND THE ACT. Asserted only before
	// the prime it could not fail: a getOldFileHandle that started erroring out
	// before it ever touched the cache would leave a stale "cache non-empty"
	// reading standing and this fixture would pass having exercised nothing.
	r.sw.oldFileCacheMutex.RLock()
	_, present := r.sw.oldFileCache[r.fileNum]
	r.sw.oldFileCacheMutex.RUnlock()
	require.True(t, present,
		"PREMISE BROKEN: file %d is not in oldFileCache after a successful getOldFileHandle, so "+
			"the call below takes the MISS path and asserts nothing", r.fileNum)

	second, err := r.sw.getOldFileHandle(r.fileNum)
	require.NoError(t, err)
	require.Same(t, first, second,
		"PREMISE BROKEN: the second getOldFileHandle returned a different handle, so it did not "+
			"take the cache-HIT path — the only path that ever held a lock at return")

	// Capture the verdict and release BEFORE asserting. A require that aborts
	// while holding this mutex would wedge the deferred WALManager.Close, and
	// per open-items item 10 every later WALManager call in this test binary
	// with it — turning one honest red into a wedged package.
	free := r.sw.oldFileCacheMutex.TryLock()
	if free {
		r.sw.oldFileCacheMutex.Unlock()
	} else {
		// The lock is held, which is the failure this fixture exists to name.
		// Nothing can hand it back — the whole point of the signature is that
		// there is no unlock to call — so the WAL is abandoned rather than
		// closed. See rolledWAL.skipClose.
		r.skipClose.Store(true)
	}

	require.True(t, free,
		"getOldFileHandle returned while STILL HOLDING oldFileCacheMutex. The caller then holds "+
			"that lock across its ReadAt and across anything else it does with the handle — which "+
			"is edge 4 of the F1 cycle: readMessageBatch takes bitmapMutex.RLock inside that scope "+
			"while tryReclaimOldFiles holds bitmapMutex.RLock and waits for oldFileCacheMutex.Lock")
}

// TestClosedOldFileHandle_ReadAtYieldsTypedErrClosed asserts the PLATFORM
// GUARANTEE that Commit 1's safety argument rests on, which was otherwise held
// by no test at all — only by a reading of the Go runtime's source.
//
// Once getOldFileHandle stops handing its lock to the caller, closeOldFileHandle
// may close the handle between the lookup and the caller's ReadAt. That is safe
// only because os.File.ReadAt -> poll.FD.Pread takes an incref that fails once
// Close has marked the descriptor closed: the read returns a TYPED os.ErrClosed
// without ever reaching Sysfd, so it can neither read through a recycled
// descriptor nor race Close's deferred destroy().
//
// CLASSIFICATION: SAFETY, and it is a premise assertion rather than a regression
// gate — it is green on both sides of Commit 1 by design. What it exists to
// catch is a TOOLCHAIN change that invalidates the commit, which nothing else in
// this suite would notice.
//
// MUTATION THAT REDDENS IT: remove the `file.Close()` from closeOldFileHandle.
// The handle stays live, ReadAt succeeds, and require.Error fires.
func TestClosedOldFileHandle_ReadAtYieldsTypedErrClosed(t *testing.T) {
	r := newRolledWAL(t)

	f, err := r.sw.getOldFileHandle(r.fileNum)
	require.NoError(t, err)

	// PREMISE: the handle reads BEFORE the close. Without it, "the read failed"
	// afterwards is not attributable to the close.
	buf := make([]byte, 8)
	_, rerr := f.ReadAt(buf, 0)
	require.NoError(t, rerr,
		"PREMISE BROKEN: the cached handle for file %d could not be read even before it was "+
			"closed, so the assertion below would fire for the wrong reason", r.fileNum)

	r.sw.closeOldFileHandle(r.fileNum)

	_, rerr = f.ReadAt(buf, 0)
	require.Error(t, rerr,
		"READING A CLOSED os.File SUCCEEDED. Commit 1 lets closeOldFileHandle close a handle a "+
			"reader is about to use; that is safe only because the read is refused. A read that "+
			"succeeds here is a read through a descriptor the runtime considers closed")
	require.True(t, errors.Is(rerr, os.ErrClosed),
		"reading a closed os.File returned %T (%v), not os.ErrClosed. Commit 1's retry keys on "+
			"errors.Is(err, os.ErrClosed); an untyped or different error means the retry never "+
			"fires and the failure is reported to the caller as a read fault", rerr, rerr)
}

// TestReadMessageAtPosition_RecoversFromAClosedCachedHandle covers the bounded
// os.ErrClosed retry — the branch that would otherwise ship unattributed.
//
// It needs no injection seam in production code. The arranged state is written
// straight into the package-private cache: a handle that is already closed while
// still installed. That is the state a reader observes in production when
// closeOldFileHandle runs between getOldFileHandle returning and the ReadAt,
// which is newly possible precisely because the cache lock is no longer held
// across the read.
//
// CLASSIFICATION: SAFETY on the recovery, with the re-open asserted as a
// distinct observable so "it happened to work" cannot pass for "it retried".
//
// MUTATION THAT REDDENS IT: set oldFileReadAttempts to 1 (or delete the retry in
// withOldFileHandle). readMessageAtPosition returns os.ErrClosed and the
// require.NoError fires.
func TestReadMessageAtPosition_RecoversFromAClosedCachedHandle(t *testing.T) {
	r := newRolledWAL(t)
	stale := injectClosedHandle(t, r)

	msg, err := r.sw.readMessageAtPosition(oldHandleQueue, r.fileNum, r.position, r.offset)
	require.NoError(t, err,
		"readMessageAtPosition did not recover from a cached handle that was closed under it. "+
			"With the cache lock no longer held across the read this is a reachable state, and "+
			"failing here surfaces a transient close as a read fault to the caller")
	require.NotNil(t, msg)
	require.Equal(t, r.offset, msg.DeliveryTag)

	assertHandleWasReopened(t, r, stale)
}

// TestReadMessageBatch_RecoversFromAClosedCachedHandle is the same arm for the
// read-ahead path. readMessageBatch is the OTHER caller of getOldFileHandle and
// the one at the centre of the F1 cycle, so it owes its own arm: covering only
// readMessageAtPosition would leave the retry unattributed exactly where it
// matters most.
//
// MUTATION THAT REDDENS IT: identical to the arm above, and it fires at this
// fixture's own require.NoError — two call sites, two assertions, so deleting
// the retry from one of them cannot hide behind the other.
func TestReadMessageBatch_RecoversFromAClosedCachedHandle(t *testing.T) {
	r := newRolledWAL(t)
	stale := injectClosedHandle(t, r)

	batch, err := r.sw.readMessageBatch(oldHandleQueue, r.offset)
	require.NoError(t, err,
		"readMessageBatch did not recover from a cached handle that was closed under it")
	require.NotEmpty(t, batch)
	require.Contains(t, batch, r.offset)

	assertHandleWasReopened(t, r, stale)
}

// injectClosedHandle installs an already-closed handle for r.fileNum in the
// old-file cache and returns it. It asserts the installation, so a fixture whose
// injection silently failed reports that rather than passing on the ordinary
// path.
func injectClosedHandle(t *testing.T, r *rolledWAL) *os.File {
	t.Helper()

	primed, err := r.sw.getOldFileHandle(r.fileNum)
	require.NoError(t, err)

	stale, err := os.Open(r.path)
	require.NoError(t, err)
	require.NoError(t, stale.Close())

	r.sw.oldFileCacheMutex.Lock()
	r.sw.oldFileCache[r.fileNum] = stale
	r.sw.oldFileCacheMutex.Unlock()
	// The primed handle is no longer reachable through the cache; close it here
	// rather than leaving a descriptor for WALManager.Close to not find.
	_ = primed.Close()

	// PREMISE, asserted between the injection and the act: the cache really is
	// serving the closed handle. Without this the fixture cannot tell a retry
	// that worked from an injection that never landed.
	r.sw.oldFileCacheMutex.RLock()
	installed := r.sw.oldFileCache[r.fileNum]
	r.sw.oldFileCacheMutex.RUnlock()
	require.Same(t, stale, installed,
		"PREMISE BROKEN: the closed handle is not the one the cache would serve for file %d, so "+
			"the read below never meets a closed descriptor", r.fileNum)

	return stale
}

// assertHandleWasReopened is the discriminator for both retry fixtures: a read
// that succeeded is not by itself evidence the retry ran, but a cache entry that
// is no longer the closed handle is.
func assertHandleWasReopened(t *testing.T, r *rolledWAL, stale *os.File) {
	t.Helper()

	r.sw.oldFileCacheMutex.RLock()
	current, present := r.sw.oldFileCache[r.fileNum]
	r.sw.oldFileCacheMutex.RUnlock()
	require.True(t, present,
		"the retry dropped the stale handle for file %d and never re-cached a replacement, so "+
			"every later read of that file pays a fresh open", r.fileNum)
	require.NotSame(t, stale, current,
		"the read succeeded while the CLOSED handle is still the cache's entry for file %d. That "+
			"cannot be the retry path — it means the read never went through the cache, and this "+
			"fixture is not covering the branch it claims", r.fileNum)
}
