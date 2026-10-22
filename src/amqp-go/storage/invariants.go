package storage

import (
	"fmt"
	"sync/atomic"
)

// WS-C invariant I3, enforced at runtime under test.
//
// *Whenever fileMutex is released and the WAL is not poisoned,
// qw.fileOffset == Stat(currentFile).Size()* — and for segments, *whenever
// qs.mutex is released and the queue is not segment-poisoned,
// currentSegment.fileSize == Stat(currentSegment.file).Size()*.
//
// The check is OFF in production (one atomic load per flush is the entire
// cost, but a Stat per flush is not free and the invariant is structural);
// the storage package's TestMain enables it package-wide so every test in
// this package runs with it armed. A test that deliberately mutates a LIVE
// active file must turn checks off locally with
// SetStorageInvariantChecksForTest(false) and a comment.

// storageInvariantChecks arms the runtime offset/size invariant assertions.
// Off by default.
var storageInvariantChecks atomic.Bool

// SetStorageInvariantChecksForTest arms or disarms the storage invariant
// assertions and returns a restore function. TEST-ONLY.
func SetStorageInvariantChecksForTest(on bool) (restore func()) {
	prev := storageInvariantChecks.Load()
	storageInvariantChecks.Store(on)
	return func() { storageInvariantChecks.Store(prev) }
}

// assertOffsetInvariantLocked verifies WAL invariant I3. Precondition: caller
// holds qw.fileMutex. Skipped while poisoned (the diverged file is never
// written again, so the invariant is intentionally suspended for it) and when
// no current file exists (construction/shutdown).
func (qw *QueueWAL) assertOffsetInvariantLocked() {
	if !storageInvariantChecks.Load() || qw.poisoned != nil || qw.currentFile == nil {
		return
	}
	st, err := qw.currentFile.Stat()
	if err != nil || st.Size() != qw.fileOffset.Load() {
		panic(fmt.Sprintf("WAL invariant I3 violated: fileOffset=%d stat=%d statErr=%v file=%s",
			qw.fileOffset.Load(), st.Size(), err, qw.currentFile.Name()))
	}
}

// assertSegmentSizeInvariantLocked verifies the segment twin of I3.
// Precondition: caller holds qs.mutex. Skipped while segment-poisoned.
func (qs *QueueSegments) assertSegmentSizeInvariantLocked() {
	if !storageInvariantChecks.Load() || qs.segPoisoned != nil || qs.currentSegment == nil {
		return
	}
	st, err := qs.currentSegment.file.Stat()
	if err != nil || st.Size() != qs.currentSegment.fileSize.Load() {
		panic(fmt.Sprintf("segment invariant I3 violated: fileSize=%d stat=%d statErr=%v file=%s",
			qs.currentSegment.fileSize.Load(), st.Size(), err, qs.currentSegment.path))
	}
}
