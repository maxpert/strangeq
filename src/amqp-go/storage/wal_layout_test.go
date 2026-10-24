package storage

// Hot-path field layout of QueueWAL, locked at runtime.
//
// WHY THIS EXISTS. The committer's group-commit critical section (commitBatch;
// flushBatch before WS-F phase 1) touches
// currentFile, fileNum, fileOffset and fileMutex on every durable write, and
// fileMutex began a fresh 64-byte cache line in the tree the zero-regression
// baseline was measured on. A cold-path change (deleting an unreferenced field
// declared ABOVE them) silently shifted all four by -8 bytes and pulled
// fileMutex into the same line as the two atomics it sits beside — a hot-path
// layout change produced by an edit that looked, hunk by hunk, entirely cold.
// The claim that such an edit is "layout-neutral by construction" is exactly
// the kind of load-bearing premise this program keeps being burned by, so it is
// asserted here instead of asserted in a comment (canon rule 11).
//
// WHAT A FAILURE MEANS. Not "you broke the broker". It means a field above the
// hot group was added, removed or resized, so the layout no longer matches the
// one the performance baseline was taken on. Either restore it (adjust
// _walLayoutPad) or run a single-hunk A/B over BenchmarkSharedWAL_Acknowledge
// and the WAL write benchmarks and record the numbers — a whole-tree A/B
// measures trees, not hunks.

import (
	"testing"
	"unsafe"
)

// walHotFieldOffsets is the layout this package's performance baseline was
// measured on (commit 60b2d21, 64-bit, sync.Mutex == 8 bytes).
var walHotFieldOffsets = []struct {
	name string
	want uintptr
}{
	{"currentFile", 160},
	{"currentReadFile", 168},
	{"fileNum", 176},
	{"fileOffset", 184},
	{"fileMutex", 192},
	{"currentFileOffsets", 200},
}

func TestQueueWALHotFieldLayoutIsPinned(t *testing.T) {
	var q QueueWAL

	if unsafe.Sizeof(uintptr(0)) != 8 || unsafe.Sizeof(q.fileMutex) != 8 {
		t.Skipf("layout table is stated for 64-bit with an 8-byte sync.Mutex; this build has "+
			"pointer=%d mutex=%d", unsafe.Sizeof(uintptr(0)), unsafe.Sizeof(q.fileMutex))
	}

	got := map[string]uintptr{
		"currentFile":        unsafe.Offsetof(q.currentFile),
		"currentReadFile":    unsafe.Offsetof(q.currentReadFile),
		"fileNum":            unsafe.Offsetof(q.fileNum),
		"fileOffset":         unsafe.Offsetof(q.fileOffset),
		"fileMutex":          unsafe.Offsetof(q.fileMutex),
		"currentFileOffsets": unsafe.Offsetof(q.currentFileOffsets),
	}

	for _, f := range walHotFieldOffsets {
		if got[f.name] != f.want {
			t.Errorf("QueueWAL.%s is at byte offset %d, the performance baseline was measured "+
				"with it at %d. A field above the group-commit hot group changed size or "+
				"existence. Restore the layout via _walLayoutPad, or discharge canon rule 2 "+
				"with a SINGLE-HUNK A/B and record the numbers. Full observed layout: %+v",
				f.name, got[f.name], f.want, got)
		}
	}

	// The property the numbers encode, stated independently of them: the
	// group-commit lock starts its own cache line, so taking it does not pull in
	// the line holding fileNum/fileOffset, which are atomically stored on the
	// same path.
	if got["fileMutex"]%64 != 0 {
		t.Errorf("QueueWAL.fileMutex is at offset %d, which does not start a 64-byte cache "+
			"line; it now shares one with the atomics the committer stores on the same path",
			got["fileMutex"])
	}

	// WS-F phase 1: the pipelined group-commit fields (commitCh, free and the
	// pipeline registry below them) are appended AFTER logger — the last field
	// of the pre-pipeline struct — and NEVER above txEpoch, so the hot-group
	// offsets pinned above are untouched. Asserting it here means a future
	// edit cannot silently move them up past the cold-path boundary the
	// layout table encodes.
	if co, te := unsafe.Offsetof(q.commitCh), unsafe.Offsetof(q.txEpoch); co <= te {
		t.Errorf("QueueWAL.commitCh is at offset %d, at or above txEpoch (%d); the WS-F "+
			"pipeline fields must stay appended after logger, never above txEpoch", co, te)
	}
	if fo, te := unsafe.Offsetof(q.free), unsafe.Offsetof(q.txEpoch); fo <= te {
		t.Errorf("QueueWAL.free is at offset %d, at or above txEpoch (%d); the WS-F "+
			"pipeline fields must stay appended after logger, never above txEpoch", fo, te)
	}
}
