package broker

import (
	"testing"
	"unsafe"
)

// TestQueueStateLayoutUnchanged pins the offsets of QueueState's dispatch-plane
// hot fields, following the precedent in protocol/consumer_layout_test.go and
// storage/segment_layout_test.go: when a hot struct is edited, pin its layout
// so a later insertion cannot silently move a hot field without a test going
// red.
//
// This is the falsifiable form of spec-b4's zero-regression claim: TagBand
// (broker/queue_dispatch.go) is a METHOD, not a field — it reads ordinalBase
// (already present) and derives its upper bound from the existing SeqMask
// constant. The purge-incarnation fix therefore adds NO field to QueueState.
// Values measured on this machine (darwin/arm64) and re-confirmed byte-for-byte
// against clean 7a09e63 with the same toolchain, so they pin the struct rather
// than the compiler. Deliberately no Go version is quoted: the pin was checked
// on go1.25.1 and go1.26.0 with identical results, and a stale version here
// sends the next reader chasing a toolchain difference that does not exist.
func TestQueueStateLayoutUnchanged(t *testing.T) {
	var qs QueueState

	for _, tc := range []struct {
		name string
		got  uintptr
		want uintptr
	}{
		{"ordinalBase", unsafe.Offsetof(qs.ordinalBase), 0},
		{"nextSeq", unsafe.Offsetof(qs.nextSeq), 8},
		{"tail", unsafe.Offsetof(qs.tail), 16},
		{"head", unsafe.Offsetof(qs.head), 24},
		{"waiting", unsafe.Offsetof(qs.waiting), 32},
		{"inflight", unsafe.Offsetof(qs.inflight), 40},
		{"closed", unsafe.Offsetof(qs.closed), 136},
	} {
		if tc.got != tc.want {
			t.Errorf("QueueState.%s offset = %d, want %d — a field was inserted or reordered "+
				"above a dispatch-plane hot field; re-derive this pin before changing it", tc.name, tc.got, tc.want)
		}
	}

	// WS-D §7 appended the reject-publish reservation fields (pendingCount,
	// pendingBytes, frontierResv) AFTER frontierMax, so every hot-field offset
	// above is unchanged and only the struct size grew (264 → measured).
	//
	// Task 2 appended the diagnostic maxBacklogSeen atomic AFTER
	// frontierResv for the same reason: a pure end-of-struct append keeps every
	// pinned hot-field offset above unchanged and only grows the size again
	// (288 → 296). Neither append reordered a hot field; the offset pins above
	// are therefore still the load-bearing assertion, and only the size moved.
	//
	// The hot-path work stream REMOVED the dead minAckCursor field (nothing
	// read it: Depth() is waiting+inflight). That shifts every field after it
	// down by one uint64 — waiting 40 → 32, inflight 48 → 40, closed 144 → 136
	// — and shrinks the struct 296 → 288. Re-derived here.
	const wantSize = 288
	if got := unsafe.Sizeof(qs); got != wantSize {
		t.Errorf("unsafe.Sizeof(QueueState) = %d, want %d — the Task 2 maxBacklogSeen "+
			"field is appended at the END of the struct; hot-field offsets are unchanged, "+
			"so re-derive this pin only if you changed the struct", got, wantSize)
	}
}
