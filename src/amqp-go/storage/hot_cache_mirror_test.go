package storage

import (
	"testing"
	"unsafe"
)

// hotCacheMirror returns the observable mirror of the cache's per-queue byte
// accounting as a map: every queue whose resident bytes are non-zero.
//
// It reads hotCacheQueue.usedMirror — the OBSERVABLE projection, an atomic —
// NOT the authoritative hotCacheQueue.used. That is what makes the accounting
// tests meaningful: they compare the mirror the pace gate and the management
// surface actually read against the authoritative per-queue counter, so a leak
// or a stale mirror shows up as a mismatch. (Reading `used` here would compare
// the authoritative value with itself and assert nothing.)
//
// It is a snapshot, so it must only be called when no other goroutine is
// writing the queues, like the tests that use it.
func hotCacheMirror(cache *hotCache) map[string]int64 {
	mirror := make(map[string]int64)
	cache.queues.Range(func(key, value any) bool {
		if used := value.(*hotCacheQueue).usedMirror.Load(); used != 0 {
			mirror[key.(string)] = used
		}
		return true
	})
	return mirror
}

// TestHotCacheHotCountersDoNotShareACacheLine pins the FALSE-SHARING fix: the
// two counters that every operation read-modify-writes (globalUsed, seq) and
// the read-mostly queues header must not share a cache line. Without the
// padding, globalUsed (offset 0), seq (offset 32) and the sync.Map header
// (offset 40) all sat inside the first 128-byte line, so every queue's every
// operation dirtied the line every other queue was reading.
//
// It pins ABSOLUTE offsets against the machine's real cache line size (128 on
// darwin/arm64, hw.cachelinesize = 128) rather than dividing by hotCacheLineSize
// — dividing by the constant under test would make the assertion tautological.
//
// The 8-byte alignment assertions matter independently: atomic.Uint64/int64
// operations require 64-bit alignment, which the padding must preserve.
func TestHotCacheHotCountersDoNotShareACacheLine(t *testing.T) {
	const line = 128 // hw.cachelinesize on darwin/arm64; see hotCacheLineSize
	var c hotCache

	globalUsed := unsafe.Offsetof(c.globalUsed)
	seq := unsafe.Offsetof(c.seq)
	queues := unsafe.Offsetof(c.queues)

	if globalUsed != 0 {
		t.Errorf("hotCache.globalUsed offset = %d, want 0 (first field, 64-bit aligned)", globalUsed)
	}
	if globalUsed%8 != 0 || seq%8 != 0 || queues%8 != 0 {
		t.Errorf("hotCache offsets must be 8-byte aligned: globalUsed=%d seq=%d queues=%d",
			globalUsed, seq, queues)
	}
	if seq/line == globalUsed/line {
		t.Errorf("globalUsed (offset %d) and seq (offset %d) share cache line %d; want "+
			"seq to begin a fresh line", globalUsed, seq, seq/line)
	}
	if queues/line == globalUsed/line || queues/line == seq/line {
		t.Errorf("the queues header (offset %d, line %d) shares a line with globalUsed "+
			"(line %d) or seq (line %d)", queues, queues/line, globalUsed/line, seq/line)
	}
	if want := uintptr(line); seq != want {
		t.Errorf("hotCache.seq offset = %d, want %d: it must begin the line after globalUsed",
			seq, want)
	}
}
