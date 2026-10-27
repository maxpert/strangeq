package storage

// THE ALLOCATION BUDGET OF THE WAL INDEX'S PER-MESSAGE UPDATE.
//
// The index's copy list used to be a Go MAP PER PUBLISHED MESSAGE
// (map[recordKey]map[recordPos]*segmentRecord): the common case is exactly
// ONE physical copy per (queue, tag), so every message allocated a map
// header plus a bucket array (~256 bytes and two allocations) to hold a
// single pointer, and threw it away when the message was acknowledged.
//
// The copy list is now a SLICE of the physical copies
// (map[recordKey][]*segmentRecord) -- the same shape the legacy segment tier
// already used -- so indexing one record costs ONE allocation (the
// one-element slice) instead of two.
//
// THIS TEST NAMES NO MAP TYPE, so it compiles against BOTH shapes and
// measures the difference directly.

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestWALIndexUpdate_AllocatesNoMapPerMessage is the ALLOCATION BUDGET that
// the per-message map broke.
//
// It is written against the index API only -- it names no map type -- so it
// compiles against BOTH shapes and measures the difference directly: how many
// heap allocations does indexing ONE record cost?
//
// The per-message map cost two allocations per record (the map header and its
// bucket array). The slice costs ONE (the one-element slice). The budget is
// 1.5 allocations per indexed record: the map shape lands at ~2.0 and fails,
// the slice shape lands at ~1.0 and passes.
func TestWALIndexUpdate_AllocatesNoMapPerMessage(t *testing.T) {
	const n = 2000
	qw := &QueueWAL{}
	qw.initIndexShards()
	// The records themselves are allocated up front: this test is about the
	// INDEX's per-record cost, not about the record allocation.
	recs := make([]segmentRecord, n)
	for i := range recs {
		recs[i].pos = recordPos{segmentID: 1, position: int64(i)}
		recs[i].queueName = "alloc-q"
		recs[i].deliveryTag = uint64(i)
	}

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for i := 0; i < n; i++ {
		qw.indexRecord(&recs[i])
	}
	runtime.ReadMemStats(&after)

	allocsPerRecord := float64(after.Mallocs-before.Mallocs) / n
	bytesPerRecord := float64(after.TotalAlloc-before.TotalAlloc) / n
	require.Less(t, allocsPerRecord, 1.5,
		"indexing ONE record must not allocate a map: %.2f allocations and %.0f bytes per indexed record "+
			"(the copy list must be a slice, not a per-message map)",
		allocsPerRecord, bytesPerRecord)
}
