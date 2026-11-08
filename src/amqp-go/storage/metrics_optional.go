package storage

// metrics_optional.go — the optional-capability metrics seam for the spec 06
// §3.3 durability families.
//
// The storage interfaces (WALMetrics, SegmentMetrics) are deliberately NOT
// widened: a custom collector that implements only the historical methods keeps
// working, exactly as the WALFullFlushSinkBinder pattern does on the server
// side. A production *metrics.Collector implements the small interfaces below,
// so the type assertion succeeds there and is a cheap miss everywhere else.

// The split WAL error counters: a failed durability BARRIER and a failed
// page-cache WRITE are different outages, so the fsync sites call these instead
// of RecordWALWriteError.
type walFsyncErrorMetrics interface{ RecordWALFsyncError() }

type walRollErrorMetrics interface{ RecordWALRollError() }

type walCompactionErrorMetrics interface{ RecordWALCompactionError() }

type walReclaimFsyncErrorMetrics interface{ RecordWALReclaimFsyncError() }

type segmentRollErrorMetrics interface{ RecordSegmentRollError() }

type segmentFsyncErrorMetrics interface{ RecordSegmentFsyncError() }

func recordWALFsyncError(m WALMetrics) {
	if m == nil {
		return
	}
	if r, ok := m.(walFsyncErrorMetrics); ok {
		r.RecordWALFsyncError()
	}
}

func recordWALRollError(m WALMetrics) {
	if m == nil {
		return
	}
	if r, ok := m.(walRollErrorMetrics); ok {
		r.RecordWALRollError()
	}
}

func recordWALCompactionError(m WALMetrics) {
	if m == nil {
		return
	}
	if r, ok := m.(walCompactionErrorMetrics); ok {
		r.RecordWALCompactionError()
	}
}

func recordWALReclaimFsyncError(m WALMetrics) {
	if m == nil {
		return
	}
	if r, ok := m.(walReclaimFsyncErrorMetrics); ok {
		r.RecordWALReclaimFsyncError()
	}
}

func recordSegmentRollError(m SegmentMetrics) {
	if m == nil {
		return
	}
	if r, ok := m.(segmentRollErrorMetrics); ok {
		r.RecordSegmentRollError()
	}
}

func recordSegmentFsyncError(m SegmentMetrics) {
	if m == nil {
		return
	}
	if r, ok := m.(segmentFsyncErrorMetrics); ok {
		r.RecordSegmentFsyncError()
	}
}
