package storage

// ColdReadMetrics is an OPTIONAL extension of the storage metrics interfaces
// (the StorageMetrics pattern), so custom collectors keep compiling. It is
// implemented by metrics.Collector, and storage type-asserts for it once in
// SetMetrics.
//
// WS-COLDREAD-PATH SAFE CORE. Only the read-path counters that survived the
// rejection of the pooled-buffer mechanism live here. There is no pool and no
// lease, so there is no pool-hit/miss/retained-bytes/leases series.
//
// The by-read counters (hot/cold, one/two pread, current/old file) are recorded
// AT THE READ, independent of whether the admission that follows succeeds, so a
// refused admission still counts its read.
type ColdReadMetrics interface {
	RecordDeliveryHotHit()
	RecordDeliveryColdRead()
	RecordColdReadOnePread()
	RecordColdReadTwoPreadFallback()
	RecordColdReadCurrentFile()
	RecordColdReadOldFile()
}
