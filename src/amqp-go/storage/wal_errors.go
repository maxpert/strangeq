package storage

import (
	"errors"
	"fmt"
)

// WS-C (C1) write-error taxonomy. The WAL previously answered a failed
// write/fsync with a bare error while leaving the partial bytes on disk and
// the in-memory fileOffset stale, so the next batch indexed wrong positions
// and the file acquired interior corruption. These types make the outcome of
// a failed batch explicit and machine-checkable.

// ErrWALClosed is returned by every write entry point once Close has begun.
var ErrWALClosed = errors.New("storage: WAL is closed")

// ErrWALUnavailable marks a write refused or failed while the WAL is degraded:
// the active file is poisoned and no replacement could be opened yet, or
// consecutive batch failures crossed walDegradedAfterFailures.
// Use errors.Is(err, ErrWALUnavailable) to distinguish "storage is sick"
// from a single transient batch failure.
var ErrWALUnavailable = errors.New("storage: WAL unavailable")

// WALWriteError describes one failed batch / tx write.
type WALWriteError struct {
	Op    string // "write" | "fsync" | "tx-write" | "tx-fsync"
	Cause error
	// Repaired is true when the file was truncated back to the pre-write
	// offset and that truncation was made durable, so the WAL's on-disk state
	// is exactly what it was before the failed batch.
	Repaired bool
}

func (e *WALWriteError) Error() string {
	return fmt.Sprintf("WAL %s failed (repaired=%t): %v", e.Op, e.Repaired, e.Cause)
}

func (e *WALWriteError) Unwrap() error { return e.Cause }
