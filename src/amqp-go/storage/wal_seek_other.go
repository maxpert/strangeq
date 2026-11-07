//go:build !unix

package storage

import "os"

// walDataExtents is the non-Unix stub: SEEK_DATA/SEEK_HOLE are not available,
// so the caller falls back to a byte scan. ok=false always.
func walDataExtents(f *os.File, from, end int64, fn func(start, end int64) error) (ok bool, err error) {
	return false, nil
}
