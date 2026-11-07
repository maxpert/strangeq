//go:build unix

package storage

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

// walDataExtents calls fn(start, end) for each ALLOCATED (data) extent of f
// that intersects [from, end), using SEEK_DATA / SEEK_HOLE. It returns
// ok=true when the walk completed with the platform's hole information, and
// ok=false when the filesystem or platform does not support the seek modes
// (the caller then falls back to a byte scan).
//
// IT EXISTS SO A SPARSE TAIL DOES NOT COST A FULL ZERO READ. A preallocated
// WAL file is sparse (the written prefix is a data extent, the tail is a hole),
// and a barrier tail the drive has not materialised is likewise a hole.
// Deciding "is the tail all zeros" by reading it would read the whole extent;
// SEEK_DATA/SEEK_HOLE answer it from metadata.
//
// APFS supports both, as do the Linux filesystems this build targets.
func walDataExtents(f *os.File, from, end int64, fn func(start, end int64) error) (ok bool, err error) {
	if from >= end {
		return true, nil
	}
	fd := int(f.Fd())
	pos := from
	for pos < end {
		ds, serr := unix.Seek(fd, pos, unix.SEEK_DATA)
		if serr != nil {
			if errors.Is(serr, unix.ENXIO) {
				// No data at or after pos: everything remaining is a hole.
				return true, nil
			}
			// EINVAL / EOPNOTSUPP / ENOTSUP: the filesystem cannot answer.
			return false, nil
		}
		if ds >= end {
			return true, nil
		}
		if ds < pos {
			ds = pos
		}
		hs, herr := unix.Seek(fd, ds, unix.SEEK_HOLE)
		if herr != nil {
			return false, nil
		}
		if hs > end || hs <= ds {
			hs = end
		}
		if e := fn(ds, hs); e != nil {
			return true, e
		}
		pos = hs
	}
	return true, nil
}
