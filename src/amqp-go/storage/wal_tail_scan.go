package storage

import (
	"io"
	"os"
	"sort"
)

// walTruncateTailDurable truncates the file at path to `size` and makes the
// truncation durable with fdatasync. It is the R-TAIL-HOLE boot cleanup: a
// newest WAL file whose first defect is a bounded unconfirmed tail is cut back
// to its last valid record before the new file is opened, so a later boot never
// sees the shape again.
//
// A no-op when the file is already at or below `size`. The file is opened
// O_RDWR because the read-only handles the walks use cannot truncate.
//
// The fsync is DELIBERATELY fdatasyncFile (F_FULLFSYNC on darwin), never the
// confirm barrier: this is a boot-time durability barrier that bounds the
// power-loss window, and it must stay a full drive-cache flush (see
// storage/fdatasync_darwin.go).
func walTruncateTailDurable(path string, size int64) error {
	if size < 0 {
		return nil
	}
	st, err := os.Stat(path)
	if err != nil {
		return err
	}
	if st.Size() <= size {
		return nil
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := f.Truncate(size); err != nil {
		return err
	}
	return fdatasyncFileSite(f, siteBootTail)
}

// R-TAIL-HOLE.
//
// Under crash_model: barrier (the durable confirm default) the confirm barrier
// hands a batch to the drive and issues an ordering barrier instead of a full
// drive-cache flush. On power loss the drive therefore holds a PREFIX of the
// log in barrier order: complete epochs 0..j, then a partial epoch j+1 (holes
// and torn records), then nothing. The ordinary shape of a crashed active WAL
// file is thus [confirmed records][hole or torn record][more unconfirmed bytes]
// followed by a long zero tail.
//
// Before R-TAIL-HOLE that shape was a BOOT-BLOCKING FATAL fault that discarded
// even the confirmed prefix — the walks treated "a defect with real data after
// it" as interior corruption. The bound makes the distinction that matters:
// everything after the FIRST defect is UNCONFIRMED (stop-replay semantics — a
// defect at offset f can only be at or above the durable end D, and no barrier
// ever covered bytes above the defect). A non-zero suffix of at most
// walMaxUnconfirmedBytes + walMaxFrameBytes on a file the crash-model rule
// allows is therefore a bounded unconfirmed tail: benign, replay stops at f, and
// boot durably truncates the file at f. A larger suffix, or a defect on any
// other file, is real interior corruption and stays FATAL.
//
// WHICH FILES THE RULE ALLOWS is chosen per boot by walTailRule:
//   - force_flush, and every non-darwin platform: the TWO NEWEST files by
//     number, unchanged. A trailing roll can leave unconfirmed bytes on both N
//     and N+1; a defect in the third-newest file cannot be an unconfirmed tail.
//   - barrier + darwin: the FRONTIER — F (the newest data-bearing file) and F-
//     (the newest data-bearing file older than F), provided every file newer
//     than F is data-empty. Under the fence, a power loss persists an epoch
//     prefix, so only F or F- can carry a tail defect, and newer files exist
//     only as data-empty stubs (the W8 pre-opened file). See
//     .notes/specs/task-barrier-everywhere.md §3.

// walDefectClass is the classification of the region after the first defect
// found in a WAL file.
type walDefectClass int

const (
	// walDefectTornTail: the whole region from the defect to the end is zero.
	// The ordinary torn tail; benign, replay stops at the defect.
	walDefectTornTail walDefectClass = iota
	// walDefectUnconfirmedTail: the region holds non-zero bytes, but the
	// suffix is within the bounded unconfirmed window AND the file is one of
	// the two newest. Benign; boot truncates the file at the defect.
	walDefectUnconfirmedTail
	// walDefectFatal: real interior corruption (or an out-of-bound suffix on
	// an old file). The file cannot be interpreted.
	walDefectFatal
)

// classifyWALDefect decides what a defect at byte offset defectAt means for
// file f, whose intelligible data ends at `end` (the scan bound / EOF).
//
// allowUnconfirmed is the per-file membership in the benign set, computed once
// per walk by walTailRule: under force_flush it is "one of the two newest files
// by number" (byte-identical to before), under barrier+darwin it is the
// frontier ("F or F-, with every newer file data-empty").
//
// It returns the class and z, the offset of the last non-zero byte in
// [defectAt, end), or -1 when that region is all zeros. A read failure while
// proving the tail is all zeros is treated as FATAL: an unprovable tail is
// interior corruption (the safe direction).
func classifyWALDefect(f *os.File, defectAt, end int64, allowUnconfirmed bool) (walDefectClass, int64) {
	z, ok := walLastNonZeroOffset(f, defectAt, end)
	if !ok {
		return walDefectFatal, -1
	}
	if z < 0 {
		return walDefectTornTail, -1
	}
	if allowUnconfirmed && z-defectAt <= walMaxUnconfirmedBytes+walMaxFrameBytes {
		return walDefectUnconfirmedTail, z
	}
	return walDefectFatal, z
}

// walFileRef is a (file number, path) pair for a WAL file on disk.
type walFileRef struct {
	num  uint64
	path string
}

// walTailRule returns the predicate "a bounded non-zero suffix after the first
// defect is a benign unconfirmed tail on this file?" for the rule selected by
// the configured crash model. files is the directory's WAL set (any order).
//
//   - fence (barrier + darwin): the FRONTIER rule. F is the newest DATA-BEARING
//     file; F- the newest data-bearing file older than F. A file is benign iff
//     it is F or F- AND every file newer than F is data-empty. Under the fence a
//     power loss persists an epoch prefix, so only the newest data-bearing file
//     (or the one before it, at a roll boundary) can carry a tail defect, and
//     newer files exist only as data-empty stubs.
//   - otherwise (force_flush, or any non-darwin): today's rule, byte-identical:
//     the two newest files by number.
func walTailRule(files []walFileRef, fence bool) func(uint64) bool {
	// The frontier walk below is order-sensitive; enumerate ascending by
	// number so it does not depend on the caller's ordering.
	if fence {
		sort.Slice(files, func(i, j int) bool { return files[i].num < files[j].num })
	}
	if !fence {
		var firstNewest, secondNewest uint64
		for _, f := range files {
			switch {
			case f.num > firstNewest:
				secondNewest = firstNewest
				firstNewest = f.num
			case f.num > secondNewest:
				secondNewest = f.num
			}
		}
		return func(n uint64) bool {
			return n == firstNewest || (secondNewest != 0 && n == secondNewest)
		}
	}

	// Frontier. Walk from the newest down to find F then F-. `bearing` records
	// the data-bearing answer for the files above F (all of which are empty, by
	// F's definition); older files are not needed for the newer-than-F check.
	bearing := make(map[uint64]bool, len(files))
	var frontier, frontierMinus uint64
	for i := len(files) - 1; i >= 0; i-- {
		b := walFileDataBearing(files[i].path)
		bearing[files[i].num] = b
		if !b {
			continue
		}
		if frontier == 0 {
			frontier = files[i].num
			continue
		}
		frontierMinus = files[i].num
		break
	}

	// Every file newer than F must be data-empty. (Files above F were visited
	// first in the descending walk; re-check defensively.)
	newerAllEmpty := true
	for _, f := range files {
		if f.num > frontier && bearing[f.num] {
			newerAllEmpty = false
			break
		}
	}

	return func(n uint64) bool {
		if !newerAllEmpty {
			return false
		}
		if n == frontier {
			return true
		}
		return frontierMinus != 0 && n == frontierMinus
	}
}

// walFileDataBearing reports whether a WAL file holds data past its header: any
// intelligible record, or any non-zero byte, past the header. A file smaller
// than the header, a header-only file (the W8 pre-opened stub) and an all-zero
// file (a prealloc crash) are DATA-EMPTY.
//
// It is conservative on error: an unreadable file is reported as data-bearing
// so it participates in the frontier and never lets a defect be waved through.
func walFileDataBearing(path string) bool {
	st, err := os.Stat(path)
	if err != nil {
		return true
	}
	if st.Size() <= int64(WALHeaderSize) {
		return false
	}
	f, err := os.Open(path)
	if err != nil {
		return true
	}
	defer f.Close()
	last, ok := walLastNonZeroOffset(f, int64(WALHeaderSize), st.Size())
	if !ok {
		return true
	}
	return last >= 0
}

// walLastNonZeroOffset returns the offset of the last non-zero byte in
// [from, end), or -1 when the region is all zeros (or empty). ok=false means
// the region could not be read.
//
// It uses SEEK_DATA/SEEK_HOLE to visit only the ALLOCATED extents, so a sparse
// zero tail (a preallocated file, or a barrier tail the drive has not
// materialised) costs metadata reads, not a full zero read. When the
// filesystem cannot answer, it falls back to a 64 KiB-chunked forward scan.
func walLastNonZeroOffset(f *os.File, from, end int64) (int64, bool) {
	if from < 0 {
		from = 0
	}
	if from >= end {
		return -1, true
	}
	last := int64(-1)
	ok, err := walDataExtents(f, from, end, func(s, e int64) error {
		l, scerr := scanChunkLastNonZero(f, s, e)
		if scerr != nil {
			return scerr
		}
		if l >= 0 {
			last = l
		}
		return nil
	})
	if err != nil {
		return -1, false
	}
	if ok {
		return last, true
	}
	// Fallback: the filesystem has no hole information. Scan forward,
	// tracking the last non-zero byte.
	return scanForwardLastNonZero(f, from, end)
}

// scanChunkLastNonZero scans [s, e) backwards in 64 KiB chunks and returns the
// absolute offset of the last non-zero byte, or -1.
func scanChunkLastNonZero(f *os.File, s, e int64) (int64, error) {
	const chunk = 64 * 1024
	buf := make([]byte, chunk)
	for pos := e; pos > s; {
		n := int64(chunk)
		if pos-n < s {
			n = pos - s
		}
		start := pos - n
		if _, err := f.ReadAt(buf[:n], start); err != nil && err != io.EOF {
			return -1, err
		}
		for i := int(n) - 1; i >= 0; i-- {
			if buf[i] != 0 {
				return start + int64(i), nil
			}
		}
		pos = start
	}
	return -1, nil
}

// scanForwardLastNonZero is the no-hole-information fallback. It scans
// [from, end) forward in 64 KiB chunks and returns the last non-zero offset.
func scanForwardLastNonZero(f *os.File, from, end int64) (int64, bool) {
	const chunk = 64 * 1024
	buf := make([]byte, chunk)
	last := int64(-1)
	for pos := from; pos < end; {
		n := int64(chunk)
		if pos+n > end {
			n = end - pos
		}
		if _, err := f.ReadAt(buf[:n], pos); err != nil && err != io.EOF {
			return -1, false
		}
		for i := int64(0); i < n; i++ {
			if buf[i] != 0 {
				last = pos + i
			}
		}
		pos += n
	}
	return last, true
}
