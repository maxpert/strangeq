//go:build darwin

package storage

// FRONTIER recovery rule (crash_model: barrier, darwin).
//
// With the fence everywhere and no flusher, a power loss persists an epoch
// PREFIX of the log: complete epochs, at most one partial epoch, then nothing.
// Newer files can exist only as data-empty stubs (their headers and dir entries
// were fenced, their data was not persisted). So the benign bounded-defect set
// is not "the two newest files by NUMBER" but the frontier:
//
//	F  = the newest DATA-BEARING file;
//	F- = the newest data-bearing file older than F;
//
// and a bounded non-zero suffix is benign ONLY on F or F-, provided every file
// newer than F is data-empty. Any other defect stays FATAL exactly as today.
//
// force_flush (and every non-darwin platform) keeps the pre-existing
// two-newest-by-number rule BYTE-IDENTICAL.
//
// These fixtures are hand-built byte streams (see wal_tail_hole_test.go for the
// helpers), so the barrier-config cases that are benign under the frontier rule
// are RED against the two-newest code.

import (
	"testing"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/stretchr/testify/require"
)

// frontierPartialFile builds a WAL file body of [records lo..split][64 KiB zero
// hole][records split+1..hi] — a bounded non-zero suffix after the first defect.
func frontierPartialFile(t *testing.T, lo, split, hi uint64) []byte {
	t.Helper()
	buf := walHeaderBytes()
	for off := lo; off <= split; off++ {
		buf = appendHoleRecord(t, buf, off)
	}
	buf = append(buf, make([]byte, 64*1024)...) // the hole
	for off := split + 1; off <= hi; off++ {
		buf = appendHoleRecord(t, buf, off)
	}
	return buf
}

// frontierCleanFile builds a WAL file body of [records lo..hi].
func frontierCleanFile(t *testing.T, lo, hi uint64) []byte {
	t.Helper()
	buf := walHeaderBytes()
	for off := lo; off <= hi; off++ {
		buf = appendHoleRecord(t, buf, off)
	}
	return buf
}

func barrierEngineConfig() interfaces.EngineConfig {
	return interfaces.EngineConfig{CrashModel: interfaces.CrashModelBarrier}
}

func forceFlushEngineConfig() interfaces.EngineConfig {
	return interfaces.EngineConfig{CrashModel: interfaces.CrashModelForceFlush}
}

// TestFrontier_BoundedDefectOnFrontierWithEmptyNewerStubsIsBenign is T-BE-5(a):
// F is partial and TWO newer files are all-zero (data-empty). The frontier rule
// tolerates the bounded defect on F; the two-newest-by-number rule refuses the
// boot (F is not one of the two newest). RED against the two-newest code.
func TestFrontier_BoundedDefectOnFrontierWithEmptyNewerStubsIsBenign(t *testing.T) {
	dir := t.TempDir()
	writeSharedWALFile(t, dir, 1, frontierPartialFile(t, 1, 5, 7))
	writeSharedWALFile(t, dir, 2, make([]byte, 64*1024)) // all-zero: data-empty
	writeSharedWALFile(t, dir, 3, make([]byte, 64*1024)) // all-zero: data-empty

	st, err := NewStorageEngineWithConfig(dir, barrierEngineConfig())
	require.NoError(t, err,
		"a bounded defect on the newest DATA-BEARING file, with every newer file data-empty, "+
			"must be a benign unconfirmed tail under the barrier frontier rule")
	defer st.Close()

	msgs, rerr := st.GetRecoverableMessages()
	require.NoError(t, rerr)
	require.Len(t, msgs["q"], 5,
		"replay must stop at the first defect and recover only file 1's contiguous prefix")
}

// TestFrontier_BoundedDefectOnFrontierWithHeaderOnlyStubIsBenign is T-BE-5(a)
// with the REAL pre-opened stub shape: a header-only file (size == WALHeaderSize)
// is data-empty, so a bounded defect on the newest data-bearing file is benign.
func TestFrontier_BoundedDefectOnFrontierWithHeaderOnlyStubIsBenign(t *testing.T) {
	dir := t.TempDir()
	writeSharedWALFile(t, dir, 1, frontierPartialFile(t, 1, 5, 7))
	writeSharedWALFile(t, dir, 2, walHeaderBytes()) // header-only pre-opened stub

	st, err := NewStorageEngineWithConfig(dir, barrierEngineConfig())
	require.NoError(t, err,
		"a header-only pre-opened stub newer than F must count as data-empty")
	defer st.Close()

	msgs, rerr := st.GetRecoverableMessages()
	require.NoError(t, rerr)
	require.Len(t, msgs["q"], 5)
}

// TestFrontier_BoundedDefectOnOlderDataBearingFileStaysFatal is T-BE-5(b)/(c):
// three data-bearing files, the defect on the OLDEST. It is neither F nor F-,
// so it is real interior corruption and stays FATAL in barrier mode.
func TestFrontier_BoundedDefectOnOlderDataBearingFileStaysFatal(t *testing.T) {
	dir := t.TempDir()
	writeSharedWALFile(t, dir, 1, frontierPartialFile(t, 1, 3, 5)) // defect here
	writeSharedWALFile(t, dir, 2, frontierCleanFile(t, 10, 12))
	writeSharedWALFile(t, dir, 3, frontierCleanFile(t, 20, 22))

	_, err := NewStorageEngineWithConfig(dir, barrierEngineConfig())
	require.Error(t, err,
		"a bounded defect on a data-bearing file that is not F or F- must stay FATAL")
	require.Contains(t, err.Error(), "00000000000000000001.wal")
}

// TestFrontier_DefectBelowTwoDataBearingFilesStaysFatal is the "(c)" shape: the
// partial file has TWO data-bearing files newer than it, so it is below the
// frontier and FATAL.
func TestFrontier_DefectBelowTwoDataBearingFilesStaysFatal(t *testing.T) {
	dir := t.TempDir()
	writeSharedWALFile(t, dir, 1, frontierCleanFile(t, 1, 2))
	writeSharedWALFile(t, dir, 2, frontierPartialFile(t, 3, 5, 7)) // defect here
	writeSharedWALFile(t, dir, 3, frontierCleanFile(t, 10, 12))
	writeSharedWALFile(t, dir, 4, frontierCleanFile(t, 20, 22))

	_, err := NewStorageEngineWithConfig(dir, barrierEngineConfig())
	require.Error(t, err,
		"a defect on a file two levels below the frontier (two data-bearing files newer) must stay FATAL")
	require.Contains(t, err.Error(), "00000000000000000002.wal")
}

// TestFrontier_BoundedDefectOnFMinusWithEmptyNewerStubIsBenign is the roll-
// boundary case F- covers: the just-retired file F- carries the defect, the
// pre-opened F is the empty stub. Benign.
func TestFrontier_BoundedDefectOnFMinusWithEmptyNewerStubIsBenign(t *testing.T) {
	dir := t.TempDir()
	writeSharedWALFile(t, dir, 1, frontierCleanFile(t, 1, 3))
	writeSharedWALFile(t, dir, 2, frontierPartialFile(t, 4, 6, 8)) // F- carries the defect
	writeSharedWALFile(t, dir, 3, walHeaderBytes())                // F: empty pre-opened stub

	st, err := NewStorageEngineWithConfig(dir, barrierEngineConfig())
	require.NoError(t, err,
		"a bounded defect on F- (the just-retired file) with F an empty stub must be benign")
	defer st.Close()
	msgs, rerr := st.GetRecoverableMessages()
	require.NoError(t, rerr)
	require.Len(t, msgs["q"], 6, "file 1 (3) plus file 2's prefix (3)")
}

// TestFrontier_ForceFlushKeepsTwoNewestRule is T-BE-5(e): the SAME fixture that
// is benign under barrier is FATAL under force_flush, because file 1 is not one
// of the two newest by number. This pins force_flush byte-identical to today.
func TestFrontier_ForceFlushKeepsTwoNewestRule(t *testing.T) {
	dir := t.TempDir()
	writeSharedWALFile(t, dir, 1, frontierPartialFile(t, 1, 5, 7))
	writeSharedWALFile(t, dir, 2, make([]byte, 64*1024))
	writeSharedWALFile(t, dir, 3, make([]byte, 64*1024))

	_, err := NewStorageEngineWithConfig(dir, forceFlushEngineConfig())
	require.Error(t, err,
		"under force_flush the two-newest-by-number rule is byte-identical to today: "+
			"file 1 is not one of the two newest, so the defect is FATAL")
	require.Contains(t, err.Error(), "00000000000000000001.wal")
}

// TestFrontier_ForceFlushTwoNewestStillBenign guards the other half: under
// force_flush a bounded defect on the second-newest-by-number file is still
// benign (today's rule), even though the file-number frontier would not reach
// it if a newer file were data-bearing.
func TestFrontier_ForceFlushTwoNewestStillBenign(t *testing.T) {
	dir := t.TempDir()
	writeSharedWALFile(t, dir, 1, frontierCleanFile(t, 1, 3))
	writeSharedWALFile(t, dir, 2, frontierPartialFile(t, 4, 6, 8))
	writeSharedWALFile(t, dir, 3, frontierCleanFile(t, 10, 12))

	st, err := NewStorageEngineWithConfig(dir, forceFlushEngineConfig())
	require.NoError(t, err,
		"force_flush keeps today's two-newest rule: the second-newest file's bounded defect is benign")
	defer st.Close()
	msgs, rerr := st.GetRecoverableMessages()
	require.NoError(t, rerr)
	require.Len(t, msgs["q"], 9, "file 1 (3) plus file 2's prefix (3) plus file 3 (3)")
}

// TestFrontier_AllZeroNewerFileIsDataEmpty pins the data-empty definition: an
// all-zero file (a prealloc crash) newer than F is data-empty, so it does not
// push F out of the frontier.
func TestFrontier_AllZeroNewerFileIsDataEmpty(t *testing.T) {
	dir := t.TempDir()
	writeSharedWALFile(t, dir, 1, frontierPartialFile(t, 1, 2, 4))
	writeSharedWALFile(t, dir, 2, make([]byte, 64*1024))

	st, err := NewStorageEngineWithConfig(dir, barrierEngineConfig())
	require.NoError(t, err,
		"an all-zero file newer than F is data-empty and must not push F out of the frontier")
	defer st.Close()
	msgs, rerr := st.GetRecoverableMessages()
	require.NoError(t, rerr)
	require.Len(t, msgs["q"], 2)
}
