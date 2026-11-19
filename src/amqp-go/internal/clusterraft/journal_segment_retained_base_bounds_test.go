package clusterraft

import (
	"math"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestJournalSegmentRetainedBaseRejectsInvalidOriginOrAnchor(t *testing.T) {
	const base = int64(journalGroupMaxFileBytes + 4096)
	valid := journalSegmentManifest{
		extentBytes: 4096,
		extents:     []journalSegmentExtent{{id: 73, base: base, firstSequence: 41}},
		checkpoint:  journalSegmentCheckpointRef{sequence: 40, end: base, size: 32, name: "checkpoint-1"},
	}
	cases := []struct {
		name, magic string
		change      func(*journalSegmentManifest)
	}{
		{"v2_nonzero_origin", "SQJSEG02", nil},
		{"v3_zero_base", journalSegmentManifestMagic, func(m *journalSegmentManifest) { m.extents[0].base = 0 }},
		{"v3_genesis_sequence", journalSegmentManifestMagic, func(m *journalSegmentManifest) { m.extents[0].firstSequence = 1 }},
		{"v3_negative_base", journalSegmentManifestMagic, func(m *journalSegmentManifest) { m.extents[0].base = -1 }},
		{"v3_missing_anchor", journalSegmentManifestMagic, func(m *journalSegmentManifest) { m.checkpoint = journalSegmentCheckpointRef{} }},
		{"v3_wrong_anchor_end", journalSegmentManifestMagic, func(m *journalSegmentManifest) { m.checkpoint.end++ }},
		{"v3_wrong_anchor_sequence", journalSegmentManifestMagic, func(m *journalSegmentManifest) { m.checkpoint.sequence++ }},
		{"v3_exhausted_sequence", journalSegmentManifestMagic, func(m *journalSegmentManifest) { m.extents[0].firstSequence = math.MaxUint64 }},
		{"v3_virtual_overflow", journalSegmentManifestMagic, func(m *journalSegmentManifest) {
			m.extents[0].base = math.MaxInt64 - minJournalSegmentFrameBytes + 1
			m.extents[0].sealedBytes = minJournalSegmentFrameBytes
			m.checkpoint.end = m.extents[0].base
			m.extents = append(m.extents, journalSegmentExtent{id: 74, base: math.MinInt64, firstSequence: 42})
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := valid
			m.extents = slices.Clone(valid.extents)
			if tc.change != nil {
				tc.change(&m)
			}
			data := retainedBaseManifestFixture(tc.magic, m)
			dir := t.TempDir()
			require.NoError(t, os.WriteFile(filepath.Join(dir, journalSegmentManifestName), data, 0600))
			_, err := loadJournalSegmentManifest(dir)
			require.ErrorIs(t, err, errJournalCorrupt)
		})
	}
}

func TestJournalSegmentRetainedBaseIndexZeroRejectsCoveredEntries(t *testing.T) {
	const base = int64(journalGroupMaxFileBytes + 4096)
	dir := t.TempDir()
	seed := &journalSegmentStorage{
		dir: dir, quota: &journalSegmentQuota{limit: 4096},
		hooks: journalSegmentHooks{syncFile: syncJournalFile, syncDir: syncDirectory},
	}
	// Produce a valid checkpoint whose entry belongs to the omitted prefix.
	// The v3 index-zero boundary must reject it without restoring group state.
	groups := map[uint64]*journalGroupState{11: {
		identity: journalCheckpointFixtureIdentity(11), hard: journalCheckpointFixtureHard(1, 1, 1),
		entries: []journalEntryRef{{offset: base - 1, index: 1, term: 1, size: 1}},
	}}
	anchor, err := seed.writeCheckpoint(40, base, groups,
		[]journalSegmentExtent{{id: 72, base: base - minJournalSegmentFrameBytes, sealedBytes: minJournalSegmentFrameBytes, firstSequence: 40}})
	require.NoError(t, err)
	m := journalSegmentManifest{extentBytes: 4096, checkpoint: anchor,
		extents: []journalSegmentExtent{{id: 73, base: base, firstSequence: 41}}}
	require.NoError(t, os.WriteFile(filepath.Join(dir, journalSegmentManifestName), retainedBaseManifestFixture(journalSegmentManifestMagic, m), 0600))
	require.NoError(t, os.WriteFile(seed.path(73), nil, 0600))
	restores := 0
	output, reader, _, err := openJournalSegmentsWithQuota(dir, false, 4096, 4096, journalSegmentHooks{
		restoreCheckpoint: func(uint64, int64, map[uint64]*journalGroupState) error { restores++; return nil },
	}, nil)
	t.Cleanup(func() {
		if output != nil {
			_ = output.Close()
		}
		if reader != nil {
			_ = reader.Close()
		}
	})
	require.ErrorIs(t, err, errJournalCorrupt)
	require.Zero(t, restores, "index-zero checkpoint must own no covered entry references")
}

func TestJournalSegmentMissingManifestDoesNotInitializeOverOwnedArtifacts(t *testing.T) {
	for _, name := range []string{"extent-0000000000000049.raft-journal", "checkpoint-1"} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			require.NoError(t, os.WriteFile(filepath.Join(dir, name), nil, 0600))
			before := retainedJournalFixtureFiles(t, dir)
			output, reader, _, err := openJournalSegmentsWithQuota(dir, true, 4096, 4096, journalSegmentHooks{}, nil)
			t.Cleanup(func() {
				if output != nil {
					_ = output.Close()
				}
				if reader != nil {
					_ = reader.Close()
				}
			})
			require.ErrorIs(t, err, errJournalCorrupt, "even a zero-byte owned artifact forbids virgin initialization")
			require.Equal(t, before, retainedJournalFixtureFiles(t, dir))
			_, err = os.Lstat(filepath.Join(dir, journalSegmentManifestName))
			require.ErrorIs(t, err, os.ErrNotExist, "recovery must not synthesize new authority")
		})
	}
}
