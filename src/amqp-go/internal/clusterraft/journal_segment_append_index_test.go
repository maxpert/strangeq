package clusterraft

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestJournalAppendIndexKeepsSealedMetadataAcrossSuffixReplacement(t *testing.T) {
	const group = uint64(11)
	original := []journalEntryRef{
		{index: 1, term: 1, offset: 32, size: 64},
		{index: 2, term: 1, offset: 112, size: 64},
		{index: 3, term: 1, offset: 192, size: 64},
	}
	index, err := newJournalAppendIndex(map[uint64]*journalGroupState{
		group: {identity: journalCheckpointFixtureIdentity(group), hard: journalCheckpointFixtureHard(1, 0, 1), entries: original},
	})
	require.NoError(t, err)
	sealed := cloneJournalGroups(index.shadow.groups)
	replacement := journalEntryRef{index: 2, term: 2, offset: 300, size: 64}
	require.NoError(t, index.append(group, journalGroupDelta{
		kind: journalGroupReady, hard: journalCheckpointFixtureHard(2, 0, 1), entries: []journalEntryRef{replacement},
	}))
	require.Equal(t, []journalEntryRef{original[0], replacement}, index.shadow.groups[group].entries)
	require.Equal(t, original, sealed[group].entries, "the sealed worker must retain its original suffix")
	require.EqualValues(t, 1, sealed[group].hard.GetTerm())
}
