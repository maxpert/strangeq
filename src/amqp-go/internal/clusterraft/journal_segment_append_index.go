package clusterraft

import (
	"fmt"
	"slices"
)

// The writer owns this view of durable Raft state. The groups maintain their
// own view for application and can publish the completed appends independently.
type journalAppendIndex struct {
	shadow journalGroupNode
	bytes  int64
}

type journalSealedIndex struct {
	sequence uint64
	end      int64
	groups   map[uint64]*journalGroupState
}

func cloneJournalGroups(groups map[uint64]*journalGroupState) map[uint64]*journalGroupState {
	result := make(map[uint64]*journalGroupState, len(groups))
	for id, state := range groups {
		cloned := *state
		cloned.entries = slices.Clone(state.entries)
		result[id] = &cloned
	}
	return result
}

func newJournalAppendIndex(groups map[uint64]*journalGroupState) (*journalAppendIndex, error) {
	index := &journalAppendIndex{
		shadow: journalGroupNode{groups: cloneJournalGroups(groups)},
		bytes:  journalCheckpointHeader + journalCheckpointTrailer,
	}
	for _, group := range index.shadow.groups {
		index.bytes += journalCheckpointGroupBytes(group)
	}
	return index, index.checkBounds()
}

func (i *journalAppendIndex) append(group uint64, delta journalGroupDelta) error {
	if prior := i.shadow.groups[group]; prior != nil {
		i.bytes -= journalCheckpointGroupBytes(prior)
	}
	if err := i.shadow.replayDelta(group, delta); err != nil {
		return err
	}
	state := i.shadow.groups[group]
	i.bytes += journalCheckpointGroupBytes(state)
	if journalCheckpointMetadataBytes(state) > maxJournalCheckpointMetadata {
		return fmt.Errorf("%w: journal checkpoint metadata limit", errJournalTooLarge)
	}
	return i.checkBounds()
}

func (i *journalAppendIndex) checkBounds() error {
	if len(i.shadow.groups) > maxJournalCheckpointGroups || i.bytes > maxJournalCheckpointBytes {
		return fmt.Errorf("%w: journal checkpoint retained state limit", errJournalTooLarge)
	}
	return nil
}
