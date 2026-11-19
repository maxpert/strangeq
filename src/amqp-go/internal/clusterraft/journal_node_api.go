package clusterraft

import (
	"slices"
)

// NodeJournal owns the single append writer and reader shared by the catalog
// and data groups on an experimental journal-backed node.
type NodeJournal struct {
	store *journalGroupNode
}

// OpenNodeJournal opens the segmented store. Only provisioning may create
// its manifest. The selected journal files are required on every reopen.
func OpenNodeJournal(dir string, create bool) (*NodeJournal, error) {
	return openNodeJournal(dir, create, false)
}

// OpenNodeJournalWithRangeSync opts into Linux synchronous range writes for
// clean-prefix append batches. Other platforms retain normal journal sync.
// The format and create semantics are identical to OpenNodeJournal.
func OpenNodeJournalWithRangeSync(dir string, create bool) (*NodeJournal, error) {
	return openNodeJournal(dir, create, true)
}

func openNodeJournal(dir string, create, rangeSync bool) (*NodeJournal, error) {
	store, err := openSegmentedGroupNode(dir, create, rangeSync, journalServingExtentBytes, journalGroupMaxFileBytes, nil, journalSegmentHooks{})
	if err != nil {
		return nil, err
	}
	return &NodeJournal{store: store}, nil
}

func (n *NodeJournal) OpenGroup(config Config) (*Group, error) { return n.store.openGroup(config) }

// GroupIDs reports the durable group inventory that survived recovery. The
// owning node applies its retirement markers before opening these groups.
func (n *NodeJournal) GroupIDs() []uint64 {
	n.store.mu.Lock()
	defer n.store.mu.Unlock()
	ids := make([]uint64, 0, len(n.store.groups))
	for id := range n.store.groups {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}

func (n *NodeJournal) HasGroup(id uint64) bool {
	n.store.mu.Lock()
	defer n.store.mu.Unlock()
	return n.store.groups[id] != nil
}

func (n *NodeJournal) Failure() error { return n.store.writer.failure() }

// Close releases node ownership after all Group handles are closed.
func (n *NodeJournal) Close() error { return n.store.close() }
