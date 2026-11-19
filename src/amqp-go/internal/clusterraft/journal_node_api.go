package clusterraft

import (
	"os"
	"slices"
)

// NodeJournal owns the single append writer and reader shared by the catalog
// and data groups on an experimental journal-backed node.
type NodeJournal struct {
	store *journalGroupNode
}

// OpenNodeJournal permits a missing active file only during node provisioning.
// An established node must pass create=false so storage loss fails closed.
func OpenNodeJournal(dir string, create bool) (*NodeJournal, error) {
	return openNodeJournal(dir, create, nil)
}

// OpenNodeJournalWithRangeSync opts into Linux synchronous range writes for
// clean-prefix append batches. Other platforms retain normal journal sync.
// The format and create semantics are identical to OpenNodeJournal.
func OpenNodeJournalWithRangeSync(dir string, create bool) (*NodeJournal, error) {
	return openNodeJournal(dir, create, wrapRangeSyncJournalFile)
}

func openNodeJournal(dir string, create bool, wrap func(*os.File) journalFile) (*NodeJournal, error) {
	store, err := openJournal(dir, create, wrap)
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
