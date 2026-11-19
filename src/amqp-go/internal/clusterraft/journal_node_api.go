package clusterraft

import (
	"slices"
)

// NodeJournal owns the single append writer and reader shared by the catalog
// and data groups on an replicated node.
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

// JournalOptions configures journal startup and append completion. Zero values
// preserve full durability.
// ProcessDurability is for an isolated one-voter owner. Its caller must invoke
// RequireFullDurability before activating transport or admitting another member.
// Linux selects versioned process recovery roots and requires a readable, nonempty
// kernel boot ID. Other platforms retain the unrooted process-write path.
// Process recovery roots require fresh storage and cannot migrate an old journal.
type JournalOptions struct {
	RangeSync         bool
	ProcessDurability bool
	// ValidateBeforeRecovery runs after acquiring ownership and before
	// modifying recovery state. A journal pair runs it under stream 0's lock.
	ValidateBeforeRecovery func() error
}

func OpenNodeJournalWithOptions(dir string, create bool, options JournalOptions) (*NodeJournal, error) {
	return openNodeJournalOptions(dir, create, options, nil)
}

func openNodeJournal(dir string, create, rangeSync bool) (*NodeJournal, error) {
	return OpenNodeJournalWithOptions(dir, create, JournalOptions{RangeSync: rangeSync})
}

func openNodeJournalOptions(dir string, create bool, options JournalOptions, durability *journalDurability) (*NodeJournal, error) {
	hooks, extent, err := processRootServingOptions(dir, create, options)
	if err != nil {
		return nil, err
	}
	hooks.validateBeforeRecovery = options.ValidateBeforeRecovery
	store, err := openSegmentedGroupNode(dir, create, options.RangeSync, extent, journalGroupMaxFileBytes, nil, hooks)
	if err != nil {
		return nil, err
	}
	if options.ProcessDurability {
		if durability == nil {
			durability = &journalDurability{}
		}
		store.writer.durability = durability
	}
	if reader, ok := store.reader.(*journalSegmentReader); ok && reader.store.processRoots != nil && !options.ProcessDurability {
		if err := store.writer.requireFullDurability(); err != nil {
			_ = store.close()
			return nil, err
		}
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

// RequireFullDurability permanently disables process completion and waits for
// every previously admitted append to cross the physical durability fence.
func (n *NodeJournal) RequireFullDurability() error { return n.store.writer.requireFullDurability() }
