package clusterraft

import "io"

// immutableJournalCommandSource certifies allocation provenance only. It must
// never retain entry.Data: cache eviction must be able to drop the entire array
// while cold bodies and captured encoders still retain this physical reader.
//
// Audited dependencies: raft v3.7.0 appendEntry proto.Clone owns local proposal
// Data; protobuf v1.36.11 mergeBytes copies bytes. journalGroupStore.Entries
// decodes with proto.Unmarshal, whose consumeBytes also copies out of pooled
// readScratch. Raft's log/Ready/storage paths retain/drop these immutable bytes
// and never recycle or mutate them. Existing recentEntries relies on this same
// ownership. A version upgrade must re-audit this opt-in experimental adapter.
type immutableJournalCommandSource struct {
	journalCommandReader
	prefix, capacity int
}

func (s *immutableJournalCommandSource) ImmutableJournalCommandBacking() (prefix, capacity int) {
	return s.prefix, s.capacity
}

func (s *immutableJournalCommandSource) Retain() (io.ReaderAt, func(), error) {
	if source, ok := s.journalCommandReader.(interface {
		Retain() (io.ReaderAt, func(), error)
	}); ok {
		return source.Retain()
	}
	return s.journalCommandReader, func() {}, nil
}

func (g *Group) immutableCommandSource(source journalCommandReader, backingCapacity int) journalCommandReader {
	machine, ok := g.cfg.Machine.(interface{ AcceptsImmutableJournalCommands() bool })
	if !ok || !machine.AcceptsImmutableJournalCommands() || !g.cfg.ProcessDurability ||
		!g.store.node.writer.durability.processAllowed() {
		return source
	}
	return &immutableJournalCommandSource{
		journalCommandReader: source,
		prefix:               len(key{}), capacity: backingCapacity,
	}
}
