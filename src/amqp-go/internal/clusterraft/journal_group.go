package clusterraft

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

const (
	journalActiveName            = "active.raft-journal"
	journalGroupMaxFileBytes     = 8 << 30
	journalGroupMaxSnapshotBytes = 8 << 30
)

type journalGroupNode struct {
	dir    string
	reader interface {
		io.ReaderAt
		io.Closer
	}
	writer        *journalWriter
	readScratch   journalScratchPool
	recentEntries journalRecentEntries
	replySlots    chan struct{}

	mu        sync.Mutex
	groups    map[uint64]*journalGroupState
	owners    map[uint64]bool
	closed    bool
	closeOnce sync.Once
	closeErr  error

	snapshotMu       sync.Mutex
	snapshotBytes    atomic.Int64
	routineSnapshots journalCheckpointAdmission
	syncFile         func(*os.File) error
	syncDir          func(string) error
}

type journalGroupState struct {
	identity []byte
	hard     *pb.HardState
	snapshot journalSnapshotRef
	entries  []journalEntryRef
}

type journalGroupStore struct {
	node       *journalGroupNode
	group      uint64
	state      *journalGroupState // Owned by Group under its mutex.
	once       sync.Once
	closeErr   error
	closeOwner bool
	// Pending commit is protected by the Group mutex. It can cover only
	// entries already durable in the local journal.
	pendingCommit     uint64
	processDurability bool
	// The per-group worker owns staged until it hands off completion. A
	// stopped Group can skip publication, but close runs after its worker
	// exits and must then preserve the durable delta for the next owner.
	staged *stagedJournalWrite
}

type stagedJournalWrite struct {
	write readyWrite
	delta journalGroupDelta
}

type journalDataFile struct{ *os.File }

func (f journalDataFile) Sync() error { return syncJournalFile(f.File) }

func journalGroupWriterLimits() journalLimits {
	return journalLimits{
		pendingBytes: 128 << 20, pendingRequests: 64, batchBytes: 8 << 20, batchRequests: 32,
		maxFileBytes: journalGroupMaxFileBytes,
	}
}

func (n *journalGroupNode) openGroup(config Config) (*Group, error) {
	return n.openValidatedGroup(config)
}

func (n *journalGroupNode) close() error {
	n.mu.Lock()
	if len(n.owners) != 0 {
		n.mu.Unlock()
		return errors.New("close node raft journal after all group handles")
	}
	n.closed = true
	n.mu.Unlock()
	n.closeOnce.Do(func() {
		n.recentEntries.clear()
		n.closeErr = errors.Join(n.writer.close(), n.reader.Close())
	})
	return n.closeErr
}

func (n *journalGroupNode) openStore(config Config) (*journalGroupStore, *pb.Snapshot, error) {
	if config.MaxEntryBytes > journalGroupMaxEntryBytes {
		return nil, nil, fmt.Errorf("journal raft groups support at most %d bytes per encoded entry: %w", journalGroupMaxEntryBytes, ErrTooLarge)
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return nil, nil, errJournalClosed
	}
	if err := n.writer.failure(); err != nil {
		return nil, nil, err
	}
	if n.owners[config.GroupID] {
		return nil, nil, errors.New("raft journal group already has an owner")
	}
	state := n.groups[config.GroupID]
	if state == nil {
		if err := n.provisionGroup(config); err != nil {
			return nil, nil, err
		}
		state = n.groups[config.GroupID]
	}
	if !checkIdentity(state.identity, config) {
		return nil, nil, errors.New("raft journal group identity mismatch")
	}
	snapshot, err := n.readSnapshot(state.snapshot)
	if err != nil {
		return nil, nil, err
	}
	n.owners[config.GroupID] = true
	return &journalGroupStore{node: n, group: config.GroupID, state: state, processDurability: config.ProcessDurability}, snapshot, nil
}

func (n *journalGroupNode) provisionGroup(config Config) error {
	identity := binary.BigEndian.AppendUint64(nil, config.GroupID)
	identity = binary.BigEndian.AppendUint64(identity, config.ID)
	for _, peer := range config.Peers {
		identity = binary.BigEndian.AppendUint64(identity, peer)
	}
	op := journalGroupOp{kind: journalGroupCreate, identity: identity}
	if len(config.Peers) != 0 {
		data, err := config.Machine.Snapshot()
		if err != nil {
			return err
		}
		snap := &pb.Snapshot{
			Metadata: &pb.SnapshotMetadata{Index: new(uint64(1)), Term: new(uint64(1)), ConfState: &pb.ConfState{Voters: config.Peers}},
			Data:     encodeSnapshot(1, data),
		}
		op.snapshot, err = n.stageSnapshot(snap)
		if err != nil {
			return err
		}
		op.hard = &pb.HardState{Term: new(uint64(1)), Commit: new(uint64(1))}
	}
	delta, err := n.persist(config.GroupID, op)
	if err != nil {
		return err
	}
	return n.replayCreate(config.GroupID, delta)
}

func (n *journalGroupNode) persist(group uint64, op journalGroupOp) (journalGroupDelta, error) {
	return n.persistMode(group, op, false)
}

func (n *journalGroupNode) persistMode(group uint64, op journalGroupOp, processEligible bool) (journalGroupDelta, error) {
	size, err := op.size()
	if err != nil {
		return journalGroupDelta{}, err
	}
	var delta journalGroupDelta
	result, err := n.writer.enqueueEncodedMode(group, size, !processEligible, processEligible, &delta, func(body []byte) error {
		var encodeErr error
		delta, encodeErr = op.encode(body)
		return encodeErr
	})
	if err != nil {
		return journalGroupDelta{}, err
	}
	completion := <-result
	if completion.err != nil {
		return journalGroupDelta{}, completion.err
	}
	return delta, nil
}

func (n *journalGroupNode) inventorySnapshots() error {
	live := make(map[string]journalSnapshotRef, len(n.groups))
	for _, state := range n.groups {
		if state.snapshot.metadata != nil {
			if _, exists := live[state.snapshot.name]; exists {
				return fmt.Errorf("%w: shared active snapshot %s", errJournalCorrupt, state.snapshot.name)
			}
			live[state.snapshot.name] = state.snapshot
		}
	}
	files, err := os.ReadDir(n.dir)
	if err != nil {
		return err
	}
	var obsolete []string
	for _, file := range files {
		if strings.HasPrefix(file.Name(), "."+journalSnapshotPrefix) && file.Type().IsRegular() {
			// Only a finalized snapshot name can be journaled. A stale
			// temporary link is safe to remove after exclusive replay.
			obsolete = append(obsolete, file.Name())
			continue
		}
		if !strings.HasPrefix(file.Name(), journalSnapshotPrefix) {
			continue
		}
		info, err := file.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() || info.Size() > maxJournalFrameBytes {
			return fmt.Errorf("%w: invalid snapshot file %s", errJournalCorrupt, file.Name())
		}
		ref, exists := live[file.Name()]
		if !exists {
			obsolete = append(obsolete, file.Name())
			continue
		}
		if info.Size() != int64(ref.size) {
			return fmt.Errorf("%w: wrong size for snapshot %s", errJournalCorrupt, file.Name())
		}
		delete(live, file.Name())
		if n.snapshotBytes.Add(info.Size()) > journalGroupMaxSnapshotBytes {
			return fmt.Errorf("raft journal snapshot directory capacity: %w", errJournalTooLarge)
		}
	}
	for name := range live {
		return fmt.Errorf("%w: missing active snapshot %s", errJournalCorrupt, name)
	}
	for _, name := range obsolete {
		if err := os.Remove(filepath.Join(n.dir, name)); err != nil {
			return err
		}
	}
	return nil
}

func (n *journalGroupNode) replayBatch(batch []journalRecord) error {
	for _, record := range batch {
		delta, err := decodeJournalGroupOp(record)
		if err != nil {
			return err
		}
		if err := n.replayDelta(record.group, delta); err != nil {
			return err
		}
	}
	return nil
}

func (n *journalGroupNode) replayDelta(group uint64, delta journalGroupDelta) error {
	if delta.kind == journalGroupCreate {
		return n.replayCreate(group, delta)
	}
	if n.groups[group] == nil || len(delta.identity) != 0 {
		return fmt.Errorf("%w: unexpected operation for group %d", errJournalCorrupt, group)
	}
	if err := n.groups[group].replay(delta); err != nil {
		return fmt.Errorf("%w: group %d: %v", errJournalCorrupt, group, err)
	}
	return nil
}

func (n *journalGroupNode) replayCreate(group uint64, delta journalGroupDelta) error {
	if n.groups[group] != nil || !validJournalGroupIdentity(group, delta.identity) || len(delta.entries) != 0 {
		return fmt.Errorf("%w: invalid provision record for group %d", errJournalCorrupt, group)
	}
	if len(delta.identity) == 16 {
		if delta.snapshot.metadata != nil || !raft.IsEmptyHardState(delta.hard) {
			return fmt.Errorf("%w: uninitialized group %d already has state", errJournalCorrupt, group)
		}
	} else {
		conf := delta.snapshot.metadata.GetConfState()
		voters := conf.GetVoters()
		valid := len(voters) == len(delta.identity)/8-2
		for i, voter := range voters {
			valid = valid && voter == binary.BigEndian.Uint64(delta.identity[16+i*8:])
		}
		if !valid || delta.snapshot.metadata.GetIndex() != 1 || delta.snapshot.metadata.GetTerm() != 1 ||
			delta.hard.GetTerm() != 1 || delta.hard.GetCommit() != 1 || delta.hard.GetVote() != 0 {
			return fmt.Errorf("%w: invalid initial raft state for group %d", errJournalCorrupt, group)
		}
	}
	if delta.hard == nil {
		delta.hard = &pb.HardState{}
	}
	n.groups[group] = &journalGroupState{identity: bytes.Clone(delta.identity), hard: delta.hard, snapshot: delta.snapshot}
	return nil
}

func validJournalGroupIdentity(group uint64, identity []byte) bool {
	if len(identity) < 16 || len(identity)%8 != 0 || binary.BigEndian.Uint64(identity[:8]) != group ||
		!ValidMemberID(binary.BigEndian.Uint64(identity[8:16])) {
		return false
	}
	if len(identity) == 16 {
		return true
	}
	owner := binary.BigEndian.Uint64(identity[8:16])
	var previous uint64
	var ownerFound bool
	for at := 16; at < len(identity); at += 8 {
		peer := binary.BigEndian.Uint64(identity[at : at+8])
		if peer <= previous || !ValidMemberID(peer) {
			return false
		}
		ownerFound = ownerFound || peer == owner
		previous = peer
	}
	return ownerFound
}

func (s *journalGroupState) replay(delta journalGroupDelta) error {
	switch delta.kind {
	case journalGroupReady:
		if len(delta.entries) == 0 && delta.snapshot.metadata == nil && raft.IsEmptyHardState(delta.hard) {
			return errors.New("journal append has no state update")
		}
		var first uint64
		if len(delta.entries) > 0 {
			first = delta.entries[0].index
			for i, ref := range delta.entries {
				if ref.index-first != uint64(i) || ref.index < first {
					return errors.New("noncontiguous raft entries in journal operation")
				}
			}
		}
		write, err := planGroupLog(s.hard, s.snapshot.metadata, len(s.entries), delta.hard, delta.snapshot.metadata, first, len(delta.entries))
		if err != nil {
			return err
		}
		s.publish(write, delta)
	case journalGroupSnap:
		if len(delta.entries) != 0 || !raft.IsEmptyHardState(delta.hard) {
			return errors.New("journal checkpoint carried log or hard-state updates")
		}
		if err := s.publishCheckpoint(delta.snapshot); err != nil {
			return err
		}
	default:
		return errors.New("unknown journal group operation")
	}
	return nil
}

func (s *journalGroupState) publish(write readyWrite, delta journalGroupDelta) {
	if delta.snapshot.metadata != nil {
		s.snapshot = delta.snapshot
		s.entries = nil
	}
	if len(delta.entries) != 0 {
		s.entries = append(s.entries[:write.offset], delta.entries...)
	}
	s.hard = write.hard
}

func (s *journalGroupState) publishCheckpoint(ref journalSnapshotRef) error {
	base, index := s.snapshot.metadata.GetIndex(), ref.metadata.GetIndex()
	if index <= base || index > s.hard.GetCommit() || index-base > uint64(len(s.entries)) ||
		s.entries[index-base-1].term != ref.metadata.GetTerm() {
		return errors.New("journal checkpoint outside committed log or term mismatch")
	}
	s.entries = slices.Clone(s.entries[index-base:])
	s.snapshot = ref
	return nil
}

func (s *journalGroupStore) InitialState() (*pb.HardState, *pb.ConfState, error) {
	return proto.CloneOf(s.state.hard), pb.EnsureConfState(proto.CloneOf(s.state.snapshot.metadata.GetConfState())), nil
}

func (s *journalGroupStore) FirstIndex() (uint64, error) {
	return s.state.snapshot.metadata.GetIndex() + 1, nil
}

func (s *journalGroupStore) LastIndex() (uint64, error) {
	return s.state.snapshot.metadata.GetIndex() + uint64(len(s.state.entries)), nil
}

func (s *journalGroupStore) Term(index uint64) (uint64, error) {
	base := s.state.snapshot.metadata.GetIndex()
	switch {
	case index < base:
		return 0, raft.ErrCompacted
	case index == base:
		return s.state.snapshot.metadata.GetTerm(), nil
	case index-base > uint64(len(s.state.entries)):
		return 0, raft.ErrUnavailable
	default:
		return s.state.entries[index-base-1].term, nil
	}
}

type journalCommandReader interface {
	io.Reader
	io.ReaderAt
	Size() int64
}

func (s *journalGroupStore) commandSource(entry *pb.Entry) (journalCommandReader, error) {
	base, index := s.state.snapshot.metadata.GetIndex(), entry.GetIndex()
	if index <= base || index-base > uint64(len(s.state.entries)) {
		return nil, fmt.Errorf("%w: no durable record for applied entry %d", errJournalCorrupt, index)
	}
	ref := s.state.entries[index-base-1]
	proposalSize := uint32(len(key{}))
	if ref.index != index || ref.term != entry.GetTerm() || int(ref.dataSize) != len(entry.GetData()) ||
		ref.dataSize < proposalSize || ref.dataOffset > ref.size || ref.dataSize > ref.size-ref.dataOffset {
		return nil, fmt.Errorf("%w: invalid durable command span for entry %d", errJournalCorrupt, index)
	}
	offset := ref.offset + int64(ref.dataOffset) + int64(proposalSize)
	size := int64(ref.dataSize - proposalSize)
	section := io.NewSectionReader(s.node.reader, offset, size)
	if reader, ok := s.node.reader.(*journalSegmentReader); ok {
		return &journalSegmentCommandSource{
			SectionReader: section,
			owner:         reader.store,
			span:          journalRetirementRange{group: s.group, start: offset, end: offset + size},
		}, nil
	}
	return section, nil
}

func (s *journalGroupStore) Entries(lo, hi, maxSize uint64) ([]*pb.Entry, error) {
	base := s.state.snapshot.metadata.GetIndex()
	if lo <= base {
		return nil, raft.ErrCompacted
	}
	if lo > hi || hi-base-1 > uint64(len(s.state.entries)) {
		return nil, raft.ErrUnavailable
	}
	var entries []*pb.Entry
	var size uint64
	var loan journalScratchLoan
	defer func() { s.node.readScratch.release(loan) }()
	for _, ref := range s.state.entries[lo-base-1 : hi-base-1] {
		if len(entries) != 0 && (size >= maxSize || uint64(ref.size) > maxSize-size) {
			break
		}
		if entry := s.node.recentEntries.lookup(ref); entry != nil {
			entries = append(entries, entry)
			size += uint64(ref.size)
			continue
		}
		if int(ref.size) > cap(loan.buffer) {
			s.node.readScratch.release(loan)
			loan = s.node.readScratch.acquire(int(ref.size))
		}
		scratch := loan.buffer[:ref.size]
		if err := readJournalAt(s.node.reader, scratch, ref.offset); err != nil {
			return nil, &raftStoreReadError{err}
		}
		entry := &pb.Entry{}
		if crc32.Checksum(scratch, journalChecksum) != ref.checksum || proto.Unmarshal(scratch, entry) != nil ||
			entry.GetIndex() != ref.index || entry.GetTerm() != ref.term {
			return nil, &raftStoreReadError{fmt.Errorf("%w: invalid entry %d at offset %d", errJournalCorrupt, ref.index, ref.offset)}
		}
		entries = append(entries, entry)
		size += uint64(ref.size)
	}
	return entries, nil
}

func (s *journalGroupStore) Snapshot() (*pb.Snapshot, error) {
	snapshot, err := s.node.readSnapshot(s.state.snapshot)
	if err != nil {
		return nil, &raftStoreReadError{err}
	}
	return snapshot, nil
}

func (s *journalGroupStore) planReady(ready raft.Ready) (readyWrite, error) {
	hard := s.state.hard
	if s.pendingCommit != 0 {
		hard = s.pendingHardState()
		if raft.IsEmptyHardState(ready.HardState) {
			// Include the current commit floor even when Raft has no new
			// HardState. This also fences response-only appends carrying debt.
			ready.HardState = hard
		}
	}
	return planGroupReady(ready, hard, s.state.snapshot.metadata, len(s.state.entries))
}

func (s *journalGroupStore) pendingHardState() *pb.HardState {
	return &pb.HardState{Term: s.state.hard.Term, Vote: s.state.hard.Vote, Commit: new(s.pendingCommit)}
}

func (s *journalGroupStore) deferCommit(write readyWrite) bool {
	// Process recovery needs explicit commit frames: unlike a device-loss model,
	// an intact kernel-written log must recover the latest completed commit.
	if s.node.writer.durability.processAllowed() {
		return false
	}
	ready := write.ready
	persisted := s.state.hard
	if ready.MustSync || len(ready.Entries) != 0 || !raft.IsEmptySnap(ready.Snapshot) ||
		write.hard.GetTerm() != persisted.GetTerm() || write.hard.GetVote() != persisted.GetVote() ||
		write.hard.GetCommit() <= max(s.pendingCommit, persisted.GetCommit()) {
		return false
	}
	s.pendingCommit = write.hard.GetCommit()
	return true
}

func (s *journalGroupStore) syncCommit() error {
	if s.pendingCommit == 0 {
		return nil
	}
	hard := s.pendingHardState()
	if _, err := s.node.persist(s.group, journalGroupOp{kind: journalGroupReady, hard: hard}); err != nil {
		return fmt.Errorf("persist raft commit progress: %w", err)
	}
	s.state.hard = hard
	s.pendingCommit = 0
	return nil
}

func (s *journalGroupStore) persistReady(write readyWrite) (journalGroupDelta, error) {
	ready := write.ready
	if !needsDurableWrite(ready) {
		// A group never submits its next operation before publication of its
		// preceding synced operation, so a response-only Ready is fenced.
		return journalGroupDelta{}, nil
	}
	var snapshot journalSnapshotRef
	if !raft.IsEmptySnap(ready.Snapshot) {
		var err error
		snapshot, err = s.node.stageSnapshot(ready.Snapshot)
		if err != nil {
			return journalGroupDelta{}, err
		}
	}
	delta, err := s.node.persistMode(s.group, journalGroupOp{
		kind: journalGroupReady, hard: ready.HardState, snapshot: snapshot, entries: ready.Entries,
	}, s.processReadyEligible(write))
	if err != nil {
		return journalGroupDelta{}, err
	}
	write.ready = raft.Ready{}
	s.staged = &stagedJournalWrite{write: write, delta: delta}
	return delta, nil
}

func (s *journalGroupStore) publishReady(write readyWrite, delta journalGroupDelta) {
	previous := s.state.snapshot
	s.state.publish(write, delta)
	s.node.recentEntries.remember(delta.entries, write.ready.Entries)
	s.pendingCommit = 0
	s.staged = nil
	if delta.snapshot.metadata != nil {
		s.reclaimSnapshot(previous)
	}
}

func (s *journalGroupStore) reclaimSnapshot(previous journalSnapshotRef) {
	if previous.metadata == nil || previous.name == s.state.snapshot.name {
		return
	}
	// The caller has fenced a replacement or knows this blob was never
	// submitted to the journal. Retain accounting on unlink failure for
	// the exclusive startup sweep.
	if err := os.Remove(filepath.Join(s.node.dir, previous.name)); err == nil {
		s.node.snapshotBytes.Add(-int64(previous.size))
	}
}

func (s *journalGroupStore) checkpointTerm(index uint64) (uint64, error) {
	if index <= s.state.snapshot.metadata.GetIndex() {
		return 0, raft.ErrSnapOutOfDate
	}
	if index > max(s.pendingCommit, s.state.hard.GetCommit()) {
		return 0, errors.New("raft checkpoint exceeds journaled commit")
	}
	return s.Term(index)
}

func (s *journalGroupStore) remainingLogBytes(appliedIndex uint64) (total, applied uint64) {
	for _, entry := range s.state.entries {
		size := uint64(entry.size)
		total += size
		if entry.index <= appliedIndex {
			applied += size
		}
	}
	return total, applied
}

func (s *journalGroupStore) checkpoint(index uint64, conf *pb.ConfState, data []byte) error {
	term, err := s.checkpointTerm(index)
	if err != nil {
		return err
	}
	if err := s.syncCommit(); err != nil {
		return err
	}
	ref, err := s.node.stageSnapshot(&pb.Snapshot{
		Metadata: &pb.SnapshotMetadata{Index: new(index), Term: new(term), ConfState: conf}, Data: data,
	})
	if err != nil {
		return err
	}
	return s.persistCheckpoint(ref)
}

// The caller has validated the checkpoint and made its commit floor durable.
func (s *journalGroupStore) persistCheckpoint(ref journalSnapshotRef) error {
	if _, err := s.node.persist(s.group, journalGroupOp{kind: journalGroupSnap, snapshot: ref}); err != nil {
		return err
	}
	previous := s.state.snapshot
	if err := s.state.publishCheckpoint(ref); err != nil {
		return err
	}
	s.reclaimSnapshot(previous)
	return nil
}

func (s *journalGroupStore) close() error {
	s.once.Do(func() {
		if s.staged != nil {
			s.publishReady(s.staged.write, s.staged.delta)
		}
		s.closeErr = s.syncCommit()
		s.node.mu.Lock()
		delete(s.node.owners, s.group)
		s.node.mu.Unlock()
		if s.closeOwner {
			s.closeErr = errors.Join(s.closeErr, s.node.close())
		}
	})
	return s.closeErr
}

func (s *journalGroupStore) failureSignal() <-chan struct{} { return s.node.writer.failed }

func (s *journalGroupStore) failureCause() error { return s.node.writer.failure() }

// Only append-only ordinary data in a stable one-voter configuration may use
// process completion. Raft MustSync does not distinguish normal entries from
// term/vote changes, so compare the validated hard-state transition directly.
func (s *journalGroupStore) processReadyEligible(write readyWrite) bool {
	if !s.processDurability || !s.node.writer.durability.processAllowed() || write.replacesLog ||
		!raft.IsEmptySnap(write.ready.Snapshot) ||
		write.hard.GetTerm() != s.state.hard.GetTerm() || write.hard.GetVote() != s.state.hard.GetVote() {
		return false
	}
	conf := s.state.snapshot.metadata.GetConfState()
	if len(conf.GetVoters()) != 1 || len(conf.GetVotersOutgoing()) != 0 ||
		len(conf.GetLearners()) != 0 || len(conf.GetLearnersNext()) != 0 || conf.GetAutoLeave() ||
		len(s.state.identity) < 16 || conf.GetVoters()[0] != binary.BigEndian.Uint64(s.state.identity[8:16]) {
		return false
	}
	for _, entry := range write.ready.Entries {
		if entry.GetType() != pb.EntryNormal {
			return false
		}
	}
	return true
}
