package clusterraft

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"maps"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"

	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

const (
	journalCheckpointMagic       = "SQJIDX02"
	journalCheckpointPrefix      = "checkpoint-"
	journalCheckpointHeader      = 28 // Magic, sequence, offset, group count.
	journalCheckpointGroupHeader = 16 // Group ID, metadata size, entry count.
	journalCheckpointEntryBytes  = 40 // Entry location, checksum and command span.
	journalCheckpointTrailer     = 4
	maxJournalCheckpointName     = 64
	maxJournalCheckpointBytes    = 64 << 20
	maxJournalCheckpointMetadata = 64 << 10
	maxJournalCheckpointGroups   = 1025 // Catalog plus configured data shards.
)

type journalSegmentCheckpointRef struct {
	sequence uint64
	end      int64
	name     string
	size     int64
	checksum uint32
}

func (ref journalSegmentCheckpointRef) valid() bool {
	if ref.sequence == 0 || ref.end <= 0 || ref.size < journalCheckpointHeader+journalCheckpointTrailer ||
		ref.size > maxJournalCheckpointBytes || len(ref.name) > maxJournalCheckpointName {
		return false
	}
	suffix, ok := strings.CutPrefix(ref.name, journalCheckpointPrefix)
	if !ok || suffix == "" {
		return false
	}
	for _, char := range suffix {
		if char < '0' || char > '9' {
			return false
		}
	}
	return true
}

type stagedJournalSegmentCheckpoint struct {
	store    *journalSegmentStorage
	previous journalSegmentCheckpointRef
	ref      journalSegmentCheckpointRef
	owners   map[uint64]bool
}

// stageCheckpoint can run concurrently with normal writes. The captured
// manifest is immutable and the prefix ending at the cutoff is sealed.
func (r *journalSegmentReader) stageCheckpoint(sequence uint64, end int64) (stagedJournalSegmentCheckpoint, error) {
	s := r.store
	manifest, _, ok := s.pinRead()
	if !ok {
		return stagedJournalSegmentCheckpoint{}, errJournalClosed
	}
	defer s.reads.Done()
	if !s.checkpointMu.TryLock() {
		return stagedJournalSegmentCheckpoint{}, errJournalBusy
	}
	defer s.checkpointMu.Unlock()
	cutoff, err := manifest.cutoff(sequence, end)
	if err != nil {
		return stagedJournalSegmentCheckpoint{}, err
	}
	previous := manifest.checkpoint
	if sequence <= previous.sequence || end <= previous.end {
		return stagedJournalSegmentCheckpoint{}, fmt.Errorf("%w: journal checkpoint must advance", errJournalCorrupt)
	}
	// loadCheckpoint stats all covered filenames before replay opens the
	// remaining prefix. Keep their full lifetime visible to cutoff admission.
	releaseRange, err := s.pinReadRange(manifest.extents[0].base, end)
	if err != nil {
		return stagedJournalSegmentCheckpoint{}, err
	}
	defer releaseRange()
	groups, start, err := s.loadCheckpoint(manifest)
	if err != nil {
		return stagedJournalSegmentCheckpoint{}, err
	}
	shadow := &journalGroupNode{groups: groups}
	retainedBytes := int64(journalCheckpointHeader + journalCheckpointTrailer)
	for _, group := range groups {
		retainedBytes += journalCheckpointGroupBytes(group)
	}
	if retainedBytes > maxJournalCheckpointBytes {
		return stagedJournalSegmentCheckpoint{}, fmt.Errorf("%w: journal checkpoint retained state limit", errJournalTooLarge)
	}
	for i := start; i < cutoff; i++ {
		file, state, err := s.replayExtent(manifest.extents[i], false, manifest.extentBytes, func(records []journalRecord) error {
			for _, record := range records {
				if err := replayJournalCheckpointRecord(shadow, &retainedBytes, record); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return stagedJournalSegmentCheckpoint{}, err
		}
		if err := file.Close(); err != nil {
			return stagedJournalSegmentCheckpoint{}, err
		}
		if state.nextSequence != manifest.extents[i+1].firstSequence {
			return stagedJournalSegmentCheckpoint{}, fmt.Errorf("%w: sequence gap through journal checkpoint cutoff", errJournalCorrupt)
		}
	}
	ref, err := s.writeCheckpoint(sequence, end, shadow.groups, manifest.extents[:cutoff])
	if err != nil {
		return stagedJournalSegmentCheckpoint{}, err
	}
	return stagedJournalSegmentCheckpoint{
		store: s, previous: previous, ref: ref, owners: journalCheckpointOwners(manifest.extents[:cutoff], shadow.groups),
	}, nil
}

// The checkpoint decoder and encoder validate each reference against its
// covering extent. The replay tail only introduces offsets from newer frames.
func journalCheckpointOwners(covered []journalSegmentExtent, groups map[uint64]*journalGroupState) map[uint64]bool {
	owners := make(map[uint64]bool)
	for _, group := range groups {
		for _, entry := range group.entries {
			at := sort.Search(len(covered), func(i int) bool { return covered[i].base > entry.offset }) - 1
			owners[covered[at].id] = true
		}
	}
	return owners
}

func journalCheckpointMetadataBytes(state *journalGroupState) int {
	return journalOpHeader + len(state.identity) + proto.Size(state.hard) +
		proto.Size(state.snapshot.metadata) + len(state.snapshot.name)
}

func journalCheckpointGroupBytes(state *journalGroupState) int64 {
	// Charge wire metadata and retained entry capacity. Frame decoding and
	// protobuf object overhead remain separate from this retained-state bound.
	return journalCheckpointGroupHeader + int64(journalCheckpointMetadataBytes(state)) +
		int64(cap(state.entries))*journalCheckpointEntryBytes
}

// Bound both the incoming metadata and its retained replacement before the
// general replay decoder allocates protobuf fields or clones the identity.
func journalCheckpointRecordMetadataBytes(record journalRecord, prior *journalGroupState) (int, error) {
	data := record.body
	if len(data) < journalOpHeader || data[0] != journalGroupVersion {
		return 0, fmt.Errorf("%w: invalid journal checkpoint operation", errJournalCorrupt)
	}
	identity := uint64(binary.BigEndian.Uint32(data[2:6]))
	hard := uint64(binary.BigEndian.Uint32(data[6:10]))
	snapshot := uint64(binary.BigEndian.Uint32(data[10:14]))
	name := uint64(binary.BigEndian.Uint32(data[14:18]))
	size := journalOpHeader + identity + hard + snapshot + name
	if size > uint64(len(data)) {
		return 0, fmt.Errorf("%w: invalid journal checkpoint metadata lengths", errJournalCorrupt)
	}
	if size > maxJournalCheckpointMetadata {
		return 0, fmt.Errorf("%w: journal checkpoint operation metadata limit", errJournalTooLarge)
	}
	if prior != nil {
		if identity == 0 {
			identity = uint64(len(prior.identity))
		}
		// An encoded empty HardState retains the previous state. A nonempty
		// replacement cannot retain more bytes than this conservative bound.
		hard = max(hard, uint64(proto.Size(prior.hard)))
		if snapshot == 0 {
			snapshot = uint64(proto.Size(prior.snapshot.metadata))
			name = uint64(len(prior.snapshot.name))
		}
	}
	size = journalOpHeader + identity + hard + snapshot + name
	if size > maxJournalCheckpointMetadata {
		return 0, fmt.Errorf("%w: journal checkpoint retained metadata limit", errJournalTooLarge)
	}
	return int(size), nil
}

func replayJournalCheckpointRecord(shadow *journalGroupNode, retainedBytes *int64, record journalRecord) error {
	prior := shadow.groups[record.group]
	var priorBytes, entryBytes int64
	if prior != nil {
		priorBytes = journalCheckpointGroupBytes(prior)
		entryBytes = int64(cap(prior.entries)) * journalCheckpointEntryBytes
	} else if len(shadow.groups) >= maxJournalCheckpointGroups {
		return fmt.Errorf("%w: journal checkpoint group limit", errJournalTooLarge)
	}
	metadataBytes, err := journalCheckpointRecordMetadataBytes(record, prior)
	if err != nil {
		return err
	}
	remaining := *retainedBytes - priorBytes
	if remaining+journalCheckpointGroupHeader+int64(metadataBytes)+entryBytes > maxJournalCheckpointBytes {
		return fmt.Errorf("%w: journal checkpoint retained state limit", errJournalTooLarge)
	}
	if err := shadow.replayBatch([]journalRecord{record}); err != nil {
		return err
	}
	state := shadow.groups[record.group]
	*retainedBytes = remaining + journalCheckpointGroupBytes(state)
	if journalCheckpointMetadataBytes(state) > maxJournalCheckpointMetadata || *retainedBytes > maxJournalCheckpointBytes {
		return fmt.Errorf("%w: journal checkpoint retained state limit", errJournalTooLarge)
	}
	return nil
}

// installCheckpoint belongs to the writer between batches. It keeps every
// extent in the latest manifest, including rotations after the build began.
func (w *journalSegmentOutput) installCheckpoint(staged stagedJournalSegmentCheckpoint) error {
	if w.file == nil {
		return errJournalClosed
	}
	if w.failed != nil {
		return w.failed
	}
	s := w.store
	manifest := s.manifest
	if staged.store != s || staged.previous != manifest.checkpoint {
		return fmt.Errorf("%w: stale journal checkpoint build", errJournalBusy)
	}
	if !staged.ref.valid() || staged.ref.sequence <= staged.previous.sequence || staged.ref.end <= staged.previous.end {
		return fmt.Errorf("%w: invalid staged journal checkpoint", errJournalCorrupt)
	}
	manifest.checkpoint = staged.ref
	if err := manifest.validate(); err != nil {
		return err
	}
	if err := manifest.publish(s.dir, s.hooks); err != nil {
		// A failed directory sync can leave the new index authoritative.
		// Preserve both indexes and require the writer to stop.
		w.failed = err
		s.failStorage(err)
		return err
	}
	s.mu.Lock()
	s.checkpointOwners = staged.owners
	s.manifest = manifest
	s.mu.Unlock()
	return nil
}

func (s *journalSegmentStorage) loadCheckpoint(manifest journalSegmentManifest) (map[uint64]*journalGroupState, int, error) {
	ref := manifest.checkpoint
	if ref.name == "" {
		return make(map[uint64]*journalGroupState), 0, nil
	}
	start, err := manifest.cutoff(ref.sequence, ref.end)
	if err != nil {
		return nil, 0, err
	}
	covered := manifest.extents[:start]
	for _, extent := range covered {
		info, err := os.Lstat(s.path(extent.id))
		if errors.Is(err, os.ErrNotExist) {
			return nil, 0, fmt.Errorf("%w: missing covered journal extent %d", errJournalCorrupt, extent.id)
		}
		if err != nil {
			return nil, 0, err
		}
		if !info.Mode().IsRegular() || info.Size() != extent.sealedBytes {
			return nil, 0, fmt.Errorf("%w: wrong size or type of covered journal extent %d", errJournalCorrupt, extent.id)
		}
	}
	groups, err := readJournalCheckpoint(filepath.Join(s.dir, ref.name), ref, covered)
	return groups, start, err
}

func (s *journalSegmentStorage) writeCheckpoint(sequence uint64, end int64, groups map[uint64]*journalGroupState,
	covered []journalSegmentExtent,
) (_ journalSegmentCheckpointRef, err error) {
	if len(groups) > maxJournalCheckpointGroups {
		return journalSegmentCheckpointRef{}, fmt.Errorf("%w: journal checkpoint group limit", errJournalTooLarge)
	}
	ids := slices.Sorted(maps.Keys(groups))
	sizes := make([]int, len(ids))
	total := int64(journalCheckpointHeader + journalCheckpointTrailer)
	for i, group := range ids {
		state := groups[group]
		if err := validateJournalCheckpointGroup(group, state, covered); err != nil {
			return journalSegmentCheckpointRef{}, err
		}
		op := journalGroupOp{kind: journalGroupCreate, identity: state.identity, hard: state.hard, snapshot: state.snapshot}
		size, err := op.size()
		if err != nil {
			return journalSegmentCheckpointRef{}, err
		}
		if size > maxJournalCheckpointMetadata || len(state.entries) > maxJournalCheckpointBytes/journalCheckpointEntryBytes {
			return journalSegmentCheckpointRef{}, fmt.Errorf("%w: journal checkpoint exceeds its size limit", errJournalTooLarge)
		}
		total += journalCheckpointGroupHeader + int64(size) + int64(len(state.entries))*journalCheckpointEntryBytes
		if total > maxJournalCheckpointBytes {
			return journalSegmentCheckpointRef{}, fmt.Errorf("%w: journal checkpoint exceeds its size limit", errJournalTooLarge)
		}
		sizes[i] = size
	}
	if err := s.quota.reserve(total); err != nil {
		return journalSegmentCheckpointRef{}, err
	}
	file, err := os.CreateTemp(s.dir, journalCheckpointPrefix)
	if err != nil {
		s.quota.release(total)
		return journalSegmentCheckpointRef{}, err
	}
	defer func() {
		if err != nil {
			_ = file.Close()
		}
	}()
	// Created indexes remain charged, even if a later write or publication
	// fails. They are never unlinked while authority may be uncertain.
	checksum := crc32.New(journalChecksum)
	buf := bufio.NewWriterSize(io.MultiWriter(file, checksum), 64<<10)
	var header [journalCheckpointHeader]byte
	copy(header[:8], journalCheckpointMagic)
	binary.BigEndian.PutUint64(header[8:16], sequence)
	binary.BigEndian.PutUint64(header[16:24], uint64(end))
	binary.BigEndian.PutUint32(header[24:28], uint32(len(groups)))
	if _, err := buf.Write(header[:]); err != nil {
		return journalSegmentCheckpointRef{}, err
	}
	for i, group := range ids {
		state := groups[group]
		// Reuse the existing wire encoding for group metadata only. Replay
		// of this index does not execute the genesis operation.
		op := journalGroupOp{kind: journalGroupCreate, identity: state.identity, hard: state.hard, snapshot: state.snapshot}
		if err := writeJournalCheckpointGroup(buf, group, sizes[i], op, state.entries); err != nil {
			return journalSegmentCheckpointRef{}, err
		}
	}
	if err := buf.Flush(); err != nil {
		return journalSegmentCheckpointRef{}, err
	}
	crc := checksum.Sum32()
	binary.BigEndian.PutUint32(header[:4], crc)
	if _, err := file.Write(header[:4]); err != nil {
		return journalSegmentCheckpointRef{}, err
	}
	if err := s.hooks.syncFile(file); err != nil {
		return journalSegmentCheckpointRef{}, err
	}
	if err := file.Close(); err != nil {
		return journalSegmentCheckpointRef{}, err
	}
	if err := s.hooks.syncDir(s.dir); err != nil {
		return journalSegmentCheckpointRef{}, err
	}
	return journalSegmentCheckpointRef{sequence: sequence, end: end, name: filepath.Base(file.Name()), size: total, checksum: crc}, nil
}

func writeJournalCheckpointGroup(buf *bufio.Writer, group uint64, size int, op journalGroupOp, entries []journalEntryRef) error {
	var fixed [journalCheckpointEntryBytes]byte
	binary.BigEndian.PutUint64(fixed[:8], group)
	binary.BigEndian.PutUint32(fixed[8:12], uint32(size))
	binary.BigEndian.PutUint32(fixed[12:16], uint32(len(entries)))
	if _, err := buf.Write(fixed[:journalCheckpointGroupHeader]); err != nil {
		return err
	}
	metadata := make([]byte, size)
	if _, err := op.encode(metadata); err != nil {
		return err
	}
	if _, err := buf.Write(metadata); err != nil {
		return err
	}
	for _, entry := range entries {
		binary.BigEndian.PutUint64(fixed[:8], uint64(entry.offset))
		binary.BigEndian.PutUint64(fixed[8:16], entry.index)
		binary.BigEndian.PutUint64(fixed[16:24], entry.term)
		binary.BigEndian.PutUint32(fixed[24:28], entry.size)
		binary.BigEndian.PutUint32(fixed[28:32], entry.checksum)
		binary.BigEndian.PutUint32(fixed[32:36], entry.dataOffset)
		binary.BigEndian.PutUint32(fixed[36:40], entry.dataSize)
		if _, err := buf.Write(fixed[:]); err != nil {
			return err
		}
	}
	return nil
}

func readJournalCheckpoint(path string, ref journalSegmentCheckpointRef, covered []journalSegmentExtent) (map[uint64]*journalGroupState, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("%w: missing selected journal checkpoint", errJournalCorrupt)
	}
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() != ref.size {
		return nil, fmt.Errorf("%w: wrong size or type of selected journal checkpoint", errJournalCorrupt)
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	checksum := crc32.New(journalChecksum)
	limited := &io.LimitedReader{R: file, N: ref.size - journalCheckpointTrailer}
	buf := bufio.NewReaderSize(io.TeeReader(limited, checksum), 64<<10)
	var header [journalCheckpointHeader]byte
	if _, err := io.ReadFull(buf, header[:]); err != nil {
		return nil, fmt.Errorf("%w: read journal checkpoint header: %v", errJournalCorrupt, err)
	}
	count := binary.BigEndian.Uint32(header[24:28])
	if string(header[:8]) != journalCheckpointMagic ||
		binary.BigEndian.Uint64(header[8:16]) != ref.sequence || int64(binary.BigEndian.Uint64(header[16:24])) != ref.end ||
		count > maxJournalCheckpointGroups {
		return nil, fmt.Errorf("%w: invalid journal checkpoint header or cutoff", errJournalCorrupt)
	}
	groups := make(map[uint64]*journalGroupState, count)
	var previous uint64
	for range count {
		group, state, err := readJournalCheckpointGroup(buf, limited, covered)
		if err != nil {
			return nil, err
		}
		if group <= previous {
			return nil, fmt.Errorf("%w: unordered journal checkpoint groups", errJournalCorrupt)
		}
		groups[group], previous = state, group
	}
	if limited.N != 0 || buf.Buffered() != 0 {
		return nil, fmt.Errorf("%w: excess data in journal checkpoint", errJournalCorrupt)
	}
	if _, err := io.ReadFull(file, header[:journalCheckpointTrailer]); err != nil {
		return nil, fmt.Errorf("%w: read journal checkpoint checksum: %v", errJournalCorrupt, err)
	}
	if binary.BigEndian.Uint32(header[:journalCheckpointTrailer]) != ref.checksum || checksum.Sum32() != ref.checksum {
		return nil, fmt.Errorf("%w: journal checkpoint checksum mismatch", errJournalCorrupt)
	}
	return groups, nil
}

func readJournalCheckpointGroup(buf *bufio.Reader, limited *io.LimitedReader, covered []journalSegmentExtent) (uint64, *journalGroupState, error) {
	var fixed [journalCheckpointEntryBytes]byte
	if _, err := io.ReadFull(buf, fixed[:journalCheckpointGroupHeader]); err != nil {
		return 0, nil, fmt.Errorf("%w: read journal checkpoint group header: %v", errJournalCorrupt, err)
	}
	group := binary.BigEndian.Uint64(fixed[:8])
	metadataSize := binary.BigEndian.Uint32(fixed[8:12])
	count := binary.BigEndian.Uint32(fixed[12:16])
	need := int64(metadataSize) + int64(count)*journalCheckpointEntryBytes
	if metadataSize < journalOpHeader || metadataSize > maxJournalCheckpointMetadata ||
		need > limited.N+int64(buf.Buffered()) {
		return 0, nil, fmt.Errorf("%w: invalid journal checkpoint group lengths", errJournalCorrupt)
	}
	metadata := make([]byte, metadataSize)
	if _, err := io.ReadFull(buf, metadata); err != nil {
		return 0, nil, fmt.Errorf("%w: read journal checkpoint group metadata: %v", errJournalCorrupt, err)
	}
	delta, err := decodeJournalGroupOp(journalRecord{group: group, body: metadata})
	if err != nil {
		return 0, nil, err
	}
	if delta.kind != journalGroupCreate || len(delta.entries) != 0 {
		return 0, nil, fmt.Errorf("%w: invalid metadata in journal checkpoint group", errJournalCorrupt)
	}
	if delta.hard == nil {
		delta.hard = &pb.HardState{}
	}
	state := &journalGroupState{
		identity: delta.identity, hard: delta.hard, snapshot: delta.snapshot,
		entries: make([]journalEntryRef, count),
	}
	for i := range state.entries {
		if _, err := io.ReadFull(buf, fixed[:]); err != nil {
			return 0, nil, fmt.Errorf("%w: read journal checkpoint entry reference: %v", errJournalCorrupt, err)
		}
		state.entries[i] = journalEntryRef{
			offset: int64(binary.BigEndian.Uint64(fixed[:8])), index: binary.BigEndian.Uint64(fixed[8:16]),
			term: binary.BigEndian.Uint64(fixed[16:24]), size: binary.BigEndian.Uint32(fixed[24:28]),
			checksum:   binary.BigEndian.Uint32(fixed[28:32]),
			dataOffset: binary.BigEndian.Uint32(fixed[32:36]), dataSize: binary.BigEndian.Uint32(fixed[36:40]),
		}
	}
	return group, state, validateJournalCheckpointGroup(group, state, covered)
}

func validateJournalCheckpointGroup(group uint64, state *journalGroupState, covered []journalSegmentExtent) error {
	if group == 0 || !validJournalGroupIdentity(group, state.identity) {
		return fmt.Errorf("%w: invalid journal checkpoint group identity", errJournalCorrupt)
	}
	snapshot := state.snapshot
	base, term := snapshot.metadata.GetIndex(), snapshot.metadata.GetTerm()
	hard := state.hard
	commit := hard.GetCommit()
	if (snapshot.metadata != nil && !validJournalSnapshot(snapshot)) || base == math.MaxUint64 ||
		hard.GetTerm() < term || (hard.GetTerm() == 0 && hard.GetVote() != 0) ||
		(hard.GetVote() != 0 && !ValidMemberID(hard.GetVote())) ||
		uint64(len(state.entries)) >= math.MaxUint64-base || commit < base || commit-base > uint64(len(state.entries)) {
		return fmt.Errorf("%w: invalid journal checkpoint Raft state for group %d", errJournalCorrupt, group)
	}
	var previousEnd int64
	for i, entry := range state.entries {
		if entry.index != base+uint64(i)+1 || entry.term == 0 || entry.term < term ||
			entry.term > hard.GetTerm() || entry.size == 0 || entry.size > maxJournalFrameBytes || entry.offset < previousEnd ||
			entry.dataOffset > entry.size || entry.dataSize > entry.size-entry.dataOffset {
			return fmt.Errorf("%w: invalid journal checkpoint entry for group %d", errJournalCorrupt, group)
		}
		at := sort.Search(len(covered), func(i int) bool { return covered[i].base > entry.offset }) - 1
		if at < 0 || int64(entry.size) > covered[at].base+covered[at].sealedBytes-entry.offset {
			return fmt.Errorf("%w: journal checkpoint entry outside covered extents", errJournalCorrupt)
		}
		previousEnd, term = entry.offset+int64(entry.size), entry.term
	}
	return nil
}
