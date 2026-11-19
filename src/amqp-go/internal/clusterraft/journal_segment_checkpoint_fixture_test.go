package clusterraft

import (
	"encoding/binary"
	"fmt"

	"google.golang.org/protobuf/proto"
)

func (s *journalSegmentStorage) pinRead() (journalSegmentManifest, int64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return journalSegmentManifest{}, 0, false
	}
	// Rotation publishes a new immutable extent list. Keep ownership while
	// reading from this view, and release the map lock before any disk I/O.
	s.reads.Add(1)
	return s.manifest, s.end, true
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
