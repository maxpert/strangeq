package clusterraft

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"

	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

const (
	journalGroupVersion = 1
	journalGroupCreate  = 1
	journalGroupReady   = 2
	journalGroupSnap    = 3
	journalOpHeader     = 30
	journalEntryHeader  = 8
	maxJournalOpEntries = 1 << 20
)

type journalSnapshotRef struct {
	metadata *pb.SnapshotMetadata
	name     string
	size     uint32
	checksum uint32
}

type journalEntryRef struct {
	offset     int64
	index      uint64
	term       uint64
	size       uint32
	checksum   uint32
	dataOffset uint32
	dataSize   uint32
}

type journalGroupOp struct {
	kind     byte
	identity []byte
	hard     *pb.HardState
	snapshot journalSnapshotRef
	entries  []*pb.Entry
}

type journalGroupDelta struct {
	kind     byte
	identity []byte
	hard     *pb.HardState
	snapshot journalSnapshotRef
	entries  []journalEntryRef
}

func (op journalGroupOp) size() (int, error) {
	bodyLimit := maxJournalFrameBytes - journalRecordHeaderSize
	size := journalOpHeader + len(op.identity) + proto.Size(op.hard) + proto.Size(op.snapshot.metadata) + len(op.snapshot.name)
	if size > bodyLimit || len(op.entries) > maxJournalOpEntries {
		return 0, errJournalTooLarge
	}
	for _, entry := range op.entries {
		entrySize := proto.Size(entry)
		if entrySize == 0 || entrySize > bodyLimit-size-journalEntryHeader {
			return 0, errJournalTooLarge
		}
		size += journalEntryHeader + entrySize
	}
	return size, nil
}

func (op journalGroupOp) encode(dst []byte) (journalGroupDelta, error) {
	hardSize, snapshotSize := proto.Size(op.hard), proto.Size(op.snapshot.metadata)
	dst[0], dst[1] = journalGroupVersion, op.kind
	binary.BigEndian.PutUint32(dst[2:6], uint32(len(op.identity)))
	binary.BigEndian.PutUint32(dst[6:10], uint32(hardSize))
	binary.BigEndian.PutUint32(dst[10:14], uint32(snapshotSize))
	binary.BigEndian.PutUint32(dst[14:18], uint32(len(op.snapshot.name)))
	binary.BigEndian.PutUint32(dst[18:22], op.snapshot.size)
	binary.BigEndian.PutUint32(dst[22:26], op.snapshot.checksum)
	binary.BigEndian.PutUint32(dst[26:journalOpHeader], uint32(len(op.entries)))
	at := journalOpHeader
	at += copy(dst[at:], op.identity)
	if err := marshalJournalMessage(dst[at:at+hardSize], op.hard); err != nil {
		return journalGroupDelta{}, err
	}
	at += hardSize
	if err := marshalJournalMessage(dst[at:at+snapshotSize], op.snapshot.metadata); err != nil {
		return journalGroupDelta{}, err
	}
	at += snapshotSize
	at += copy(dst[at:], op.snapshot.name)
	entries := make([]journalEntryRef, 0, len(op.entries))
	for _, entry := range op.entries {
		size := proto.Size(entry)
		start := at + journalEntryHeader
		body := dst[start : start+size]
		if err := marshalJournalMessage(body, entry); err != nil {
			return journalGroupDelta{}, err
		}
		meta, err := parseEntryWire(body)
		if err != nil {
			return journalGroupDelta{}, err
		}
		checksum := crc32.Checksum(body, journalChecksum)
		binary.BigEndian.PutUint32(dst[at:at+4], uint32(size))
		binary.BigEndian.PutUint32(dst[at+4:start], checksum)
		entries = append(entries, journalEntryRef{
			offset: int64(start), index: entry.GetIndex(), term: entry.GetTerm(), size: uint32(size), checksum: checksum,
			dataOffset: meta.dataOffset, dataSize: meta.dataSize,
		})
		at = start + size
	}
	return journalGroupDelta{kind: op.kind, identity: op.identity, hard: op.hard, snapshot: op.snapshot, entries: entries}, nil
}

func marshalJournalMessage(dst []byte, message proto.Message) error {
	if len(dst) == 0 {
		return nil
	}
	out, err := proto.MarshalOptions{}.MarshalAppend(dst[:0:len(dst)], message)
	if err != nil {
		return err
	}
	if len(out) != len(dst) || &out[0] != &dst[0] {
		return fmt.Errorf("raft journal message size changed during encoding")
	}
	return nil
}

func decodeJournalGroupOp(record journalRecord) (journalGroupDelta, error) {
	if len(record.body) < journalOpHeader || record.body[0] != journalGroupVersion {
		return journalGroupDelta{}, fmt.Errorf("%w: invalid journal operation at offset %d", errJournalCorrupt, record.location.offset)
	}
	data := record.body
	kind := data[1]
	identitySize := uint64(binary.BigEndian.Uint32(data[2:6]))
	hardSize := uint64(binary.BigEndian.Uint32(data[6:10]))
	metaSize := uint64(binary.BigEndian.Uint32(data[10:14]))
	nameSize := uint64(binary.BigEndian.Uint32(data[14:18]))
	snapshotSize := binary.BigEndian.Uint32(data[18:22])
	checksum := binary.BigEndian.Uint32(data[22:26])
	entryCount := uint64(binary.BigEndian.Uint32(data[26:journalOpHeader]))
	fixedEnd := journalOpHeader + identitySize + hardSize + metaSize + nameSize
	if fixedEnd > uint64(len(data)) || entryCount > maxJournalOpEntries ||
		entryCount > (uint64(len(data))-fixedEnd)/(journalEntryHeader+1) ||
		nameSize > 128 || snapshotSize > maxJournalFrameBytes {
		return journalGroupDelta{}, fmt.Errorf("%w: invalid journal operation lengths at offset %d", errJournalCorrupt, record.location.offset)
	}
	at := uint64(journalOpHeader)
	identity := data[at : at+identitySize]
	at += identitySize
	delta := journalGroupDelta{kind: kind, identity: identity}
	if hardSize != 0 {
		delta.hard = &pb.HardState{}
		if err := proto.Unmarshal(data[at:at+hardSize], delta.hard); err != nil {
			return journalGroupDelta{}, fmt.Errorf("%w: decode hard state: %v", errJournalCorrupt, err)
		}
	}
	at += hardSize
	if metaSize != 0 {
		meta := &pb.SnapshotMetadata{}
		if err := proto.Unmarshal(data[at:at+metaSize], meta); err != nil {
			return journalGroupDelta{}, fmt.Errorf("%w: decode snapshot metadata: %v", errJournalCorrupt, err)
		}
		name := string(data[at+metaSize : fixedEnd])
		delta.snapshot = journalSnapshotRef{metadata: meta, name: name, size: snapshotSize, checksum: checksum}
		if !validJournalSnapshot(delta.snapshot) {
			return journalGroupDelta{}, fmt.Errorf("%w: invalid snapshot reference", errJournalCorrupt)
		}
	} else if nameSize != 0 || snapshotSize != 0 || checksum != 0 {
		return journalGroupDelta{}, fmt.Errorf("%w: snapshot reference without metadata", errJournalCorrupt)
	}
	delta.entries = make([]journalEntryRef, 0, entryCount)
	at = fixedEnd
	for range entryCount {
		if uint64(len(data))-at < journalEntryHeader {
			return journalGroupDelta{}, fmt.Errorf("%w: short journal entry header", errJournalCorrupt)
		}
		size := binary.BigEndian.Uint32(data[at : at+4])
		checksum = binary.BigEndian.Uint32(data[at+4 : at+journalEntryHeader])
		at += journalEntryHeader
		if size == 0 || uint64(size) > uint64(len(data))-at {
			return journalGroupDelta{}, fmt.Errorf("%w: short journal entry body", errJournalCorrupt)
		}
		entry := data[at : at+uint64(size)]
		meta, err := parseEntryWire(entry)
		if err != nil || meta.index == 0 || meta.term == 0 || crc32.Checksum(entry, journalChecksum) != checksum {
			return journalGroupDelta{}, fmt.Errorf("%w: invalid journal entry metadata or checksum", errJournalCorrupt)
		}
		delta.entries = append(delta.entries, journalEntryRef{
			offset: record.location.offset + int64(at), index: meta.index, term: meta.term, size: size, checksum: checksum,
			dataOffset: meta.dataOffset, dataSize: meta.dataSize,
		})
		at += uint64(size)
	}
	if at != uint64(len(data)) {
		return journalGroupDelta{}, fmt.Errorf("%w: excess bytes after journal operation", errJournalCorrupt)
	}
	return delta, nil
}
