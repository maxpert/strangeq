package clusterraft

import (
	"encoding/binary"
	"fmt"
	"github.com/maxpert/amqp-go/internal/clusterproposal"
	"hash/crc32"

	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

const journalPreparedMaxEntries = 64

// Entry.Data is the final field of the pinned raftpb.Entry encoding. Unknown
// fields, new known fields, non-normal entries and unusual metadata fall back.
// Prefixes own all scalar values; only immutable Ready Data backing is borrowed.
// The 40-byte bound covers journal header + Type + max Term/Index + Data length.
// There is no suffix in this guarded schema.
type journalPreparedEntry struct {
	prefix     [40]byte
	prefixSize uint8
	data       []byte
}

type journalPreparedRecord struct {
	metadata []byte
	entries  []journalPreparedEntry
	size     int
	checksum uint32
}

func (w *journalWriter) preparedProcessAllowed() bool {
	segment, ok := w.file.(*journalSegmentOutput)
	return ok && w.durability.processAllowed() && segment.store.processRoots != nil && segment.store.processRoots.process.Load()
}

func (op journalGroupOp) canPrepareJournalRecord() bool {
	if op.kind != journalGroupReady || op.identity != nil || op.snapshot.metadata != nil || op.snapshot.name != "" || op.snapshot.size != 0 || op.snapshot.checksum != 0 || len(op.entries) == 0 || len(op.entries) > journalPreparedMaxEntries {
		return false
	}
	if op.hard != nil && len(op.hard.ProtoReflect().GetUnknown()) != 0 {
		return false
	}
	large := false
	for _, entry := range op.entries {
		if entry == nil || entry.GetType() != pb.EntryNormal || entry.Term == nil || entry.Index == nil || entry.GetTerm() == 0 || entry.GetIndex() == 0 || len(entry.ProtoReflect().GetUnknown()) != 0 || entry.ProtoReflect().Descriptor().Fields().Len() != 4 {
			return false
		}
		large = large || len(entry.Data) >= journalFrameCRCReuseMinBytes
	}
	return large
}

func (w *journalWriter) enqueuePreparedMode(group uint64, size int, delta *journalGroupDelta, op journalGroupOp) (<-chan journalCompletion, error) {
	return w.enqueueRecordMode(group, size, false, true, delta, func() (journalRecord, journalScratchLoan, error) {
		// Activation can withdraw relaxation while this constructor waits for
		// admission. If so, retain the original staging encoder and sync behavior.
		if !w.preparedProcessAllowed() || !op.canPrepareJournalRecord() {
			staging := w.staging.acquire(size)
			body := staging.buffer[:size:size]
			var err error
			*delta, err = op.encode(body)
			return journalRecord{group: group, body: body}, staging, err
		}
		prepared, d, err := prepareJournalRecord(op, size)
		if err != nil {
			return journalRecord{}, journalScratchLoan{}, err
		}
		*delta = d
		return journalRecord{group: group, prepared: prepared}, journalScratchLoan{}, nil
	})
}

// Runs only in the group's append worker, after admission. All protobuf prefix
// construction and all payload CRC work happen here, never on the node writer.
func prepareJournalRecord(op journalGroupOp, size int) (*journalPreparedRecord, journalGroupDelta, error) {
	hardSize := proto.Size(op.hard)
	prepared := &journalPreparedRecord{metadata: make([]byte, journalOpHeader+hardSize), entries: make([]journalPreparedEntry, len(op.entries)), size: size}
	h := prepared.metadata
	h[0], h[1] = journalGroupVersion, op.kind
	binary.BigEndian.PutUint32(h[6:10], uint32(hardSize))
	binary.BigEndian.PutUint32(h[26:journalOpHeader], uint32(len(op.entries)))
	if err := marshalJournalMessage(h[journalOpHeader:], op.hard); err != nil {
		return nil, journalGroupDelta{}, err
	}
	delta := journalGroupDelta{kind: op.kind, hard: op.hard, entries: make([]journalEntryRef, 0, len(op.entries))}
	checksum := crc32.Checksum(h, journalChecksum)
	at := len(h)
	for i, entry := range op.entries {
		e := &prepared.entries[i]
		prefix := e.prefix[:journalEntryHeader]
		if entry.Type != nil {
			prefix = protowire.AppendVarint(prefix, 8)
			prefix = protowire.AppendVarint(prefix, uint64(entry.GetType()))
		}
		prefix = protowire.AppendVarint(prefix, 16)
		prefix = protowire.AppendVarint(prefix, entry.GetTerm())
		prefix = protowire.AppendVarint(prefix, 24)
		prefix = protowire.AppendVarint(prefix, entry.GetIndex())
		var dataOffset uint32
		if entry.Data != nil {
			prefix = protowire.AppendVarint(prefix, 34)
			prefix = protowire.AppendVarint(prefix, uint64(len(entry.Data)))
			dataOffset = uint32(len(prefix) - journalEntryHeader)
		}
		entrySize := len(prefix) - journalEntryHeader + len(entry.Data)
		if len(prefix) > len(e.prefix) || entrySize != proto.Size(entry) || entrySize > size-at-journalEntryHeader {
			return nil, journalGroupDelta{}, fmt.Errorf("raft journal prepared entry size changed")
		}
		// Prefix capacity is statically bounded; retain its owned fixed array.
		copy(e.prefix[:], prefix)
		e.prefixSize = uint8(len(prefix))
		e.data = entry.Data
		entryCRC, bodyChecksums := prepareJournalBodyChecksums(crc32.Checksum(e.prefix[journalEntryHeader:len(prefix)], journalChecksum), entry.Data, op.bodyCRCRanges)
		if len(bodyChecksums) != 0 {
			if delta.bodyChecksums == nil {
				delta.bodyChecksums = make([][]clusterproposal.BodyChecksum, len(op.entries))
			}
			delta.bodyChecksums[i] = bodyChecksums
		}
		binary.BigEndian.PutUint32(e.prefix[:4], uint32(entrySize))
		binary.BigEndian.PutUint32(e.prefix[4:8], entryCRC)
		start := at + journalEntryHeader
		delta.entries = append(delta.entries, journalEntryRef{offset: int64(start), index: entry.GetIndex(), term: entry.GetTerm(), size: uint32(entrySize), checksum: entryCRC, dataOffset: dataOffset, dataSize: uint32(len(entry.Data))})
		checksum = crc32.Update(checksum, journalChecksum, e.prefix[:journalEntryHeader])
		checksum = journalCombineCRC32C(checksum, entryCRC, uint32(entrySize))
		at = start + entrySize
	}
	if at != size {
		return nil, journalGroupDelta{}, fmt.Errorf("raft journal prepared operation size changed")
	}
	prepared.checksum = checksum
	return prepared, delta, nil
}

func (p *journalPreparedRecord) copyInto(dst []byte) int {
	at := copy(dst, p.metadata)
	for _, entry := range p.entries {
		at += copy(dst[at:], entry.prefix[:entry.prefixSize])
		at += copy(dst[at:], entry.data)
	}
	return at
}
