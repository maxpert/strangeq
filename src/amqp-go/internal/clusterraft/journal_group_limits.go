package clusterraft

import (
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

const (
	// Cluster mode already uses this maximum for the encoded command,
	// including the proposal key. A larger experimental configuration is
	// rejected before creating a journal group.
	journalGroupMaxEntryBytes = 32 << 20
	// Reserve room for the operation header, hard state, and local control
	// entries that Raft can generate from at most 64 buffered inputs.
	journalReadyHeadroom   = 1 << 20
	journalReadyInputBytes = maxJournalFrameBytes - journalReadyHeadroom
	// Charge each received or proposed entry for at least this many bytes in
	// addition to its encoding. This bounds record headers as well as entry
	// counts below maxJournalOpEntries across a whole accepted Ready.
	journalEntryInputOverhead = maxJournalFrameBytes / maxJournalOpEntries
)

func journalReadyInputLimit(pendingBytes int) uint64 {
	if pendingBytes >= maxJournalFrameBytes {
		return journalReadyInputBytes
	}
	// Writer capacity excludes frame headers but includes the record header.
	// Small extents use less of the general reserve, with at least 4 KiB for
	// local control entries, the operation header, and hard state.
	headroom := min(journalReadyHeadroom, max(4<<10, pendingBytes/16))
	return uint64(max(0, pendingBytes-journalRecordHeaderSize-headroom))
}

func journalProposalInputBytes(commandBytes uint64) uint64 {
	return commandBytes + 32 + journalEntryInputOverhead
}

func (g *Group) proposalInputBytes(dataBytes int) (uint64, error) {
	commandBytes := uint64(dataBytes) + uint64(len(key{}))
	// Include the entry fields and enclosing MsgApp fields so that an
	// accepted proposal also fits peer admission on a stream of this size.
	if commandBytes > g.cfg.MaxEntryBytes || commandBytes+128+journalEntryInputOverhead > g.maxReadyInputBytes {
		return 0, ErrTooLarge
	}
	return journalProposalInputBytes(commandBytes), nil
}

func (g *Group) journalPeerInputBytes(message *pb.Message, encodedSize uint64) (uint64, error) {
	entries := message.GetEntries()
	for _, entry := range entries {
		if uint64(len(entry.GetData())) > g.cfg.MaxEntryBytes {
			return 0, ErrTooLarge
		}
	}
	if message.GetType() == pb.MsgSnap {
		// Snapshots enter the exclusive path after pending appends drain.
		// Their body is written separately from the bounded journal record.
		snapshot := message.GetSnapshot()
		if len(entries) != 0 || len(snapshot.GetData()) > maxJournalFrameBytes ||
			uint64(proto.Size(snapshot.GetMetadata())) > g.maxReadyInputBytes {
			return 0, ErrTooLarge
		}
		return encodedSize, nil
	}
	charge := encodedSize + uint64(len(entries))*journalEntryInputOverhead
	if charge < encodedSize || charge > g.maxReadyInputBytes {
		return 0, ErrTooLarge
	}
	return charge, nil
}
