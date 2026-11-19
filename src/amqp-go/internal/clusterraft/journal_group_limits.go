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
	journalReadyInputBytes = maxJournalFrameBytes - (1 << 20)
	// Charge each received or proposed entry for at least this many bytes in
	// addition to its encoding. This bounds record headers as well as entry
	// counts below maxJournalOpEntries across a whole accepted Ready.
	journalEntryInputOverhead = maxJournalFrameBytes / maxJournalOpEntries
)

func journalPeerInputBytes(message *pb.Message, maxEntryBytes, encodedSize uint64) (uint64, error) {
	entries := message.GetEntries()
	for _, entry := range entries {
		if uint64(len(entry.GetData())) > maxEntryBytes {
			return 0, ErrTooLarge
		}
	}
	if message.GetType() == pb.MsgSnap {
		// Snapshots enter the exclusive path after pending appends drain.
		// Their body is written separately from the bounded journal record.
		snapshot := message.GetSnapshot()
		if len(entries) != 0 || len(snapshot.GetData()) > maxJournalFrameBytes ||
			proto.Size(snapshot.GetMetadata()) > journalReadyInputBytes {
			return 0, ErrTooLarge
		}
		return encodedSize, nil
	}
	charge := encodedSize + uint64(len(entries))*journalEntryInputOverhead
	if charge < encodedSize || charge > journalReadyInputBytes {
		return 0, ErrTooLarge
	}
	return charge, nil
}
