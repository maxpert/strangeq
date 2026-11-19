package clusterraft

import pb "go.etcd.io/raft/v3/raftpb"

// CheckPeerInputSize checks a decoded peer message against the group's
// storage limits without waiting for its Raft lock.
func (g *Group) CheckPeerInputSize(message *pb.Message, encodedSize uint64) error {
	if g.journalInput {
		_, err := journalPeerInputBytes(message, g.cfg.MaxEntryBytes, encodedSize)
		return err
	}
	for _, entry := range message.GetEntries() {
		if uint64(len(entry.GetData())) > g.cfg.MaxEntryBytes {
			return ErrTooLarge
		}
	}
	return nil
}
