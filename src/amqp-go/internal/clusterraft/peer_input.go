package clusterraft

import pb "go.etcd.io/raft/v3/raftpb"

// CheckPeerInputSize checks a decoded peer message against the group's
// storage limits without waiting for its Raft lock.
func (g *Group) CheckPeerInputSize(message *pb.Message, encodedSize uint64) error {
	_, err := journalPeerInputBytes(message, g.cfg.MaxEntryBytes, encodedSize)
	return err
}
