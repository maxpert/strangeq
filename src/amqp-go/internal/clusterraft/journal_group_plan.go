package clusterraft

import (
	"errors"
	"math"

	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

// planGroupReady validates the logical transition independently of a storage
// backend. The prepared plan can be persisted without mutating the visible
// suffix, then published under the Group lock before releasing Raft responses.
func planGroupReady(ready raft.Ready, hard *pb.HardState, snapshot *pb.SnapshotMetadata, suffix int) (readyWrite, error) {
	var first uint64
	if len(ready.Entries) > 0 {
		first = ready.Entries[0].GetIndex()
		for i, entry := range ready.Entries {
			index := entry.GetIndex()
			if index < first || index-first != uint64(i) || entry.GetTerm() == 0 {
				return readyWrite{}, errors.New("invalid entry in raft append")
			}
		}
	}
	var incoming *pb.SnapshotMetadata
	if !raft.IsEmptySnap(ready.Snapshot) {
		incoming = ready.Snapshot.GetMetadata()
	}
	write, err := planGroupLog(hard, snapshot, suffix, ready.HardState, incoming, first, len(ready.Entries))
	write.ready = ready
	return write, err
}

func planGroupLog(hard *pb.HardState, snapshot *pb.SnapshotMetadata, suffix int,
	hardUpdate *pb.HardState, incoming *pb.SnapshotMetadata, first uint64, count int,
) (readyWrite, error) {
	write := readyWrite{hard: hard}
	base, length := snapshot.GetIndex(), uint64(suffix)
	if incoming != nil {
		index := incoming.GetIndex()
		if index <= base || index < hard.GetCommit() || index == math.MaxUint64 ||
			incoming.GetTerm() == 0 || incoming.GetConfState() == nil {
			return readyWrite{}, errors.New("invalid incoming raft snapshot")
		}
		write.replacesLog = true
		base, length = index, 0
	}
	if count != 0 {
		if first <= base || first-base-1 > length || first <= hard.GetCommit() ||
			uint64(count-1) >= math.MaxUint64-first {
			return readyWrite{}, errors.New("raft append has a gap, overflow, or overwrites committed state")
		}
		write.offset = first - base - 1
		write.replacesLog = write.replacesLog || write.offset < length
		length = write.offset + uint64(count)
	}
	if !raft.IsEmptyHardState(hardUpdate) {
		if hardUpdate.GetTerm() < hard.GetTerm() || hardUpdate.GetCommit() < hard.GetCommit() ||
			(hardUpdate.GetTerm() == hard.GetTerm() && hard.GetVote() != 0 && hardUpdate.GetVote() != hard.GetVote()) {
			return readyWrite{}, errors.New("raft hard state regresses committed entries, term, or vote")
		}
		write.hard = hardUpdate
	}
	if write.hard.GetCommit() < base || write.hard.GetCommit()-base > length {
		return readyWrite{}, errors.New("raft commit outside persisted log")
	}
	return write, nil
}
