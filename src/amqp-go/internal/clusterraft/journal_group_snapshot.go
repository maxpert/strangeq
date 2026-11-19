package clusterraft

import (
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"

	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

const journalSnapshotPrefix = "raft-snap-"

func validJournalSnapshot(ref journalSnapshotRef) bool {
	return ref.metadata.GetIndex() != 0 && ref.metadata.GetTerm() != 0 && ref.metadata.GetConfState() != nil &&
		ref.size > 0 && ref.size <= maxJournalFrameBytes && strings.HasPrefix(ref.name, journalSnapshotPrefix) &&
		len(ref.name) > len(journalSnapshotPrefix) && !strings.ContainsAny(ref.name, "/\\\x00")
}

func (n *journalGroupNode) stageSnapshot(snapshot *pb.Snapshot) (journalSnapshotRef, error) {
	data := snapshot.GetData()
	if len(data) == 0 || len(data) > maxJournalFrameBytes || snapshot.GetMetadata().GetIndex() == 0 ||
		snapshot.GetMetadata().GetTerm() == 0 || snapshot.GetMetadata().GetConfState() == nil {
		return journalSnapshotRef{}, fmt.Errorf("invalid raft journal snapshot")
	}
	if bytes := n.snapshotBytes.Add(int64(len(data))); bytes > journalGroupMaxSnapshotBytes {
		n.snapshotBytes.Add(-int64(len(data)))
		return journalSnapshotRef{}, fmt.Errorf("raft journal snapshot directory capacity: %w", errJournalTooLarge)
	}
	published := false
	defer func() {
		if !published {
			n.snapshotBytes.Add(-int64(len(data)))
		}
	}()

	// Limit copying and syncing snapshot blobs to one at a time. The shared
	// append writer holds no mutex or admission while a snapshot is staged.
	n.snapshotMu.Lock()
	defer n.snapshotMu.Unlock()
	file, err := os.CreateTemp(n.dir, "."+journalSnapshotPrefix+"*")
	if err != nil {
		return journalSnapshotRef{}, err
	}
	defer os.Remove(file.Name())
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return journalSnapshotRef{}, err
	}
	if err := n.syncFile(file); err != nil {
		_ = file.Close()
		return journalSnapshotRef{}, err
	}
	if err := file.Close(); err != nil {
		return journalSnapshotRef{}, err
	}
	name := strings.TrimPrefix(filepath.Base(file.Name()), ".")
	if err := os.Link(file.Name(), filepath.Join(n.dir, name)); err != nil {
		return journalSnapshotRef{}, err
	}
	published = true
	if err := n.syncDir(n.dir); err != nil {
		return journalSnapshotRef{}, err
	}
	return journalSnapshotRef{
		metadata: proto.CloneOf(snapshot.GetMetadata()), name: name,
		size: uint32(len(data)), checksum: crc32.Checksum(data, journalChecksum),
	}, nil
}

func (n *journalGroupNode) readSnapshot(ref journalSnapshotRef) (*pb.Snapshot, error) {
	if ref.metadata == nil {
		return &pb.Snapshot{}, nil
	}
	if !validJournalSnapshot(ref) {
		return nil, fmt.Errorf("%w: invalid raft journal snapshot reference", errJournalCorrupt)
	}
	path := filepath.Join(n.dir, ref.name)
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() != int64(ref.size) {
		return nil, fmt.Errorf("%w: wrong size or type for snapshot %s", errJournalCorrupt, ref.name)
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err = file.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() != int64(ref.size) {
		return nil, fmt.Errorf("%w: wrong size for snapshot %s", errJournalCorrupt, ref.name)
	}
	data := make([]byte, ref.size)
	if _, err := io.ReadFull(file, data); err != nil {
		return nil, err
	}
	if crc32.Checksum(data, journalChecksum) != ref.checksum {
		return nil, fmt.Errorf("%w: invalid snapshot checksum for %s", errJournalCorrupt, ref.name)
	}
	return &pb.Snapshot{Data: data, Metadata: proto.CloneOf(ref.metadata)}, nil
}
