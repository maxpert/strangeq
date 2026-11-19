package clusterraft

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"slices"

	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/encoding/protowire"
)

// etcd Raft panics on unexpected Storage errors. Tag local read failures so
// Group stops the affected replica while unrelated groups keep running.
type raftStoreReadError struct{ error }

func (e *raftStoreReadError) Unwrap() error { return e.error }

func checkIdentity(value []byte, config Config) bool {
	if len(value) < 16 || len(value)%8 != 0 ||
		binary.BigEndian.Uint64(value[:8]) != config.GroupID ||
		binary.BigEndian.Uint64(value[8:16]) != config.ID {
		return false
	}
	if len(config.Peers) == 0 {
		return true
	}
	if len(value) != 16+8*len(config.Peers) {
		return false
	}
	for i, id := range config.Peers {
		if binary.BigEndian.Uint64(value[16+8*i:]) != id {
			return false
		}
	}
	return true
}

func syncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

type entryWireMetadata struct {
	index, term          uint64
	dataOffset, dataSize uint32
}

// parseEntryWire validates the Entry wire encoding without copying its body.
// The field numbers are fixed by raftpb/raft.proto.
func parseEntryWire(data []byte) (entryWireMetadata, error) {
	var meta entryWireMetadata
	total := len(data)
	for len(data) > 0 {
		field, wire, size := protowire.ConsumeTag(data)
		if size < 0 {
			return entryWireMetadata{}, protowire.ParseError(size)
		}
		data = data[size:]
		size = protowire.ConsumeFieldValue(field, wire, data)
		if size < 0 {
			return entryWireMetadata{}, protowire.ParseError(size)
		}
		if wire == protowire.VarintType {
			switch field {
			case 2:
				meta.term, _ = protowire.ConsumeVarint(data)
			case 3:
				meta.index, _ = protowire.ConsumeVarint(data)
			}
		}
		if field == 4 && wire == protowire.BytesType {
			body, _ := protowire.ConsumeBytes(data)
			meta.dataOffset = uint32(total - len(data) + size - len(body))
			meta.dataSize = uint32(len(body))
		}
		data = data[size:]
	}
	return meta, nil
}

type readyWrite struct {
	ready       raft.Ready
	hard        *pb.HardState
	offset      uint64
	replacesLog bool
}

func needsDurableWrite(ready raft.Ready) bool {
	return !raft.IsEmptyHardState(ready.HardState) || !raft.IsEmptySnap(ready.Snapshot) || len(ready.Entries) != 0
}

const snapshotVersion = "SQRaft01"

func encodeSnapshot(configIndex uint64, data []byte) []byte {
	out := make([]byte, 0, len(snapshotVersion)+8+len(data))
	out = append(out, snapshotVersion...)
	out = binary.BigEndian.AppendUint64(out, configIndex)
	return append(out, data...)
}

func decodeSnapshot(snapshot *pb.Snapshot) (uint64, []byte, error) {
	data := snapshot.GetData()
	if len(data) < len(snapshotVersion)+8 || string(data[:len(snapshotVersion)]) != snapshotVersion {
		return 0, nil, errors.New("unknown raft snapshot format")
	}
	index := binary.BigEndian.Uint64(data[len(snapshotVersion):])
	if index == 0 || index > snapshot.GetMetadata().GetIndex() {
		return 0, nil, fmt.Errorf("invalid configuration index %d", index)
	}
	return index, data[len(snapshotVersion)+8:], nil
}

func validatePeers(config *Config) error {
	config.Peers = slices.Clone(config.Peers)
	slices.Sort(config.Peers)
	var prior uint64
	for _, id := range config.Peers {
		if !ValidMemberID(id) || id <= prior {
			return errors.New("raft bootstrap peers require distinct positive IDs")
		}
		prior = id
	}
	if len(config.Peers) > 0 && !slices.Contains(config.Peers, config.ID) {
		return errors.New("raft bootstrap peers must include local node")
	}
	return nil
}
