package clusterraft

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"time"

	"go.etcd.io/bbolt"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

var (
	stateBucket = []byte("state-v1")
	logBucket   = []byte("log-v1")
	keyIdentity = []byte("identity")
	keyHard     = []byte("hard-state")
	keySnapshot = []byte("snapshot")
)

const dbName = "raft.db"

type persisted struct {
	hard     *pb.HardState
	snapshot *pb.Snapshot
	entries  []*pb.Entry
}

func Exists(dir string) (bool, error) {
	_, err := os.Stat(filepath.Join(dir, dbName))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return err == nil, err
}

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

func createStore(cfg Config) (*bbolt.DB, error) {
	path := filepath.Join(cfg.Dir, dbName)
	db, err := openStore(cfg, path)
	if !errors.Is(err, os.ErrNotExist) {
		return db, err
	}
	if err := os.MkdirAll(cfg.Dir, 0700); err != nil {
		return nil, err
	}
	if err := provisionStore(cfg, path); err != nil {
		return nil, err
	}
	return openStore(cfg, path)
}

func openStore(cfg Config, path string) (*bbolt.DB, error) {
	options := &bbolt.Options{
		Timeout: time.Second,
		OpenFile: func(path string, flags int, mode os.FileMode) (*os.File, error) {
			return os.OpenFile(path, flags&^os.O_CREATE, mode)
		},
	}
	db, err := bbolt.Open(path, 0600, options)
	if err != nil {
		return nil, err
	}
	err = db.View(func(tx *bbolt.Tx) error {
		state := tx.Bucket(stateBucket)
		if state == nil {
			return errors.New("existing raft store is missing its persistent identity")
		}
		if !checkIdentity(state.Get(keyIdentity), cfg) || tx.Bucket(logBucket) == nil {
			return errors.New("raft store identity or format mismatch")
		}
		return nil
	})
	// A prior process may have stopped just after linking the final path.
	// Sync both directory entries while holding the store lock before use.
	for _, dir := range []string{cfg.Dir, filepath.Dir(cfg.Dir)} {
		if err == nil {
			err = syncDirectory(dir)
		}
	}
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

// Publish the initialized store atomically. A crash before publication leaves
// raft.db absent. openStore locks and syncs the published entry before use.
func provisionStore(cfg Config, path string) error {
	stage, err := os.CreateTemp(cfg.Dir, ".raft-store-*")
	if err != nil {
		return err
	}
	defer os.Remove(stage.Name())
	if err := stage.Close(); err != nil {
		return err
	}
	db, err := bbolt.Open(stage.Name(), 0600, nil)
	if err != nil {
		return err
	}
	defer db.Close()
	err = db.Update(func(tx *bbolt.Tx) error {
		state, err := tx.CreateBucket(stateBucket)
		if err != nil {
			return err
		}
		if _, err := tx.CreateBucket(logBucket); err != nil {
			return err
		}
		id := binary.BigEndian.AppendUint64(nil, cfg.GroupID)
		id = binary.BigEndian.AppendUint64(id, cfg.ID)
		for _, peer := range cfg.Peers {
			id = binary.BigEndian.AppendUint64(id, peer)
		}
		if err := state.Put(keyIdentity, id); err != nil {
			return err
		}
		if len(cfg.Peers) == 0 {
			return nil
		}
		data, err := cfg.Machine.Snapshot()
		if err != nil {
			return err
		}
		snapshot := &pb.Snapshot{
			Data:     encodeSnapshot(1, data),
			Metadata: &pb.SnapshotMetadata{Index: new(uint64(1)), Term: new(uint64(1)), ConfState: &pb.ConfState{Voters: cfg.Peers}},
		}
		if err := putMessage(state, keySnapshot, snapshot); err != nil {
			return err
		}
		return putMessage(state, keyHard, &pb.HardState{Term: new(uint64(1)), Commit: new(uint64(1))})
	})
	if err != nil {
		return err
	}
	// Both paths are in the same directory. A link publishes without
	// replacing an existing store opened by a concurrent provisioner.
	if err := os.Link(stage.Name(), path); errors.Is(err, os.ErrExist) {
		return nil
	} else if err != nil {
		return err
	}
	return os.Remove(stage.Name())
}

func syncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func putMessage(bucket *bbolt.Bucket, key []byte, message proto.Message) error {
	data, err := proto.Marshal(message)
	if err != nil {
		return err
	}
	return bucket.Put(key, data)
}

func loadStore(db *bbolt.DB) (persisted, error) {
	store := persisted{hard: &pb.HardState{}}
	err := db.View(func(tx *bbolt.Tx) error {
		state := tx.Bucket(stateBucket)
		if data := state.Get(keyHard); data != nil {
			if err := proto.Unmarshal(data, store.hard); err != nil {
				return err
			}
		}
		var previous uint64
		if data := state.Get(keySnapshot); data != nil {
			store.snapshot = &pb.Snapshot{}
			if err := proto.Unmarshal(data, store.snapshot); err != nil {
				return err
			}
			previous = store.snapshot.GetMetadata().GetIndex()
			if previous == 0 || store.snapshot.GetMetadata().GetTerm() == 0 || store.snapshot.GetMetadata().GetConfState() == nil {
				return errors.New("invalid persisted raft snapshot")
			}
		}
		if err := tx.Bucket(logBucket).ForEach(func(key, data []byte) error {
			if len(key) != 8 || binary.BigEndian.Uint64(key) != previous+1 {
				return errors.New("gap in persisted raft log")
			}
			entry := &pb.Entry{}
			if err := proto.Unmarshal(data, entry); err != nil {
				return err
			}
			if entry.GetIndex() != previous+1 || entry.GetTerm() == 0 {
				return errors.New("invalid persisted raft entry")
			}
			previous++
			store.entries = append(store.entries, entry)
			return nil
		}); err != nil {
			return err
		}
		if store.hard.GetCommit() > previous || store.hard.GetCommit() < store.snapshot.GetMetadata().GetIndex() {
			return errors.New("invalid persisted raft commit")
		}
		return nil
	})
	return store, err
}

// saveReady atomically commits snapshot, conflicting suffix replacement, and
// HardState. A successful synchronous bbolt commit precedes every Raft reply.
func saveReady(db *bbolt.DB, ready raft.Ready) error {
	if raft.IsEmptyHardState(ready.HardState) && raft.IsEmptySnap(ready.Snapshot) && len(ready.Entries) == 0 {
		return nil
	}
	return db.Update(func(tx *bbolt.Tx) error {
		state, entries := tx.Bucket(stateBucket), tx.Bucket(logBucket)
		if !raft.IsEmptySnap(ready.Snapshot) {
			if err := putMessage(state, keySnapshot, ready.Snapshot); err != nil {
				return err
			}
			// Installing a remote snapshot discards every locally buffered suffix.
			if err := tx.DeleteBucket(logBucket); err != nil {
				return err
			}
			var err error
			entries, err = tx.CreateBucket(logBucket)
			if err != nil {
				return err
			}
		}
		if len(ready.Entries) > 0 {
			prefix := binary.BigEndian.AppendUint64(nil, ready.Entries[0].GetIndex())
			cursor := entries.Cursor()
			for key, _ := cursor.Seek(prefix); key != nil; key, _ = cursor.Seek(prefix) {
				if err := cursor.Delete(); err != nil {
					return err
				}
			}
			for _, entry := range ready.Entries {
				key := binary.BigEndian.AppendUint64(nil, entry.GetIndex())
				if err := putMessage(entries, key, entry); err != nil {
					return err
				}
			}
		}
		if !raft.IsEmptyHardState(ready.HardState) {
			return putMessage(state, keyHard, ready.HardState)
		}
		return nil
	})
}

func saveSnapshot(db *bbolt.DB, snapshot *pb.Snapshot) error {
	return db.Update(func(tx *bbolt.Tx) error {
		if err := putMessage(tx.Bucket(stateBucket), keySnapshot, snapshot); err != nil {
			return err
		}
		limit := binary.BigEndian.AppendUint64(nil, snapshot.GetMetadata().GetIndex())
		cursor := tx.Bucket(logBucket).Cursor()
		for key, _ := cursor.First(); key != nil && bytes.Compare(key, limit) <= 0; key, _ = cursor.First() {
			if err := cursor.Delete(); err != nil {
				return err
			}
		}
		return nil
	})
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
