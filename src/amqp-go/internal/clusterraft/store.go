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
	"google.golang.org/protobuf/encoding/protowire"
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

type groupStore interface {
	raft.Storage
	planReady(raft.Ready) (readyWrite, error)
	persistReady(readyWrite) (journalGroupDelta, error)
	publishReady(readyWrite, journalGroupDelta)
	checkpoint(uint64, *pb.ConfState, []byte) error
	failureSignal() <-chan struct{}
	failureCause() error
	close() error
}

type boltGroupStore struct{ *diskStorage }

func openBoltGroupStore(config Config) (groupStore, *pb.Snapshot, error) {
	db, err := createStore(config)
	if err != nil {
		return nil, nil, err
	}
	store, snapshot, err := loadStore(db)
	if err != nil {
		_ = db.Close()
		return nil, nil, err
	}
	return &boltGroupStore{store}, snapshot, nil
}

func (s *boltGroupStore) persistReady(write readyWrite) (journalGroupDelta, error) {
	return journalGroupDelta{}, saveReady(s.db, write.ready)
}

func (s *boltGroupStore) publishReady(write readyWrite, _ journalGroupDelta) {
	s.diskStorage.publishReady(write)
}

func (s *boltGroupStore) close() error { return s.db.Close() }

func (*boltGroupStore) failureSignal() <-chan struct{} { return nil }

func (*boltGroupStore) failureCause() error { return nil }

// diskStorage is owned by Group under its mutex. Index and term lookups do
// not read payloads, and entries and snapshots outlive their read transactions.
type diskStorage struct {
	db       *bbolt.DB
	hard     *pb.HardState
	snapshot *pb.SnapshotMetadata
	terms    []uint64 // The first term follows the snapshot index.
}

// etcd Raft panics on unexpected Storage errors. Tag local read failures so
// Group stops the affected replica while unrelated groups keep running.
type raftStoreReadError struct{ error }

func (e *raftStoreReadError) Unwrap() error { return e.error }

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

func loadStore(db *bbolt.DB) (*diskStorage, *pb.Snapshot, error) {
	store := &diskStorage{db: db, hard: &pb.HardState{}}
	var snapshot *pb.Snapshot
	err := db.View(func(tx *bbolt.Tx) error {
		state := tx.Bucket(stateBucket)
		if data := state.Get(keyHard); data != nil {
			if err := proto.Unmarshal(data, store.hard); err != nil {
				return err
			}
		}
		var previous uint64
		if data := state.Get(keySnapshot); data != nil {
			snapshot = &pb.Snapshot{}
			if err := proto.Unmarshal(data, snapshot); err != nil {
				return err
			}
			store.snapshot = snapshot.GetMetadata()
			previous = store.snapshot.GetIndex()
			if previous == 0 || store.snapshot.GetTerm() == 0 || store.snapshot.GetConfState() == nil {
				return errors.New("invalid persisted raft snapshot")
			}
		}
		if err := tx.Bucket(logBucket).ForEach(func(key, data []byte) error {
			if len(key) != 8 || binary.BigEndian.Uint64(key) != previous+1 {
				return errors.New("gap in persisted raft log")
			}
			index, term, err := entryMetadata(data)
			if err != nil {
				return err
			}
			if index != previous+1 || term == 0 {
				return errors.New("invalid persisted raft entry")
			}
			previous++
			store.terms = append(store.terms, term)
			return nil
		}); err != nil {
			return err
		}
		if store.hard.GetCommit() > previous || store.hard.GetCommit() < store.snapshot.GetIndex() {
			return errors.New("invalid persisted raft commit")
		}
		return nil
	})
	return store, snapshot, err
}

type entryWireMetadata struct {
	index, term          uint64
	dataOffset, dataSize uint32
}

func entryMetadata(data []byte) (index, term uint64, err error) {
	meta, err := parseEntryWire(data)
	return meta.index, meta.term, err
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

func (s *diskStorage) InitialState() (*pb.HardState, *pb.ConfState, error) {
	// Raft's normalization fills absent protobuf fields by mutating its
	// argument. Leave the persisted snapshot metadata intact for comparison.
	conf := proto.CloneOf(s.snapshot.GetConfState())
	return s.hard, pb.EnsureConfState(conf), nil
}

func (s *diskStorage) FirstIndex() (uint64, error) {
	return s.snapshot.GetIndex() + 1, nil
}

func (s *diskStorage) LastIndex() (uint64, error) {
	return s.snapshot.GetIndex() + uint64(len(s.terms)), nil
}

func (s *diskStorage) Term(index uint64) (uint64, error) {
	base := s.snapshot.GetIndex()
	switch {
	case index < base:
		return 0, raft.ErrCompacted
	case index == base:
		return s.snapshot.GetTerm(), nil
	case index-base > uint64(len(s.terms)):
		return 0, raft.ErrUnavailable
	default:
		return s.terms[index-base-1], nil
	}
}

func (s *diskStorage) Entries(lo, hi, maxSize uint64) ([]*pb.Entry, error) {
	base := s.snapshot.GetIndex()
	if lo <= base {
		return nil, raft.ErrCompacted
	}
	if lo > hi || hi-base-1 > uint64(len(s.terms)) {
		return nil, raft.ErrUnavailable
	}
	if lo == hi {
		return nil, nil
	}
	var entries []*pb.Entry
	err := s.db.View(func(tx *bbolt.Tx) error {
		cursor := tx.Bucket(logBucket).Cursor()
		start := binary.BigEndian.AppendUint64(nil, lo)
		key, data := cursor.Seek(start)
		var size uint64
		for index := lo; index < hi; index++ {
			if len(key) != 8 || binary.BigEndian.Uint64(key) != index {
				return fmt.Errorf("missing raft entry %d", index)
			}
			// bbolt stores the protobuf encoding unchanged. Include the first
			// entry even if it exceeds the byte limit, as required by Raft.
			if len(entries) > 0 && (size >= maxSize || uint64(len(data)) > maxSize-size) {
				break
			}
			entry := &pb.Entry{}
			if err := proto.Unmarshal(data, entry); err != nil {
				return fmt.Errorf("decode raft entry %d: %w", index, err)
			}
			if entry.GetIndex() != index || entry.GetTerm() != s.terms[index-base-1] {
				return fmt.Errorf("invalid raft entry %d", index)
			}
			entries = append(entries, entry)
			size += uint64(len(data))
			if size >= maxSize || index+1 == hi {
				break
			}
			key, data = cursor.Next()
		}
		return nil
	})
	if err != nil {
		return nil, &raftStoreReadError{err}
	}
	return entries, nil
}

func (s *diskStorage) Snapshot() (*pb.Snapshot, error) {
	if s.snapshot == nil {
		return &pb.Snapshot{}, nil
	}
	snapshot := &pb.Snapshot{}
	err := s.db.View(func(tx *bbolt.Tx) error {
		if err := proto.Unmarshal(tx.Bucket(stateBucket).Get(keySnapshot), snapshot); err != nil {
			return fmt.Errorf("decode raft snapshot: %w", err)
		}
		if !proto.Equal(snapshot.GetMetadata(), s.snapshot) {
			return errors.New("persisted raft snapshot metadata changed")
		}
		return nil
	})
	if err != nil {
		return nil, &raftStoreReadError{err}
	}
	return snapshot, nil
}

type readyWrite struct {
	ready       raft.Ready
	hard        *pb.HardState
	offset      uint64
	replacesLog bool
}

func (s *diskStorage) planReady(ready raft.Ready) (readyWrite, error) {
	write := readyWrite{ready: ready, hard: s.hard}
	base, length := s.snapshot.GetIndex(), uint64(len(s.terms))
	if !raft.IsEmptySnap(ready.Snapshot) {
		incoming := ready.Snapshot.GetMetadata()
		if incoming.GetIndex() <= base || incoming.GetTerm() == 0 || incoming.GetConfState() == nil {
			return readyWrite{}, errors.New("invalid incoming raft snapshot")
		}
		write.replacesLog = true
		base, length = incoming.GetIndex(), 0
	}
	if len(ready.Entries) > 0 {
		first := ready.Entries[0].GetIndex()
		if first <= base || first-base-1 > length {
			return readyWrite{}, errors.New("gap or overlap with snapshot in raft append")
		}
		write.offset = first - base - 1
		write.replacesLog = write.replacesLog || write.offset < length
		length = write.offset + uint64(len(ready.Entries))
		for i, entry := range ready.Entries {
			if entry.GetIndex() != first+uint64(i) || entry.GetTerm() == 0 {
				return readyWrite{}, errors.New("invalid entry in raft append")
			}
		}
	}
	if !raft.IsEmptyHardState(ready.HardState) {
		write.hard = ready.HardState
	}
	if write.hard.GetCommit() < base || write.hard.GetCommit()-base > length {
		return readyWrite{}, errors.New("raft commit outside persisted log")
	}
	return write, nil
}

func (s *diskStorage) publishReady(write readyWrite) {
	ready := write.ready
	if !raft.IsEmptySnap(ready.Snapshot) {
		s.snapshot = ready.Snapshot.GetMetadata()
		s.terms = nil
	}
	if len(ready.Entries) > 0 {
		s.terms = s.terms[:write.offset]
		for _, entry := range ready.Entries {
			s.terms = append(s.terms, entry.GetTerm())
		}
	}
	s.hard = write.hard
}

func (s *diskStorage) checkpoint(index uint64, conf *pb.ConfState, data []byte) error {
	base := s.snapshot.GetIndex()
	if index <= base {
		return raft.ErrSnapOutOfDate
	}
	if index > s.hard.GetCommit() {
		return errors.New("raft checkpoint exceeds persisted commit")
	}
	term, err := s.Term(index)
	if err != nil {
		return err
	}
	snapshot := &pb.Snapshot{
		Metadata: &pb.SnapshotMetadata{Index: new(index), Term: new(term), ConfState: conf},
		Data:     data,
	}
	if err := saveSnapshot(s.db, snapshot); err != nil {
		return err
	}
	s.snapshot = snapshot.GetMetadata()
	s.terms = slices.Clone(s.terms[index-base:])
	return nil
}

// saveReady atomically commits snapshot, conflicting suffix replacement, and
// HardState. A successful synchronous bbolt commit precedes every Raft reply.
func saveReady(db *bbolt.DB, ready raft.Ready) error {
	if !needsDurableWrite(ready) {
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

func needsDurableWrite(ready raft.Ready) bool {
	return !raft.IsEmptyHardState(ready.HardState) || !raft.IsEmptySnap(ready.Snapshot) || len(ready.Entries) != 0
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
