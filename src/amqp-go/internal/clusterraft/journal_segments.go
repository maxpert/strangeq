package clusterraft

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
)

const (
	journalSegmentLockName        = "segments.lock"
	maxJournalSegmentNameAttempts = 64
	journalSegmentReadSlots       = 8
)

type journalSegmentHooks struct {
	sharedQuota       *journalSegmentQuota
	syncFile          func(*os.File) error
	syncDir           func(string) error
	writeAt           func(*os.File, []byte, int64) (int, error)
	publishManifest   func(install func() error) error
	restoreCheckpoint func(uint64, int64, map[uint64]*journalGroupState) error
	recoveredSeal     func(uint64, int64)
}

type journalSegmentStorage struct {
	dir             string
	owner           *os.File
	hooks           journalSegmentHooks
	quota           *journalSegmentQuota
	checkpointFloor atomic.Int64

	mu                   sync.RWMutex
	manifest             journalSegmentManifest
	checkpointOwners     map[uint64]bool
	end                  int64
	durableEnd           int64 // Physically fenced prefix; used for recovery authority.
	completedEnd         int64 // Complete checked frames available for process reads.
	recoveryErr          error
	retainedSourceOwners uint64
	retainedSources      map[journalRetirementRange]uint64
	captures             map[uint64]*journalCaptureQueue
	readRanges           map[*journalReadRange]struct{}
	retiring             []journalSegmentExtent
	writerClosed         bool
	closed               bool

	readSlots chan struct{}
	reads     sync.WaitGroup
	closeOnce sync.Once
	closeErr  error
}

// journalSegmentOutput is used by a single journalWriter. Closing it does not
// release node ownership or invalidate the reader.
type journalSegmentOutput struct {
	rangeSync       bool
	lastMaintain    int64
	checkpointBuild <-chan journalSegmentCheckpointResult
	index           *journalAppendIndex
	sealedIndex     *journalSealedIndex
	store           *journalSegmentStorage
	file            *os.File
	nextSequence    uint64
	dirty           bool
	failed          error
}

type journalSegmentReader struct{ store *journalSegmentStorage }

func openJournalSegmentsWithQuota(dir string, create bool, extentBytes, retainedBytes int64, hooks journalSegmentHooks,
	visit func([]journalRecord) error,
) (_ *journalSegmentOutput, _ *journalSegmentReader, _ journalScan, err error) {
	if extentBytes < minJournalSegmentFrameBytes || extentBytes > journalGroupMaxFileBytes {
		return nil, nil, journalScan{}, fmt.Errorf("invalid raft journal extent size")
	}
	if retainedBytes <= 0 || retainedBytes > journalGroupMaxFileBytes {
		return nil, nil, journalScan{}, fmt.Errorf("invalid retained journal byte limit")
	}
	if hooks.syncFile == nil {
		hooks.syncFile = syncJournalFile
	}
	if hooks.syncDir == nil {
		hooks.syncDir = syncDirectory
	}
	if hooks.writeAt == nil {
		hooks.writeAt = (*os.File).WriteAt
	}
	if hooks.publishManifest == nil {
		hooks.publishManifest = func(install func() error) error { return install() }
	}
	if create {
		if err := os.MkdirAll(dir, 0700); err != nil {
			return nil, nil, journalScan{}, err
		}
	}
	owner, err := os.OpenFile(filepath.Join(dir, journalSegmentLockName), os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, nil, journalScan{}, err
	}
	defer func() {
		if err != nil {
			_ = owner.Close()
		}
	}()
	if err := lockGroupJournal(owner); err != nil {
		return nil, nil, journalScan{}, err
	}
	// A segmented node never initializes over another journal format.
	if _, err := os.Lstat(filepath.Join(dir, journalActiveName)); !errors.Is(err, os.ErrNotExist) {
		if err == nil {
			err = fmt.Errorf("%w: flat raft journal conflicts with segmented storage", errJournalCorrupt)
		}
		return nil, nil, journalScan{}, err
	}
	quota := &journalSegmentQuota{limit: retainedBytes}
	artifacts, err := inventoryJournalSegmentArtifacts(dir, quota)
	if err != nil {
		return nil, nil, journalScan{}, err
	}
	store := &journalSegmentStorage{
		dir: dir, owner: owner, hooks: hooks, quota: quota,
		readSlots: make(chan struct{}, journalSegmentReadSlots),
	}
	manifest, err := loadJournalSegmentManifest(dir)
	if errors.Is(err, os.ErrNotExist) && create {
		if len(artifacts) != 0 {
			return nil, nil, journalScan{}, fmt.Errorf("%w: missing segment manifest with retained journal artifacts", errJournalCorrupt)
		}
		manifest, err = store.initialize(extentBytes)
	}
	if err != nil {
		return nil, nil, journalScan{}, err
	}
	if manifest.extentBytes != extentBytes {
		return nil, nil, journalScan{}, fmt.Errorf("%w: segment extent size differs from the durable manifest", errJournalCorrupt)
	}
	start := 0
	if manifest.checkpoint.name != "" {
		groups, next, err := store.loadCheckpoint(manifest)
		if err != nil {
			return nil, nil, journalScan{}, err
		}
		if hooks.restoreCheckpoint == nil {
			return nil, nil, journalScan{}, errors.New("opening an anchored raft journal requires a checkpoint restore callback")
		}
		store.checkpointOwners = journalCheckpointOwners(manifest.extents[:next], groups)
		// The caller stages restored groups and tail replay until this open
		// has recovered and synced the authoritative active extent.
		if err := hooks.restoreCheckpoint(manifest.checkpoint.sequence, manifest.checkpoint.end, groups); err != nil {
			return nil, nil, journalScan{}, err
		}
		start = next
	}
	active, state, err := store.replay(manifest, start, visit)
	if err != nil {
		return nil, nil, journalScan{}, err
	}
	if err = hooks.syncDir(dir); err == nil {
		err = hooks.syncDir(filepath.Dir(dir))
	}
	if err != nil {
		_ = active.Close()
		return nil, nil, journalScan{}, err
	}
	if err = store.collectUnselected(manifest, artifacts); err != nil {
		_ = active.Close()
		return nil, nil, journalScan{}, err
	}
	if quota.used > quota.limit {
		_ = active.Close()
		return nil, nil, journalScan{}, fmt.Errorf("%w: recovered journal exceeds retained byte capacity", errJournalTooLarge)
	}
	// These callbacks only stage startup recovery. Drop their captured groups
	// before the writer takes over.
	store.hooks.restoreCheckpoint = nil
	store.hooks.recoveredSeal = nil
	store.manifest, store.end = manifest, state.end
	store.advanceCheckpointFloor(manifest.extents[len(manifest.extents)-1].base, manifest.extentBytes)
	store.durableEnd = state.end
	store.completedEnd = state.end
	if hooks.sharedQuota != nil {
		if err = hooks.sharedQuota.reserve(quota.used); err != nil {
			_ = active.Close()
			return nil, nil, journalScan{}, err
		}
		store.quota = hooks.sharedQuota
	}
	return &journalSegmentOutput{store: store, file: active, nextSequence: state.nextSequence},
		&journalSegmentReader{store: store}, state, nil
}

func (s *journalSegmentStorage) path(id uint64) string {
	return filepath.Join(s.dir, fmt.Sprintf("extent-%016x.raft-journal", id))
}

func (s *journalSegmentStorage) initialize(extentBytes int64) (journalSegmentManifest, error) {
	file, id, err := s.stageExtent(0)
	if err != nil {
		return journalSegmentManifest{}, err
	}
	if err := file.Close(); err != nil {
		return journalSegmentManifest{}, err
	}
	manifest := journalSegmentManifest{extentBytes: extentBytes,
		extents: []journalSegmentExtent{{id: id, firstSequence: 1}}}
	return manifest, manifest.publish(s.dir, s.hooks)
}

func (s *journalSegmentStorage) stageExtent(previousID uint64) (*os.File, uint64, error) {
	id := previousID
	for range maxJournalSegmentNameAttempts {
		if id == math.MaxUint64 {
			break
		}
		id++
		file, err := os.OpenFile(s.path(id), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
		if errors.Is(err, os.ErrExist) {
			continue // This name may belong to an earlier uncertain publication.
		}
		if err != nil {
			return nil, 0, err
		}
		if err = s.hooks.syncFile(file); err == nil {
			err = s.hooks.syncDir(s.dir)
		}
		if err != nil {
			_ = file.Close()
			return nil, 0, err
		}
		return file, id, nil
	}
	return nil, 0, fmt.Errorf("%w: exhausted unused journal extent names", errJournalTooLarge)
}

func (s *journalSegmentStorage) replay(manifest journalSegmentManifest, start int, visit func([]journalRecord) error) (*os.File, journalScan, error) {
	for i := start; i < len(manifest.extents); i++ {
		extent := manifest.extents[i]
		active := i == len(manifest.extents)-1
		file, state, err := s.replayExtent(extent, active, manifest.extentBytes, visit)
		if err != nil {
			return nil, journalScan{}, err
		}
		if active {
			return file, state, nil
		}
		if err := file.Close(); err != nil {
			return nil, journalScan{}, err
		}
		if state.nextSequence != manifest.extents[i+1].firstSequence {
			return nil, journalScan{}, fmt.Errorf("%w: frame sequence changed across sealed extent %d", errJournalCorrupt, extent.id)
		}
		if s.hooks.recoveredSeal != nil && i == len(manifest.extents)-2 {
			s.hooks.recoveredSeal(state.nextSequence-1, state.end)
		}
	}
	return nil, journalScan{}, fmt.Errorf("%w: missing active journal extent", errJournalCorrupt)
}

func (s *journalSegmentStorage) replayExtent(extent journalSegmentExtent, active bool, extentBytes int64,
	visit func([]journalRecord) error,
) (_ *os.File, _ journalScan, err error) {
	path := s.path(extent.id)
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, journalScan{}, fmt.Errorf("%w: missing journal extent %d", errJournalCorrupt, extent.id)
	}
	if err != nil {
		return nil, journalScan{}, err
	}
	if !info.Mode().IsRegular() || info.Size() > extentBytes || extent.base < 0 || info.Size() > math.MaxInt64-extent.base {
		return nil, journalScan{}, fmt.Errorf("%w: invalid journal extent %d", errJournalCorrupt, extent.id)
	}
	flags, sealedBytes := os.O_RDONLY, extent.sealedBytes
	if active {
		flags, sealedBytes = os.O_RDWR, -1
	}
	file, err := os.OpenFile(path, flags, 0600)
	if err != nil {
		return nil, journalScan{}, err
	}
	defer func() {
		if err != nil {
			_ = file.Close()
		}
	}()
	state, err := scanJournal(file, info.Size(), sealedBytes, extent.firstSequence, func(records []journalRecord) error {
		if visit == nil {
			return nil
		}
		for i := range records {
			records[i].location.offset += extent.base
		}
		return visit(records)
	})
	if err != nil {
		return nil, journalScan{}, err
	}
	if active {
		var truncated int64
		if state.partialTail {
			if err := file.Truncate(state.end); err != nil {
				return nil, journalScan{}, err
			}
			truncated = info.Size() - state.end
			state.partialTail = false
		}
		// Reopen must make all selected frames durable before admitting appends.
		if err := s.hooks.syncFile(file); err != nil {
			return nil, journalScan{}, err
		}
		if truncated != 0 {
			s.quota.release(truncated)
		}
	}
	state.end += extent.base
	return file, state, nil
}

func (w *journalSegmentOutput) rotate(offset int64) error {
	s := w.store
	manifest := s.manifest
	if len(manifest.extents) >= maxJournalSegmentCount {
		return fmt.Errorf("%w: raft journal extent count limit", errJournalTooLarge)
	}
	if err := w.Sync(); err != nil {
		return err
	}
	manifest.extents = slices.Clone(manifest.extents)
	last := &manifest.extents[len(manifest.extents)-1]
	last.sealedBytes = offset - last.base
	file, id, err := s.stageExtent(last.id)
	if err != nil {
		return err
	}
	manifest.extents = append(manifest.extents, journalSegmentExtent{
		id: id, base: offset, firstSequence: w.nextSequence,
	})
	if err := manifest.publish(s.dir, s.hooks); err != nil {
		// The manifest may have been renamed before a directory sync failed.
		// Leave both extents in place and let the next exclusive open decide.
		s.failStorage(err)
		_ = file.Close()
		return err
	}
	old := w.file
	w.file = file
	s.mu.Lock()
	s.manifest = manifest
	s.mu.Unlock()
	s.advanceCheckpointFloor(offset, manifest.extentBytes)
	if w.index != nil {
		w.sealedIndex = &journalSealedIndex{
			sequence: w.nextSequence - 1, end: offset, groups: cloneJournalGroups(w.index.shadow.groups),
		}
	}
	return old.Close()
}

func (w *journalSegmentOutput) WriteAt(frame []byte, offset int64) (int, error) {
	n, _, err := w.writeFrame(frame, offset, false)
	return n, err
}

func (w *journalSegmentOutput) writeFrame(frame []byte, offset int64, syncRange bool) (int, bool, error) {
	if w.file == nil {
		return 0, false, errJournalClosed
	}
	if w.failed != nil {
		return 0, false, w.failed
	}
	s := w.store
	size := int64(len(frame))
	if offset != s.end {
		return 0, false, fmt.Errorf("%w: nonappend journal offset %d", errJournalCorrupt, offset)
	}
	if size > s.manifest.extentBytes || offset < 0 || size > math.MaxInt64-offset {
		return 0, false, fmt.Errorf("%w: journal extent or total byte limit", errJournalTooLarge)
	}
	if !validJournalSegmentFrame(frame, w.nextSequence) {
		return 0, false, fmt.Errorf("%w: extent append requires one complete journal frame", errJournalCorrupt)
	}
	if err := w.reserveFrame(size); err != nil {
		return 0, false, err
	}
	writeStarted := false
	defer func() {
		if !writeStarted {
			s.quota.release(size)
		}
	}()
	base := s.manifest.extents[len(s.manifest.extents)-1].base
	if size > s.manifest.extentBytes-(offset-base) {
		if err := w.rotate(offset); err != nil {
			w.failed = err
			s.failStorage(err)
			return 0, false, err
		}
		base = offset
	}
	// Once a write is attempted, keep the full reservation even on a short
	// write or error. Exclusive recovery will inventory the retained bytes.
	writeStarted = true
	var n int
	var synced bool
	var err error
	if syncRange && w.rangeSync && !w.dirty {
		n, synced, err = writeJournalFrame(wrapRangeSyncJournalFile(w.file), frame, offset-base, true)
	} else {
		n, err = s.hooks.writeAt(w.file, frame, offset-base)
	}
	if err == nil && n != len(frame) {
		err = io.ErrShortWrite
	}
	if err != nil {
		w.failed = err
		s.failStorage(err)
		return n, false, err
	}
	w.dirty = !synced
	w.nextSequence++
	s.mu.Lock()
	s.end += size
	if synced {
		s.durableEnd = s.end
		s.completedEnd = s.end
	}
	s.mu.Unlock()
	return n, synced, nil
}

// The writer already checksums the frame payload. Check framing and sequence
// here without making a second pass through entry bodies on every append.
func validJournalSegmentFrame(frame []byte, sequence uint64) bool {
	if len(frame) < minJournalSegmentFrameBytes || sequence == math.MaxUint64 {
		return false
	}
	header := frame[:journalFrameHeaderSize]
	trailer := frame[len(frame)-journalFrameTrailerSize:]
	size := len(frame) - len(header) - len(trailer)
	count := int(binary.BigEndian.Uint32(header[16:20]))
	return string(header[:4]) == "SQJ1" && binary.BigEndian.Uint64(header[4:12]) == sequence &&
		int(binary.BigEndian.Uint32(header[12:16])) == size && size <= maxJournalFrameBytes &&
		count > 0 && count <= maxJournalFrameRecords && count <= size/(journalRecordHeaderSize+1) &&
		binary.BigEndian.Uint32(header[20:24]) == crc32.Checksum(header[:20], journalChecksum) &&
		string(trailer[4:8]) == "SQJE" && binary.BigEndian.Uint64(trailer[8:]) == sequence
}

func (w *journalSegmentOutput) Sync() error {
	if w.file == nil {
		return errJournalClosed
	}
	if w.failed != nil {
		return w.failed
	}
	if w.dirty {
		if err := w.store.hooks.syncFile(w.file); err != nil {
			w.failed = err
			w.store.failStorage(err)
			return err
		}
		w.dirty = false
		w.store.mu.Lock()
		w.store.durableEnd = w.store.end
		w.store.completedEnd = w.store.end
		w.store.mu.Unlock()
	}
	return nil
}

func (w *journalSegmentOutput) Close() error {
	if w.file == nil {
		return nil
	}
	var maintenanceErr error
	if w.index != nil && w.failed == nil && !w.dirty {
		maintenanceErr = w.maintain(true)
	}
	if w.checkpointBuild != nil {
		// A failed writer can't publish any new checkpoint. Recovery
		// collects the unselected index after validating its authority.
		<-w.checkpointBuild
		w.checkpointBuild = nil
	}
	err := w.file.Close()
	w.file = nil
	if err != nil {
		w.store.failStorage(err)
	}
	w.store.mu.Lock()
	w.store.writerClosed = true
	w.store.mu.Unlock()
	return errors.Join(err, maintenanceErr)
}

func (r *journalSegmentReader) ReadAt(data []byte, offset int64) (int, error) {
	return r.readAt(data, offset, (*os.File).ReadAt)
}

func (r *journalSegmentReader) readAt(data []byte, offset int64, diskRead func(*os.File, []byte, int64) (int, error)) (int, error) {
	if offset < 0 {
		return 0, fmt.Errorf("invalid raft journal offset %d", offset)
	}
	if len(data) == 0 {
		return 0, nil
	}
	s := r.store
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return 0, errJournalClosed
	}
	manifest, visibleEnd := s.manifest, s.end
	if offset < manifest.extents[0].base || offset >= visibleEnd {
		s.mu.Unlock()
		return 0, io.EOF
	}
	// Capture the immutable filename view and issue its range under the
	// same lock as retirement. This also protects reads queued for a slot.
	release, err := s.pinReadRangeLocked(offset, offset+min(int64(len(data)), visibleEnd-offset))
	if err != nil {
		s.mu.Unlock()
		return 0, err
	}
	s.reads.Add(1)
	s.mu.Unlock()
	defer s.reads.Done()
	defer release()
	return s.readAtView(manifest, visibleEnd, data, offset, diskRead)
}

// The caller holds a lifetime pin and either an issued range or a source lease.
func (s *journalSegmentStorage) readAtView(manifest journalSegmentManifest, visibleEnd int64,
	data []byte, offset int64, diskRead func(*os.File, []byte, int64) (int, error),
) (int, error) {
	extents := manifest.extents
	s.readSlots <- struct{}{}
	defer func() { <-s.readSlots }()

	index := sort.Search(len(extents), func(i int) bool { return extents[i].base > offset }) - 1
	read := 0
	for ; index < len(extents) && read < len(data); index++ {
		extent := extents[index]
		end := visibleEnd
		if extent.sealedBytes != 0 {
			end = extent.base + extent.sealedBytes
		}
		if offset < extent.base || offset >= end {
			break
		}
		count := int(min(int64(len(data)-read), end-offset))
		file, err := os.Open(s.path(extent.id))
		if err != nil {
			return read, err
		}
		n, err := diskRead(file, data[read:read+count], offset-extent.base)
		closeErr := file.Close()
		read += n
		offset += int64(n)
		if err != nil {
			return read, err
		}
		if closeErr != nil {
			return read, closeErr
		}
	}
	if read < len(data) {
		return read, io.EOF
	}
	return read, nil
}

func (r *journalSegmentReader) Close() error {
	s := r.store
	s.mu.Lock()
	if !s.writerClosed {
		s.mu.Unlock()
		return errors.New("close segmented raft journal reader after the writer")
	}
	s.closed = true
	s.mu.Unlock()
	s.closeOnce.Do(func() {
		s.reads.Wait()
		s.closeErr = s.owner.Close()
	})
	return s.closeErr
}

func (s *journalSegmentStorage) failStorage(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.recoveryErr == nil {
		s.recoveryErr = err
	}
}

func (s *journalSegmentStorage) advanceCheckpointFloor(activeBase, extentBytes int64) {
	const checkpointLagExtents = 4
	s.checkpointFloor.Store(max(0, activeBase-checkpointLagExtents*extentBytes))
}
