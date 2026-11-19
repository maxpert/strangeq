package clusterraft

// Process recovery roots distinguish the live kernel image from the last
// physically certified journal closure.
import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
)

const processPhysicalRoot = "process-physical.root"
const processLiveRoot = "process-live.root"
const processRootLimit = 1 << 20
const processRootTempPrefix = ".process-root-"
const processRecoveryRootVersion = 1

type processRecoveryRoot struct {
	Version        uint32 `json:"version"`
	Boot, Epoch    string
	Manifest       []byte
	End            int64
	Next           uint64
	Snapshots      []string
	SnapshotGroups map[string]uint64
}

type journalProcessRoots struct {
	dir, boot, epoch     string
	strict               journalSegmentHooks
	process              atomic.Bool
	mu                   sync.RWMutex
	physical             processRecoveryRoot
	physicalManifest     journalSegmentManifest
	pendingSnapshots     map[uint64][]string
	pendingSnapshotCount atomic.Int64
	retiredSnapshots     map[string]uint64
	snapshotBytes        *atomic.Int64
	liveSealedEnd        int64 // Owner only; explicitly NOT a physical certificate.
	recoverEnd           int64 // -1 scans the selected same-kernel live suffix.
}

func readProcessRecoveryRoot(path string) (processRecoveryRoot, error) {
	file, err := os.Open(path)
	if err != nil {
		return processRecoveryRoot{}, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, processRootLimit+1))
	if err != nil {
		return processRecoveryRoot{}, err
	}
	if len(data) < 4 || len(data) > processRootLimit || binary.BigEndian.Uint32(data[len(data)-4:]) != crc32.Checksum(data[:len(data)-4], journalChecksum) {
		return processRecoveryRoot{}, fmt.Errorf("%w: invalid process recovery root checksum", errJournalCorrupt)
	}
	var root processRecoveryRoot
	if err := json.Unmarshal(data[:len(data)-4], &root); err != nil {
		return root, fmt.Errorf("%w: process root decode", errJournalCorrupt)
	}
	if root.Version != processRecoveryRootVersion {
		return root, fmt.Errorf("%w: unsupported process recovery root version %d", errJournalCorrupt, root.Version)
	}
	if root.Boot == "" || root.Epoch == "" || root.Next == 0 || root.End < 0 {
		return root, fmt.Errorf("%w: process root bounds", errJournalCorrupt)
	}
	if _, err := decodeJournalSegmentManifest(root.Manifest); err != nil {
		return root, err
	}
	for _, name := range root.Snapshots {
		if !strings.HasPrefix(name, journalSnapshotPrefix) || filepath.Base(name) != name {
			return root, errJournalCorrupt
		}
	}
	return root, nil
}

func (r *journalProcessRoots) writeRoot(name string, root processRecoveryRoot, physical bool) error {
	root.Version = processRecoveryRootVersion
	data, err := json.Marshal(root)
	if err != nil {
		return err
	}
	data = binary.BigEndian.AppendUint32(data, crc32.Checksum(data, journalChecksum))
	if len(data) > processRootLimit {
		return errJournalTooLarge
	}
	return r.writeBytes(name, data, physical)
}
func (r *journalProcessRoots) writeBytes(name string, data []byte, physical bool) error {
	file, err := os.CreateTemp(r.dir, processRootTempPrefix)
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	n, err := file.Write(data)
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	if err == nil && physical {
		err = r.strict.syncFile(file)
	}
	err = errors.Join(err, file.Close())
	if err != nil {
		return err
	}
	if err = os.Rename(file.Name(), filepath.Join(r.dir, name)); err != nil {
		return err
	}
	if physical {
		return r.strict.syncDir(r.dir)
	}
	return nil
}
func (r *journalProcessRoots) publishLive() error {
	data, err := os.ReadFile(filepath.Join(r.dir, journalSegmentManifestName))
	if err != nil {
		return err
	}
	if _, err = decodeJournalSegmentManifest(data); err != nil {
		return err
	}
	return r.writeRoot(processLiveRoot, processRecoveryRoot{Version: processRecoveryRootVersion, Boot: r.boot, Epoch: r.epoch, Manifest: data, End: 0, Next: 1}, false)
}

// Called only after the ordinary exclusive directory lock is acquired.
func prepareProcessRoots(dir string, create bool, hooks journalSegmentHooks) (*journalProcessRoots, journalSegmentHooks, error) {
	physical, err := readProcessRecoveryRoot(filepath.Join(dir, processPhysicalRoot))
	if errors.Is(err, os.ErrNotExist) {
		if _, liveErr := os.Stat(filepath.Join(dir, processLiveRoot)); liveErr == nil {
			return nil, hooks, fmt.Errorf("%w: orphan process live root", errJournalCorrupt)
		} else if !errors.Is(liveErr, os.ErrNotExist) {
			return nil, hooks, liveErr
		}
		if !hooks.processRoots {
			return nil, hooks, nil
		}
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, hooks, err
	}
	if hooks.processBootID == "" {
		return nil, hooks, errors.New("process recovery requires explicit kernel boot identity")
	}
	var nonce [16]byte
	if _, e := rand.Read(nonce[:]); e != nil {
		return nil, hooks, e
	}
	r := &journalProcessRoots{dir: dir, boot: hooks.processBootID, epoch: hex.EncodeToString(nonce[:]), strict: hooks, recoverEnd: -1}
	r.process.Store(hooks.processRoots)
	if err == nil {
		r.physical = physical
		r.physicalManifest, err = decodeJournalSegmentManifest(physical.Manifest)
		if err != nil {
			return nil, hooks, err
		}
		selected := physical
		if physical.Boot == r.boot {
			selected, err = readProcessRecoveryRoot(filepath.Join(dir, processLiveRoot))
			if err != nil {
				return nil, hooks, fmt.Errorf("same-kernel live root required: %w", err)
			}
			if selected.Boot != r.boot || selected.Epoch != physical.Epoch {
				return nil, hooks, fmt.Errorf("%w: same-kernel root epoch", errJournalCorrupt)
			}
			r.epoch = physical.Epoch
		} else {
			r.recoverEnd = physical.End
		}
		if err = r.writeBytes(journalSegmentManifestName, selected.Manifest, false); err != nil {
			return nil, hooks, err
		}

	} else {
		if !create {
			return nil, hooks, errors.New("process recovery roots cannot convert an existing journal")
		}
		if _, e := os.Stat(filepath.Join(dir, journalSegmentManifestName)); !errors.Is(e, os.ErrNotExist) {
			return nil, hooks, errors.New("process recovery roots require a fresh journal directory")
		}
	}
	working := hooks
	working.syncFile = func(file *os.File) error {
		if r.process.Load() {
			return nil
		}
		return r.strict.syncFile(file)
	}
	working.syncDir = func(dir string) error {
		if r.process.Load() {
			return nil
		}
		return r.strict.syncDir(dir)
	}
	working.publishManifest = func(install func() error) error {
		if err := r.strict.publishManifest(install); err != nil {
			return err
		}
		return r.publishLive()
	}
	return r, working, nil
}

func (r *journalProcessRoots) pinsExtent(id uint64) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, e := range r.physicalManifest.extents {
		if e.id == id {
			return true
		}
	}
	return false
}
func (r *journalProcessRoots) pinsName(name string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return name == r.physicalManifest.checkpoint.name || slices.Contains(r.physical.Snapshots, name)
}
func (r *journalProcessRoots) preserveArtifacts(artifacts map[string]int64) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, e := range r.physicalManifest.extents {
		delete(artifacts, fmt.Sprintf("extent-%016x.raft-journal", e.id))
	}
	delete(artifacts, r.physicalManifest.checkpoint.name)
}

// Certification always walks the selected closure, even if the current active
// descriptor is clean. A process seal/rename is never a physical certificate.
func (r *journalProcessRoots) syncClosure(w *journalSegmentOutput) error {
	s := w.store
	manifest, err := r.minimalPhysicalManifest(s)
	if err != nil {
		return err
	}
	names := make(map[string]bool)
	for _, e := range manifest.extents {
		names[fmt.Sprintf("extent-%016x.raft-journal", e.id)] = true
	}
	if manifest.checkpoint.name != "" {
		names[manifest.checkpoint.name] = true
	}
	if w.index != nil {
		for _, group := range w.index.shadow.groups {
			if group.snapshot.name != "" {
				names[group.snapshot.name] = true
			}
		}
	}
	for name := range names {
		file, err := os.OpenFile(filepath.Join(s.dir, name), os.O_RDWR, 0)
		if err != nil {
			return err
		}
		err = errors.Join(r.strict.syncFile(file), file.Close())
		if err != nil {
			return err
		}
	}
	return r.strict.syncDir(s.dir)
}
func (r *journalProcessRoots) certify(w *journalSegmentOutput) error {
	s := w.store
	if err := r.syncClosure(w); err != nil {
		return err
	}
	manifest, err := r.minimalPhysicalManifest(s)
	if err != nil {
		return err
	}
	data, err := manifest.encode()
	if err != nil {
		return err
	}
	root := processRecoveryRoot{Version: processRecoveryRootVersion, Boot: r.boot, Epoch: r.epoch, Manifest: data, End: s.end, Next: w.nextSequence, SnapshotGroups: make(map[string]uint64)}
	if w.index != nil {
		for id, g := range w.index.shadow.groups {
			if g.snapshot.name != "" {
				root.Snapshots = append(root.Snapshots, g.snapshot.name)
				root.SnapshotGroups[g.snapshot.name] = id
			}
		}
	}
	if err = r.publishLive(); err != nil {
		return err
	}
	if err = r.writeRoot(processPhysicalRoot, root, true); err != nil {
		return err
	}
	r.mu.Lock()
	previous, previousManifest := r.physical, r.physicalManifest
	for _, name := range previous.Snapshots {
		if !slices.Contains(root.Snapshots, name) {
			// Only an owning Group's post-publication proof permits immediate GC.
			if _, proven := r.retiredSnapshots[name]; proven {
				continue
			}
			id := previous.SnapshotGroups[name]
			if id != 0 {
				if r.pendingSnapshots == nil {
					r.pendingSnapshots = make(map[uint64][]string)
				}
				r.pendingSnapshots[id] = append(r.pendingSnapshots[id], name)
				r.pendingSnapshotCount.Add(1)
			}
		}
	}
	r.physical = root
	r.physicalManifest = manifest
	r.mu.Unlock()
	r.reapRetiredSnapshots()
	if previousManifest.checkpoint.name != "" && previousManifest.checkpoint.name != manifest.checkpoint.name && previousManifest.checkpoint.name != s.manifest.checkpoint.name {
		path := filepath.Join(s.dir, previousManifest.checkpoint.name)
		info, err := os.Stat(path)
		if err != nil {
			return err
		}
		if err = os.Remove(path); err != nil {
			return err
		}
		if err = s.hooks.syncDir(s.dir); err != nil {
			return err
		}
		s.quota.release(info.Size())
	}
	if manifest.checkpoint.valid() {
		if err = w.reclaimSealedFiles(); err != nil {
			return err
		}
	}
	s.mu.Lock()
	s.durableEnd = s.end
	s.mu.Unlock()
	w.dirty = false
	return nil
}

func (r *journalProcessRoots) finishPhysicalRecovery() error {
	physical := r.physical
	physical.Boot, physical.Epoch = r.boot, r.epoch
	if err := r.publishLive(); err != nil {
		return err
	}
	if err := r.writeRoot(processPhysicalRoot, physical, true); err != nil {
		return err
	}
	r.mu.Lock()
	r.physical = physical
	r.mu.Unlock()
	return nil
}

// Existing roots are always discovered, including strict restart after peer
// activation. New process roots use the normal isolated-owner durability option
// on Linux; other platforms retain their existing unrooted journal behavior.
func processRootServingOptions(dir string, create bool, options JournalOptions) (journalSegmentHooks, int64, error) {
	return processRootServingOptionsForPlatform(dir, create, options, runtime.GOOS, os.ReadFile)
}

func processRootServingOptionsForPlatform(dir string, create bool, options JournalOptions, platform string, readBoot func(string) ([]byte, error)) (journalSegmentHooks, int64, error) {
	hooks := journalSegmentHooks{}
	extent := int64(journalServingExtentBytes)
	physical, err := readProcessRecoveryRoot(filepath.Join(dir, processPhysicalRoot))
	exists := err == nil
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return hooks, extent, err
	}
	requested := options.ProcessDurability && platform == "linux"
	if !exists {
		if _, liveErr := os.Stat(filepath.Join(dir, processLiveRoot)); liveErr == nil {
			return hooks, extent, fmt.Errorf("%w: orphan process live root", errJournalCorrupt)
		} else if !errors.Is(liveErr, os.ErrNotExist) {
			return hooks, extent, liveErr
		}
	}
	if !requested && !exists {
		return hooks, extent, nil
	}
	if platform != "linux" {
		return hooks, extent, errors.New("process recovery roots require Linux kernel boot identity")
	}
	if exists {
		manifest, err := decodeJournalSegmentManifest(physical.Manifest)
		if err != nil {
			return hooks, extent, err
		}
		extent = manifest.extentBytes
	} else if create {
		extent = 32 << 20
	} else {
		return hooks, extent, errors.New("process recovery root missing on reopen")
	}
	boot, err := readBoot("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return hooks, extent, fmt.Errorf("process recovery requires Linux kernel boot identity: %w", err)
	}
	hooks.processBootID = strings.TrimSpace(string(boot))
	if hooks.processBootID == "" {
		return hooks, extent, errors.New("process recovery requires nonempty Linux kernel boot identity")
	}
	hooks.processRoots = options.ProcessDurability
	return hooks, extent, nil
}

func (r *journalProcessRoots) minimalPhysicalManifest(s *journalSegmentStorage) (journalSegmentManifest, error) {
	manifest := s.manifest
	if !manifest.checkpoint.valid() {
		return manifest, nil
	}
	cutoff, err := manifest.cutoff(manifest.checkpoint.sequence, manifest.checkpoint.end)
	if err != nil {
		return manifest, err
	}
	extents := make([]journalSegmentExtent, 0, len(manifest.extents))
	for _, extent := range manifest.extents[:cutoff] {
		if s.checkpointOwners[extent.id] {
			extents = append(extents, extent)
		}
	}
	manifest.extents = append(extents, manifest.extents[cutoff:]...)
	return manifest, manifest.validate()
}

// Only the owning Group drains its names after publication or while closing.
// The append writer never waits for Group.mu and never removes an unpublished
// Group snapshot, preventing a writer/Group lock inversion.
func (s *journalGroupStore) sweepProcessSnapshots() {
	reader, ok := s.node.reader.(*journalSegmentReader)
	if !ok || reader.store.processRoots == nil {
		return
	}
	roots := reader.store.processRoots
	if roots.pendingSnapshotCount.Load() == 0 {
		return
	}
	roots.mu.Lock()
	names := roots.pendingSnapshots[s.group]
	delete(roots.pendingSnapshots, s.group)
	roots.pendingSnapshotCount.Add(-int64(len(names)))
	roots.mu.Unlock()
	var pending []string
	for _, name := range names {
		if name == s.state.snapshot.name || roots.pinsName(name) {
			pending = append(pending, name)
			continue
		}
		path := filepath.Join(s.node.dir, name)
		info, err := os.Stat(path)
		if err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				pending = append(pending, name)
			}
			continue
		}
		if err = os.Remove(path); err != nil {
			pending = append(pending, name)
			continue
		}
		s.node.snapshotBytes.Add(-info.Size())
	}
	if len(pending) != 0 {
		roots.mu.Lock()
		roots.pendingSnapshots[s.group] = append(roots.pendingSnapshots[s.group], pending...)
		roots.pendingSnapshotCount.Add(int64(len(pending)))
		roots.mu.Unlock()
	}
}

// Called only by reclaimSnapshot after its owning Group has published a
// replacement (or discarded an unpublished blob). This is stronger evidence
// than a writer index that can run ahead of Group publication.
func (r *journalProcessRoots) deferRetiredSnapshot(group uint64, name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !slices.Contains(r.physical.Snapshots, name) {
		// Certification may have inferred this retirement just before the
		// owning Group arrived at its publication proof. Cancel that retry now.
		queued := r.pendingSnapshots[group]
		kept := queued[:0]
		for _, pending := range queued {
			if pending != name {
				kept = append(kept, pending)
			}
		}
		if len(kept) != len(queued) {
			r.pendingSnapshotCount.Add(int64(len(kept) - len(queued)))
			if len(kept) == 0 {
				delete(r.pendingSnapshots, group)
			} else {
				r.pendingSnapshots[group] = kept
			}
		}
		return false
	}
	if r.retiredSnapshots == nil {
		r.retiredSnapshots = make(map[string]uint64)
	}
	r.retiredSnapshots[name] = group
	return true
}

// Certification can reclaim these proven-retired names even when the owning
// Group is idle. Inferred old names continue through that Group's publication
// barrier. File removal/accounting never occurs while holding the root mutex.
func (r *journalProcessRoots) reapRetiredSnapshots() {
	r.mu.Lock()
	var names []string
	for name := range r.retiredSnapshots {
		if !slices.Contains(r.physical.Snapshots, name) {
			names = append(names, name)
			delete(r.retiredSnapshots, name)
		}
	}
	r.mu.Unlock()
	var retry []string
	for _, name := range names {
		path := filepath.Join(r.dir, name)
		info, err := os.Stat(path)
		if err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				retry = append(retry, name)
			}
			continue
		}
		if err = os.Remove(path); err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				retry = append(retry, name)
			}
			continue
		}
		if r.snapshotBytes != nil {
			r.snapshotBytes.Add(-info.Size())
		}
	}
	if len(retry) != 0 {
		r.mu.Lock()
		for _, name := range retry {
			r.retiredSnapshots[name] = 0
		}
		r.mu.Unlock()
	}
}
