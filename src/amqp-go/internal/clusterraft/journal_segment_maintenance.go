package clusterraft

import (
	"fmt"
	"os"
	"path/filepath"
)

type journalSegmentCheckpointResult struct {
	staged stagedJournalSegmentCheckpoint
	err    error
}

func (w *journalSegmentOutput) reserveFrame(size int64) error {
	q := w.store.quota
	if w.index == nil {
		return q.reserve(size)
	}
	// Each stream can stage a replacement index independently.
	headroom := min(int64(maxJournalCheckpointBytes)*max(1, q.streams), q.limit/8)
	if err := q.reserveWithHeadroom(size, headroom); err == nil {
		return nil
	}
	if err := w.Sync(); err != nil {
		return err
	}
	if err := w.maintain(true); err != nil {
		return err
	}
	if err := q.reserveWithHeadroom(size, headroom); err != nil {
		return fmt.Errorf("%w (%s)", err, w.store.retentionSummary())
	}
	return nil
}

// The writer alone selects checkpoints and retires files. Index construction
// writes a metadata image captured at the last rotation. The worker needs
// only the immutable references built by successful appends.
func (w *journalSegmentOutput) maintain(wait bool) error {
	return w.maintainSealed(wait, false)
}

// Process completion can leave the active suffix dirty indefinitely. Its
// owner may maintain the separately fenced sealed prefix without syncing or
// changing either dirty flag. Other profiles retain their existing schedule.
func (w *journalSegmentOutput) maintainSealed(wait, process bool) error {
	s := w.store
	if w.index == nil || (w.dirty && !process) {
		return nil
	}
	if w.checkpointBuild != nil {
		if !wait {
			select {
			case result := <-w.checkpointBuild:
				if err := w.finishCheckpoint(result); err != nil {
					return err
				}
			default:
				return nil
			}
			// A process writer may have rolled again while the old build ran.
			// Continue to its latest sealed cut before becoming idle.
			if !process {
				return nil
			}
		} else if err := w.finishCheckpoint(<-w.checkpointBuild); err != nil {
			return err
		}
	} else if !wait && s.end-w.lastMaintain < max(1, s.manifest.extentBytes/8) {
		return nil
	}
	w.lastMaintain = s.end
	if s.manifest.checkpoint.valid() {
		if err := w.reclaimSealedFiles(); err != nil {
			return err
		}
	}
	sealed := w.sealedIndex
	if sealed == nil {
		return nil
	}
	w.sealedIndex = nil
	manifest := s.manifest
	cutoff, err := w.sealedCheckpointCutoff(sealed.sequence, sealed.end)
	if err != nil {
		return err
	}
	ready := make(chan journalSegmentCheckpointResult, 1)
	w.checkpointBuild = ready
	go func() {
		covered := manifest.extents[:cutoff]
		ref, err := s.writeCheckpoint(sealed.sequence, sealed.end, sealed.groups, covered)
		result := journalSegmentCheckpointResult{err: err}
		if err == nil {
			result.staged = stagedJournalSegmentCheckpoint{
				store: s, previous: manifest.checkpoint, ref: ref, owners: journalCheckpointOwners(covered, sealed.groups),
			}
		}
		ready <- result
		// The bounded wake merely schedules owner-thread publication. A builder
		// never mutates the manifest, reclaims files, or fences the active suffix.
		if process && w.checkpointReady != nil {
			w.checkpointReady()
		}
	}()
	if wait {
		return w.finishCheckpoint(<-ready)
	}
	return nil
}

func (w *journalSegmentOutput) finishCheckpoint(result journalSegmentCheckpointResult) error {
	w.checkpointBuild = nil
	if result.err != nil {
		return result.err
	}
	if _, err := w.sealedCheckpointCutoff(result.staged.ref.sequence, result.staged.ref.end); err != nil {
		return err
	}
	if err := w.installCheckpoint(result.staged); err != nil {
		return err
	}
	s := w.store
	if old := result.staged.previous; old.name != "" && (s.processRoots == nil || !s.processRoots.pinsName(old.name)) {
		if err := os.Remove(filepath.Join(s.dir, old.name)); err != nil {
			return err
		}
		if err := s.hooks.syncDir(s.dir); err != nil {
			return err
		}
		s.quota.release(old.size)
	}
	return w.reclaimSealedFiles()
}

// A readable process frame is never checkpoint authority. Both a captured
// seal and a completed build must fit the physically fenced sealed prefix.
func (w *journalSegmentOutput) sealedCheckpointCutoff(sequence uint64, end int64) (int, error) {
	s := w.store
	s.mu.RLock()
	durableEnd := s.durableEnd
	if s.processRoots != nil {
		durableEnd = s.processRoots.liveSealedEnd
	}
	s.mu.RUnlock()
	if end > durableEnd {
		return 0, fmt.Errorf("%w: checkpoint exceeds physically durable journal prefix", errJournalCorrupt)
	}
	return s.manifest.cutoff(sequence, end)
}
