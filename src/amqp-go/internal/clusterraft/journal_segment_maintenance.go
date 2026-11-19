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
	if !w.maintenance {
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
// reads the sealed prefix on a worker, while the writer may append and rotate.
// It doesn't remove any files while this worker still needs the old manifest.
func (w *journalSegmentOutput) maintain(wait bool) error {
	s := w.store
	if !w.maintenance || w.dirty {
		return nil
	}
	if w.checkpointBuild != nil {
		if !wait {
			select {
			case result := <-w.checkpointBuild:
				return w.finishCheckpoint(result)
			default:
				return nil
			}
		}
		if err := w.finishCheckpoint(<-w.checkpointBuild); err != nil {
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
	last := s.manifest.extents[len(s.manifest.extents)-1]
	if last.base <= s.manifest.checkpoint.end {
		return nil
	}
	ready := make(chan journalSegmentCheckpointResult, 1)
	w.checkpointBuild = ready
	go func() {
		reader := &journalSegmentReader{store: s}
		staged, err := reader.stageCheckpoint(last.firstSequence-1, last.base)
		ready <- journalSegmentCheckpointResult{staged: staged, err: err}
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
	if err := w.installCheckpoint(result.staged); err != nil {
		return err
	}
	s := w.store
	if old := result.staged.previous; old.name != "" {
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
