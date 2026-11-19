package clusterraft

import (
	"fmt"
	"os"
	"sort"
)

// Only the writer calls reclaimSealedFiles, between durable append batches.
// The selected checkpoint and current suffix must be sufficient for replay.
func (w *journalSegmentOutput) reclaimSealedFiles() error {
	s := w.store
	reader := &journalSegmentReader{store: s}
	manifest, removed, release, err := reader.reserveReclaim()
	if err != nil || len(removed) == 0 {
		return err
	}
	defer release()

	if err := manifest.publish(s.dir, s.hooks); err != nil {
		// Even if rename succeeded, the directory fence may have failed.
		// Both possible recovery authorities must retain their files.
		w.failed = err
		s.failStorage(err)
		return err
	}
	s.mu.Lock()
	s.manifest = manifest
	s.mu.Unlock()
	var reclaimed int64
	for _, extent := range removed {
		if err := s.closeReadFile(extent.id); err != nil {
			return fmt.Errorf("close retired journal extent %d: %w", extent.id, err)
		}
		if err := os.Remove(s.path(extent.id)); err != nil {
			return fmt.Errorf("remove retired journal extent %d: %w", extent.id, err)
		}
		reclaimed += extent.sealedBytes
	}
	if err := s.hooks.syncDir(s.dir); err != nil {
		return fmt.Errorf("sync retired journal extents: %w", err)
	}
	s.quota.release(reclaimed)
	return nil
}

// reserveReclaim selects sealed files with no replay root, live message, or
// issued read. The owner lock makes selection and read admission atomic.
func (r *journalSegmentReader) reserveReclaim() (
	journalSegmentManifest, []journalSegmentExtent, func(), error,
) {
	s := r.store
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.writerClosed {
		return journalSegmentManifest{}, nil, nil, errJournalClosed
	}
	if s.recoveryErr != nil {
		return journalSegmentManifest{}, nil, nil, s.recoveryErr
	}
	if len(s.retiring) != 0 {
		return journalSegmentManifest{}, nil, nil, errJournalBusy
	}
	manifest := s.manifest
	covered, err := manifest.cutoff(manifest.checkpoint.sequence, manifest.checkpoint.end)
	if err != nil {
		return journalSegmentManifest{}, nil, nil, err
	}
	candidates := manifest.extents[:covered]
	protected := make([]bool, covered)
	protect := func(start, end int64) {
		for i := firstJournalExtentOverlap(candidates, start); i < len(candidates) && candidates[i].base < end; i++ {
			protected[i] = true
		}
	}
	for i, extent := range candidates {
		protected[i] = s.checkpointOwners[extent.id]
	}
	for span := range s.retainedSources {
		protect(span.start, span.end)
	}
	for span := range s.readRanges {
		protect(span.start, span.end)
	}
	kept := make([]journalSegmentExtent, 0, len(manifest.extents))
	var removed []journalSegmentExtent
	for i, extent := range candidates {
		if protected[i] {
			kept = append(kept, extent)
		} else {
			removed = append(removed, extent)
		}
	}
	if len(removed) == 0 {
		return manifest, nil, nil, nil
	}
	manifest.extents = append(kept, manifest.extents[covered:]...)
	s.retiring = removed
	s.reads.Add(1)
	return manifest, removed, func() {
		s.mu.Lock()
		s.retiring = nil
		s.mu.Unlock()
		s.reads.Done()
	}, nil
}

func firstJournalExtentOverlap(extents []journalSegmentExtent, start int64) int {
	return sort.Search(len(extents), func(i int) bool {
		return extents[i].base+extents[i].sealedBytes > start
	})
}

func journalExtentOverlaps(extents []journalSegmentExtent, start, end int64) bool {
	i := firstJournalExtentOverlap(extents, start)
	return i < len(extents) && extents[i].base < end
}

// Report the independently held extents on disk exhaustion. The same
// extent can have both a recovery reference and a live body owner.
func (s *journalSegmentStorage) retentionSummary() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var recovery, live, reads int
	for _, extent := range s.manifest.extents {
		if s.checkpointOwners[extent.id] {
			recovery++
		}
		end := extent.base + extent.sealedBytes
		for span := range s.retainedSources {
			if span.start < end && span.end > extent.base {
				live++
				break
			}
		}
		for span := range s.readRanges {
			if span.start < end && span.end > extent.base {
				reads++
				break
			}
		}
	}
	return fmt.Sprintf("%d segments, %d held by checkpoint entries, %d by live bodies, %d by issued reads",
		len(s.manifest.extents), recovery, live, reads)
}
