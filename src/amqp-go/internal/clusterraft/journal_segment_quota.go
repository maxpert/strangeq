package clusterraft

import (
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// The quota charges file lengths of owned extent and checkpoint artifacts,
// including unselected files, plus outstanding reservations. It excludes
// snapshots, manifests, lock files and filesystem allocation overhead.
type journalSegmentQuota struct {
	mu          sync.Mutex
	limit, used int64
	streams     int64
}

func inventoryJournalSegmentArtifacts(dir string, quota *journalSegmentQuota) (map[string]int64, error) {
	file, err := os.Open(dir)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	artifacts := make(map[string]int64)
	for {
		files, readErr := file.ReadDir(128)
		for _, file := range files {
			name := file.Name()
			owned, err := journalSegmentArtifact(name)
			if err != nil {
				return nil, err
			}
			manifestTemp := strings.HasPrefix(name, journalSegmentManifestTempPrefix)
			rootTemp := strings.HasPrefix(name, processRootTempPrefix)
			if !owned && !manifestTemp && !rootTemp {
				continue
			}
			info, err := file.Info()
			if err != nil {
				return nil, err
			}
			if !info.Mode().IsRegular() || info.Size() < 0 || info.Size() > math.MaxInt64-quota.used {
				return nil, fmt.Errorf("%w: invalid retained journal artifact %s", errJournalCorrupt, name)
			}
			if owned {
				quota.used += info.Size()
				artifacts[name] = info.Size()
			} else {
				// Temporary authority publications are collected but uncharged.
				artifacts[name] = 0
			}
		}
		if readErr == io.EOF {
			return artifacts, nil
		}
		if readErr != nil {
			return nil, readErr
		}
	}
}

// Recovery calls this only after syncing the selected manifest's directory.
// Files absent from that manifest cannot be needed by either future replay
// or any live readers: the opener still holds exclusive ownership.
func (s *journalSegmentStorage) collectUnselected(manifest journalSegmentManifest, artifacts map[string]int64) error {
	if s.processRoots != nil {
		s.processRoots.preserveArtifacts(artifacts)
	}
	delete(artifacts, manifest.checkpoint.name)
	for _, extent := range manifest.extents {
		delete(artifacts, filepath.Base(s.path(extent.id)))
	}
	if len(artifacts) == 0 {
		return nil
	}
	var reclaimed int64
	for name, size := range artifacts {
		if err := os.Remove(filepath.Join(s.dir, name)); err != nil {
			return fmt.Errorf("remove unselected journal file %s: %w", name, err)
		}
		reclaimed += size
	}
	if err := s.hooks.syncDir(s.dir); err != nil {
		return err
	}
	s.quota.release(reclaimed)
	return nil
}

func journalSegmentArtifact(name string) (bool, error) {
	if suffix, ok := strings.CutPrefix(name, "extent-"); ok {
		id, valid := strings.CutSuffix(suffix, ".raft-journal")
		valid = valid && len(id) == 16
		for _, char := range id {
			valid = valid && (char >= '0' && char <= '9' || char >= 'a' && char <= 'f')
		}
		if valid {
			return true, nil
		}
		return false, fmt.Errorf("%w: invalid retained journal extent name %s", errJournalCorrupt, name)
	}
	if suffix, ok := strings.CutPrefix(name, journalCheckpointPrefix); ok {
		valid := suffix != "" && len(name) <= maxJournalCheckpointName
		for _, char := range suffix {
			valid = valid && char >= '0' && char <= '9'
		}
		if valid {
			return true, nil
		}
		return false, fmt.Errorf("%w: invalid retained journal checkpoint name %s", errJournalCorrupt, name)
	}
	return false, nil
}

func (q *journalSegmentQuota) reserve(size int64) error {
	return q.reserveWithHeadroom(size, 0)
}

func (q *journalSegmentQuota) reserveWithHeadroom(size, headroom int64) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if size < 0 || size > q.limit-q.used-headroom {
		return fmt.Errorf("%w: retained journal byte capacity", errJournalTooLarge)
	}
	q.used += size
	return nil
}

// Release only when no file was created, or deletion or truncation is durable.
func (q *journalSegmentQuota) release(size int64) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if size < 0 || size > q.used {
		panic("invalid retained journal byte release")
	}
	q.used -= size
}
