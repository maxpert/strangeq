package server

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	"github.com/maxpert/amqp-go/storage"
)

// startSystemMetricsCollection starts a background goroutine to collect system metrics
func (s *Server) startSystemMetricsCollection(ctx context.Context) {
	if s.MetricsCollector == nil {
		return
	}

	ticker := time.NewTicker(60 * time.Second) // 60s — filepath.Walk over data dir is O(files)
	defer ticker.Stop()

	// Collect once immediately
	s.collectSystemMetrics()

	for {
		select {
		case <-ticker.C:
			s.collectSystemMetrics()
		case <-ctx.Done():
			return
		}
	}
}

// collectSystemMetrics collects various system-level metrics
func (s *Server) collectSystemMetrics() {
	if s.MetricsCollector == nil {
		return
	}

	// Update uptime
	uptime := time.Since(s.StartTime).Seconds()
	s.MetricsCollector.UpdateServerUptime(uptime)

	// Collect memory stats
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	// Total memory used (heap + stack + other)
	s.MetricsCollector.UpdateMemoryMetrics(float64(m.Sys), float64(m.HeapInuse))

	// Record recent GC pauses
	if m.NumGC > 0 {
		// Get the most recent GC pause
		lastPause := m.PauseNs[(m.NumGC+255)%256]
		s.MetricsCollector.RecordGCPause(float64(lastPause) / 1e9) // Convert to seconds
	}

	// Update goroutine count
	s.MetricsCollector.UpdateGoroutines(float64(runtime.NumGoroutine()))

	// Update file descriptor count (Unix only)
	s.updateFileDescriptors()

	// Update disk metrics (if storage path is configured)
	s.updateDiskMetrics()

	// Update WAL metrics
	s.updateWALMetrics()

	// Update segment metrics
	s.updateSegmentMetrics()
}

// updateFileDescriptors updates file descriptor metrics (Unix systems only)
func (s *Server) updateFileDescriptors() {
	// Count open file descriptors by reading /proc/self/fd
	// This is Linux-specific; on other systems this will be skipped
	fdDir := "/proc/self/fd"
	if entries, err := os.ReadDir(fdDir); err == nil {
		s.MetricsCollector.UpdateFileDescriptors(float64(len(entries)))
	}
}

// updateDiskMetrics updates disk usage metrics
func (s *Server) updateDiskMetrics() {
	if s.Config == nil {
		return
	}

	dataDir := s.Config.Storage.Path
	if dataDir == "" {
		dataDir = "./data"
	}

	// Get disk usage stats
	var stat syscall.Statfs_t
	if err := syscall.Statfs(dataDir, &stat); err != nil {
		// Silently fail - disk metrics are optional
		return
	}

	// Available space
	freeBytes := float64(stat.Bavail * uint64(stat.Bsize))
	s.MetricsCollector.UpdateDiskMetrics(freeBytes, 0) // We'll calculate used bytes separately

	// Calculate total disk used by server data
	usedBytes := s.calculateDataDirSize(dataDir)
	s.MetricsCollector.UpdateDiskMetrics(freeBytes, float64(usedBytes))
}

// calculateDataDirSize calculates the total size of files in the data directory
func (s *Server) calculateDataDirSize(dataDir string) int64 {
	var totalSize int64

	// Walk the data directory and sum up file sizes
	filepath.Walk(dataDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // Skip files with errors
		}
		if !info.IsDir() {
			// ALLOCATED bytes, not apparent: the WAL is sparse, so info.Size()
			// over-reports it (see allocatedFileSize).
			totalSize += allocatedFileSize(info)
		}
		return nil
	})

	return totalSize
}

// updateWALMetrics collects metrics about WAL files
func (s *Server) updateWALMetrics() {
	if s.Config == nil {
		return
	}

	dataDir := s.Config.Storage.Path
	if dataDir == "" {
		dataDir = "./data"
	}

	walDir := filepath.Join(dataDir, "wal", "shared")

	entries, err := os.ReadDir(walDir)
	if err != nil {
		// Silently fail - WAL directory might not exist yet
		return
	}

	var totalSize int64
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if filepath.Ext(entry.Name()) == ".wal" {
			info, err := entry.Info()
			if err == nil {
				// ALLOCATED bytes: a sparse WAL file's apparent size is larger
				// than what it stores, so info.Size() would over-report wal_size
				// (see allocatedFileSize).
				totalSize += allocatedFileSize(info)
			}
		}
	}

	s.MetricsCollector.UpdateWALSize(float64(totalSize))
}

// updateSegmentMetrics collects metrics about segment files per queue
func (s *Server) updateSegmentMetrics() {
	if s.Config == nil {
		return
	}

	dataDir := s.Config.Storage.Path
	if dataDir == "" {
		dataDir = "./data"
	}

	segmentDir := filepath.Join(dataDir, "segments")

	entries, err := os.ReadDir(segmentDir)
	if err != nil {
		// Silently fail - segment directory might not exist yet
		return
	}

	// Map of queue name to segment count and size
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		// The directory entry's name is NOT necessarily the queue name — a name
		// that cannot be a path element gets a generated directory whose owner
		// is recorded inside it (review-4 B-1). Labelling the metric with the
		// entry name would show an operator a hex string.
		//
		// A directory that cannot be attributed is SKIPPED rather than labelled
		// with a guess: a wrong label silently folds one queue's segment counts
		// into another queue's series, which is worse than a missing series
		// (review-5 MINOR-5).
		queueName, attributed := storage.QueueNameForSegmentDir(segmentDir, entry.Name())
		if !attributed {
			continue
		}
		queueDir := filepath.Join(segmentDir, entry.Name())

		segmentFiles, err := os.ReadDir(queueDir)
		if err != nil {
			continue
		}

		var count int
		var size int64

		for _, segFile := range segmentFiles {
			if filepath.Ext(segFile.Name()) == ".seg" {
				count++
				info, err := segFile.Info()
				if err == nil {
					size += info.Size()
				}
			}
		}

		if count > 0 {
			s.MetricsCollector.UpdateSegmentMetrics(queueName, float64(count), float64(size))
		}
	}
}
