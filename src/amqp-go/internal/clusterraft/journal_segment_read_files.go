package clusterraft

import (
	"errors"
	"fmt"
	"os"
	"sync"
)

// Each disk reader borrows at most one descriptor at a time. The slot bound
// therefore leaves an idle eviction candidate on every full-cache miss.
// Idle handles carry no range or recovery ownership.
const maxJournalReadFiles = journalSegmentReadSlots

type journalExtentReadFile struct {
	file    *os.File
	readers int
	used    uint64
}

type journalExtentReadFiles struct {
	mu    sync.Mutex // Separate from storage admission and never held across ReadAt.
	clock uint64
	files map[uint64]*journalExtentReadFile
}

// The caller holds its existing issued-range or retained-source lease and a
// disk slot. Those leases, rather than cached descriptors, prevent retirement.
func (s *journalSegmentStorage) borrowReadFile(id uint64) (*journalExtentReadFile, error) {
	c := &s.readFiles
	c.mu.Lock()
	defer c.mu.Unlock()
	c.clock++
	if entry := c.files[id]; entry != nil {
		entry.readers++
		entry.used = c.clock
		return entry, nil
	}
	if len(c.files) == maxJournalReadFiles {
		var oldestID uint64
		var oldest *journalExtentReadFile
		for id, entry := range c.files {
			if entry.readers == 0 && (oldest == nil || entry.used < oldest.used) {
				oldestID, oldest = id, entry
			}
		}
		if oldest == nil {
			// This caller owns a disk slot but has no descriptor yet, so at
			// most maxJournalReadFiles-1 other descriptors can be borrowed.
			return nil, fmt.Errorf("%w: journal read descriptor bound exceeded", errJournalCorrupt)
		}
		delete(c.files, oldestID)
		if err := oldest.file.Close(); err != nil {
			return nil, err
		}
	}
	file, err := os.Open(s.path(id))
	if err != nil {
		return nil, err
	}
	if c.files == nil {
		c.files = make(map[uint64]*journalExtentReadFile, maxJournalReadFiles)
	}
	entry := &journalExtentReadFile{file: file, readers: 1, used: c.clock}
	c.files[id] = entry
	return entry, nil
}

func (s *journalSegmentStorage) releaseReadFile(loan *journalExtentReadFile) {
	c := &s.readFiles
	c.mu.Lock()
	loan.readers--
	c.mu.Unlock()
}

// Retirement has already excluded new range/source leases under storage.mu.
// An idle descriptor must close before unlink so it cannot retain disk bytes.
func (s *journalSegmentStorage) closeReadFile(id uint64) error {
	c := &s.readFiles
	c.mu.Lock()
	defer c.mu.Unlock()
	entry := c.files[id]
	if entry == nil {
		return nil
	}
	if entry.readers != 0 {
		return fmt.Errorf("%w: retiring journal extent has an active read descriptor", errJournalCorrupt)
	}
	delete(c.files, id)
	return entry.file.Close()
}

// The reader has stopped admission and drained all existing read/source pins.
func (s *journalSegmentStorage) closeReadFiles() error {
	c := &s.readFiles
	c.mu.Lock()
	defer c.mu.Unlock()
	var err error
	for id, entry := range c.files {
		err = errors.Join(err, entry.file.Close())
		delete(c.files, id)
	}
	return err
}
