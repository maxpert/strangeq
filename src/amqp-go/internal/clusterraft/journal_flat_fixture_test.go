package clusterraft

import (
	"fmt"
	"os"
	"path/filepath"
)

func openJournalGroupNode(dir string, wrap func(*os.File) journalFile) (*journalGroupNode, error) {
	return openJournal(dir, true, wrap)
}

func openJournal(dir string, create bool, wrap func(*os.File) journalFile) (_ *journalGroupNode, err error) {
	return openJournalWithBudget(dir, create, wrap, nil)
}

func openJournalWithBudget(dir string, create bool, wrap func(*os.File) journalFile, budget *journalBudget) (_ *journalGroupNode, err error) {
	flags := os.O_RDWR
	if create {
		if err := os.MkdirAll(dir, 0700); err != nil {
			return nil, err
		}
		flags |= os.O_CREATE
	}
	path := filepath.Join(dir, journalActiveName)
	file, err := os.OpenFile(path, flags, 0600)
	if err != nil {
		return nil, err
	}
	var writeFile *os.File
	defer func() {
		if err != nil {
			if writeFile != nil {
				_ = writeFile.Close()
			}
			_ = file.Close()
		}
	}()
	if err := lockGroupJournal(file); err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() > journalGroupMaxFileBytes {
		return nil, fmt.Errorf("raft journal file capacity: %w", errJournalTooLarge)
	}
	node := &journalGroupNode{
		dir: dir, reader: file, groups: make(map[uint64]*journalGroupState), owners: make(map[uint64]bool),
		readScratch: newJournalScratchPool(4, journalScratchMaxBytes),
		replySlots:  make(chan struct{}, journalReplySlots),
		syncFile:    syncJournalFile, syncDir: syncDirectory,
	}
	state, err := scanJournal(file, info.Size(), -1, 1, node.replayBatch)
	if err != nil {
		return nil, err
	}
	if state.partialTail {
		if err := file.Truncate(state.end); err != nil {
			return nil, err
		}
	}
	// The node owns the locked reader until every Group exits. Give the
	// writer a separate open file description so an I/O failure can close
	// its handle without releasing Node ownership or invalidating reads.
	writeFile, err = os.OpenFile(path, os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	writeInfo, err := writeFile.Stat()
	if err != nil {
		return nil, err
	}
	if !os.SameFile(info, writeInfo) {
		return nil, fmt.Errorf("%w: node raft journal replaced during open", errJournalCorrupt)
	}
	var output journalFile = journalDataFile{writeFile}
	if wrap != nil {
		output = wrap(writeFile)
	}
	if err := output.Sync(); err != nil {
		return nil, err
	}
	// Replay can see a complete batch left by a failed sync. Make the
	// recovered references durable before removing any previous blobs.
	if err := node.inventorySnapshots(); err != nil {
		return nil, err
	}
	// Before admitting writes, sync the journal directory entry as well as its
	// parent. The surrounding broker provisions the storage root separately.
	if err := node.syncDir(dir); err != nil {
		return nil, err
	}
	if err := node.syncDir(filepath.Dir(dir)); err != nil {
		return nil, err
	}
	writer, err := newJournalWriterWithBudget(output, state.end, state.nextSequence, journalGroupWriterLimits(), budget)
	if err != nil {
		return nil, err
	}
	node.writer = writer
	return node, nil
}

func openNodeJournalPair(root string, create bool, peer uint64, wrap func(int, *os.File) journalFile) (*NodeJournalPair, error) {
	return openJournalPair(root, peer, func(dir string, index int, budget *journalBudget) (*journalGroupNode, error) {
		return openJournalWithBudget(dir, create, func(file *os.File) journalFile { return wrap(index, file) }, budget)
	})
}
