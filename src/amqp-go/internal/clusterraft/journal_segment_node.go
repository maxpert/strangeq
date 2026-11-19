package clusterraft

import "errors"

const journalServingExtentBytes = 256 << 20

func openSegmentedGroupNode(dir string, create, rangeSync bool, extentBytes, retainedBytes int64,
	budget *journalBudget, hooks journalSegmentHooks,
) (_ *journalGroupNode, err error) {
	node := &journalGroupNode{
		dir: dir, groups: make(map[uint64]*journalGroupState), owners: make(map[uint64]bool),
		readScratch: newJournalScratchPool(4, journalScratchMaxBytes),
		replySlots:  make(chan struct{}, journalReplySlots),
		syncFile:    syncJournalFile, syncDir: syncDirectory,
	}
	hooks.restoreCheckpoint = func(_ uint64, _ int64, groups map[uint64]*journalGroupState) error {
		node.groups = groups
		return nil
	}
	output, reader, state, err := openJournalSegmentsWithQuota(dir, create, extentBytes, retainedBytes, hooks, node.replayBatch)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, output.Close(), reader.Close())
		}
	}()
	node.reader = reader
	if err := node.inventorySnapshots(); err != nil {
		return nil, err
	}
	output.rangeSync = rangeSync
	output.maintenance = true
	limits := journalGroupWriterLimits()
	limits.maxFileBytes = 0
	limits.batchBytes = min(limits.batchBytes, int(extentBytes)-journalFrameHeaderSize-journalFrameTrailerSize)
	limits.pendingBytes = min(limits.pendingBytes, int(extentBytes)-journalFrameHeaderSize-journalFrameTrailerSize)
	node.writer, err = newJournalWriterWithBudget(output, state.end, state.nextSequence, limits, budget)
	return node, err
}
