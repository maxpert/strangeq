package clusterraft

import "math"

type checkpointCostMachine interface {
	CheckpointCostBytes() uint64
}

// Journal data shards amortize a checkpoint against the current live image.
// A separate applied-tail ceiling applies even when the routine slot is busy.
func (g *Group) checkpointPressure() (due, force bool) {
	machine, costAware := g.cfg.Machine.(checkpointCostMachine)
	if !g.journalInput || !g.cfg.BackgroundJournalCheckpoints || !costAware {
		return g.checkpointDue(), false
	}
	baseline := g.cfg.SnapshotBytes
	target := min(scaleCheckpointThreshold(baseline, 32),
		max(scaleCheckpointThreshold(baseline, 4), scaleCheckpointThreshold(machine.CheckpointCostBytes(), 64)))
	hardBytes := scaleCheckpointThreshold(baseline, 64)
	hardEntries := scaleCheckpointThreshold(g.cfg.SnapshotEntries, 64)
	return g.appliedLogBytes >= target,
		g.appliedLogBytes >= hardBytes || g.applied-g.snapshotIndex >= hardEntries
}

func scaleCheckpointThreshold(value, multiplier uint64) uint64 {
	if value > math.MaxUint64/multiplier {
		return math.MaxUint64
	}
	return value * multiplier
}
