package storage

import (
	"math"
	"os"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
)

// raceTestBody returns a deterministic body for the shared-body race test.
func raceTestBody(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte('A' + (i % 26))
	}
	return b
}

// raceEnqueueShared enqueues ONE fused shared-body unit (one BodyBlock record
// plus one reference record) for the given queue/tag. The reference record
// carries a bodyRef, so settling it (markAck) looks its BodyBlock up in the
// segment inventory's `blocks` map.
//
// onDone (may be nil) is called when the unit is durable AND indexed.
func raceEnqueueShared(wm *WALManager, queue string, body []byte, offset uint64, onDone func()) error {
	msg := &protocol.Message{
		Exchange:     "ex",
		RoutingKey:   "rk",
		Body:         body,
		DeliveryMode: 2,
		DeliveryTag:  offset,
	}
	subs := []sharedSub{{
		queueName: queue,
		offset:    offset,
		message:   msg,
		onDone: func(error) {
			if onDone != nil {
				onDone()
			}
		},
	}}
	return wm.WriteSharedAsync(subs, body, 1)
}

// TestSegmentInventory_BlocksRace is the regression test for the data race on
// segmentInventory.blocks.
//
// commitBatch registers the batch's shared BodyBlocks in the inventory's
// `blocks` map. That registration MUST happen under offsetIndexMutex, because
// the readers that settle a record (WALManager.Acknowledge /
// WALManager.PurgeQueue -> segmentRecord.markAck) look the block up in the
// SAME map while holding only the READ lock. A registration performed outside
// the lock is an unsynchronised write against that read: the Go runtime
// aborts with "fatal error: concurrent map read and map write" and the race
// detector reports the race.
//
// The test seeds shared units, then runs PurgeQueue in one goroutine while
// WriteSharedAsync runs in another, so the settling path (map READ) and the
// commit path (map WRITE) overlap. Under `go test -race` the test must be
// clean; with the block registration moved back outside the lock it reports
// the race.
//
// Two details make that overlap deterministic instead of lucky:
//
//   - The purge band advances in steps, so every purge pass settles fresh
//     records and therefore performs block lookups (an already-acked record
//     short-circuits in markAck and touches no map at all).
//   - The commit path is lined up with the purge path through the TEST-ONLY
//     fsync hook: the hook performs the real fdatasync (durability is
//     unchanged) and then waits until the purger is inside its
//     read-locked section, so the block registration that follows runs
//     INSIDE that section - exactly the unsynchronised write the fix
//     removes.
func TestSegmentInventory_BlocksRace(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := DefaultWALConfig()
	cfg.BatchSize = 1000
	wm, err := NewWALManagerWithConfig(tmpDir, cfg)
	require.NoError(t, err)
	defer wm.Close()

	const queue = "q-blocks-race"
	const seedUnits = 300
	const writeUnits = 20000
	const purgeStep = 1000

	body := raceTestBody(256)

	// Seed: write shared units through the shared-body write path and wait
	// until every one of them is durable and indexed, so the inventory holds
	// records whose bodyRef points at a registered BodyBlock.
	var durable atomic.Int64
	for i := 0; i < seedUnits; i++ {
		require.NoError(t, raceEnqueueShared(wm, queue, body, uint64(i+1), func() {
			durable.Add(1)
		}))
	}
	require.Eventually(t, func() bool { return durable.Load() == seedUnits },
		30*time.Second, time.Millisecond, "seed units must become durable")

	// Line the commit path up with the purge path (see the doc comment).
	// purgeInside is raised by the purger for the whole duration of its
	// PurgeQueue call, i.e. while it holds offsetIndexMutex for reading. The
	// commit path waits for it, so the block registration that follows runs
	// INSIDE the purger's read-locked section.
	var purgeInside atomic.Bool
	var commits atomic.Int64
	restore := SetWALGroupCommitFsyncForTest(func(f *os.File) error {
		if err := fdatasyncFile(f); err != nil {
			return err
		}
		commits.Add(1)
		// Wait until the purger is provably INSIDE its read-locked section:
		// the flag is raised just before PurgeQueue takes the read lock, so
		// the flag alone is not enough - it must still be raised one settle
		// interval later, which proves the read lock was taken and is still
		// held.
		for {
			for !purgeInside.Load() {
				runtime.Gosched()
			}
			time.Sleep(50 * time.Microsecond)
			if purgeInside.Load() {
				break
			}
		}
		return nil
	})
	defer restore()

	// Concurrent phase: one goroutine keeps committing shared units (each
	// commit registers a BodyBlock in the inventory) while another keeps
	// purging the queue (each purge settles records and therefore READS the
	// inventory's blocks map).
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		for i := seedUnits; i < seedUnits+writeUnits; i++ {
			if err := raceEnqueueShared(wm, queue, body, uint64(i+1), func() {
				durable.Add(1)
			}); err != nil {
				t.Errorf("WriteSharedAsync: %v", err)
				return
			}
		}
	}()

	purgeStop := make(chan struct{})
	purgeDone := make(chan struct{})
	go func() {
		defer close(purgeDone)
		maxTag := uint64(seedUnits)
		for {
			purgeInside.Store(true)
			wm.PurgeQueue(queue, 1, maxTag)
			purgeInside.Store(false)
			maxTag += purgeStep
			if maxTag > seedUnits+writeUnits {
				maxTag = seedUnits + writeUnits
			}
			select {
			case <-purgeStop:
				return
			default:
			}
		}
	}()

	<-writerDone
	require.Eventually(t, func() bool { return durable.Load() == seedUnits+writeUnits },
		60*time.Second, time.Millisecond, "written units must become durable")
	close(purgeStop)
	<-purgeDone

	// Final settle: every seeded/written record must be acknowledged.
	wm.PurgeQueue(queue, 1, math.MaxUint64)
	require.Greater(t, commits.Load(), int64(0), "the commit path must have run")
}
