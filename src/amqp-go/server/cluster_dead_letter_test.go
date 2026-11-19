package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/internal/clusterqueue"
	"github.com/maxpert/amqp-go/internal/clusterraft"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"
)

type deadLetterWorkerFixture struct {
	localClusterMachine
	cancel             context.CancelFunc
	attempts           int
	lostReceipt        bool
	blockedExchange    string
	localUpdates       [][2]int
	lostSourceReceipt  bool
	commitSourceUpdate bool
	readErr            error
	encodedPages       int
	beforeSourceUpdate func([]clusterqueue.DeadLetterUpdate)
}

func (*deadLetterWorkerFixture) WorkShard(string) uint64 { return 2 }
func (*deadLetterWorkerFixture) LocalWorkShards(func(clusterraft.Machine) bool) []uint64 {
	return []uint64{1}
}

func (f *deadLetterWorkerFixture) ReadLocalWork(_ context.Context, _ uint64, capture func(clusterraft.Machine) (func() ([]byte, error), int, error)) ([]byte, error) {
	if f.readErr != nil {
		return nil, f.readErr
	}
	encode, _, err := capture(&f.machine)
	if err != nil {
		return nil, err
	}
	f.encodedPages++
	return encode()
}

func (f *deadLetterWorkerFixture) SubmitLocalWork(ctx context.Context, _ uint64, data []byte) ([]byte, error) {
	command, err := clusterqueue.DecodeCommand(data)
	if err != nil {
		return nil, err
	}
	var operations [2]int
	if command.DeadLetterUpdates != nil {
		if f.beforeSourceUpdate != nil {
			f.beforeSourceUpdate(*command.DeadLetterUpdates)
		}
		for _, update := range *command.DeadLetterUpdates {
			switch {
			case len(update.Plan) != 0:
				operations[0]++
			case update.Completed != "":
				operations[1]++
			}
		}
		f.localUpdates = append(f.localUpdates, operations)
	}
	if operations[1] != 0 && f.lostSourceReceipt {
		f.lostSourceReceipt = false
		if f.commitSourceUpdate {
			if _, err := f.localClusterMachine.Submit(ctx, "", data); err != nil {
				return nil, err
			}
		}
		return nil, errors.New("source update became uncertain")
	}
	return f.localClusterMachine.Submit(ctx, "", data)
}

func (f *deadLetterWorkerFixture) Query(ctx context.Context, key string, data []byte) ([]byte, error) {
	if f.blockedExchange != "" && strings.HasSuffix(key, f.blockedExchange) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return f.machine.Query(data)
}

func (f *deadLetterWorkerFixture) Submit(ctx context.Context, key string, data []byte) ([]byte, error) {
	if f.cancel != nil {
		f.attempts++
		f.cancel()
		return nil, ctx.Err()
	}
	reply, err := f.localClusterMachine.Submit(ctx, key, data)
	if f.lostReceipt && err == nil {
		f.lostReceipt = false
		return nil, errors.New("destination committed but its response was lost")
	}
	return reply, err
}

func TestClusterDeadLetterCancellationStopsWideFanout(t *testing.T) {
	const source, dlx = "source", "dlx"
	f := &deadLetterWorkerFixture{}
	b := NewClusterBroker(f)
	ctx := context.Background()
	_, err := b.DeclareQueue(source, true, false, false, map[string]interface{}{
		"x-dead-letter-exchange": dlx, "x-dead-letter-strategy": "at-least-once", "x-overflow": "reject-publish",
	})
	require.NoError(t, err)
	require.NoError(t, b.DeclareExchange(dlx, "fanout", true, false, false, nil))
	for i := range 64 {
		name := fmt.Sprintf("destination-%02d", i)
		_, err := b.DeclareQueue(name, true, false, false, nil)
		require.NoError(t, err)
		require.NoError(t, b.BindQueue(name, dlx, "", nil))
	}
	require.NoError(t, b.PublishMessage("", source, &protocol.Message{Body: []byte("rejected")}))
	_, tag, _, err := b.GetMessageForGetOwned(source, false, "reader")
	require.NoError(t, err)
	t.Cleanup(func() { b.RequeueGetDeliveries("reader") })
	require.NoError(t, b.NackGetDelivery(tag, false))

	// The plan commits, then cancellation interrupts the first target.
	attempt, cancel := context.WithCancel(ctx)
	defer cancel()
	f.cancel = cancel
	_, progress := b.advanceClusterDeadLetter(attempt, f, 1, deadLetterCursor{})
	f.cancel = nil
	require.True(t, progress)
	require.Equal(t, 1, f.attempts)
	require.True(t, f.machine.queues.HasDeadLetters())
}

const deadLetterBatchSource, deadLetterBatchTarget = "source", "destination"

func stageDeadLetterPage(t *testing.T, count int) (*ClusterBroker, *deadLetterWorkerFixture) {
	t.Helper()
	const source, target = deadLetterBatchSource, deadLetterBatchTarget
	f := &deadLetterWorkerFixture{}
	b := NewClusterBroker(f)
	_, err := b.DeclareQueue(source, true, false, false, map[string]interface{}{
		"x-dead-letter-exchange": "", "x-dead-letter-routing-key": target,
		"x-dead-letter-strategy": "at-least-once", "x-overflow": "reject-publish",
	})
	require.NoError(t, err)
	_, err = b.DeclareQueue(target, true, false, false, nil)
	require.NoError(t, err)
	t.Cleanup(func() { b.RequeueGetDeliveries("reader") })
	for i := range count {
		body := make([]byte, 64<<10)
		body[0] = byte(i)
		require.NoError(t, b.PublishMessage("", source, &protocol.Message{Body: body}))
		_, tag, _, err := b.GetMessageForGetOwned(source, false, "reader")
		require.NoError(t, err)
		require.NoError(t, b.NackGetDelivery(tag, false))
	}
	return b, f
}

func TestClusterDeadLetterTransfersPageInOnePass(t *testing.T) {
	const count = 8
	b, f := stageDeadLetterPage(t, count)
	cursor, progressed := b.advanceClusterDeadLetter(t.Context(), f, 1, deadLetterCursor{})
	require.True(t, progressed)
	require.Len(t, cursor.receipts, count)
	info, err := b.describe(t.Context(), deadLetterBatchTarget)
	require.NoError(t, err)
	require.EqualValues(t, count, info.Ready)
	_, progressed = b.advanceClusterDeadLetter(t.Context(), f, 1, cursor)
	require.True(t, progressed)
	require.False(t, f.machine.queues.HasDeadLetters())
}

func TestClusterDeadLetterOversizedPageWaitsForPayloadReservation(t *testing.T) {
	b, f := stageDeadLetterPage(t, 0)
	body := make([]byte, clusterqueue.DeadLetterPageBytes+1)
	require.NoError(t, b.PublishMessage("", deadLetterBatchSource, &protocol.Message{Body: body}))
	_, tag, _, err := b.GetMessageForGetOwned(deadLetterBatchSource, false, "reader")
	require.NoError(t, err)
	require.NoError(t, b.NackGetDelivery(tag, false))

	cursor, progress := b.advanceClusterDeadLetter(t.Context(), f, 1, deadLetterCursor{})
	require.False(t, progress)
	require.Greater(t, cursor.pageBytes, clusterqueue.DeadLetterPageBytes)
	require.Zero(t, cursor.after, "a reservation retry must not advance the sweep")
	require.Zero(t, f.encodedPages, "unreserved payload must not be read or encoded")
	require.Empty(t, f.localUpdates, "unreserved payload must not commit a destination plan")
	info, err := b.describe(t.Context(), deadLetterBatchTarget)
	require.NoError(t, err)
	require.Zero(t, info.Ready)
	require.True(t, f.machine.queues.HasDeadLetters())

	cursor, progress = b.advanceClusterDeadLetter(t.Context(), f, 1, cursor)
	require.True(t, progress)
	require.Equal(t, 1, f.encodedPages)
	require.Len(t, cursor.receipts, 1)
	require.Zero(t, cursor.pageBytes, "the larger reservation belongs only to this attempt")
	info, err = b.describe(t.Context(), deadLetterBatchTarget)
	require.NoError(t, err)
	require.EqualValues(t, 1, info.Ready)
	cursor, progress = b.advanceClusterDeadLetter(t.Context(), f, 1, cursor)
	require.False(t, progress)
	require.Greater(t, cursor.pageBytes, clusterqueue.DeadLetterPageBytes)
	_, progress = b.advanceClusterDeadLetter(t.Context(), f, 1, cursor)
	require.True(t, progress)
	require.False(t, f.machine.queues.HasDeadLetters())
}

func TestClusterDeadLetterPayloadWaitCancelsWithoutCapturing(t *testing.T) {
	b, f := stageDeadLetterPage(t, 1)
	pages := make(chan struct{}, 1)
	payload := semaphore.NewWeighted(clusterDeadLetterPayloadBudget)
	require.NoError(t, payload.Acquire(t.Context(), clusterDeadLetterPayloadBudget))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		b.runClusterDeadLetterShard(ctx, f, 1, pages, payload)
	}()
	// Reaching the page gate establishes that the worker is attempting
	// admission while the entire payload budget is held elsewhere.
	require.Eventually(t, func() bool { return len(pages) == 1 }, time.Second, time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("worker did not cancel its payload reservation")
	}
	require.Empty(t, pages)
	require.Zero(t, f.encodedPages)
	require.Empty(t, f.localUpdates)
	payload.Release(clusterDeadLetterPayloadBudget)
	require.True(t, payload.TryAcquire(clusterDeadLetterPayloadBudget), "cancellation must not leak payload permits")
	payload.Release(clusterDeadLetterPayloadBudget)
}

func TestClusterDeadLetterRetriesAnAmbiguousTargetBatch(t *testing.T) {
	const count = 8
	b, f := stageDeadLetterPage(t, count)
	f.lostReceipt = true
	cursor, progress := b.advanceClusterDeadLetter(t.Context(), f, 1, deadLetterCursor{})
	require.True(t, progress, "the plan was committed")
	target, err := b.describe(t.Context(), deadLetterBatchTarget)
	require.NoError(t, err)
	require.EqualValues(t, count, target.Ready)
	require.True(t, f.machine.queues.HasDeadLetters(), "uncertain batches must retain the source")
	cursor, progress = b.advanceClusterDeadLetter(t.Context(), f, 1, cursor)
	require.True(t, progress)
	require.Len(t, cursor.receipts, count)
	_, progress = b.advanceClusterDeadLetter(t.Context(), f, 1, cursor)
	require.True(t, progress)
	require.False(t, f.machine.queues.HasDeadLetters())
	target, err = b.describe(t.Context(), deadLetterBatchTarget)
	require.NoError(t, err)
	require.EqualValues(t, 2*count, target.Ready, "ambiguous cross-shard outcomes may duplicate")
}

func TestClusterDeadLetterBlockedExchangeDoesNotStallPage(t *testing.T) {
	for _, blockedFirst := range []bool{true, false} {
		t.Run(fmt.Sprint(blockedFirst), func(t *testing.T) {
			f := &deadLetterWorkerFixture{blockedExchange: "blocked"}
			b := NewClusterBroker(f)
			target := "available"
			_, err := b.DeclareQueue(target, true, false, false, nil)
			require.NoError(t, err)
			t.Cleanup(func() { b.RequeueGetDeliveries("reader") })
			for i, blocked := range []bool{blockedFirst, !blockedFirst} {
				exchange := ""
				if blocked {
					exchange = f.blockedExchange
				}
				queue := fmt.Sprintf("source-%d", i)
				_, err := b.DeclareQueue(queue, true, false, false, map[string]interface{}{
					"x-dead-letter-exchange": exchange, "x-dead-letter-routing-key": target,
					"x-dead-letter-strategy": "at-least-once", "x-overflow": "reject-publish",
				})
				require.NoError(t, err)
				require.NoError(t, b.PublishMessage("", queue, &protocol.Message{Body: []byte(queue)}))
				_, tag, _, err := b.GetMessageForGetOwned(queue, false, "reader")
				require.NoError(t, err)
				require.NoError(t, b.NackGetDelivery(tag, false))
			}
			page, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			_, progress := b.advanceClusterDeadLetter(page, f, 1, deadLetterCursor{})
			require.True(t, progress)
			info, err := b.describe(t.Context(), target)
			require.NoError(t, err)
			require.EqualValues(t, 1, info.Ready)
			require.True(t, f.machine.queues.HasDeadLetters(), "blocked transfer must remain pending")
		})
	}
}

func TestClusterDeadLetterCoalescesSourceReceiptsAndPlans(t *testing.T) {
	b, f := stageDeadLetterPage(t, 40)
	cursor, progress := b.advanceClusterDeadLetter(t.Context(), f, 1, deadLetterCursor{})
	require.True(t, progress)
	firstCount := len(cursor.receipts)
	require.Greater(t, firstCount, 1)
	require.Less(t, firstCount, 40)
	cursor, progress = b.advanceClusterDeadLetter(t.Context(), f, 1, cursor)
	require.True(t, progress)
	require.Len(t, cursor.receipts, 40-firstCount)
	require.Equal(t, [][2]int{{firstCount, 0}, {40 - firstCount, firstCount}}, f.localUpdates)
	_, progress = b.advanceClusterDeadLetter(t.Context(), f, 1, cursor)
	require.True(t, progress)
	require.False(t, f.machine.queues.HasDeadLetters())
}

func TestClusterDeadLetterRetriesSourceProgressWithoutRepublishing(t *testing.T) {
	for _, committed := range []bool{false, true} {
		t.Run(fmt.Sprintf("committed=%t", committed), func(t *testing.T) {
			const count = 8
			b, f := stageDeadLetterPage(t, count)
			cursor, progress := b.advanceClusterDeadLetter(t.Context(), f, 1, deadLetterCursor{})
			require.True(t, progress)
			f.lostSourceReceipt = true
			f.commitSourceUpdate = committed
			cursor, progress = b.advanceClusterDeadLetter(t.Context(), f, 1, cursor)
			require.False(t, progress)
			require.Len(t, cursor.receipts, count)
			require.Equal(t, !committed, f.machine.queues.HasDeadLetters())
			cursor, progress = b.advanceClusterDeadLetter(t.Context(), f, 1, cursor)
			require.True(t, progress)
			require.Empty(t, cursor.receipts)
			require.False(t, f.machine.queues.HasDeadLetters())
			info, err := b.describe(t.Context(), deadLetterBatchTarget)
			require.NoError(t, err)
			require.EqualValues(t, count, info.Ready)
		})
	}
}

func TestClusterDeadLetterWorkerDropsVolatileReceiptsAfterLeadershipLoss(t *testing.T) {
	for _, cause := range []error{clusterraft.ErrNotLeader, clusterraft.ErrLeadershipLost, clusterraft.ErrStopped} {
		t.Run(cause.Error(), func(t *testing.T) {
			b, f := stageDeadLetterPage(t, 2)
			cursor, progress := b.advanceClusterDeadLetter(t.Context(), f, 1, deadLetterCursor{})
			require.True(t, progress)
			require.Len(t, cursor.receipts, 2)
			f.readErr = cause
			cursor, progress = b.advanceClusterDeadLetter(t.Context(), f, 1, cursor)
			require.False(t, progress)
			require.Equal(t, deadLetterCursor{}, cursor)
			require.Len(t, f.localUpdates, 1, "a follower must not retry source progress")
			require.True(t, f.machine.queues.HasDeadLetters(), "the replicated source retains ownership")
		})
	}
}

func TestClusterDeadLetterAuthorizationAloneDoesNotReportProgress(t *testing.T) {
	b, f := stageDeadLetterPage(t, 2)
	_, err := b.DeleteQueue(deadLetterBatchTarget, false, false)
	require.NoError(t, err)
	cursor, progress := b.advanceClusterDeadLetter(t.Context(), f, 1, deadLetterCursor{})
	require.True(t, progress, "the plan was installed")
	require.Empty(t, cursor.receipts)
	_, progress = b.advanceClusterDeadLetter(t.Context(), f, 1, cursor)
	require.False(t, progress, "an unavailable destination needs retry backoff")
	require.True(t, f.machine.queues.HasDeadLetters())
}

func TestClusterDeadLetterAuthorizationRejectsStaleCapturedPlan(t *testing.T) {
	const count = 2
	b, f := stageDeadLetterPage(t, count)
	f.lostReceipt = true
	cursor, progress := b.advanceClusterDeadLetter(t.Context(), f, 1, deadLetterCursor{})
	require.True(t, progress)
	require.Empty(t, cursor.receipts)
	f.beforeSourceUpdate = func(updates []clusterqueue.DeadLetterUpdate) {
		require.Len(t, updates, count)
		completed := make([]clusterqueue.DeadLetterUpdate, count)
		for i, update := range updates {
			require.Equal(t, deadLetterBatchTarget, update.Authorize)
			completed[i] = clusterqueue.DeadLetterUpdate{ID: update.ID, Completed: update.Authorize}
		}
		command, err := clusterqueue.EncodeDeadLetterUpdates(completed)
		require.NoError(t, err)
		f.machine.Apply(command)
	}
	_, progress = b.advanceClusterDeadLetter(t.Context(), f, 1, cursor)
	require.False(t, progress, "a stale local capture cannot authorize a send")
	info, err := b.describe(t.Context(), deadLetterBatchTarget)
	require.NoError(t, err)
	require.EqualValues(t, count, info.Ready)
}
