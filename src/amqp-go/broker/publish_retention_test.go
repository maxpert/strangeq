package broker

import (
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/maxpert/amqp-go/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// WS-I Phase B: broker retention accounting (RB-*). PublishMessageAsyncConfirm
// gains an onReleased callback that must fire EXACTLY ONCE — after every
// accepted durable copy completes, or synchronously on every non-durable /
// error / teardown path — so the server's publish lease is never stranded and
// never double-released.

// retentionEvents records callback firing order and counts.
type retentionEvents struct {
	mu     sync.Mutex
	events []string
}

func (e *retentionEvents) add(ev string) {
	e.mu.Lock()
	e.events = append(e.events, ev)
	e.mu.Unlock()
}

func (e *retentionEvents) count(ev string) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	n := 0
	for _, x := range e.events {
		if x == ev {
			n++
		}
	}
	return n
}

func (e *retentionEvents) snapshot() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.events...)
}

func (e *retentionEvents) callbacks() (func(error), protocol.PublishLeaseToken) {
	return func(error) { e.add("durable") }, newLeaseSinkToken(e)
}

// testLeaseSink implements the protocol lease-ops interface (exported
// methods) so broker tests observe BrokerDone firings — the old onReleased
// callback — through the generational-token API.
type testLeaseSink struct{ ev *retentionEvents }

func (l testLeaseSink) LeaseAbort(uint32)        {}
func (l testLeaseSink) LeaseTransfer(uint32)     {}
func (l testLeaseSink) LeaseServerDone(uint32)   {}
func (l testLeaseSink) LeaseBrokerDone(uint32)   { l.ev.add("released") }
func (l testLeaseSink) LeaseServerFinish(uint32) {}
func (l testLeaseSink) LeaseState() int32        { return 0 }

func newLeaseSinkToken(e *retentionEvents) protocol.PublishLeaseToken {
	return protocol.LeaseTokenOf(testLeaseSink{e}, 1)
}

// gateWALFsync holds every WAL group-commit fsync on a token channel so no
// durable completion fires until the test releases it.
func gateWALFsync(t *testing.T) (release func()) {
	t.Helper()
	gate := make(chan struct{})
	var once sync.Once
	restore := storage.SetWALGroupCommitFsyncForTest(func(f *os.File) error {
		<-gate
		return nil
	})
	t.Cleanup(restore)
	t.Cleanup(func() { once.Do(func() { close(gate) }) })
	return func() { once.Do(func() { close(gate) }) }
}

// retentionStorage wraps a real backend and intercepts the async durable
// write seams, so the tests can gate, error, or synchronously fire
// completions deterministically.
type retentionStorage struct {
	interfaces.Storage
	storeMessageAsync func(queueName string, message *protocol.Message, onDurable func(error)) error
	storeSharedAsync  func(subs []interfaces.SharedDurableSub) error
}

func (f *retentionStorage) StoreMessageAsync(queueName string, message *protocol.Message, onDurable func(error)) error {
	if f.storeMessageAsync != nil {
		return f.storeMessageAsync(queueName, message, onDurable)
	}
	return f.Storage.(asyncDurableWriter).StoreMessageAsync(queueName, message, onDurable)
}

func (f *retentionStorage) StoreSharedAsync(subs []interfaces.SharedDurableSub) error {
	if f.storeSharedAsync != nil {
		return f.storeSharedAsync(subs)
	}
	return f.Storage.(sharedDurableWriter).StoreSharedAsync(subs)
}

// syncOnlyStorage hides the async capabilities: the broker must fall back to
// the synchronous publish path.
type syncOnlyStorage struct{ interfaces.Storage }

func newRetentionBroker(t *testing.T, mutate func(*retentionStorage)) (*StorageBroker, *retentionStorage, func()) {
	t.Helper()
	tmpDir := t.TempDir()
	store, err := storage.NewDisruptorStorageWithDataDir(tmpDir)
	require.NoError(t, err)
	fake := &retentionStorage{Storage: store}
	if mutate != nil {
		mutate(fake)
	}
	engineConfig := interfaces.EngineConfig{
		RingBufferSize:          65536,
		SpillThresholdPercent:   80,
		WALBatchSize:            1000,
		WALBatchTimeoutMS:       10,
		ConsumerSelectTimeoutMS: 1,
		ConsumerMaxBatchSize:    100,
	}
	b := NewStorageBroker(fake, engineConfig)
	cleanup := func() {
		b.Close()
		_ = store.Close()
	}
	return b, fake, cleanup
}

func durableMsg(routingKey string) *protocol.Message {
	return &protocol.Message{Body: []byte("durable-body"), RoutingKey: routingKey, DeliveryMode: 2}
}

// RB-1: a single durable copy: nothing is released before the fsync; then
// onDurable(nil) followed by onReleased, once each, in that order.
func TestRetention_SingleDurableCopy(t *testing.T) {
	release := gateWALFsync(t)
	b, _, cleanup := newRetentionBroker(t, nil)
	defer cleanup()

	_, err := b.DeclareQueue("dq", true, false, false, nil)
	require.NoError(t, err)

	ev := &retentionEvents{}
	onDurable, lease := ev.callbacks()
	durableInflight, _, perr := b.PublishMessageAsyncConfirm("", "dq", durableMsg("dq"), onDurable, lease)
	require.NoError(t, perr)
	require.True(t, durableInflight)

	assert.Equal(t, 0, ev.count("released"), "nothing may be released before the fsync")
	assert.Equal(t, 0, ev.count("durable"), "onDurable must not fire before the fsync")

	release()
	require.Eventually(t, func() bool { return ev.count("released") == 1 }, 30*time.Second, time.Millisecond)
	assert.Equal(t, []string{"durable", "released"}, ev.snapshot(),
		"onReleased must fire after onDurable, exactly once each")
}

// RB-2: transient / no-route / mandatory-ErrNoRoute: synchronous release; no
// onDurable.
func TestRetention_TransientAndNoRouteSync(t *testing.T) {
	b, _, cleanup := newRetentionBroker(t, nil)
	defer cleanup()

	_, err := b.DeclareQueue("tq", false, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, b.DeclareExchange("ex0", "direct", true, false, false, nil))

	// Transient: synchronous release before return, no onDurable.
	ev := &retentionEvents{}
	onDurable, lease := ev.callbacks()
	msg := durableMsg("tq")
	msg.DeliveryMode = 1
	durableInflight, _, perr := b.PublishMessageAsyncConfirm("", "tq", msg, onDurable, lease)
	require.NoError(t, perr)
	assert.False(t, durableInflight)
	assert.Equal(t, 1, ev.count("released"), "transient publish must release synchronously")
	assert.Equal(t, 0, ev.count("durable"))

	// No route, non-mandatory: synchronous release, nil error.
	ev2 := &retentionEvents{}
	od2, lease2 := ev2.callbacks()
	durableInflight, _, perr = b.PublishMessageAsyncConfirm("ex0", "nowhere", durableMsg("nowhere"), od2, lease2)
	require.NoError(t, perr)
	assert.False(t, durableInflight)
	assert.Equal(t, 1, ev2.count("released"), "no-route publish must release synchronously")
	assert.Equal(t, 0, ev2.count("durable"))

	// No route, mandatory: ErrNoRoute with a synchronous release.
	ev3 := &retentionEvents{}
	od3, lease3 := ev3.callbacks()
	m := durableMsg("nowhere")
	m.Mandatory = true
	_, _, perr = b.PublishMessageAsyncConfirm("ex0", "nowhere", m, od3, lease3)
	require.ErrorIs(t, perr, ErrNoRoute)
	assert.Equal(t, 1, ev3.count("released"), "mandatory no-route must release synchronously")
	assert.Equal(t, 0, ev3.count("durable"))
}

// declareRejectQueue declares a durable queue that always rejects publishes
// (x-max-length=0 + x-overflow=reject-publish).
func declareRejectQueue(t *testing.T, b *StorageBroker, name string) {
	t.Helper()
	_, err := b.DeclareQueue(name, true, false, false, map[string]interface{}{
		"x-max-length": int64(0), "x-overflow": "reject-publish",
	})
	require.NoError(t, err)
}

// RB-3: 3 queues, one rejecting: ErrMaxLengthExceeded; no onDurable; release
// only after the 2 accepted copies complete.
func TestRetention_PartialFanoutReject(t *testing.T) {
	release := gateWALFsync(t)
	b, _, cleanup := newRetentionBroker(t, nil)
	defer cleanup()

	require.NoError(t, b.DeclareExchange("fx", "fanout", true, false, false, nil))
	for _, q := range []string{"q1", "q3"} {
		_, err := b.DeclareQueue(q, true, false, false, nil)
		require.NoError(t, err)
		require.NoError(t, b.BindQueue(q, "fx", "", nil))
	}
	declareRejectQueue(t, b, "q2")
	require.NoError(t, b.BindQueue("q2", "fx", "", nil))

	ev := &retentionEvents{}
	onDurable, lease := ev.callbacks()
	// Small body: per-queue path (below the shared-body threshold).
	_, _, perr := b.PublishMessageAsyncConfirm("fx", "", durableMsg(""), onDurable, lease)
	require.ErrorIs(t, perr, ErrMaxLengthExceeded)

	assert.Equal(t, 0, ev.count("durable"), "a rejected publish never fires onDurable")
	assert.Equal(t, 0, ev.count("released"), "release must wait for the 2 accepted copies")

	release()
	require.Eventually(t, func() bool { return ev.count("released") == 1 }, 30*time.Second, time.Millisecond)
	assert.Equal(t, 1, ev.count("released"))
	assert.Equal(t, 0, ev.count("durable"))
}

// RB-4: a queue torn down mid-fanout (StopCh): release after the 1st enqueued
// copy completes.
func TestRetention_StopChMidFanout(t *testing.T) {
	release := gateWALFsync(t)
	b, _, cleanup := newRetentionBroker(t, nil)
	defer cleanup()

	require.NoError(t, b.DeclareExchange("fx", "fanout", true, false, false, nil))
	_, err := b.DeclareQueue("q1", true, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, b.BindQueue("q1", "fx", "", nil))
	_, err = b.DeclareQueue("q2", true, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, b.BindQueue("q2", "fx", "", nil))

	// Route order is binding-filename (queue-name) order: q1 is enqueued
	// first, then q2's StopCh check bails with ErrQueueClosed.
	b.getOrCreateQueueState("q2").Close()

	ev := &retentionEvents{}
	onDurable, lease := ev.callbacks()
	_, _, perr := b.PublishMessageAsyncConfirm("fx", "", durableMsg(""), onDurable, lease)
	require.ErrorIs(t, perr, ErrQueueClosed)

	assert.Equal(t, 0, ev.count("released"), "release must wait for the enqueued q1 copy")
	release()
	require.Eventually(t, func() bool { return ev.count("released") == 1 }, 30*time.Second, time.Millisecond)
	assert.Equal(t, 1, ev.count("released"))
}

// RB-5: the shared (fused) fan-out: release after all subs complete; a
// StoreSharedAsync error releases exactly once, synchronously.
func TestRetention_SharedFanoutSuccessAndEnqueueError(t *testing.T) {
	release := gateWALFsync(t)
	b, _, cleanup := newRetentionBroker(t, nil)
	defer cleanup()

	require.NoError(t, b.DeclareExchange("fx", "fanout", true, false, false, nil))
	for _, q := range []string{"q1", "q2"} {
		_, err := b.DeclareQueue(q, true, false, false, nil)
		require.NoError(t, err)
		require.NoError(t, b.BindQueue(q, "fx", "", nil))
	}

	bigBody := make([]byte, DefaultSharedBodyThreshold+1)
	ev := &retentionEvents{}
	onDurable, lease := ev.callbacks()
	durableInflight, _, perr := b.PublishMessageAsyncConfirm("fx", "",
		&protocol.Message{Body: bigBody, DeliveryMode: 2}, onDurable, lease)
	require.NoError(t, perr)
	require.True(t, durableInflight)
	assert.Equal(t, 0, ev.count("released"), "release must wait for every sub")

	release()
	require.Eventually(t, func() bool { return ev.count("released") == 1 }, 30*time.Second, time.Millisecond)
	assert.Equal(t, []string{"durable", "released"}, ev.snapshot())

	// Enqueue error on the shared path: exactly one synchronous release.
	b2, fake2, cleanup2 := newRetentionBroker(t, nil)
	defer cleanup2()
	require.NoError(t, b2.DeclareExchange("fx", "fanout", true, false, false, nil))
	for _, q := range []string{"q1", "q2"} {
		_, err := b2.DeclareQueue(q, true, false, false, nil)
		require.NoError(t, err)
		require.NoError(t, b2.BindQueue(q, "fx", "", nil))
	}
	fake2.storeSharedAsync = func(subs []interfaces.SharedDurableSub) error {
		return errors.New("injected shared enqueue error")
	}
	ev2 := &retentionEvents{}
	od2, lease2 := ev2.callbacks()
	_, _, perr = b2.PublishMessageAsyncConfirm("fx", "",
		&protocol.Message{Body: bigBody, DeliveryMode: 2}, od2, lease2)
	require.Error(t, perr)
	assert.Equal(t, 1, ev2.count("released"), "shared enqueue error must release exactly once, synchronously")
	assert.Equal(t, 0, ev2.count("durable"))
}

// RB-6: a closed WAL completes synchronously with ErrWALClosed and returns
// nil (wal_manager.go's documented behavior): exactly one release, onDurable
// carries the error.
func TestRetention_WALClosed(t *testing.T) {
	b, fake, cleanup := newRetentionBroker(t, nil)
	defer cleanup()

	_, err := b.DeclareQueue("dq", true, false, false, nil)
	require.NoError(t, err)

	fake.storeMessageAsync = func(queueName string, message *protocol.Message, onDurable func(error)) error {
		onDurable(storage.ErrWALClosed) // closed WAL: synchronous completion, nil return
		return nil
	}

	ev := &retentionEvents{}
	onDurable, lease := ev.callbacks()
	durableInflight, _, perr := b.PublishMessageAsyncConfirm("", "dq", durableMsg("dq"), onDurable, lease)
	require.NoError(t, perr)
	require.True(t, durableInflight)
	assert.Equal(t, 1, ev.count("released"), "synchronous ErrWALClosed completion must release exactly once")
	assert.Equal(t, []string{"durable", "released"}, ev.snapshot())
}

// RB-7: a backend without async durable support: synchronous release.
func TestRetention_NonAsyncBackendFallback(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := storage.NewDisruptorStorageWithDataDir(tmpDir)
	require.NoError(t, err)
	engineConfig := interfaces.EngineConfig{
		RingBufferSize:          65536,
		SpillThresholdPercent:   80,
		WALBatchSize:            1000,
		WALBatchTimeoutMS:       10,
		ConsumerSelectTimeoutMS: 1,
		ConsumerMaxBatchSize:    100,
	}
	b := NewStorageBroker(&syncOnlyStorage{store}, engineConfig)
	defer func() {
		b.Close()
		_ = store.Close()
	}()

	_, err = b.DeclareQueue("dq", true, false, false, nil)
	require.NoError(t, err)

	ev := &retentionEvents{}
	onDurable, lease := ev.callbacks()
	durableInflight, _, perr := b.PublishMessageAsyncConfirm("", "dq", durableMsg("dq"), onDurable, lease)
	require.NoError(t, perr)
	assert.False(t, durableInflight, "non-async backend must report durableInflight=false")
	assert.Equal(t, 1, ev.count("released"), "synchronous fallback must release synchronously")
	assert.Equal(t, 0, ev.count("durable"))
}

// RB-8: per copy, exactly one of {completion fires, enqueue returns an
// error}. A 5-queue fan-out where q5's enqueue fails: 4 completions + 1
// enqueue error = 5 copies; the release fires exactly once, after the 4
// completions.
func TestRetention_PerCopyExactlyOne(t *testing.T) {
	b, fake, cleanup := newRetentionBroker(t, nil)
	defer cleanup()

	require.NoError(t, b.DeclareExchange("fx", "fanout", true, false, false, nil))
	for _, q := range []string{"q1", "q2", "q3", "q4", "q5"} {
		_, err := b.DeclareQueue(q, true, false, false, nil)
		require.NoError(t, err)
		require.NoError(t, b.BindQueue(q, "fx", "", nil))
	}

	var mu sync.Mutex
	var pending []func(error) // gated completions for accepted copies
	fake.storeMessageAsync = func(queueName string, message *protocol.Message, onDurable func(error)) error {
		if queueName == "q5" {
			return errors.New("injected enqueue error") // no completion will fire
		}
		mu.Lock()
		pending = append(pending, onDurable)
		mu.Unlock()
		return nil
	}

	ev := &retentionEvents{}
	onDurable, lease := ev.callbacks()
	_, _, perr := b.PublishMessageAsyncConfirm("fx", "", durableMsg(""), onDurable, lease)
	require.Error(t, perr, "the enqueue error must propagate")

	mu.Lock()
	require.Len(t, pending, 4, "exactly 4 copies were accepted and enqueued")
	mu.Unlock()
	assert.Equal(t, 0, ev.count("released"), "release must wait for the 4 enqueued completions")

	// Fire each accepted copy's completion exactly once.
	for i, c := range pending {
		c(nil)
		assert.Equal(t, 0, ev.count("durable"), "onDurable abandoned on the enqueue-error path (copy %d)", i)
	}
	require.Eventually(t, func() bool { return ev.count("released") == 1 }, 30*time.Second, time.Millisecond)
	assert.Equal(t, 1, ev.count("released"), "4 completions + 1 enqueue error = 5 copies: exactly one release")
}

// RB-9: a StoreSharedAsync that fires all completions synchronously before
// returning must not release early: addN(len(subs)) runs BEFORE the call and
// the setup guard is still held, so onReleased fires only after the call
// returns and the guard drops. The error variant uses doneN(len(subs)) and
// fires once.
func TestRetention_SharedAddNBeforeStore(t *testing.T) {
	b, fake, cleanup := newRetentionBroker(t, nil)
	defer cleanup()

	require.NoError(t, b.DeclareExchange("fx", "fanout", true, false, false, nil))
	for _, q := range []string{"q1", "q2", "q3"} {
		_, err := b.DeclareQueue(q, true, false, false, nil)
		require.NoError(t, err)
		require.NoError(t, b.BindQueue(q, "fx", "", nil))
	}

	ev := &retentionEvents{}
	fake.storeSharedAsync = func(subs []interfaces.SharedDurableSub) error {
		require.Len(t, subs, 3)
		for _, sub := range subs {
			sub.OnDurable(nil) // synchronous completion INSIDE the call
			assert.Equal(t, 0, ev.count("released"),
				"onReleased must not fire while StoreSharedAsync is still executing")
		}
		return nil
	}

	onDurable, lease := ev.callbacks()
	durableInflight, _, perr := b.PublishMessageAsyncConfirm("fx", "",
		&protocol.Message{Body: make([]byte, DefaultSharedBodyThreshold+1), DeliveryMode: 2}, onDurable, lease)
	require.NoError(t, perr)
	require.True(t, durableInflight)
	assert.Equal(t, []string{"durable", "released"}, ev.snapshot(),
		"exactly one release, after onDurable, once the guard dropped")

	// Error variant: no completions; doneN(len(subs)) + the guard → one
	// synchronous release.
	b2, fake2, cleanup2 := newRetentionBroker(t, nil)
	defer cleanup2()
	require.NoError(t, b2.DeclareExchange("fx", "fanout", true, false, false, nil))
	for _, q := range []string{"q1", "q2"} {
		_, err := b2.DeclareQueue(q, true, false, false, nil)
		require.NoError(t, err)
		require.NoError(t, b2.BindQueue(q, "fx", "", nil))
	}
	fake2.storeSharedAsync = func(subs []interfaces.SharedDurableSub) error {
		return errors.New("injected shared store error")
	}
	ev2 := &retentionEvents{}
	od2, lease2 := ev2.callbacks()
	_, _, perr = b2.PublishMessageAsyncConfirm("fx", "",
		&protocol.Message{Body: make([]byte, DefaultSharedBodyThreshold+1), DeliveryMode: 2}, od2, lease2)
	require.Error(t, perr)
	assert.Equal(t, 1, ev2.count("released"))
	assert.Equal(t, 0, ev2.count("durable"))
}

// RB-10: 4 targets — accepted, rejected, accepted, enqueue error — release
// exactly after the 2 accepted copies complete; no onDurable.
func TestRetention_AcceptedRejectEnqueueMix(t *testing.T) {
	b, fake, cleanup := newRetentionBroker(t, nil)
	defer cleanup()

	require.NoError(t, b.DeclareExchange("dx", "direct", true, false, false, nil))
	for _, q := range []string{"q1", "q3"} {
		_, err := b.DeclareQueue(q, true, false, false, nil)
		require.NoError(t, err)
		require.NoError(t, b.BindQueue(q, "dx", "k", nil))
	}
	declareRejectQueue(t, b, "q2")
	require.NoError(t, b.BindQueue("q2", "dx", "k", nil))
	_, err := b.DeclareQueue("q4", true, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, b.BindQueue("q4", "dx", "k", nil))

	// Route order is binding-filename (queue-name) order: q1 accept, q2
	// reject, q3 accept, q4 enqueue error.
	var mu sync.Mutex
	var pending []func(error)
	fake.storeMessageAsync = func(queueName string, message *protocol.Message, onDurable func(error)) error {
		if queueName == "q4" {
			return errors.New("injected enqueue error")
		}
		mu.Lock()
		pending = append(pending, onDurable)
		mu.Unlock()
		return nil
	}

	ev := &retentionEvents{}
	onDurable, lease := ev.callbacks()
	_, _, perr := b.PublishMessageAsyncConfirm("dx", "k", durableMsg("k"), onDurable, lease)
	require.Error(t, perr, "the q4 enqueue error must propagate")

	mu.Lock()
	require.Len(t, pending, 2, "exactly q1 and q3 were accepted and enqueued")
	mu.Unlock()
	assert.Equal(t, 0, ev.count("released"), "release must wait for the 2 accepted copies")
	assert.Equal(t, 0, ev.count("durable"))

	for _, c := range pending {
		c(nil)
	}
	require.Eventually(t, func() bool { return ev.count("released") == 1 }, 30*time.Second, time.Millisecond)
	assert.Equal(t, 1, ev.count("released"))
	assert.Equal(t, 0, ev.count("durable"), "no onDurable on the reject/enqueue-error path")
}

// RB-12: the PER-QUEUE multi-copy happy path (2+ targets, body BELOW the
// shared-body threshold => the per-queue loop, not the fused shared path):
// the confirm fan-in drops the setup guard, onDurable fires once when the
// LAST accepted copy completed, and the lease settles EXACTLY ONCE, after
// onDurable. This is the only path where the lease fan-in rides the multi-copy
// aggregator's zero transition on the SUCCESS path (RB-5 covers the fused
// shared path), so it is the path that would silently leak the lease if the
// guard drop were lost.
func TestRetention_PerQueueMultiCopySuccess(t *testing.T) {
	release := gateWALFsync(t)
	b, _, cleanup := newRetentionBroker(t, nil)
	defer cleanup()

	require.NoError(t, b.DeclareExchange("fx", "fanout", true, false, false, nil))
	for _, q := range []string{"q1", "q2"} {
		_, err := b.DeclareQueue(q, true, false, false, nil)
		require.NoError(t, err)
		require.NoError(t, b.BindQueue(q, "fx", "", nil))
	}

	ev := &retentionEvents{}
	onDurable, lease := ev.callbacks()
	// Small body: the per-queue path (below the shared-body threshold).
	durableInflight, _, perr := b.PublishMessageAsyncConfirm("fx", "", durableMsg(""), onDurable, lease)
	require.NoError(t, perr)
	require.True(t, durableInflight)

	assert.Equal(t, 0, ev.count("released"), "release must wait for every accepted copy")
	assert.Equal(t, 0, ev.count("durable"), "onDurable must not fire before the fsync")

	release()
	require.Eventually(t, func() bool { return ev.count("released") == 1 }, 30*time.Second, time.Millisecond)
	assert.Equal(t, []string{"durable", "released"}, ev.snapshot(),
		"exactly one durable + one release, in that order, once every copy completed")
}

// RB-13: the mid-fanout StopCh bail with NO copy ever accepted (the FIRST
// routed target is already closed): the fan-in is abandoned with an empty
// outstanding set, so the lease settles IMMEDIATELY (synchronously, before the
// call returns) and onDurable never fires.
func TestRetention_StopChBeforeAnyCopyAccepted(t *testing.T) {
	b, _, cleanup := newRetentionBroker(t, nil)
	defer cleanup()

	require.NoError(t, b.DeclareExchange("fx", "fanout", true, false, false, nil))
	for _, q := range []string{"q1", "q2"} {
		_, err := b.DeclareQueue(q, true, false, false, nil)
		require.NoError(t, err)
		require.NoError(t, b.BindQueue(q, "fx", "", nil))
	}

	// Route order is binding-filename (queue-name) order: q1 is checked first,
	// so the bail happens before any copy was enqueued.
	b.getOrCreateQueueState("q1").Close()

	ev := &retentionEvents{}
	onDurable, lease := ev.callbacks()
	_, _, perr := b.PublishMessageAsyncConfirm("fx", "", durableMsg(""), onDurable, lease)
	require.ErrorIs(t, perr, ErrQueueClosed)

	assert.Equal(t, 1, ev.count("released"),
		"no copy was ever accepted: the lease must settle synchronously")
	assert.Equal(t, 0, ev.count("durable"))
}

// RB-14: the exchange-error path (a durable publish to a non-existent
// exchange): the broker settles the lease SYNCHRONOUSLY (exactly once) before
// returning the routing error, and onDurable never fires.
func TestRetention_ExchangeErrorSettlesLease(t *testing.T) {
	b, _, cleanup := newRetentionBroker(t, nil)
	defer cleanup()

	ev := &retentionEvents{}
	onDurable, lease := ev.callbacks()
	_, _, perr := b.PublishMessageAsyncConfirm("no-such-exchange", "k", durableMsg("k"), onDurable, lease)
	require.Error(t, perr, "the missing exchange must surface as an error")

	assert.Equal(t, 1, ev.count("released"),
		"the exchange-error path must settle the lease synchronously, exactly once")
	assert.Equal(t, 0, ev.count("durable"), "a routing error never fires onDurable")
}

// RB-11: a nil onReleased is safe on every path.
func TestRetention_NilOnReleased(t *testing.T) {
	b, fake, cleanup := newRetentionBroker(t, nil)
	defer cleanup()

	_, err := b.DeclareQueue("tq", false, false, false, nil)
	require.NoError(t, err)
	_, err = b.DeclareQueue("dq", true, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, b.DeclareExchange("ex0", "direct", true, false, false, nil))
	require.NoError(t, b.DeclareExchange("fx", "fanout", true, false, false, nil))
	_, err = b.DeclareQueue("fq", true, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, b.BindQueue("fq", "fx", "", nil))

	// Transient.
	m := durableMsg("tq")
	m.DeliveryMode = 1
	_, _, err = b.PublishMessageAsyncConfirm("", "tq", m, func(error) {}, protocol.PublishLeaseToken{})
	require.NoError(t, err)

	// Durable success (real WAL, real fsync).
	_, _, err = b.PublishMessageAsyncConfirm("", "dq", durableMsg("dq"), func(error) {}, protocol.PublishLeaseToken{})
	require.NoError(t, err)

	// No route.
	_, _, err = b.PublishMessageAsyncConfirm("ex0", "nowhere", durableMsg("nowhere"), func(error) {}, protocol.PublishLeaseToken{})
	require.NoError(t, err)

	// Enqueue error.
	fake.storeMessageAsync = func(queueName string, message *protocol.Message, onDurable func(error)) error {
		return errors.New("injected enqueue error")
	}
	_, _, err = b.PublishMessageAsyncConfirm("fx", "", durableMsg(""), func(error) {}, protocol.PublishLeaseToken{})
	require.Error(t, err)
}
