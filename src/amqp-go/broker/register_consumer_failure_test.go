package broker

import (
	"errors"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/maxpert/amqp-go/storage"
	"github.com/stretchr/testify/require"
)

// storeConsumerFailStorage wraps a real storage and fails StoreConsumer, the one
// call in RegisterConsumer that can return an error after the argument checks
// pass. Every implementation in the tree is a documented no-op today, so this is
// the only way to reach the failure path at all.
type storeConsumerFailStorage struct {
	interfaces.Storage
	fails atomic.Int64
}

func (s *storeConsumerFailStorage) StoreConsumer(queueName, consumerID string, consumer *protocol.Consumer) error {
	s.fails.Add(1)
	return errors.New("injected StoreConsumer failure")
}

func newStoreConsumerFailBroker(t *testing.T) (*StorageBroker, *storeConsumerFailStorage) {
	t.Helper()
	real, err := storage.NewDisruptorStorageWithDataDir(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = real.Close() })

	fs := &storeConsumerFailStorage{Storage: real}
	return NewStorageBroker(fs, interfaces.EngineConfig{
		DepthHighWM:             52428,
		WALBatchSize:            1000,
		WALBatchTimeoutMS:       10,
		ConsumerSelectTimeoutMS: 1,
		ConsumerMaxBatchSize:    100,
	}), fs
}

// TestRegisterConsumer_StoreFailureLeavesNoBrokerState pins that a failed
// registration is atomic: it publishes nothing.
//
// RegisterConsumer used to persist LAST, after it had already stored a
// ConsumerState in activeConsumers, registered a storage ack cursor, appended to
// queueConsumers and spawned a poll-loop goroutine. Returning the store error
// from there left all four behind with no handle any caller could use to undo
// them — a leaked goroutine and a leaked ConsumerState per failure. It was
// invisible only because StoreConsumer is a no-op in every implementation, i.e.
// a correctness bug held harmless by dead code. The fix persists write-ahead, so
// there is no window in which partial state exists.
//
// The residue assertions are deliberately about broker-visible state rather than
// about goroutine counts: activeConsumers is the only handle the poll-loop
// goroutine is reachable through, so proving that map is clean proves the
// goroutine was never started.
func TestRegisterConsumer_StoreFailureLeavesNoBrokerState(t *testing.T) {
	b, fs := newStoreConsumerFailBroker(t)

	const (
		queue = "regfail-q"
		id    = "regfail-c1"
	)
	if _, err := b.DeclareQueue(queue, false, false, false, nil); err != nil {
		t.Fatalf("DeclareQueue: %v", err)
	}

	before := runtime.NumGoroutine()

	consumer := &protocol.Consumer{
		Tag:           id,
		ID:            id,
		Queue:         queue,
		PrefetchCount: 10,
		Messages:      make(chan *protocol.Delivery, 10),
		Cancel:        make(chan struct{}),
	}
	err := b.RegisterConsumer(queue, id, consumer)
	if err == nil {
		t.Fatal("RegisterConsumer returned nil while the storage layer rejected the consumer")
	}

	// Active-assertion check: the seam must actually have fired, or every
	// assertion below is about a registration that simply succeeded.
	if n := fs.fails.Load(); n != 1 {
		t.Fatalf("injected StoreConsumer was called %d time(s), want exactly 1; "+
			"the failure path was never reached and this test proved nothing", n)
	}

	if _, ok := b.GetConsumers()[id]; ok {
		t.Errorf("a consumer whose registration FAILED is live in activeConsumers under %q; "+
			"its poll-loop goroutine is running and will never be stopped", id)
	}
	if ids := b.GetQueueConsumerIDs(queue); len(ids) != 0 {
		t.Errorf("queueConsumers holds %v after a failed registration; want empty", ids)
	}
	if n := b.GetQueueConsumerCount(queue); n != 0 {
		t.Errorf("GetQueueConsumerCount = %d after a failed registration; want 0", n)
	}
	// Nothing to tear down is the point: an UnregisterConsumer that SUCCEEDS
	// here means there was state to remove.
	if uerr := b.UnregisterConsumer(id); uerr == nil {
		t.Error("UnregisterConsumer succeeded after a failed registration; " +
			"the failed register left broker state behind")
	}

	// Corroborating (not load-bearing) goroutine check: the poll loop is the
	// only goroutine RegisterConsumer starts.
	deadline := time.Now().Add(2 * time.Second)
	after := runtime.NumGoroutine()
	for after > before && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
		after = runtime.NumGoroutine()
	}
	if after > before {
		t.Errorf("goroutine count %d -> %d across a failed registration; a poll loop leaked", before, after)
	}
}

// TestRegisterConsumer_RequiresIdentityAgreementAndNeverWritesIt pins the other
// half of the same contract: RegisterConsumer takes the identity as an argument
// and treats *consumer as read-only.
//
// By the time the server calls it, the consumer is already installed in
// channel.Consumers and the connection's delivery loop reads consumer.ID under
// the channel mutex. The old code carried an `if consumer.ID == "" { consumer.ID
// = consumerID }` fallback — a write to that published struct from outside the
// mutex, unreachable from the server only because the server always sets ID
// first. Safe by accident is not safe; the fallback now lives in this package's
// test helper (registerConsumer), which is where it was actually used.
func TestRegisterConsumer_RequiresIdentityAgreementAndNeverWritesIt(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()
	if _, err := b.DeclareQueue("agree-q", false, false, false, nil); err != nil {
		t.Fatalf("DeclareQueue: %v", err)
	}

	newConsumer := func(id string) *protocol.Consumer {
		return &protocol.Consumer{
			Tag:           "agree-tag",
			ID:            id,
			Queue:         "agree-q",
			PrefetchCount: 10,
			Messages:      make(chan *protocol.Delivery, 10),
			Cancel:        make(chan struct{}),
		}
	}

	// An unset identity must be REJECTED, not silently filled in.
	unset := newConsumer("")
	if err := b.RegisterConsumer("agree-q", "agree-c1", unset); err == nil {
		t.Fatal("RegisterConsumer accepted a consumer with an empty ID; " +
			"the identity must come from the caller, not from a fallback write")
	}
	if unset.ID != "" {
		t.Fatalf("RegisterConsumer WROTE consumer.ID = %q on a struct the caller owns; "+
			"on the server path that struct is already published to the delivery loop", unset.ID)
	}
	if _, ok := b.GetConsumers()["agree-c1"]; ok {
		t.Error("a rejected registration is nonetheless live in activeConsumers")
	}

	// A disagreeing identity must be rejected too, and equally must not be
	// overwritten to paper over the disagreement.
	mismatch := newConsumer("agree-other")
	if err := b.RegisterConsumer("agree-q", "agree-c1", mismatch); err == nil {
		t.Fatal("RegisterConsumer accepted consumerID != consumer.ID; " +
			"the broker would then hold one consumer under two identities")
	}
	if mismatch.ID != "agree-other" {
		t.Fatalf("RegisterConsumer overwrote a disagreeing consumer.ID to %q", mismatch.ID)
	}

	// Agreement is accepted, so the rejections above are not vacuous.
	ok := newConsumer("agree-c1")
	if err := b.RegisterConsumer("agree-q", "agree-c1", ok); err != nil {
		t.Fatalf("RegisterConsumer rejected an agreeing identity: %v", err)
	}
	defer func() { _ = b.UnregisterConsumer("agree-c1") }()
	if _, live := b.GetConsumers()["agree-c1"]; !live {
		t.Fatal("an accepted registration is not in activeConsumers")
	}
}
