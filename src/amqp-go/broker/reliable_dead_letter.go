package broker

import (
	"errors"
	"fmt"
	"iter"
	"slices"
	"sync"
	"time"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/maxpert/amqp-go/storage"
)

const (
	deadLetterRetryInterval    = 200 * time.Millisecond
	deadLetterFailuresPerBatch = 8
)

type deadLetterOutbox interface {
	SupportsReliableDeadLetters() bool
	StageDeadLetter(source string, message *protocol.Message) error
	PendingDeadLetters(stop <-chan struct{}) iter.Seq2[string, error]
	ReadDeadLetter(id string) (*storage.PendingDeadLetter, error)
	UpdateDeadLetter(id string, entry *storage.PendingDeadLetter) (replaced bool, err error)
	RemoveDeadLetter(id string) error
}

type deadLetterWorker struct {
	mu     sync.Mutex
	wake   chan struct{}
	stop   chan struct{}
	done   chan struct{}
	closed bool
}

func (b *StorageBroker) validateDeadLetterPolicy(p *QueuePolicy, durable bool) error {
	if p == nil || !p.ReliableDeadLetter {
		return nil
	}
	if !durable {
		return fmt.Errorf("%w: at-least-once dead-lettering requires a durable queue", ErrInvalidQueueArgument)
	}
	outbox, ok := b.storage.(deadLetterOutbox)
	if !ok || !outbox.SupportsReliableDeadLetters() || b.engineConfig.WALSyncDisabled {
		return fmt.Errorf("%w: at-least-once dead-lettering requires storage with WAL sync enabled", ErrInvalidQueueArgument)
	}
	return nil
}

func (b *StorageBroker) stageDeadLetter(source string, msg *protocol.Message) error {
	outbox, ok := b.storage.(deadLetterOutbox)
	if !ok || !outbox.SupportsReliableDeadLetters() || b.engineConfig.WALSyncDisabled {
		return errors.New("durable dead-letter storage unavailable")
	}
	if err := outbox.StageDeadLetter(source, msg); err != nil {
		b.logDeadLetterRetry("pending dead-letter could not be persisted; source kept", source, "", err)
		return fmt.Errorf("persist dead letter from %q: %w", source, err)
	}
	b.deadLetters.signal()
	return nil
}

// StartDeadLetterWorker resumes pending dead letters after queue frontiers have
// been restored.
func (b *StorageBroker) StartDeadLetterWorker() {
	outbox, ok := b.storage.(deadLetterOutbox)
	if !ok || !outbox.SupportsReliableDeadLetters() || b.engineConfig.WALSyncDisabled {
		return
	}
	w := &b.deadLetters
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return
	}
	if w.stop == nil {
		w.wake = make(chan struct{}, 1)
		w.stop = make(chan struct{})
		w.done = make(chan struct{})
		go b.runDeadLetterWorker(outbox, w)
	}
	w.signalLocked()
}

func (w *deadLetterWorker) signal() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.signalLocked()
}

func (w *deadLetterWorker) signalLocked() {
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

func (w *deadLetterWorker) close() {
	w.mu.Lock()
	if !w.closed && w.stop != nil {
		close(w.stop)
	}
	w.closed = true
	done := w.done
	w.mu.Unlock()
	if done != nil {
		<-done
	}
}

func (b *StorageBroker) runDeadLetterWorker(outbox deadLetterOutbox, w *deadLetterWorker) {
	defer close(w.done)
	rescan := false
	for {
		if !rescan {
			select {
			case <-w.stop:
				return
			case <-w.wake:
			}
		}
		select {
		case <-w.stop:
			return
		default:
		}
		rescan = b.scanPendingDeadLetters(outbox, w.stop)
	}
}

func (b *StorageBroker) scanPendingDeadLetters(outbox deadLetterOutbox, stop <-chan struct{}) bool {
	rescan := false
	failed := 0
	for id, err := range outbox.PendingDeadLetters(stop) {
		select {
		case <-stop:
			return false
		default:
		}
		if err != nil {
			b.logDeadLetterRetry("could not list pending dead letters", "", "", err)
			return waitDeadLetterWorkerRetry(stop)
		}
		rescan = true
		if b.deliverPendingDeadLetter(outbox, id, stop) {
			continue
		}
		failed++
		if failed == deadLetterFailuresPerBatch {
			// Keep the cursor so a backlog of failures cannot starve later entries.
			if !waitDeadLetterWorkerRetry(stop) {
				return false
			}
			failed = 0
		}
	}
	// Rescan even after a pass of successful removals, since removing entries
	// while reading the directory can cause the scan to skip other files.
	return rescan && (failed == 0 || waitDeadLetterWorkerRetry(stop))
}

func waitDeadLetterWorkerRetry(stop <-chan struct{}) bool {
	timer := time.NewTimer(deadLetterRetryInterval)
	defer timer.Stop()
	select {
	case <-stop:
		return false
	case <-timer.C:
		return true
	}
}

// deliverPendingDeadLetter returns true once the entry is complete and removed.
func (b *StorageBroker) deliverPendingDeadLetter(outbox deadLetterOutbox, id string, stop <-chan struct{}) bool {
	entry, err := outbox.ReadDeadLetter(id)
	if err != nil {
		b.logDeadLetterRetry("could not read pending dead letter", "", id, err)
		return false
	}
	if entry.Targets == nil {
		targets, resolved := b.routePendingDeadLetter(entry)
		if !resolved {
			return false
		}
		entry.Targets = targets
		if !b.saveDeadLetterProgress(outbox, id, entry, false, stop) {
			return false
		}
	}
	remaining := make([]string, 0, len(entry.Targets))
	for _, target := range entry.Targets {
		if !b.deliverDurableDeadLetter(entry, target) {
			remaining = append(remaining, target)
		}
	}
	if len(remaining) < len(entry.Targets) {
		entry.Targets = remaining
		if !b.saveDeadLetterProgress(outbox, id, entry, true, stop) {
			return false
		}
	}
	if len(entry.Targets) != 0 {
		return false
	}
	if err := outbox.RemoveDeadLetter(id); err != nil {
		b.logDeadLetterRetry("could not remove completed dead letter", entry.Source, id, err)
		return false
	}
	return true
}

func (b *StorageBroker) saveDeadLetterProgress(outbox deadLetterOutbox, id string, entry *storage.PendingDeadLetter, mustPersist bool, stop <-chan struct{}) bool {
	for {
		replaced, err := outbox.UpdateDeadLetter(id, entry)
		if err == nil {
			return true
		}
		b.logDeadLetterRetry("could not persist pending dead-letter progress", entry.Source, id, err)
		// Keep this entry once any target has committed or a route update is
		// visible without a successful sync. Earlier failures can be deferred.
		mustPersist = mustPersist || replaced
		if !mustPersist || !waitDeadLetterWorkerRetry(stop) {
			return false
		}
	}
}

func (b *StorageBroker) routePendingDeadLetter(entry *storage.PendingDeadLetter) ([]string, bool) {
	msg := entry.Message
	ex, err := b.storage.GetExchange(msg.Exchange)
	if err != nil || ex == nil {
		return nil, false
	}
	targets, err := b.findTargetQueues(ex, msg.RoutingKey, msg)
	if err != nil || len(targets) == 0 {
		return nil, false
	}
	// The freshly annotated occurrence is the first x-death entry, even
	// after WAL payload decoding, so it identifies this hop's reason.
	if deaths, _ := msg.Headers[xDeathHeader].([]interface{}); len(deaths) > 0 {
		if current, _ := deaths[0].(map[string]interface{}); current[xDeathReason] == string(DeadLetterRejected) {
			return targets, true
		}
	}
	cyclic := xDeathQueueSet(msg.Headers)
	allowed := targets[:0]
	for _, target := range targets {
		if _, skip := cyclic[target]; !skip {
			allowed = append(allowed, target)
		}
	}
	return allowed, true
}

func (b *StorageBroker) deliverDurableDeadLetter(entry *storage.PendingDeadLetter, target string) bool {
	// Serialize the WAL completion with queue teardown. Its successful return
	// must refer to a live durable queue incarnation at the time of the write.
	mu := b.getQueueCreateMutex(target)
	mu.Lock()
	q, err := b.storage.GetQueue(target)
	if err != nil || q == nil || !q.Durable {
		mu.Unlock()
		return false
	}
	// A queue newly declared under the same name can accept the pending
	// transfer once it is routed by the DLX again.
	if queues, ok := b.routePendingDeadLetter(entry); !ok || !slices.Contains(queues, target) {
		mu.Unlock()
		return false
	}
	qs := b.createQueueStateLocked(target)
	p := qs.Policy()
	copy := cloneForDeadLetter(entry.Message)
	copy.DeliveryMode = 2
	copy.EnqueueUnixMilli = 0
	if _, ok := parsePerMessageTTLMillis(copy.Expiration); ok || p != nil && p.HasMessageTTL {
		copy.EnqueueUnixMilli = b.ttlNowMillis()
	}
	err = b.republishToTarget(qs, p, target, copy)
	mu.Unlock()
	switch err {
	case nil:
		b.trimDeadLetterTarget(qs, p, target)
		return true
	case errDeadLetterTargetGone, errDeadLetterTargetFull, errDeadLetterOverflow:
	default:
		b.logDeadLetterRetry("pending dead-letter target store failed; will retry", entry.Source, target, err)
	}
	return false
}

func waitDeadLetterReadRetry(qs *QueueState, state *ConsumerState) {
	timer := time.NewTimer(deadLetterRetryInterval)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-state.stopCh:
	case <-qs.StopCh():
	}
}

func (b *StorageBroker) logDeadLetterRetry(message, source, target string, err error) {
	if b.logger != nil {
		b.logger.Warn(message,
			interfaces.LogField{Key: "source_queue", Value: source},
			interfaces.LogField{Key: "target", Value: target},
			interfaces.LogField{Key: "error", Value: err})
	}
}
