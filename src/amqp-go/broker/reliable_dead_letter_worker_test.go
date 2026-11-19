package broker

import (
	"errors"
	"fmt"
	"iter"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/maxpert/amqp-go/storage"
	"github.com/stretchr/testify/require"
)

type unreadableDeadLetterOutbox struct {
	*storage.StorageEngine
	ids       []string
	mu        sync.Mutex
	reads     []string
	openScans int
}

func (s *unreadableDeadLetterOutbox) PendingDeadLetters(_ <-chan struct{}) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		s.mu.Lock()
		s.openScans++
		s.mu.Unlock()
		defer func() {
			s.mu.Lock()
			s.openScans--
			s.mu.Unlock()
		}()
		for _, id := range s.ids {
			if !yield(id, nil) {
				return
			}
		}
	}
}

func (s *unreadableDeadLetterOutbox) ReadDeadLetter(id string) (*storage.PendingDeadLetter, error) {
	s.mu.Lock()
	s.reads = append(s.reads, id)
	s.mu.Unlock()
	return nil, errors.New("injected outbox read failure")
}

func (s *unreadableDeadLetterOutbox) snapshot() ([]string, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.reads), s.openScans
}

func TestDeadLetterWorkerPacesFailuresAndResumesScan(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		outbox := &unreadableDeadLetterOutbox{}
		for i := range 32 {
			outbox.ids = append(outbox.ids, fmt.Sprint(i))
		}
		b := &StorageBroker{}
		w := &b.deadLetters
		w.wake = make(chan struct{}, 1)
		w.stop = make(chan struct{})
		w.done = make(chan struct{})
		go b.runDeadLetterWorker(outbox, w)
		defer w.close()

		w.signal()
		synctest.Wait()
		reads, _ := outbox.snapshot()
		firstBatch := len(reads)
		require.Positive(t, firstBatch)
		require.Less(t, firstBatch, len(outbox.ids)/2, "an outage must not replay the whole backlog in one burst")
		for range 4 {
			w.signal()
			synctest.Wait()
		}
		reads, openScans := outbox.snapshot()
		require.Len(t, reads, firstBatch, "incoming work must not bypass retry pacing")
		require.Equal(t, 1, openScans)

		time.Sleep(deadLetterRetryInterval)
		synctest.Wait()
		reads, openScans = outbox.snapshot()
		require.Len(t, reads, 2*firstBatch)
		require.Equal(t, outbox.ids[:2*firstBatch], reads, "the next batch must continue through the backlog")
		require.Equal(t, 1, openScans)

		w.close()
		_, openScans = outbox.snapshot()
		require.Zero(t, openScans, "stopping must close the suspended scan")
	})
}

type deadLetterRouteWriteFaultStorage struct {
	*storage.StorageEngine
	failed chan struct{}
}

func (s *deadLetterRouteWriteFaultStorage) UpdateDeadLetter(id string, entry *storage.PendingDeadLetter) (bool, error) {
	if string(entry.Message.Body) == "oversized" {
		select {
		case s.failed <- struct{}{}:
		default:
		}
		return false, errors.New("insufficient space for outbox routing update")
	}
	return s.StorageEngine.UpdateDeadLetter(id, entry)
}

func TestDeadLetterWorkerDefersFailedInitialRouteWrite(t *testing.T) {
	engine, err := storage.NewStorageEngineWithDataDir(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	outbox := &deadLetterRouteWriteFaultStorage{StorageEngine: engine, failed: make(chan struct{}, 1)}
	b := NewStorageBroker(outbox, interfaces.EngineConfig{MaxQueueBacklog: interfaces.DefaultMaxQueueBacklog})
	t.Cleanup(b.Close)
	require.NoError(t, b.InitError())
	require.NoError(t, b.DeclareExchange("reliable-dlx", "fanout", true, false, false, nil))
	declareReliableTarget(t, b, "retry-target", nil)

	require.NoError(t, b.stageDeadLetter("source", &protocol.Message{Exchange: "reliable-dlx", Body: []byte("oversized")}))
	b.StartDeadLetterWorker()
	select {
	case <-outbox.failed:
	case <-time.After(3 * time.Second):
		t.Fatal("worker never attempted the initial route write")
	}

	require.NoError(t, b.stageDeadLetter("source", &protocol.Message{Exchange: "reliable-dlx", Body: []byte("fits")}))
	require.Eventually(t, func() bool { return b.GetQueueReadyCount("retry-target") > 0 }, 3*time.Second, 10*time.Millisecond,
		"an initial route write that never installed its record must not block other entries")
	msg := drainOne(t, b, "retry-target")
	require.NotNil(t, msg)
	require.Equal(t, "fits", string(msg.Body))
}
