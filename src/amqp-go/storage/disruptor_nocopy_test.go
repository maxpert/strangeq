package storage

import (
	"bytes"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// THE HOT CACHE IS A RAM HOLDER, NOT THE VISIBILITY GATE.
//
// The body is admitted to the hot cache the moment the WAL write returns
// (the bytes are in the file and re-readable), but the QUEUE DEPTH — the
// gate every consumer's claim goes through — still advances only in the
// completion, i.e. AFTER the fdatasync. A consumer must never be handed a
// message the broker has not yet made durable.

// atomic32 is a tiny atomic counter for the test callbacks.
type atomic32 struct {
	v atomic.Int32
}

func (a *atomic32) set(n int32) { a.v.Store(n) }
func (a *atomic32) get() int32  { return a.v.Load() }

// newNoCopyStorage builds a DisruptorStorage over a temp dir.
func newNoCopyStorage(t *testing.T) *DisruptorStorage {
	t.Helper()
	ds, err := NewDisruptorStorageWithDataDir(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds.Close() })
	return ds
}

// TestStorage_MessageNotVisibleBeforeTheFsync (TDD): with the committer
// parked inside its fdatasync the message is in the file but NOT durable, so
// the queue depth must not have advanced. After the fsync returns, it must.
func TestStorage_MessageNotVisibleBeforeTheFsync(t *testing.T) {
	ds := newNoCopyStorage(t)

	fsyncEntered := make(chan struct{})
	releaseFsync := make(chan struct{})
	var once sync.Once
	restore := SetWALGroupCommitFsyncForTest(func(f *os.File) error {
		once.Do(func() { close(fsyncEntered) })
		<-releaseFsync
		return nil
	})
	t.Cleanup(restore)

	msg := &protocol.Message{
		Exchange:     "amq.direct",
		RoutingKey:   "q",
		Body:         nocopyBody(4096),
		DeliveryMode: 2,
		DeliveryTag:  1,
	}
	var written, durable atomic32
	err := ds.StoreMessageAsync("q", msg, func() { written.set(1) }, func(e error) { durable.set(1) })
	require.NoError(t, err)

	select {
	case <-fsyncEntered:
	case <-time.After(10 * time.Second):
		t.Fatal("the committer never reached the fsync")
	}

	// The write happened (the body is in the file and in the hot cache) ...
	assert.EqualValues(t, 1, written.get(),
		"the hot-cache admission must happen at the write")
	//
	// ... as a NOT-YET-DURABLE entry: the covering fdatasync has not
	// returned, so the entry is NOT served. (GetResident is the residency
	// probe that ignores the durability state; Get is the one the delivery
	// path uses, and it must not serve a message that is not durable yet.)
	_, _, resident := ds.hot.GetResident("q", 1)
	assert.True(t, resident,
		"the body must be admitted to the hot cache at the WRITE, not at the completion")
	_, _, served := ds.hot.Get("q", 1)
	assert.False(t, served,
		"a message that is not durable yet must not be served from the hot cache")
	_, gerr := ds.GetMessage("q", 1)
	assert.Error(t, gerr,
		"a message that is not durable yet must not be retrievable (the durability contract)")
	// ... but the message is NOT visible yet: the queue depth has not
	// advanced, so no consumer can claim it.
	hot := ds.getQueueHot("q")
	require.NotNil(t, hot)
	assert.EqualValues(t, 0, hot.ack.Head(),
		"the queue depth must not advance before the fdatasync")

	close(releaseFsync)
	require.Eventually(t, func() bool { return durable.get() == 1 }, 10*time.Second, 5*time.Millisecond,
		"the completion must fire after the fsync")
	assert.EqualValues(t, 1, hot.ack.Head(),
		"the queue depth must advance once the record is durable")
}

// TestStorage_FailedFsync_IsNotVisibleAndNotCached (TDD): a record whose
// fdatasync fails is NOT durable, so it must not be visible and must not be
// left in the hot cache.
func TestStorage_FailedFsync_IsNotVisibleAndNotCached(t *testing.T) {
	ds := newNoCopyStorage(t)

	restore := SetWALGroupCommitFsyncForTest(func(f *os.File) error {
		return os.ErrInvalid
	})
	t.Cleanup(restore)

	msg := &protocol.Message{
		Exchange:     "amq.direct",
		RoutingKey:   "q",
		Body:         nocopyBody(4096),
		DeliveryMode: 2,
		DeliveryTag:  1,
	}
	errCh := make(chan error, 1)
	require.NoError(t, ds.StoreMessageAsync("q", msg, nil, func(e error) { errCh <- e }))

	select {
	case e := <-errCh:
		require.Error(t, e, "a failed fsync must nack the publish")
	case <-time.After(10 * time.Second):
		t.Fatal("the completion never fired")
	}

	hot := ds.getQueueHot("q")
	require.NotNil(t, hot)
	assert.EqualValues(t, 0, hot.ack.Head(),
		"a record that is not durable must not be visible")
	_, _, ok := ds.hot.Get("q", 1)
	assert.False(t, ok, "a record that is not durable must not be left in the hot cache")
}

// TestStorage_DeliveryWorksWhenTheBodyIsNoLongerInRAM (TDD): the body lives
// in the WAL, not in RAM. With the hot-cache entry gone, the delivery path
// must read the body back from the file and hand it back intact.
func TestStorage_DeliveryWorksWhenTheBodyIsNoLongerInRAM(t *testing.T) {
	ds := newNoCopyStorage(t)

	body := nocopyBody(64 * 1024)
	msg := &protocol.Message{
		Exchange:     "amq.direct",
		RoutingKey:   "q",
		Body:         body,
		DeliveryMode: 2,
		DeliveryTag:  7,
	}
	done := make(chan error, 1)
	require.NoError(t, ds.StoreMessageAsync("q", msg, nil, func(e error) { done <- e }))
	require.NoError(t, <-done)

	// The body is no longer in RAM: the cache entry is dropped (the WAL file
	// is the only copy left).
	ds.hot.Delete("q", 7)
	_, _, ok := ds.hot.Get("q", 7)
	require.False(t, ok)

	got, err := ds.GetMessage("q", 7)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.True(t, bytes.Equal(got.Body, body),
		"the body read back from the WAL must be byte-identical")
	assert.EqualValues(t, 7, got.DeliveryTag)
}

// TestStorage_ReadPathPopulatesTheHotCache (TDD): a cold DELIVERY read
// admits what it just read, so the consumer's working set is RAM-resident.
//
// IT IS THE DELIVERY READ, NOT THE PEEK: the write-through admission is
// attributable to a CONSUMER DELIVERY. A probe or a peek (storage.GetMessage)
// must not admit — see storage/cold_read_count_test.go (F8).
func TestStorage_ReadPathPopulatesTheHotCache(t *testing.T) {
	ds := newNoCopyStorage(t)

	body := nocopyBody(4096)
	msg := &protocol.Message{
		Exchange:     "amq.direct",
		RoutingKey:   "q",
		Body:         body,
		DeliveryMode: 2,
		DeliveryTag:  3,
	}
	done := make(chan error, 1)
	require.NoError(t, ds.StoreMessageAsync("q", msg, nil, func(e error) { done <- e }))
	require.NoError(t, <-done)

	// Drop the publish-path admission so the next read is a genuine miss.
	ds.hot.Delete("q", 3)

	got, err := ds.GetMessageForDelivery("q", 3)
	require.NoError(t, err)
	require.NotNil(t, got)

	_, _, ok := ds.hot.Get("q", 3)
	assert.True(t, ok, "a cold delivery read must admit what it read back into the hot cache")

	// AND THE PEEK MUST NOT ADMIT: the same message, read again through the
	// non-admitting read, leaves the count alone.
	ds.hot.Delete("q", 3)
	peeked, err := ds.GetMessage("q", 3)
	require.NoError(t, err)
	require.NotNil(t, peeked)
	_, _, resident := ds.hot.Get("q", 3)
	assert.False(t, resident, "a peek must not admit what it read")
}
