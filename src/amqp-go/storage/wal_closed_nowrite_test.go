package storage

// THE onWritten HOOK FIRES ONLY WHEN A WRITE ACTUALLY HAPPENED.
//
// onWritten is the RAM-ADMISSION point: "the record's bytes are in the WAL
// file". Its whole contract is that the caller may take the body into a
// cache as BACKED — because there is a copy of it in the file, and the body
// can be read back from there.
//
// A CLOSED WAL is not a write. Firing onWritten there would admit a body
// into the hot cache as backed=true even though there is NO WAL copy of it
// at all: the message would be RAM-resident, undurable, and unreadable —
// the worst of both worlds. It also contradicts the hook's own contract.
//
// The completion still fires exactly once, with ErrWALClosed: the caller
// settles whatever it must settle on the failure path (the broker's publish
// lease is settled by the completion's fallback).

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/maxpert/amqp-go/protocol"
)

// TestWriteAsyncObserved_ClosedWALDoesNotFireOnWritten is the WAL-level
// contract: with the WAL closed, NO write happens, so onWritten must not
// fire, and the completion must fire exactly once with ErrWALClosed.
func TestWriteAsyncObserved_ClosedWALDoesNotFireOnWritten(t *testing.T) {
	dir := t.TempDir()
	wm, err := NewWALManagerWithConfig(dir, DefaultWALConfig())
	require.NoError(t, err)
	require.NoError(t, wm.Close())

	var written, completed atomic.Int32
	var completionErr error
	wm.WriteAsyncObserved("q", &protocol.Message{
		Body: []byte("never-written"), RoutingKey: "q", DeliveryMode: 2, DeliveryTag: 1,
	}, 1,
		func() { written.Add(1) },
		func(e error) {
			completed.Add(1)
			completionErr = e
		})

	require.EqualValues(t, 0, written.Load(),
		"no write happened (the WAL is closed), so the onWritten hook must NOT fire: firing it "+
			"would admit a body into the hot cache as backed=true with no WAL copy behind it")
	require.EqualValues(t, 1, completed.Load(),
		"the completion must fire exactly once")
	require.ErrorIs(t, completionErr, ErrWALClosed)
}

// TestStoreMessageAsync_ClosedWALDoesNotAdmitOrFire is the storage-level
// contract: with the WAL closed, the body is never admitted to the hot cache
// and the publish is nacked exactly once.
func TestStoreMessageAsync_ClosedWALDoesNotAdmitOrFire(t *testing.T) {
	ds := newColdReadStorage(t, t.TempDir())
	require.NoError(t, ds.wal.Close())

	msg := &protocol.Message{
		Exchange: "amq.direct", RoutingKey: "closed-q",
		Body: []byte("never-written"), DeliveryMode: 2, DeliveryTag: 5,
	}

	var written, completed atomic.Int32
	var completionErr error
	require.NoError(t, ds.StoreMessageAsync("closed-q", msg,
		func() { written.Add(1) },
		func(e error) {
			completed.Add(1)
			completionErr = e
		}))

	require.EqualValues(t, 0, written.Load(),
		"no write happened, so the onWritten hook must NOT fire")
	require.EqualValues(t, 1, completed.Load(),
		"the completion must fire exactly once")
	require.ErrorIs(t, completionErr, ErrWALClosed)

	// NO HOT-CACHE ENTRY: there is no WAL copy of the body, so nothing may
	// be RAM-resident.
	_, _, resident := ds.hot.GetResident("closed-q", 5)
	require.False(t, resident, "a message with no WAL copy must not be admitted to the hot cache")
	_, _, served := ds.hot.Get("closed-q", 5)
	require.False(t, served, "a message with no WAL copy must not be served")

	// AND THE QUEUE DEPTH MUST NOT ADVANCE.
	hot := ds.getQueueHot("closed-q")
	require.NotNil(t, hot)
	require.EqualValues(t, 0, hot.ack.Head(),
		"a publish that never reached the WAL must not advance the queue depth")
}
