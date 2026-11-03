package broker

// A CLOSED WAL MUST NOT SETTLE THE PUBLISH LEASE TWICE, AND MUST NOT CONFIRM
// A MESSAGE THAT WAS NEVER WRITTEN.
//
// The storage's onWritten hook is the RAM-ADMISSION point: it fires when the
// record's bytes are in the WAL file. A CLOSED WAL is not a write, so the
// hook must NOT fire there (storage/wal_closed_nowrite_test.go pins that
// half). The broker's completion then settles the publish lease exactly once
// through its own fallback — the lease must never be stranded and never
// released twice.
//
// The confirm must report the failure: the message was never durable, so the
// publisher must be told so.

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/maxpert/amqp-go/storage"
)

// TestBroker_ClosedWALReleasesTheLeaseExactlyOnce: with the WAL closed, the
// publish lease is released EXACTLY ONCE and the confirm fires exactly once,
// with the WAL's error.
func TestBroker_ClosedWALReleasesTheLeaseExactlyOnce(t *testing.T) {
	b, fake, cleanup := newReadTriageBroker(t)
	defer cleanup()

	_, err := b.DeclareQueue("closed-wal-q", false, false, false, nil)
	require.NoError(t, err)

	// CLOSE THE WAL. The storage's Close closes the WAL (and only the WAL
	// matters here).
	require.NoError(t, fake.DisruptorStorage.Close())

	ev := &retentionEvents{}
	onDurable, lease := ev.callbacks()

	confirmed := make(chan error, 1)
	_, _, err = b.PublishMessageAsyncConfirm("", "closed-wal-q",
		&protocol.Message{RoutingKey: "closed-wal-q", Body: []byte("payload"), DeliveryMode: 2},
		func(e error) {
			onDurable(e)
			confirmed <- e
		},
		lease)
	require.NoError(t, err, "the publish is accepted; the failure is reported through the confirm")

	select {
	case e := <-confirmed:
		require.ErrorIs(t, e, storage.ErrWALClosed,
			"the confirm must report that the WAL was closed: the message was never written")
	case <-time.After(10 * time.Second):
		t.Fatal("the publisher confirm never fired")
	}

	require.Eventually(t, func() bool { return ev.count("released") == 1 }, 10*time.Second, time.Millisecond,
		"the publish lease must be released exactly once")
	require.Equal(t, 1, ev.count("released"),
		"the publish lease must be released exactly once, never twice")
	require.Equal(t, 1, ev.count("durable"), "the confirm must fire exactly once")
}
