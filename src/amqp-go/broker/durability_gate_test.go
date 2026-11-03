package broker

// A CONSUMER IS NEVER HANDED A MESSAGE THAT IS NOT YET DURABLE.
//
// The hot cache is a RAM HOLDER, and the change under review moved the
// body into it at the WAL WRITE — before the covering fdatasync. That is
// fine for the RAM's sake, but the message must NOT be HANDED OUT before it
// is durable: a consumer that receives a message the broker has not made
// durable would see data that a crash can take away.
//
// The durability barrier is unchanged: the publisher confirm and the queue's
// visible depth both advance in the COMPLETION, i.e. after the fdatasync. The
// fixture below holds the fdatasync open and observes the whole window: the
// record's bytes ARE in the file (the write returned), and the consumer is
// still not handed the message.

import (
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/maxpert/amqp-go/storage"
)

// observeWALWrite installs a write observer that lets the write through and
// signals when it happened. TEST-ONLY.
func observeWALWrite(t *testing.T) (written <-chan struct{}) {
	t.Helper()
	ch := make(chan struct{})
	var once sync.Once
	restore := storage.SetWALWriteVecForTest(func(f *os.File, parts [][]byte) (int, error) {
		total := 0
		for _, p := range parts {
			if len(p) == 0 {
				continue
			}
			n, err := f.Write(p)
			total += n
			if err != nil {
				return total, err
			}
		}
		once.Do(func() { close(ch) })
		return total, nil
	})
	t.Cleanup(restore)
	return ch
}

// TestBroker_ConsumerIsNeverHandedANonDurableMessage holds the fdatasync
// open and pins the whole window: the record's bytes are in the file, the
// publisher confirm has NOT fired, and the consumer is NOT handed the
// message. After the fdatasync returns, the message is delivered.
func TestBroker_ConsumerIsNeverHandedANonDurableMessage(t *testing.T) {
	release := gateWALFsync(t)
	defer release()
	written := observeWALWrite(t)

	b, _, cleanup := newReadTriageBroker(t)
	defer cleanup()

	_, err := b.DeclareQueue("durability-q", false, false, false, nil)
	require.NoError(t, err)

	confirmed := make(chan error, 1)
	_, _, err = b.PublishMessageAsyncConfirm("", "durability-q",
		&protocol.Message{RoutingKey: "durability-q", Body: []byte("payload"), DeliveryMode: 2},
		func(e error) { confirmed <- e },
		protocol.PublishLeaseToken{})
	require.NoError(t, err)

	// The WRITE happened: the record's bytes are in the WAL file.
	select {
	case <-written:
	case <-time.After(10 * time.Second):
		t.Fatal("the WAL write never happened")
	}

	// The fdatasync is still gated, so the record is NOT durable.
	select {
	case e := <-confirmed:
		t.Fatalf("the publisher confirm must not fire before the fdatasync (fired with %v)", e)
	default:
	}

	// THE CONSUMER MUST NOT BE HANDED THE MESSAGE.
	msg, _, _, err := b.GetMessageForGet("durability-q", true)
	require.NoError(t, err)
	require.Nil(t, msg,
		"a consumer must never be handed a message that is not durable yet")

	// Release durability: the confirm fires and the message is delivered.
	release()
	select {
	case e := <-confirmed:
		require.NoError(t, e, "the publish must be confirmed once the fdatasync returned")
	case <-time.After(10 * time.Second):
		t.Fatal("the publisher confirm never fired")
	}

	msg, _, _, err = b.GetMessageForGet("durability-q", true)
	require.NoError(t, err)
	require.NotNil(t, msg, "the message must be delivered once it is durable")
	require.Equal(t, []byte("payload"), msg.Body)
}
