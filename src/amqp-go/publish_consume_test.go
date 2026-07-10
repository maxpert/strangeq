package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/server"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBasicPublishConsume tests basic message publishing and consumption
func TestBasicPublishConsume(t *testing.T) {
	// Start a server in a goroutine
	srv := server.NewServer(":5690") // Use a different port to avoid conflicts
	go func() {
		_ = srv.Start()
	}()

	// Give the server a moment to start
	time.Sleep(200 * time.Millisecond)

	// Connect using the standard AMQP client
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5690//")
	require.NoError(t, err, "Should connect to server")
	defer conn.Close()

	channel, err := conn.Channel()
	require.NoError(t, err, "Should open channel")
	defer channel.Close()

	// Declare a queue
	queue, err := channel.QueueDeclare(
		"test-publish-consume-queue", // name
		false,                        // durable
		true,                         // delete when unused
		false,                        // exclusive
		false,                        // no-wait
		nil,                          // arguments
	)
	require.NoError(t, err, "Should declare queue")

	// Consume the message FIRST to ensure we're ready to receive
	msgs, err := channel.Consume(
		queue.Name, // queue
		"",         // consumer
		true,       // auto-ack (acknowledge automatically)
		false,      // exclusive
		false,      // no-local
		false,      // no-wait
		nil,        // args
	)
	require.NoError(t, err, "Should start consuming")

	// Give consumer time to register
	time.Sleep(100 * time.Millisecond)

	// Publish a message
	err = channel.Publish(
		"",         // exchange
		queue.Name, // routing key
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte("Hello, World!"),
		},
	)
	assert.NoError(t, err, "Should publish message")

	// Try to receive the message
	select {
	case msg := <-msgs:
		assert.Equal(t, "Hello, World!", string(msg.Body))
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Did not receive message within timeout")
	}
}

// TestManualAcknowledgment tests manual message acknowledgment
func TestManualAcknowledgment(t *testing.T) {
	// Start a server in a goroutine
	srv := server.NewServer(":5691") // Use a different port to avoid conflicts
	go func() {
		_ = srv.Start()
	}()

	// Give the server a moment to start
	time.Sleep(200 * time.Millisecond)

	// Connect using the standard AMQP client
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5691//")
	require.NoError(t, err, "Should connect to server")
	defer conn.Close()

	channel, err := conn.Channel()
	require.NoError(t, err, "Should open channel")
	defer channel.Close()

	// Declare a queue
	queue, err := channel.QueueDeclare(
		"test-manual-ack-queue", // name
		false,                   // durable
		true,                    // delete when unused
		false,                   // exclusive
		false,                   // no-wait
		nil,                     // arguments
	)
	require.NoError(t, err, "Should declare queue")

	// Consume the message with manual acknowledgment FIRST
	msgs, err := channel.Consume(
		queue.Name, // queue
		"",         // consumer
		false,      // auto-ack (manual acknowledgment)
		false,      // exclusive
		false,      // no-local
		false,      // no-wait
		nil,        // args
	)
	require.NoError(t, err, "Should start consuming")

	// Give consumer time to register
	time.Sleep(100 * time.Millisecond)

	// Publish a message
	err = channel.Publish(
		"",         // exchange
		queue.Name, // routing key
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte("Test message for manual ack"),
		},
	)
	assert.NoError(t, err, "Should publish message")

	// Try to receive the message
	select {
	case msg := <-msgs:
		assert.Equal(t, "Test message for manual ack", string(msg.Body))

		// Manually acknowledge the message
		err = msg.Ack(false)
		assert.NoError(t, err, "Should acknowledge message")
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Did not receive message within timeout")
	}
}

// TestMessageRejection tests message rejection with requeuing
func TestMessageRejection(t *testing.T) {
	// Start a server in a goroutine
	srv := server.NewServer(":5692") // Use a different port to avoid conflicts
	go func() {
		_ = srv.Start()
	}()

	// Give the server a moment to start
	time.Sleep(200 * time.Millisecond)

	// Connect using the standard AMQP client
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5692//")
	require.NoError(t, err, "Should connect to server")
	defer conn.Close()

	channel, err := conn.Channel()
	require.NoError(t, err, "Should open channel")
	defer channel.Close()

	// Declare a queue
	queue, err := channel.QueueDeclare(
		"test-reject-queue", // name
		false,               // durable
		true,                // delete when unused
		false,               // exclusive
		false,               // no-wait
		nil,                 // arguments
	)
	require.NoError(t, err, "Should declare queue")

	// Consume the message with manual acknowledgment FIRST
	msgs, err := channel.Consume(
		queue.Name, // queue
		"",         // consumer
		false,      // auto-ack (manual acknowledgment)
		false,      // exclusive
		false,      // no-local
		false,      // no-wait
		nil,        // args
	)
	require.NoError(t, err, "Should start consuming")

	// Give consumer time to register
	time.Sleep(100 * time.Millisecond)

	// Publish a message
	err = channel.Publish(
		"",         // exchange
		queue.Name, // routing key
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte("Test message for rejection"),
		},
	)
	assert.NoError(t, err, "Should publish message")

	// Try to receive the message
	select {
	case msg := <-msgs:
		assert.Equal(t, "Test message for rejection", string(msg.Body))

		// Reject the message with requeuing
		err = msg.Reject(true) // true = requeue
		assert.NoError(t, err, "Should reject message with requeuing")
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Did not receive message within timeout")
	}
}

// TestMultipleConsumers tests multiple consumers on the same queue
func TestMultipleConsumers(t *testing.T) {
	// Start a server in a goroutine
	srv := server.NewServer(":5694") // Use a different port to avoid conflicts
	go func() {
		_ = srv.Start()
	}()

	// Give the server a moment to start
	time.Sleep(200 * time.Millisecond)

	// Connect using the standard AMQP client
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5694//")
	require.NoError(t, err, "Should connect to server")
	defer conn.Close()

	// Open two channels
	channel1, err := conn.Channel()
	require.NoError(t, err, "Should open first channel")
	defer channel1.Close()

	channel2, err := conn.Channel()
	require.NoError(t, err, "Should open second channel")
	defer channel2.Close()

	// Declare a queue
	queue, err := channel1.QueueDeclare(
		"test-multi-consumer-queue", // name
		false,                       // durable
		true,                        // delete when unused
		false,                       // exclusive
		false,                       // no-wait
		nil,                         // arguments
	)
	require.NoError(t, err, "Should declare queue")

	// Start two consumers FIRST
	msgs1, err := channel1.Consume(
		queue.Name,   // queue
		"consumer-1", // consumer
		false,        // auto-ack (manual acknowledgment)
		false,        // exclusive
		false,        // no-local
		false,        // no-wait
		nil,          // args
	)
	require.NoError(t, err, "Should start first consumer")

	msgs2, err := channel2.Consume(
		queue.Name,   // queue
		"consumer-2", // consumer
		false,        // auto-ack (manual acknowledgment)
		false,        // exclusive
		false,        // no-local
		false,        // no-wait
		nil,          // args
	)
	require.NoError(t, err, "Should start second consumer")

	// Give consumers time to register
	time.Sleep(100 * time.Millisecond)

	// Publish multiple messages
	messages := []string{"Message A", "Message B", "Message C", "Message D"}
	for _, msg := range messages {
		err = channel1.Publish(
			"",         // exchange
			queue.Name, // routing key
			false,      // mandatory
			false,      // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(msg),
			},
		)
		assert.NoError(t, err, "Should publish message")
	}

	// Collect messages from both consumers
	receivedMessages := make(map[string]bool)

	// Receive messages with timeout
	timeout := time.After(3 * time.Second)
	done := make(chan bool)

	go func() {
		for {
			select {
			case msg1 := <-msgs1:
				receivedMessages[string(msg1.Body)] = true
				msg1.Ack(false)
			case msg2 := <-msgs2:
				receivedMessages[string(msg2.Body)] = true
				msg2.Ack(false)
			case <-timeout:
				done <- true
				return
			}
		}
	}()

	<-done

	// Verify we received all messages
	expectedMessages := map[string]bool{
		"Message A": true,
		"Message B": true,
		"Message C": true,
		"Message D": true,
	}

	assert.Equal(t, expectedMessages, receivedMessages, "Should receive all messages distributed among consumers")
}

// TestNegativeAcknowledgment tests message negative acknowledgment (nack)
func TestNegativeAcknowledgment(t *testing.T) {
	// Start a server in a goroutine
	srv := server.NewServer(":5695") // Use a different port to avoid conflicts
	go func() {
		_ = srv.Start()
	}()

	// Give the server a moment to start
	time.Sleep(200 * time.Millisecond)

	// Connect using the standard AMQP client
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5695//")
	require.NoError(t, err, "Should connect to server")
	defer conn.Close()

	channel, err := conn.Channel()
	require.NoError(t, err, "Should open channel")
	defer channel.Close()

	// Declare a queue
	queue, err := channel.QueueDeclare(
		"test-nack-queue", // name
		false,             // durable
		true,              // delete when unused
		false,             // exclusive
		false,             // no-wait
		nil,               // arguments
	)
	require.NoError(t, err, "Should declare queue")

	// Consume the message with manual acknowledgment FIRST
	msgs, err := channel.Consume(
		queue.Name, // queue
		"",         // consumer
		false,      // auto-ack (manual acknowledgment)
		false,      // exclusive
		false,      // no-local
		false,      // no-wait
		nil,        // args
	)
	require.NoError(t, err, "Should start consuming")

	// Give consumer time to register
	time.Sleep(100 * time.Millisecond)

	// Publish a message
	err = channel.Publish(
		"",         // exchange
		queue.Name, // routing key
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte("Test message for nack"),
		},
	)
	assert.NoError(t, err, "Should publish message")

	// Try to receive the message
	select {
	case msg := <-msgs:
		assert.Equal(t, "Test message for nack", string(msg.Body))

		// Negative acknowledge the message with requeuing
		err = msg.Nack(false, true) // multiple=false, requeue=true
		assert.NoError(t, err, "Should negative acknowledge message with requeuing")
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Did not receive message within timeout")
	}
}

// ============================================================================
// STAGE B — single-frame content-body DONATION on the receive path.
//
// When a message body arrives in exactly ONE content-body frame, the server
// donates the freshly-read frame payload directly to message.Body (zero copy)
// instead of copying it into a pre-allocated buffer. Multi-frame bodies keep the
// make+append assemble path. Donation is a behaviour-PRESERVING optimization, so
// these are correctness/regression guards: every delivered (and recovered) body
// must remain bit-identical whether it took the single-frame donation branch or
// the multi-frame assemble branch, and no subsequent publish may corrupt an
// in-flight donated body (the PutFrame/PutPendingMessage nil-never-recycle
// contract). They must stay green across the donation change.
// ============================================================================

var stageBPortCounter atomic.Int64

// stageBBody builds a deterministic, seed-distinct body of length n so a swapped
// or overwritten buffer is detectable.
func stageBBody(seed byte, n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = seed ^ byte((i*31+i/251+int(seed))&0xff)
	}
	return b
}

// stageBServer builds and starts a persistent server rooted at dir (so durable
// messages survive a restart onto the same dir) and returns it with its URI.
func stageBServer(t *testing.T, dir string) (*server.Server, string) {
	t.Helper()
	port := 18900 + int(stageBPortCounter.Add(1))
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	cfg := config.DefaultConfig()
	cfg.Network.Address = addr
	cfg.Storage.Path = dir

	srv, err := server.NewServerBuilder().WithConfig(cfg).Build()
	require.NoError(t, err, "server build")
	go func() { _ = srv.Start() }()
	for i := 0; i < 200; i++ {
		if srv.IsListening() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	require.True(t, srv.IsListening(), "server listener not ready")
	return srv, fmt.Sprintf("amqp://guest:guest@%s/", addr)
}

// TestStageB_DonationFrameBoundaries publishes bodies straddling the single-vs-
// multi frame boundary, transient AND durable, and asserts each delivered body
// is bit-identical. With the default negotiated frame-max (131072), bodies up to
// 131064 arrive in one frame (donation branch) and larger bodies split into
// several frames (assemble branch), so the table exercises both branches.
func TestStageB_DonationFrameBoundaries(t *testing.T) {
	_, uri := stageBServer(t, t.TempDir())

	conn, err := amqp.Dial(uri)
	require.NoError(t, err)
	defer conn.Close()

	// maxBodyPerFrame = negotiated frame-max (131072) - 8 bytes frame overhead.
	const maxBodyPerFrame = 131072 - 8
	sizes := []int{0, 1, 4096, 65536, maxBodyPerFrame, maxBodyPerFrame + 1}

	for _, durable := range []bool{false, true} {
		for _, size := range sizes {
			name := fmt.Sprintf("durable=%v/size=%d", durable, size)
			t.Run(name, func(t *testing.T) {
				ch, err := conn.Channel()
				require.NoError(t, err)
				defer ch.Close()

				qName := fmt.Sprintf("stageb-fb-%v-%d", durable, size)
				_, err = ch.QueueDeclare(qName, durable, !durable, false, false, nil)
				require.NoError(t, err)

				msgs, err := ch.Consume(qName, "", true, false, false, false, nil)
				require.NoError(t, err)
				time.Sleep(50 * time.Millisecond)

				body := stageBBody(byte(size), size)
				pub := amqp.Publishing{ContentType: "application/octet-stream", Body: body}
				if durable {
					pub.DeliveryMode = amqp.Persistent
				}
				require.NoError(t, ch.PublishWithContext(context.Background(), "", qName, false, false, pub))

				select {
				case m := <-msgs:
					require.True(t, bytes.Equal(body, m.Body),
						"delivered body must be bit-identical (len want=%d got=%d)", len(body), len(m.Body))
				case <-time.After(5 * time.Second):
					t.Fatalf("no delivery for %s", name)
				}
			})
		}
	}
}

// TestStageB_MultiFrameSmallFrameMax forces the multi-frame assemble fall-through
// by negotiating a small frame-max (4096) so a 64KB body arrives as ~17 body
// frames — the donation gate is false on every frame — and asserts the assembled
// (and, for the durable case, recovered) body stays bit-identical. This is the
// path donation does NOT cover.
func TestStageB_MultiFrameSmallFrameMax(t *testing.T) {
	_, uri := stageBServer(t, t.TempDir())

	conn, err := amqp.DialConfig(uri, amqp.Config{FrameSize: 4096})
	require.NoError(t, err)
	defer conn.Close()

	for _, durable := range []bool{false, true} {
		t.Run(fmt.Sprintf("durable=%v", durable), func(t *testing.T) {
			ch, err := conn.Channel()
			require.NoError(t, err)
			defer ch.Close()

			qName := fmt.Sprintf("stageb-mf-%v", durable)
			_, err = ch.QueueDeclare(qName, durable, !durable, false, false, nil)
			require.NoError(t, err)

			msgs, err := ch.Consume(qName, "", true, false, false, false, nil)
			require.NoError(t, err)
			time.Sleep(50 * time.Millisecond)

			body := stageBBody(0x7e, 65536)
			pub := amqp.Publishing{ContentType: "application/octet-stream", Body: body}
			if durable {
				pub.DeliveryMode = amqp.Persistent
			}
			require.NoError(t, ch.PublishWithContext(context.Background(), "", qName, false, false, pub))

			select {
			case m := <-msgs:
				require.True(t, bytes.Equal(body, m.Body), "multi-frame assembled body must be bit-identical")
			case <-time.After(5 * time.Second):
				t.Fatal("no delivery for multi-frame body")
			}
		})
	}
}

// TestStageB_DurableRecoveryBitIdentity publishes durable bodies (single- and
// multi-frame) with publisher confirms, restarts the server against the SAME
// data directory, and asserts every recovered body is bit-identical. This proves
// a donated single-frame body survives the full receive -> WAL -> crash-recovery
// path unchanged (the donated buffer is the live message body, retained until
// ack, so the async WAL flush copies the correct bytes).
func TestStageB_DurableRecoveryBitIdentity(t *testing.T) {
	dir := t.TempDir()
	srv1, uri := stageBServer(t, dir)

	const qName = "stageb-recover-q"
	// Distinct sizes so a recovered body can be keyed by length (message
	// properties such as CorrelationId are not part of the durable WAL record,
	// only exchange/routing-key/delivery-mode/body/extensions are). The last
	// size is multi-frame at the default frame-max.
	sizes := []int{1, 4096, 65536, 131065}
	want := make(map[int][]byte, len(sizes))

	func() {
		conn, err := amqp.Dial(uri)
		require.NoError(t, err)
		defer conn.Close()
		ch, err := conn.Channel()
		require.NoError(t, err)
		_, err = ch.QueueDeclare(qName, true, false, false, false, nil)
		require.NoError(t, err)
		require.NoError(t, ch.Confirm(false))
		confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 16))

		for i, size := range sizes {
			body := stageBBody(byte(0xC0+i), size)
			want[size] = body
			require.NoError(t, ch.PublishWithContext(context.Background(), "", qName, false, false, amqp.Publishing{
				DeliveryMode: amqp.Persistent,
				Body:         body,
			}))
			select {
			case c := <-confirms:
				require.True(t, c.Ack, "publish %d must be confirmed (durable)", i)
			case <-time.After(5 * time.Second):
				t.Fatalf("no confirm for publish %d", i)
			}
		}
	}()

	// Restart onto the same dir: recovery must reload the durable messages.
	require.NoError(t, srv1.Stop())
	_, uri2 := stageBServer(t, dir)

	conn2, err := amqp.Dial(uri2)
	require.NoError(t, err)
	defer conn2.Close()
	ch2, err := conn2.Channel()
	require.NoError(t, err)
	_, err = ch2.QueueDeclare(qName, true, false, false, false, nil)
	require.NoError(t, err)

	msgs, err := ch2.Consume(qName, "", true, false, false, false, nil)
	require.NoError(t, err)

	got := make(map[int][]byte, len(sizes))
	deadline := time.After(15 * time.Second)
	for len(got) < len(sizes) {
		select {
		case m := <-msgs:
			got[len(m.Body)] = append([]byte(nil), m.Body...)
		case <-deadline:
			t.Fatalf("recovered only %d/%d durable messages", len(got), len(sizes))
		}
	}
	for size, body := range want {
		require.True(t, bytes.Equal(body, got[size]),
			"recovered body for size=%d must be bit-identical (got len=%d)", size, len(got[size]))
	}
}

// stageBSelfDescribingBody builds a body whose first 16 bytes encode its own
// (seq, size) so a consumer can reconstruct the exact expected content from the
// delivered bytes alone. Cross-message aliasing (a donated buffer overwritten by
// another message) would corrupt these bytes and be detected.
func stageBSelfDescribingBody(seq uint64, n int) []byte {
	b := make([]byte, n)
	binary.BigEndian.PutUint64(b[0:8], seq)
	binary.BigEndian.PutUint64(b[8:16], uint64(n))
	for i := 16; i < n; i++ {
		b[i] = byte((int(seq)*31 + i*7) & 0xff)
	}
	return b
}

// TestStageB_ConcurrentNoCorruption drives several concurrent publishers, each
// interleaving single-frame (donated) and multi-frame (assembled) bodies of
// distinct, self-describing content, and asserts every delivered body is
// bit-identical to what was published — zero cross-message corruption. Run under
// -race it proves the receive reader always allocates a fresh payload and that
// donation never aliases a reused buffer across concurrent messages.
//
// Each producer uses its OWN queue and consumer. This deliberately isolates the
// per-queue delivery path so the test measures exactly the shared RECEIVE path
// (concurrent readFrames across connections + the global frame pool) that
// donation touches, without coupling to the unrelated multi-producer/single-queue
// delivery ordering machinery.
func TestStageB_ConcurrentNoCorruption(t *testing.T) {
	_, uri := stageBServer(t, t.TempDir())

	const producers = 6
	const perProducer = 120
	// Mixed sizes interleaved per producer: the first two are single-frame
	// (donated) at the default frame-max, the last is multi-frame (assembled).
	sizes := []int{1024, 65536, 200000}

	var corrupt int64
	var delivered int64

	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(pid int) {
			defer wg.Done()
			qName := fmt.Sprintf("stageb-conc-q-%d", pid)

			// Consumer on its own connection (auto-ack keeps every enqueued
			// message flowing without a manual-ack credit window).
			cconn, err := amqp.Dial(uri)
			if err != nil {
				t.Errorf("producer %d consumer dial: %v", pid, err)
				return
			}
			defer cconn.Close()
			cch, err := cconn.Channel()
			if err != nil {
				t.Errorf("producer %d consumer channel: %v", pid, err)
				return
			}
			if _, err := cch.QueueDeclare(qName, false, false, false, false, nil); err != nil {
				t.Errorf("producer %d queue declare: %v", pid, err)
				return
			}
			msgs, err := cch.Consume(qName, "", true, false, false, false, nil)
			if err != nil {
				t.Errorf("producer %d consume: %v", pid, err)
				return
			}

			recvDone := make(chan struct{})
			go func() {
				got := 0
				for m := range msgs {
					seq := binary.BigEndian.Uint64(m.Body[0:8])
					sz := int(binary.BigEndian.Uint64(m.Body[8:16]))
					if len(m.Body) != sz || !bytes.Equal(m.Body, stageBSelfDescribingBody(seq, sz)) {
						atomic.AddInt64(&corrupt, 1)
					}
					atomic.AddInt64(&delivered, 1)
					if got++; got == perProducer {
						close(recvDone)
						return
					}
				}
			}()

			// Publisher on its own connection, in confirm mode so every message
			// is enqueued before we wait for delivery.
			pconn, err := amqp.Dial(uri)
			if err != nil {
				t.Errorf("producer %d publisher dial: %v", pid, err)
				return
			}
			defer pconn.Close()
			pch, err := pconn.Channel()
			if err != nil {
				t.Errorf("producer %d publisher channel: %v", pid, err)
				return
			}
			if err := pch.Confirm(false); err != nil {
				t.Errorf("producer %d confirm: %v", pid, err)
				return
			}
			confirms := pch.NotifyPublish(make(chan amqp.Confirmation, perProducer+1))
			for k := 0; k < perProducer; k++ {
				seq := uint64(pid*perProducer + k)
				size := sizes[(pid+k)%len(sizes)]
				if err := pch.PublishWithContext(context.Background(), "", qName, false, false, amqp.Publishing{
					Body: stageBSelfDescribingBody(seq, size),
				}); err != nil {
					t.Errorf("producer %d publish: %v", pid, err)
					return
				}
			}
			for k := 0; k < perProducer; k++ {
				select {
				case c := <-confirms:
					if !c.Ack {
						t.Errorf("producer %d publish %d nacked", pid, k)
						return
					}
				case <-time.After(60 * time.Second):
					t.Errorf("producer %d confirm %d timeout", pid, k)
					return
				}
			}
			select {
			case <-recvDone:
			case <-time.After(60 * time.Second):
				t.Errorf("producer %d did not receive all %d messages", pid, perProducer)
			}
		}(p)
	}
	wg.Wait()

	require.EqualValues(t, producers*perProducer, atomic.LoadInt64(&delivered),
		"every published message must be delivered")
	require.EqualValues(t, 0, atomic.LoadInt64(&corrupt),
		"no delivered body may be corrupted (donation must never alias a reused buffer)")
}

// TestStageB_NoRecycleAfterDonation publishes one large message, leaves it queued
// (no consumer yet), then churns many subsequent publishes of DIFFERENT content
// through the same receive path, and finally consumes everything asserting the
// first message's body is unchanged. This guards the invariant that PutFrame and
// PutPendingMessage only NIL the donated buffer's reference — they never recycle
// the backing array — so a later publish can never overwrite an in-flight body.
func TestStageB_NoRecycleAfterDonation(t *testing.T) {
	_, uri := stageBServer(t, t.TempDir())

	conn, err := amqp.Dial(uri)
	require.NoError(t, err)
	defer conn.Close()
	ch, err := conn.Channel()
	require.NoError(t, err)

	const qName = "stageb-norecycle-q"
	_, err = ch.QueueDeclare(qName, false, false, false, false, nil)
	require.NoError(t, err)

	// The first (single-frame, donated) message. Distinct content.
	first := stageBBody(0x11, 65536)
	require.NoError(t, ch.PublishWithContext(context.Background(), "", qName, false, false, amqp.Publishing{
		CorrelationId: "first",
		Body:          first,
	}))

	// Churn many subsequent publishes with different content sizes through the
	// same connection's receive path; if any recycled the first buffer it would
	// overwrite `first` in the ring.
	const churn = 200
	for i := 0; i < churn; i++ {
		size := 4096 + (i%8)*8192
		body := stageBBody(byte(0x80+i), size)
		require.NoError(t, ch.PublishWithContext(context.Background(), "", qName, false, false, amqp.Publishing{
			CorrelationId: fmt.Sprintf("churn-%d", i),
			Body:          body,
		}))
	}

	msgs, err := ch.Consume(qName, "", true, false, false, false, nil)
	require.NoError(t, err)

	// The first message out (FIFO) must be the original, byte-intact.
	select {
	case m := <-msgs:
		require.Equal(t, "first", m.CorrelationId, "FIFO: the first published message must be delivered first")
		require.True(t, bytes.Equal(first, m.Body),
			"an in-flight donated body must not be recycled/overwritten by later publishes")
	case <-time.After(5 * time.Second):
		t.Fatal("no delivery for the first message")
	}

	// Drain the churn messages, verifying none corrupted either.
	for i := 0; i < churn; i++ {
		select {
		case m := <-msgs:
			var idx int
			_, perr := fmt.Sscanf(m.CorrelationId, "churn-%d", &idx)
			require.NoError(t, perr)
			size := 4096 + (idx%8)*8192
			require.True(t, bytes.Equal(stageBBody(byte(0x80+idx), size), m.Body),
				"churn message %d body must be intact", idx)
		case <-time.After(5 * time.Second):
			t.Fatalf("missing churn delivery %d", i)
		}
	}
}

// TestNegativeAcknowledgmentMultiple tests message negative acknowledgment with multiple flag
