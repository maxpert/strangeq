package server

import (
	"errors"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/auth"
	"github.com/maxpert/amqp-go/broker"
	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/maxpert/amqp-go/storage"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// startWSBPanicBrokerServer builds a real server whose UnifiedBroker is pb
// wrapping a real storage broker, so tests can inject panics/errors.
func startWSBPanicBrokerServer(t *testing.T, pb *panicBroker, tweak func(*Server)) (*Server, string) {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.Network.Address = "127.0.0.1:0"
	cfg.Storage.Path = t.TempDir()
	cfg.Server.LogLevel = "silent"
	storageImpl, err := storage.NewDisruptorStorageWithDataDir(cfg.Storage.Path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = storageImpl.Close() })
	storageBroker := broker.NewStorageBroker(storageImpl, cfg.GetEngine())
	t.Cleanup(storageBroker.Close)
	pb.UnifiedBroker = NewStorageBrokerAdapter(storageBroker)
	srv, err := NewServerBuilder().WithConfig(cfg).WithUnifiedBroker(pb).WithStorage(storageImpl).Build()
	require.NoError(t, err)
	if tweak != nil {
		tweak(srv)
	}

	errCh := make(chan error, 1)
	go func() { errCh <- srv.Start() }()
	deadline := time.Now().Add(2 * time.Second)
	for !srv.IsListening() {
		select {
		case err := <-errCh:
			require.NoError(t, err, "server failed to start")
			require.FailNow(t, "server Start() returned without ever listening")
		default:
		}
		if time.Now().After(deadline) {
			require.FailNow(t, "server listener not ready within 2s")
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Cleanup(func() { wsbStopServer(srv) })

	srv.Mutex.RLock()
	addr := srv.Listener.Addr().String()
	srv.Mutex.RUnlock()
	return srv, addr
}

// TestWSB_ChannelError404_OtherChannelSurvives is the MARQUEE channel-isolation
// test: a 404 on one channel closes ONLY that channel; the connection and its
// other channels keep working.
func TestWSB_ChannelError404_OtherChannelSurvives(t *testing.T) {
	srv, addr := startWSBServer(t, nil, nil)

	conn, err := amqp.Dial("amqp://guest:guest@" + addr + "/")
	require.NoError(t, err)
	defer conn.Close()

	ch2, err := conn.Channel()
	require.NoError(t, err)
	_, err = ch2.QueueDeclare("iso-q", false, true, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, ch2.Confirm(false))

	ch1, err := conn.Channel()
	require.NoError(t, err)
	closeCh := ch1.NotifyClose(make(chan *amqp.Error, 1))

	_, err = ch1.Consume("no-such-queue", "", false, false, false, false, nil)
	require.Error(t, err, "consume on a missing queue must fail")
	var amqpErr *amqp.Error
	require.True(t, errors.As(err, &amqpErr), "err must be *amqp.Error, got %T", err)
	assert.Equal(t, 404, amqpErr.Code)

	select {
	case e := <-closeCh:
		require.NotNil(t, e)
		assert.Equal(t, 404, e.Code)
	case <-time.After(2 * time.Second):
		t.Fatal("ch1 NotifyClose did not fire within 2s")
	}

	require.False(t, conn.IsClosed(), "connection must survive a channel-level 404")

	// ch2 still works: publish with confirm, then get the body back.
	pub, err := ch2.PublishWithDeferredConfirm("", "iso-q", false, false, amqp.Publishing{Body: []byte("iso-1")})
	require.NoError(t, err)
	ack := pub.Wait()
	assert.True(t, ack, "publish confirm must be acked")

	msg, ok, err := ch2.Get("iso-q", true)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, "iso-1", string(msg.Body))

	// A third channel opens fine (the server tore ch1 down and reached
	// close-ok, so the client may reuse ID 1).
	ch3, err := conn.Channel()
	require.NoError(t, err)
	require.NoError(t, ch3.Close())

	assert.Equal(t, 1, wsbConnCount(srv))
}

// TestWSB_ChannelError406_TypeMismatch_OtherChannelSurvives: redeclaring an
// exchange with a different type gives 406 on that channel only.
func TestWSB_ChannelError406_TypeMismatch_OtherChannelSurvives(t *testing.T) {
	_, addr := startWSBServer(t, nil, nil)

	conn, err := amqp.Dial("amqp://guest:guest@" + addr + "/")
	require.NoError(t, err)
	defer conn.Close()

	ch2, err := conn.Channel()
	require.NoError(t, err)
	_, err = ch2.QueueDeclare("iso-q2", false, true, false, false, nil)
	require.NoError(t, err)

	ch1, err := conn.Channel()
	require.NoError(t, err)
	require.NoError(t, ch1.ExchangeDeclare("mixed-ex", "direct", false, false, false, false, nil))
	closeCh := ch1.NotifyClose(make(chan *amqp.Error, 1))

	err = ch1.ExchangeDeclare("mixed-ex", "fanout", false, false, false, false, nil)
	require.Error(t, err)
	var amqpErr *amqp.Error
	require.True(t, errors.As(err, &amqpErr))
	assert.Equal(t, 406, amqpErr.Code)

	select {
	case e := <-closeCh:
		require.NotNil(t, e)
		assert.Equal(t, 406, e.Code)
	case <-time.After(2 * time.Second):
		t.Fatal("ch1 NotifyClose did not fire within 2s")
	}

	require.False(t, conn.IsClosed())
	require.NoError(t, ch2.Publish("", "iso-q2", false, false, amqp.Publishing{Body: []byte("x")}))
	msg, ok, err := ch2.Get("iso-q2", true)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, "x", string(msg.Body))
}

// TestWSB_ChannelError403_Authz_OtherChannelSurvives: an authorization denial
// is a channel-level 403; other channels on the connection keep working.
func TestWSB_ChannelError403_Authz_OtherChannelSurvives(t *testing.T) {
	var users []auth.UserEntry
	_, addr := startWSBServer(t, func(cfg *config.AMQPConfig) {
		cfg.Security.AuthorizationEnabled = true
		users = []auth.UserEntry{
			wsbUser(t, "admin", "secret", ".*", ".*", ".*"),
			wsbUser(t, "restricted", "secret", "^$", ".*", ".*"), // no configure
		}
		withFileAuth(t, cfg, users)
	}, nil)

	// Pre-declare a queue as admin.
	admin, err := amqp.Dial("amqp://admin:secret@" + addr + "/")
	require.NoError(t, err)
	ach, err := admin.Channel()
	require.NoError(t, err)
	_, err = ach.QueueDeclare("authz-q", false, true, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, ach.Close())
	require.NoError(t, admin.Close())

	conn, err := amqp.Dial("amqp://restricted:secret@" + addr + "/")
	require.NoError(t, err)
	defer conn.Close()

	ch1, err := conn.Channel()
	require.NoError(t, err)
	closeCh := ch1.NotifyClose(make(chan *amqp.Error, 1))

	_, err = ch1.QueueDeclare("forbidden-q", false, true, false, false, nil)
	require.Error(t, err)
	var amqpErr *amqp.Error
	require.True(t, errors.As(err, &amqpErr))
	assert.Equal(t, 403, amqpErr.Code)

	select {
	case e := <-closeCh:
		require.NotNil(t, e)
		assert.Equal(t, 403, e.Code)
	case <-time.After(2 * time.Second):
		t.Fatal("ch1 NotifyClose did not fire within 2s")
	}

	require.False(t, conn.IsClosed(), "connection must survive a channel-level 403")

	ch2, err := conn.Channel()
	require.NoError(t, err)
	_, _, err = ch2.Get("authz-q", true)
	require.NoError(t, err, "basic.get on a pre-declared queue must still work")
}

// TestWSB_DeniedPublish_ContentFramesDiscarded: a publish denied by
// authorization is answered with channel.close 403, and the following content
// header/body frames are discarded by the closing gate — NOT a 505.
func TestWSB_DeniedPublish_ContentFramesDiscarded(t *testing.T) {
	_, addr := startWSBServer(t, func(cfg *config.AMQPConfig) {
		cfg.Security.AuthorizationEnabled = true
		users := []auth.UserEntry{
			wsbUser(t, "admin", "secret", ".*", ".*", ".*"),
			// write "amq.default": may publish ONLY to the default exchange
			// (the auth layer maps the empty exchange name to "amq.default"
			// before matching, interfaces.DefaultExchangeName).
			wsbUser(t, "wrestricted", "secret", "^$", "amq\\.default", ".*"),
		}
		withFileAuth(t, cfg, users)
	}, nil)

	admin, err := amqp.Dial("amqp://admin:secret@" + addr + "/")
	require.NoError(t, err)
	ach, err := admin.Channel()
	require.NoError(t, err)
	_, err = ach.QueueDeclare("w-q", false, true, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, ach.Close())
	require.NoError(t, admin.Close())

	conn, err := amqp.Dial("amqp://wrestricted:secret@" + addr + "/")
	require.NoError(t, err)
	defer conn.Close()

	ch1, err := conn.Channel()
	require.NoError(t, err)
	closeCh := ch1.NotifyClose(make(chan *amqp.Error, 1))

	// Async publish to a named exchange: denied. The client has already sent
	// the method + content header + body frames; the server must answer 403
	// and discard the trailing content frames, not die with a 505.
	require.NoError(t, ch1.Publish("denied-ex", "k", false, false, amqp.Publishing{Body: []byte("nope")}))

	select {
	case e := <-closeCh:
		require.NotNil(t, e)
		assert.Equal(t, 403, e.Code)
	case <-time.After(2 * time.Second):
		t.Fatal("ch1 NotifyClose did not fire within 2s")
	}

	require.False(t, conn.IsClosed(), "connection must survive; trailing content frames are discarded")

	// ch2 round-trip on the default exchange (write "^$" permits it).
	ch2, err := conn.Channel()
	require.NoError(t, err)
	require.NoError(t, ch2.Publish("", "w-q", false, false, amqp.Publishing{Body: []byte("ok")}))
	msg, ok, err := ch2.Get("w-q", true)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, "ok", string(msg.Body))
}


// TestWSB_ChannelError_UnackedRequeued: when a channel dies with unacked
// deliveries, teardownChannel requeues them immediately — another channel can
// get the message (redelivered) without waiting for connection close.
func TestWSB_ChannelError_UnackedRequeued(t *testing.T) {
	_, addr := startWSBServer(t, nil, nil)

	conn, err := amqp.Dial("amqp://guest:guest@" + addr + "/")
	require.NoError(t, err)
	defer conn.Close()

	setup, err := conn.Channel()
	require.NoError(t, err)
	// NOT auto-delete: the queue must survive its last consumer going away
	// (that is exactly what the test exercises).
	_, err = setup.QueueDeclare("rq-q", false, false, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, setup.Publish("", "rq-q", false, false, amqp.Publishing{Body: []byte("rq-1")}))

	ch1, err := conn.Channel()
	require.NoError(t, err)
	deliveries, err := ch1.Consume("rq-q", "c1", false, false, false, false, nil)
	require.NoError(t, err)

	select {
	case d := <-deliveries:
		assert.Equal(t, "rq-1", string(d.Body))
		// deliberately NOT acked
	case <-time.After(2 * time.Second):
		t.Fatal("no delivery within 2s")
	}

	// Trigger a 404 on ch1: teardown must requeue the unacked delivery.
	_, err = ch1.Consume("no-such-queue", "c2", false, false, false, false, nil)
	require.Error(t, err)

	ch2, err := conn.Channel()
	require.NoError(t, err)
	var got amqp.Delivery
	require.Eventually(t, func() bool {
		d, ok, err := ch2.Get("rq-q", true)
		if err != nil || !ok {
			return false
		}
		got = d
		return true
	}, 2*time.Second, 20*time.Millisecond, "unacked delivery must be requeued on channel teardown")
	assert.Equal(t, "rq-1", string(got.Body))
	assert.True(t, got.Redelivered, "requeued delivery must be marked redelivered")
}

// TestWSB_ChannelError_FramesDiscardedUntilCloseOK (raw): after the server
// sends channel.close, every frame on that channel except close/close-ok is
// discarded (AMQP 0-9-1 §2.3.5.2).
func TestWSB_ChannelError_FramesDiscardedUntilCloseOK(t *testing.T) {
	_, addr := startWSBServer(t, nil, nil)

	r := dialRaw(t, addr)
	r.handshake("guest", "guest", 0, 0)
	r.openChannel(1)

	// basic.consume on a missing queue → channel.close 404
	consume := &protocol.BasicConsumeMethod{Queue: "no-such-queue", ConsumerTag: "c1"}
	data, err := consume.Serialize()
	require.NoError(t, err)
	r.send(1, 60, 20, data)
	r.expectChannelClose(1, 404)

	// A queue.declare on the closing channel must be discarded silently.
	decl := &protocol.QueueDeclareMethod{Queue: "late-q"}
	data, err = decl.Serialize()
	require.NoError(t, err)
	r.send(1, 50, 10, data)
	r.expectSilence(200 * time.Millisecond)

	// close-ok completes the handshake; the channel ID is reusable.
	r.send(1, 20, 41, []byte{})
	r.openChannel(1)
}

// TestWSB_ChannelCloseCrossing (raw): both peers close at once — the client
// answers the server's channel.close with channel.close (not close-ok); the
// server replies close-ok and the channel ID is reusable.
func TestWSB_ChannelCloseCrossing(t *testing.T) {
	_, addr := startWSBServer(t, nil, nil)

	r := dialRaw(t, addr)
	r.handshake("guest", "guest", 0, 0)
	r.openChannel(1)

	consume := &protocol.BasicConsumeMethod{Queue: "no-such-queue", ConsumerTag: "c1"}
	data, err := consume.Serialize()
	require.NoError(t, err)
	r.send(1, 60, 20, data)
	r.expectChannelClose(1, 404)

	// Crossing close: client sends channel.close too.
	closeM := &protocol.ChannelCloseMethod{ReplyCode: 200, ReplyText: "bye", ClassID: 60, MethodID: 20}
	data, err = closeM.Serialize()
	require.NoError(t, err)
	r.send(1, 20, 40, data)
	r.expectMethodCh(1, 20, 41) // channel.close-ok

	r.openChannel(1) // reopen succeeds
}

// TestWSB_UnknownClass_Gets501 (raw): an unknown class id is a hard 501.
func TestWSB_UnknownClass_Gets501(t *testing.T) {
	_, addr := startWSBServer(t, nil, nil)

	r := dialRaw(t, addr)
	r.handshake("guest", "guest", 0, 0)
	r.openChannel(1)

	r.send(1, 99, 10, []byte{})
	r.expectConnectionClose(501)
	r.send(0, 10, 51, []byte{}) // close-ok
	r.expectEOF()
}

// TestWSB_UnknownMethod_Gets501 (raw): an unknown method in a known class is a
// hard 501.
func TestWSB_UnknownMethod_Gets501(t *testing.T) {
	_, addr := startWSBServer(t, nil, nil)

	r := dialRaw(t, addr)
	r.handshake("guest", "guest", 0, 0)
	r.openChannel(1)

	r.send(1, 50, 99, []byte{})
	r.expectConnectionClose(501)
	r.send(0, 10, 51, []byte{})
	r.expectEOF()
}

// TestWSB_PrefetchSize_Gets540ConnectionClose: prefetch_size != 0 is
// not-implemented, which is a HARD error (540) per RabbitMQ.
func TestWSB_PrefetchSize_Gets540ConnectionClose(t *testing.T) {
	_, addr := startWSBServer(t, nil, nil)

	conn, err := amqp.Dial("amqp://guest:guest@" + addr + "/")
	require.NoError(t, err)
	defer conn.Close()

	closeCh := conn.NotifyClose(make(chan *amqp.Error, 1))
	ch, err := conn.Channel()
	require.NoError(t, err)
	_ = ch.Qos(0, 4096, false)

	select {
	case e := <-closeCh:
		require.NotNil(t, e)
		assert.Equal(t, 540, e.Code)
	case <-time.After(2 * time.Second):
		t.Fatal("connection NotifyClose did not fire within 2s")
	}
}

// TestWSB_EmptyQueueNameNoCurrent_Gets404: an empty queue name with no
// previously declared queue on the channel is a channel-level 404 (RabbitMQ
// parity), and the connection survives.
func TestWSB_EmptyQueueNameNoCurrent_Gets404(t *testing.T) {
	_, addr := startWSBServer(t, nil, nil)

	conn, err := amqp.Dial("amqp://guest:guest@" + addr + "/")
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)

	err = ch.QueueBind("", "k", "amq.direct", false, nil)
	require.Error(t, err)
	var amqpErr *amqp.Error
	require.True(t, errors.As(err, &amqpErr))
	assert.Equal(t, 404, amqpErr.Code)

	require.False(t, conn.IsClosed(), "connection must survive")
}

// TestWSB_UnclassifiedBrokerError_Gets541NotEOF: an unclassified broker error
// maps to a connection.close 541 (RabbitMQ internal_error is hard) — the
// client receives a close REASON, not a bare EOF.
func TestWSB_UnclassifiedBrokerError_Gets541NotEOF(t *testing.T) {
	pb := &panicBroker{purgeErr: errors.New("boom")}
	_, addr := startWSBPanicBrokerServer(t, pb, nil)

	conn, err := amqp.Dial("amqp://guest:guest@" + addr + "/")
	require.NoError(t, err)
	defer conn.Close()

	closeCh := conn.NotifyClose(make(chan *amqp.Error, 1))
	ch, err := conn.Channel()
	require.NoError(t, err)
	_, err = ch.QueueDeclare("purge-q", false, true, false, false, nil)
	require.NoError(t, err)
	_, _ = ch.QueuePurge("purge-q", false)

	select {
	case e := <-closeCh:
		require.NotNil(t, e, "client must receive a close reason, not a bare EOF")
		assert.Equal(t, 541, e.Code)
		assert.True(t, e.Server, "error must come from the server")
	case <-time.After(2 * time.Second):
		t.Fatal("connection NotifyClose did not fire within 2s")
	}
}

// TestWSB_ProcessorTeardownRunsOnlyOnProcessor: a ChannelError raised on the
// ack processor marks the channel closing but does NOT tear it down (teardown
// is serialized on the processor); the client's close-ok completes it.
func TestWSB_ProcessorTeardownRunsOnlyOnProcessor(t *testing.T) {
	srv := newAckTestServer(newAckTestBroker())
	conn, frameCh := newPipeConnWithFrames(t)

	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch)
	conn.ChannelCount.Store(1)

	rt := srv.runtimeFor(conn)

	// Ack processor raises a 406 on ch 1 (unknown delivery tag).
	err := srv.unknownDeliveryTag(conn, 1, 999, protocol.BasicAck)
	require.Error(t, err)
	var ce *ChannelError
	require.True(t, errors.As(err, &ce), "want *ChannelError, got %T", err)
	assert.Equal(t, uint16(406), ce.Code)

	fatal := srv.handleFrameError(rt, frameCtx{channel: 1, classID: 60, methodID: protocol.BasicAck}, err, false)
	require.False(t, fatal, "channel error must not be fatal to the ack processor")

	// The channel is still registered, marked Closed, in the closing set.
	v, ok := conn.Channels.Load(uint16(1))
	require.True(t, ok, "channel must NOT be torn down off the processor")
	require.True(t, v.(*protocol.Channel).Closed)
	require.True(t, rt.isChannelClosing(1))
	require.Equal(t, int32(1), conn.ChannelCount.Load())

	drainFrame(t, frameCh) // the channel.close frame

	// The client's close-ok completes the teardown.
	require.NoError(t, srv.processChannelSpecificMethod(conn, 1, protocol.ChannelCloseOK, nil))
	_, ok = conn.Channels.Load(uint16(1))
	require.False(t, ok, "close-ok must tear the channel down")
	assert.Equal(t, int32(0), conn.ChannelCount.Load(), "ChannelCount drops by exactly 1")
	assert.False(t, rt.isChannelClosing(1), "closing set is empty")
}
