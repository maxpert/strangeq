package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/auth"
	"github.com/maxpert/amqp-go/broker"
	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/maxpert/amqp-go/server"
	"github.com/maxpert/amqp-go/storage"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/bcrypt"
)

// --- minimal raw-frame helpers for protocol-violation probes ---

type rawFrameInfo struct {
	classID, methodID, replyCode uint16
}

func bufioReader(c net.Conn) *bufio.Reader { return bufio.NewReader(c) }

func writeMethodFrame(t *testing.T, c net.Conn, ch, class, method uint16, args []byte) {
	t.Helper()
	require.NoError(t, protocol.WriteFrame(c, protocol.EncodeMethodFrameForChannel(ch, class, method, args)))
}

// readMethodFrame reads one frame (skipping heartbeats) and, for
// connection.close, extracts the reply code.
func readMethodFrame(t *testing.T, r *bufio.Reader) rawFrameInfo {
	t.Helper()
	for {
		f, err := protocol.ReadFrameOptimizedWithLimit(r, 131072+8)
		require.NoError(t, err)
		if f.Type == byte(protocol.FrameHeartbeat) {
			continue
		}
		require.Equal(t, byte(protocol.FrameMethod), f.Type)
		require.GreaterOrEqual(t, len(f.Payload), 4)
		info := rawFrameInfo{
			classID:  uint16(f.Payload[0])<<8 | uint16(f.Payload[1]),
			methodID: uint16(f.Payload[2])<<8 | uint16(f.Payload[3]),
		}
		if info.classID == 10 && info.methodID == 50 {
			cm := &protocol.ConnectionCloseMethod{}
			require.NoError(t, cm.Deserialize(f.Payload[4:]))
			info.replyCode = cm.ReplyCode
		}
		return info
	}
}

// Root-level WS-B end-to-end tests: the same behaviors the server package
// covers unit-wise, exercised through the public builder API over real
// sockets. Naming follows the parent verification pattern
// (TestChannelIsolation|TestHandshake|TestShutdown|TestPreAuth|TestHeartbeat|
//  TestGetRequeue|TestPanicRecovery).

func wsbRootServer(t *testing.T, mutate func(*config.AMQPConfig)) (*server.Server, string) {
	t.Helper()
	cfg := isolatedTestConfig(t, "127.0.0.1:0")
	cfg.Server.LogLevel = "silent"
	if mutate != nil {
		mutate(cfg)
	}
	srv := newIsolatedTestServer(t, cfg)
	waitForListening(t, srv)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})
	srv.Mutex.RLock()
	addr := srv.Listener.Addr().String()
	srv.Mutex.RUnlock()
	return srv, addr
}

func wsbRoundTrip(t *testing.T, conn *amqp.Connection, queue, body string) {
	t.Helper()
	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()
	_, err = ch.QueueDeclare(queue, false, true, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, ch.Publish("", queue, false, false, amqp.Publishing{Body: []byte(body)}))
	msg, ok, err := ch.Get(queue, true)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, body, string(msg.Body))
}

// TestChannelIsolationWSB: a 404 channel error closes only the channel.
func TestChannelIsolationWSB(t *testing.T) {
	srv, addr := wsbRootServer(t, nil)

	conn, err := amqp.Dial("amqp://guest:guest@" + addr + "/")
	require.NoError(t, err)
	defer conn.Close()

	ch2, err := conn.Channel()
	require.NoError(t, err)
	_, err = ch2.QueueDeclare("e2e-iso-q", false, true, false, false, nil)
	require.NoError(t, err)

	ch1, err := conn.Channel()
	require.NoError(t, err)
	closeCh := ch1.NotifyClose(make(chan *amqp.Error, 1))

	_, err = ch1.Consume("no-such-queue", "", false, false, false, false, nil)
	require.Error(t, err)
	var amqpErr *amqp.Error
	require.True(t, errors.As(err, &amqpErr))
	assert.Equal(t, 404, amqpErr.Code)

	select {
	case e := <-closeCh:
		require.NotNil(t, e)
		assert.Equal(t, 404, e.Code)
	case <-time.After(2 * time.Second):
		t.Fatal("channel close not received")
	}

	require.False(t, conn.IsClosed(), "connection must survive a channel-level 404")
	require.NoError(t, ch2.Publish("", "e2e-iso-q", false, false, amqp.Publishing{Body: []byte("x")}))
	msg, ok, err := ch2.Get("e2e-iso-q", true)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, "x", string(msg.Body))

	srv.Mutex.RLock()
	n := len(srv.Connections)
	srv.Mutex.RUnlock()
	assert.Equal(t, 1, n)
}

// TestHandshakeWSB: with authentication enabled, a valid client connects; a
// protocol violation (tune-ok first) gets a 503, not an unauthenticated tune.
func TestHandshakeWSB(t *testing.T) {
	srv, addr := wsbRootServer(t, func(cfg *config.AMQPConfig) {
		hash, err := bcrypt.GenerateFromPassword([]byte("secret"), bcrypt.MinCost)
		require.NoError(t, err)
		dir := t.TempDir()
		authPath := filepath.Join(dir, "auth.json")
		type authFile struct {
			Users []auth.UserEntry `json:"users"`
		}
		data, err := json.MarshalIndent(authFile{Users: []auth.UserEntry{{
			Username:     "alice",
			PasswordHash: string(hash),
		}}}, "", "  ")
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(authPath, data, 0600))
		cfg.Security.AuthenticationEnabled = true
		cfg.Security.AuthenticationFilePath = authPath
		cfg.Security.AuthorizationEnabled = false
	})

	conn, err := amqp.Dial("amqp://alice:secret@" + addr + "/")
	require.NoError(t, err)
	defer conn.Close()
	wsbRoundTrip(t, conn, "e2e-hs-q", "m")

	// tune-ok before start-ok → connection.close 503 (no tune first).
	c, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer c.Close()
	_, err = c.Write([]byte("AMQP\x00\x00\x09\x01"))
	require.NoError(t, err)
	r := bufioReader(c)
	readMethodFrame(t, r) // connection.start
	writeMethodFrame(t, c, 0, 10, 31, make([]byte, 8))
	closeFrame := readMethodFrame(t, r)
	require.Equal(t, uint16(10), closeFrame.classID)
	require.Equal(t, uint16(50), closeFrame.methodID, "expected connection.close")
	require.Equal(t, uint16(503), closeFrame.replyCode)
	_ = srv
}

// TestShutdownWSB: connected clients see 320 CONNECTION_FORCED; the listener
// stops; storage closes (builder-owned) — and Shutdown is idempotent.
func TestShutdownWSB(t *testing.T) {
	srv, addr := wsbRootServer(t, nil)

	conn, err := amqp.Dial("amqp://guest:guest@" + addr + "/")
	require.NoError(t, err)
	defer conn.Close()
	errCh := conn.NotifyClose(make(chan *amqp.Error, 1))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, srv.Shutdown(ctx))

	select {
	case e := <-errCh:
		require.NotNil(t, e)
		assert.Equal(t, 320, e.Code)
		assert.True(t, e.Server)
	case <-time.After(2 * time.Second):
		t.Fatal("client never saw connection.close 320")
	}

	assert.False(t, srv.IsListening())
	_, err = amqp.Dial("amqp://guest:guest@" + addr + "/")
	assert.Error(t, err)

	// Idempotent: second call returns the same result.
	require.NoError(t, srv.Shutdown(ctx))
}

// TestPreAuthWSB: garbage headers get the supported-protocol reply; silent
// pre-auth sockets are capped and never starve real clients.
func TestPreAuthWSB(t *testing.T) {
	srv, addr := wsbRootServer(t, nil)

	c, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer c.Close()
	_, err = c.Write([]byte("GET / HT"))
	require.NoError(t, err)
	buf := make([]byte, 8)
	require.NoError(t, c.SetReadDeadline(time.Now().Add(2*time.Second)))
	_, err = io.ReadFull(c, buf)
	require.NoError(t, err)
	assert.Equal(t, []byte("AMQP\x00\x00\x09\x01"), buf)
	_ = c.Close()

	// 20 silent sockets (well under the pending cap) must not block a client.
	silent := make([]net.Conn, 0, 20)
	for i := 0; i < 20; i++ {
		sc, err := net.Dial("tcp", addr)
		require.NoError(t, err)
		silent = append(silent, sc)
	}
	defer func() {
		for _, sc := range silent {
			_ = sc.Close()
		}
	}()

	conn, err := amqp.Dial("amqp://guest:guest@" + addr + "/")
	require.NoError(t, err)
	defer conn.Close()
	wsbRoundTrip(t, conn, "e2e-pa-q", "m")
	_ = srv
}

// TestHeartbeatWSB: heartbeat=0 connections are not torn down after open.
func TestHeartbeatWSB(t *testing.T) {
	srv, addr := wsbRootServer(t, func(cfg *config.AMQPConfig) {
		cfg.Network.HeartbeatIntervalMS = 0
	})

	conn, err := amqp.DialConfig("amqp://guest:guest@"+addr+"/", amqp.Config{Heartbeat: 0})
	require.NoError(t, err)
	defer conn.Close()

	closeCh := conn.NotifyClose(make(chan *amqp.Error, 1))
	ch, err := conn.Channel()
	require.NoError(t, err)
	_, err = ch.QueueDeclare("e2e-hb-q", false, true, false, false, nil)
	require.NoError(t, err)

	for i := 0; i < 5; i++ {
		require.NoError(t, ch.Publish("", "e2e-hb-q", false, false, amqp.Publishing{Body: []byte("m")}))
		_, ok, err := ch.Get("e2e-hb-q", true)
		require.NoError(t, err)
		require.True(t, ok)
		time.Sleep(200 * time.Millisecond)
	}

	assert.False(t, conn.IsClosed())
	select {
	case e := <-closeCh:
		t.Fatalf("hb=0 connection closed: %v", e)
	default:
	}
	srv.Mutex.RLock()
	n := len(srv.Connections)
	srv.Mutex.RUnlock()
	assert.Equal(t, 1, n)
}

// TestGetRequeueWSB: connection close requeues only its own basic.get
// deliveries.
func TestGetRequeueWSB(t *testing.T) {
	_, addr := wsbRootServer(t, nil)

	setup, err := amqp.Dial("amqp://guest:guest@" + addr + "/")
	require.NoError(t, err)
	defer setup.Close()
	sch, err := setup.Channel()
	require.NoError(t, err)
	_, err = sch.QueueDeclare("e2e-gq", false, false, false, false, nil)
	require.NoError(t, err)
	for _, b := range []string{"A", "B"} {
		require.NoError(t, sch.Publish("", "e2e-gq", false, false, amqp.Publishing{Body: []byte(b)}))
	}

	conn1, err := amqp.Dial("amqp://guest:guest@" + addr + "/")
	require.NoError(t, err)
	c1, err := conn1.Channel()
	require.NoError(t, err)
	a, ok, err := c1.Get("e2e-gq", false)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, "A", string(a.Body))

	conn2, err := amqp.Dial("amqp://guest:guest@" + addr + "/")
	require.NoError(t, err)
	defer conn2.Close()
	c2, err := conn2.Channel()
	require.NoError(t, err)
	b, ok, err := c2.Get("e2e-gq", false)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, "B", string(b.Body))

	require.NoError(t, conn1.Close())

	var gotA amqp.Delivery
	require.Eventually(t, func() bool {
		d, ok, err := sch.Get("e2e-gq", false)
		if err != nil || !ok {
			return false
		}
		gotA = d
		return true
	}, 2*time.Second, 20*time.Millisecond)
	assert.Equal(t, "A", string(gotA.Body))
	assert.True(t, gotA.Redelivered)

	// B belongs to conn2: not requeued, its ack still works.
	_, ok, err = sch.Get("e2e-gq", true)
	require.NoError(t, err)
	assert.False(t, ok, "B must still be owned by conn2")
	require.NoError(t, b.Ack(false))
	require.NoError(t, gotA.Ack(false))
}

// rootPanicBroker panics on DeclareQueue for a matching queue name — the
// root-level analogue of the server package's panicBroker, built from
// exported pieces only.
type rootPanicBroker struct {
	server.UnifiedBroker
	declarePanic string
	panicked     atomic.Bool
}

func (p *rootPanicBroker) DeclareQueue(name string, durable, autoDelete, exclusive bool, arguments map[string]interface{}) (*protocol.Queue, error) {
	if p.declarePanic != "" && name == p.declarePanic {
		p.panicked.Store(true)
		panic("rootPanicBroker: DeclareQueue(" + name + ")")
	}
	return p.UnifiedBroker.DeclareQueue(name, durable, autoDelete, exclusive, arguments)
}

// PublishMessageAdmitted forwards non-blocking admission (WS-I) so Build's
// lease-enabled broker check passes for this panic-injection server.
func (p *rootPanicBroker) PublishMessageAdmitted(exchangeName, routingKey string, message *protocol.Message) (protocol.DepthGate, error) {
	return p.UnifiedBroker.(server.NonBlockingPublisher).PublishMessageAdmitted(exchangeName, routingKey, message)
}

// TestPanicRecoveryWSB: a panic in a connection goroutine is recovered — the
// connection dies, PanicCount increments, and the broker keeps serving.
func TestPanicRecoveryWSB(t *testing.T) {
	cfg := isolatedTestConfig(t, "127.0.0.1:0")
	cfg.Server.LogLevel = "silent"
	st, err := storage.NewDisruptorStorageWithDataDir(cfg.Storage.Path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	pb := &rootPanicBroker{declarePanic: "boom-q"}
	pb.UnifiedBroker = server.NewStorageBrokerAdapter(broker.NewStorageBroker(st, cfg.GetEngine()))
	srv, err := server.NewServerBuilder().WithConfig(cfg).WithUnifiedBroker(pb).WithStorage(st).Build()
	require.NoError(t, err)
	waitForListening(t, srv)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})
	srv.Mutex.RLock()
	addr := srv.Listener.Addr().String()
	srv.Mutex.RUnlock()

	conn, err := amqp.Dial("amqp://guest:guest@" + addr + "/")
	require.NoError(t, err)
	defer conn.Close()
	ch, err := conn.Channel()
	require.NoError(t, err)
	_, _ = ch.QueueDeclare("boom-q", false, true, false, false, nil)

	require.Eventually(t, func() bool {
		return srv.PanicCount() >= 1
	}, 2*time.Second, 10*time.Millisecond, "the panic must be recovered and counted")

	conn2, err := amqp.Dial("amqp://guest:guest@" + addr + "/")
	require.NoError(t, err, "broker must stay alive after a connection-goroutine panic")
	defer conn2.Close()
	wsbRoundTrip(t, conn2, "e2e-alive-q", "m")
}
