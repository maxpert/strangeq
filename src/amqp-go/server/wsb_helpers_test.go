package server

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/auth"
	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/bcrypt"
)

// startWSBServer builds a real Server with storage in t.TempDir(), Address
// 127.0.0.1:0, LogLevel "silent". It applies mutate(cfg) before Build and
// tweak(srv) after, starts it (go srv.Start(); poll srv.IsListening() ≤2s,
// surfacing Start's error like testserver_test.go), and registers a cleanup
// that stops the server.
func startWSBServer(t *testing.T, mutate func(*config.AMQPConfig), tweak func(*Server)) (*Server, string) {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.Network.Address = "127.0.0.1:0"
	cfg.Storage.Path = t.TempDir()
	cfg.Server.LogLevel = "silent"
	if mutate != nil {
		mutate(cfg)
	}
	srv, err := NewServerBuilder().WithConfig(cfg).Build()
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

// wsbStopServer is the single cleanup seam for startWSBServer (Shutdown with a
// 5s context, §11).
func wsbStopServer(srv *Server) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
}

// withFileAuth writes an auth file (same shape as authz_handler_test.go) and
// sets AuthenticationEnabled/AuthenticationFilePath on cfg.
func withFileAuth(t *testing.T, cfg *config.AMQPConfig, users []auth.UserEntry) {
	t.Helper()
	dir := t.TempDir()
	authPath := filepath.Join(dir, "auth.json")
	type authFile struct {
		Users []auth.UserEntry `json:"users"`
	}
	data, err := json.MarshalIndent(authFile{Users: users}, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(authPath, data, 0600))
	cfg.Security.AuthenticationEnabled = true
	cfg.Security.AuthenticationFilePath = authPath
}

// wsbUser builds an auth.UserEntry with a real bcrypt hash for password and the
// given vhost permission (configure/write/read regexps on vhost "/").
func wsbUser(t *testing.T, username, password, configure, write, read string) auth.UserEntry {
	t.Helper()
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
	require.NoError(t, err)
	return auth.UserEntry{
		Username:     username,
		PasswordHash: string(hash),
		VHostPermissions: []interfaces.VHostPermission{
			{VHost: "/", Permission: interfaces.Permission{Configure: configure, Write: write, Read: read}},
		},
	}
}

// wsbConnCount returns len(srv.Connections) under the read lock.
func wsbConnCount(srv *Server) int {
	srv.Mutex.RLock()
	defer srv.Mutex.RUnlock()
	return len(srv.Connections)
}

// wsbServerConn returns the single server-side connection (tests with exactly
// one client), or nil.
func wsbServerConn(srv *Server) *protocol.Connection {
	srv.Mutex.RLock()
	defer srv.Mutex.RUnlock()
	for _, c := range srv.Connections {
		return c
	}
	return nil
}

// --- rawAMQP: a minimal hand-driven client for protocol-violation tests ---

type rawAMQP struct {
	t *testing.T
	c net.Conn
	r *bufio.Reader
}

func dialRaw(t *testing.T, addr string) *rawAMQP {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	return &rawAMQP{t: t, c: c, r: bufio.NewReader(c)}
}

func (r *rawAMQP) sendHeader() {
	r.t.Helper()
	_, err := r.c.Write([]byte("AMQP\x00\x00\x09\x01"))
	require.NoError(r.t, err)
}

func (r *rawAMQP) send(ch, class, method uint16, args []byte) {
	r.t.Helper()
	frame := protocol.EncodeMethodFrameForChannel(ch, class, method, args)
	require.NoError(r.t, protocol.WriteFrame(r.c, frame))
}

func (r *rawAMQP) sendFrame(f *protocol.Frame) {
	r.t.Helper()
	require.NoError(r.t, protocol.WriteFrame(r.c, f))
}

func (r *rawAMQP) readFrame() *protocol.Frame {
	r.t.Helper()
	require.NoError(r.t, r.c.SetReadDeadline(time.Now().Add(2*time.Second)))
	f, err := protocol.ReadFrameOptimizedWithLimit(r.r, 131072+8)
	require.NoError(r.t, err)
	return f
}

// expectMethod reads frames (skipping heartbeats) until a method frame arrives
// and asserts its class/method, returning the payload after the 4-byte prefix.
func (r *rawAMQP) expectMethod(class, method uint16) []byte {
	r.t.Helper()
	for {
		f := r.readFrame()
		if f.Type == byte(protocol.FrameHeartbeat) {
			continue
		}
		require.Equal(r.t, byte(protocol.FrameMethod), f.Type, "expected method frame %d.%d", class, method)
		require.GreaterOrEqual(r.t, len(f.Payload), 4, "method payload")
		cls := uint16(f.Payload[0])<<8 | uint16(f.Payload[1])
		mth := uint16(f.Payload[2])<<8 | uint16(f.Payload[3])
		require.Equal(r.t, class, cls, "class id")
		require.Equal(r.t, method, mth, "method id")
		return f.Payload[4:]
	}
}

// expectMethodCh is expectMethod plus a channel assertion.
func (r *rawAMQP) expectMethodCh(ch, class, method uint16) []byte {
	r.t.Helper()
	for {
		f := r.readFrame()
		if f.Type == byte(protocol.FrameHeartbeat) {
			continue
		}
		require.Equal(r.t, byte(protocol.FrameMethod), f.Type, "expected method frame %d.%d", class, method)
		require.Equal(r.t, ch, f.Channel, "frame channel")
		require.GreaterOrEqual(r.t, len(f.Payload), 4, "method payload")
		cls := uint16(f.Payload[0])<<8 | uint16(f.Payload[1])
		mth := uint16(f.Payload[2])<<8 | uint16(f.Payload[3])
		require.Equal(r.t, class, cls, "class id")
		require.Equal(r.t, method, mth, "method id")
		return f.Payload[4:]
	}
}

func (r *rawAMQP) expectConnectionClose(code uint16) *protocol.ConnectionCloseMethod {
	r.t.Helper()
	payload := r.expectMethod(10, 50)
	m := &protocol.ConnectionCloseMethod{}
	require.NoError(r.t, m.Deserialize(payload))
	require.Equal(r.t, code, m.ReplyCode)
	return m
}

func (r *rawAMQP) expectChannelClose(ch, code uint16) {
	r.t.Helper()
	payload := r.expectMethodCh(ch, 20, 40)
	require.GreaterOrEqual(r.t, len(payload), 2, "channel.close payload")
	got := binary.BigEndian.Uint16(payload[:2])
	require.Equal(r.t, code, got)
}

func (r *rawAMQP) expectEOF() {
	r.t.Helper()
	require.NoError(r.t, r.c.SetReadDeadline(time.Now().Add(2*time.Second)))
	_, err := r.r.ReadByte()
	require.Error(r.t, err, "expected EOF/reset")
	require.True(r.t,
		errors.Is(err, io.EOF) || errors.Is(err, syscall.ECONNRESET) || errors.Is(err, net.ErrClosed),
		"expected EOF/reset, got %v", err)
}

// expectSilence asserts that no frame arrives within d.
func (r *rawAMQP) expectSilence(d time.Duration) {
	r.t.Helper()
	require.NoError(r.t, r.c.SetReadDeadline(time.Now().Add(d)))
	_, err := r.r.ReadByte()
	require.Error(r.t, err, "expected read timeout (silence)")
	var ne net.Error
	require.True(r.t, errors.As(err, &ne) && ne.Timeout(), "expected read timeout, got %v", err)
}

func (r *rawAMQP) startOK(mech string, resp []byte) {
	r.t.Helper()
	m := &protocol.ConnectionStartOKMethod{
		ClientProperties: map[string]interface{}{},
		Mechanism:        mech,
		Response:         resp,
		Locale:           "en_US",
	}
	data, err := m.Serialize()
	require.NoError(r.t, err)
	r.send(0, 10, 11, data)
}

// handshake runs the full happy-path AMQP handshake: header → start → start-ok
// (PLAIN) → tune → tune-ok → open → open-ok.
func (r *rawAMQP) handshake(user, pass string, chMax uint16, hb uint16) {
	r.t.Helper()
	r.sendHeader()
	r.expectMethod(10, 10) // connection.start
	resp := append([]byte{0}, []byte(user)...)
	resp = append(resp, 0)
	resp = append(resp, []byte(pass)...)
	r.startOK("PLAIN", resp)
	r.expectMethod(10, 30) // connection.tune
	tuneOK := make([]byte, 8)
	binary.BigEndian.PutUint16(tuneOK[0:2], chMax)
	binary.BigEndian.PutUint32(tuneOK[2:6], 131072)
	binary.BigEndian.PutUint16(tuneOK[6:8], hb)
	r.send(0, 10, 31, tuneOK)
	open := &protocol.ConnectionOpenMethod{VirtualHost: "/", Reserved1: "", Reserved2: 0}
	data, err := open.Serialize()
	require.NoError(r.t, err)
	r.send(0, 10, 40, data)
	r.expectMethod(10, 41) // connection.open-ok
}

func (r *rawAMQP) openChannel(ch uint16) {
	r.t.Helper()
	r.send(ch, 20, 10, []byte{})
	r.expectMethodCh(ch, 20, 11) // channel.open-ok
}

func (r *rawAMQP) declareQueue(ch uint16, name string) {
	r.t.Helper()
	m := &protocol.QueueDeclareMethod{Queue: name}
	data, err := m.Serialize()
	require.NoError(r.t, err)
	r.send(ch, 50, 10, data)
	r.expectMethodCh(ch, 50, 11) // queue.declare-ok
}

func (r *rawAMQP) consume(ch uint16, queue, tag string) {
	r.t.Helper()
	m := &protocol.BasicConsumeMethod{Queue: queue, ConsumerTag: tag}
	data, err := m.Serialize()
	require.NoError(r.t, err)
	r.send(ch, 60, 20, data)
	r.expectMethodCh(ch, 60, 21) // basic.consume-ok
}

// --- wrapping test doubles ---

// panicBroker wraps a real storage broker adapter and panics / errors on
// demand. It is installed with WithUnifiedBroker via startPanicBrokerServer.
type panicBroker struct {
	UnifiedBroker
	declarePanic string
	ackPanic     atomic.Bool
	purgeErr     error
}

func (p *panicBroker) DeclareQueue(name string, durable, autoDelete, exclusive bool, arguments map[string]interface{}) (*protocol.Queue, error) {
	if p.declarePanic != "" && name == p.declarePanic {
		panic("panicBroker: DeclareQueue(" + name + ")")
	}
	return p.UnifiedBroker.DeclareQueue(name, durable, autoDelete, exclusive, arguments)
}

// PublishMessageAdmitted forwards non-blocking admission (WS-I) to the
// wrapped broker when it supports it, so Build's lease-enabled broker check
// passes for panic-injection test servers.
func (p *panicBroker) PublishMessageAdmitted(exchangeName, routingKey string, message *protocol.Message) (protocol.DepthGate, error) {
	if np, ok := p.UnifiedBroker.(NonBlockingPublisher); ok {
		return np.PublishMessageAdmitted(exchangeName, routingKey, message)
	}
	return nil, p.UnifiedBroker.PublishMessage(exchangeName, routingKey, message)
}

func (p *panicBroker) AcknowledgeMessage(consumerID string, deliveryTag uint64, multiple bool) error {
	if p.ackPanic.Load() {
		panic("panicBroker: AcknowledgeMessage")
	}
	return p.UnifiedBroker.AcknowledgeMessage(consumerID, deliveryTag, multiple)
}

func (p *panicBroker) PurgeQueue(name string) (int, error) {
	if p.purgeErr != nil {
		return 0, p.purgeErr
	}
	return p.UnifiedBroker.PurgeQueue(name)
}

// panicMetrics panics once in RecordMessageDelivered / RecordConnectionCreated
// when the matching flag is armed.
type panicMetrics struct {
	NoOpMetricsCollector
	onDelivered atomic.Bool
	onCreated   atomic.Bool
}

func (m *panicMetrics) RecordMessageDelivered(size int) {
	if m.onDelivered.CompareAndSwap(true, false) {
		panic("panicMetrics: RecordMessageDelivered")
	}
}

func (m *panicMetrics) RecordConnectionCreated() {
	if m.onCreated.CompareAndSwap(true, false) {
		panic("panicMetrics: RecordConnectionCreated")
	}
}
