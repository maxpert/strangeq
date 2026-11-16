package clusterwire

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

const testCluster = "peer-transport"

func token(fill byte) string {
	return base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{fill}, 32))
}

type handlerFunc struct {
	command func(context.Context, []byte) ([]byte, error)
	raft    func(context.Context, uint64, []*raftpb.Message) error
}

func (h handlerFunc) Command(ctx context.Context, payload []byte) ([]byte, error) {
	if h.command == nil {
		return payload, nil
	}
	return h.command(ctx, payload)
}

func (h handlerFunc) Raft(ctx context.Context, group uint64, batch []*raftpb.Message) error {
	if h.raft == nil {
		return nil
	}
	return h.raft(ctx, group, batch)
}

func (h handlerFunc) Membership(_ context.Context, payload []byte) ([]byte, error) {
	return append([]byte("member: "), payload...), nil
}

func startPeer(t *testing.T, cfg Config, handler Handler) (*Client, string) {
	t.Helper()
	server, err := NewServer(cfg, handler)
	if err != nil {
		t.Fatal(err)
	}
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := server.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	client, err := NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	return client, server.Addr().String()
}

func TestPeerRoundTrip(t *testing.T) {
	t.Parallel()
	first := &raftpb.Message{Type: raftpb.MsgApp.Enum(), From: new(uint64(1)), To: new(uint64(2)),
		Entries: []*raftpb.Entry{{Data: []byte("durable entry")}}}
	second := &raftpb.Message{Type: raftpb.MsgHeartbeat.Enum(), From: new(uint64(1)), To: new(uint64(2))}
	done := make(chan struct{}, 1)
	client, address := startPeer(t, Config{
		Listen: "127.0.0.1:0", ClusterID: testCluster, Token: token(1), AllowInsecure: true,
	}, handlerFunc{
		raft: func(_ context.Context, group uint64, batch []*raftpb.Message) error {
			if group != 65 || len(batch) != 2 || !proto.Equal(batch[0], first) || !proto.Equal(batch[1], second) {
				return errors.New("wrong received group or messages")
			}
			done <- struct{}{}
			return nil
		},
	})
	ctx := context.Background()
	if err := client.Raft(ctx, address, 65, []*raftpb.Message{first, second}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	default:
		t.Fatal("raft handler did not run")
	}
	output, err := client.Command(ctx, address, []byte{0, 1, 255})
	if err != nil || !bytes.Equal(output, []byte{0, 1, 255}) {
		t.Fatalf("command output %x, error %v", output, err)
	}
	output, err = client.Membership(ctx, address, []byte("broker"))
	if err != nil || string(output) != "member: broker" {
		t.Fatalf("membership output %q, error %v", output, err)
	}
}

func TestPeerRejectsWrongTokenClusterAndOversize(t *testing.T) {
	t.Parallel()
	cfg := Config{Listen: "127.0.0.1:0", ClusterID: testCluster, Token: token(1), AllowInsecure: true, MaxBodyBytes: 64}
	executed := make(chan struct{}, 1)
	valid, address := startPeer(t, cfg, handlerFunc{
		command: func(_ context.Context, data []byte) ([]byte, error) {
			executed <- struct{}{}
			return data, nil
		},
	})
	ctx := context.Background()
	if _, err := valid.Command(ctx, address, make([]byte, 65)); !errors.Is(err, ErrTooLarge) || !errors.Is(err, ErrUnsent) {
		t.Fatalf("oversized local command: %v", err)
	}
	for _, tc := range []struct {
		cfg  Config
		size int
	}{
		{Config{ClusterID: testCluster, Token: token(2), AllowInsecure: true}, 8},
		{Config{ClusterID: "another-cluster", Token: token(1), AllowInsecure: true}, 8},
		{Config{ClusterID: testCluster, Token: token(1), AllowInsecure: true, MaxBodyBytes: 100}, 65},
	} {
		client, err := NewClient(tc.cfg)
		if err != nil {
			t.Fatal(err)
		}
		_, err = client.Command(ctx, address, make([]byte, tc.size))
		client.Close()
		if err == nil {
			t.Fatal("unauthorized or oversized command went through")
		}
	}
	select {
	case <-executed:
		t.Fatal("a disallowed request reached the handler")
	default:
	}
}

func TestPeerVerifiesBodyAndRejectsReplay(t *testing.T) {
	t.Parallel()
	client, address := startPeer(t, Config{
		Listen: "127.0.0.1:0", ClusterID: testCluster, Token: token(1), AllowInsecure: true,
	}, handlerFunc{})
	body := []byte("signed")
	target := "http://" + address + commandPath
	newRequest := func(data []byte) *http.Request {
		t.Helper()
		req, err := http.NewRequest(http.MethodPost, target, bytes.NewReader(data))
		if err != nil {
			t.Fatal(err)
		}
		return req
	}
	send := func(req *http.Request, want int) {
		t.Helper()
		res, err := client.rpc.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		_, _ = io.Copy(io.Discard, res.Body)
		if res.StatusCode != want {
			t.Fatalf("status %d, want %d", res.StatusCode, want)
		}
	}
	tampered := newRequest([]byte("altered"))
	if _, err := client.auth.signRequest(tampered, body); err != nil {
		t.Fatal(err)
	}
	send(tampered, http.StatusBadRequest)

	original := newRequest(body)
	if _, err := client.auth.signRequest(original, body); err != nil {
		t.Fatal(err)
	}
	send(original, http.StatusOK)
	replay := newRequest(body)
	replay.Header = original.Header.Clone()
	send(replay, http.StatusUnauthorized)
}

func TestClientKeepsLostCommandOutcomeUncertain(t *testing.T) {
	t.Parallel()
	seen := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		_, _ = io.Copy(io.Discard, req.Body)
		seen <- struct{}{}
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		_ = conn.Close()
	}))
	defer server.Close()
	client, err := NewClient(Config{ClusterID: testCluster, Token: token(1), AllowInsecure: true})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	_, err = client.Command(context.Background(), server.Listener.Addr().String(), []byte("uncertain"))
	if err == nil || errors.Is(err, ErrUnsent) {
		t.Fatalf("could replay a command which reached the server: %v", err)
	}
	select {
	case <-seen:
	default:
		t.Fatal("server never received the command")
	}
}

func TestTLSRejectsUntrustedPeerAndAcceptsPinnedCA(t *testing.T) {
	t.Parallel()
	donor := httptest.NewTLSServer(http.NotFoundHandler())
	defer donor.Close()
	roots := x509.NewCertPool()
	roots.AddCert(donor.Certificate())
	tlsConfig := &tls.Config{Certificates: donor.TLS.Certificates, RootCAs: roots}
	cfg := Config{Listen: "127.0.0.1:0", ClusterID: testCluster, Token: token(1), TLS: tlsConfig}
	client, address := startPeer(t, cfg, handlerFunc{})
	ctx := context.Background()
	data, err := client.Command(ctx, address, []byte("hello"))
	if err != nil || string(data) != "hello" {
		t.Fatalf("trusted result %q, error %v", data, err)
	}
	cfg.TLS = nil
	noTrust, err := NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer noTrust.Close()
	if _, err := noTrust.Command(ctx, address, nil); !errors.Is(err, ErrUnsent) {
		t.Fatalf("untrusted certificate: %v", err)
	}
}

func TestTLSHandshakeTimeoutLeavesTimeToTryAnotherPeer(t *testing.T) {
	t.Parallel()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	blackhole := make(chan net.Conn, 1)
	go func() {
		conn, err := listener.Accept()
		if err == nil {
			blackhole <- conn
		}
	}()
	client, err := NewClient(Config{ClusterID: testCluster, Token: token(1), Timeout: 4 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	start := time.Now()
	_, err = client.Command(context.Background(), listener.Addr().String(), []byte("never sent"))
	select {
	case conn := <-blackhole:
		_ = conn.Close()
	default:
		t.Fatal("test never established the blackholed TCP connection")
	}
	if !errors.Is(err, ErrUnsent) || time.Since(start) > 2*time.Second {
		t.Fatalf("no time to retry another peer after %s, error %v", time.Since(start), err)
	}
}

func TestPeerPlaintextRequiresNumericLoopback(t *testing.T) {
	t.Parallel()
	cfg := Config{Listen: "0.0.0.0:0", ClusterID: testCluster, Token: token(1), AllowInsecure: true}
	if _, err := NewServer(cfg, handlerFunc{}); err == nil {
		t.Fatal("plaintext nonloopback listener allowed")
	}
	cfg.Listen = "127.0.0.1:0"
	cfg.TLS = nil
	cfg.AllowInsecure = false
	if _, err := NewServer(cfg, handlerFunc{}); err == nil {
		t.Fatal("peer listener allowed plaintext by default")
	}
}
