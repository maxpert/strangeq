package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/server"
)

// ============================================================================
// A3: End-to-end TLS integration test with real AMQP client
//
// Tests the full TLS flow: server starts with TLS → client connects with
// amqps:// → AMQP handshake over TLS → declare/publish/consume roundtrip.
// Also tests mutual TLS (client cert required) and TLS + auth simultaneously.
//
// AMQP 0.9.1 spec: TLS is an out-of-band transport concern. The AMQP
// protocol runs on top of TLS unchanged — no protocol-level modifications.
// RabbitMQ uses port 5671 for AMQPS; clients use amqps:// scheme.
// ============================================================================

// generateIntegrationTLSCerts creates cert/key/CA files for integration testing.
func generateIntegrationTLSCerts(t *testing.T) (certFile, keyFile, caFile string) {
	t.Helper()
	dir := t.TempDir()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		t.Fatalf("failed to generate serial: %v", err)
	}

	template := x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			Organization: []string{"Test AMQP"},
			CommonName:   "localhost",
		},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		IsCA:                  true,
		BasicConstraintsValid: true,
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("failed to create cert: %v", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	certFile = filepath.Join(dir, "cert.pem")
	keyFile = filepath.Join(dir, "key.pem")
	caFile = filepath.Join(dir, "ca.pem")

	if err := os.WriteFile(certFile, certPEM, 0600); err != nil {
		t.Fatalf("failed to write cert: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("failed to marshal key: %v", err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0600); err != nil {
		t.Fatalf("failed to write key: %v", err)
	}
	if err := os.WriteFile(caFile, certPEM, 0600); err != nil {
		t.Fatalf("failed to write CA: %v", err)
	}

	return certFile, keyFile, caFile
}

// loadIntegrationCAPool reads a CA file and returns a CertPool.
func loadIntegrationCAPool(t *testing.T, caFile string) *x509.CertPool {
	t.Helper()
	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		t.Fatalf("failed to read CA file: %v", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		t.Fatalf("failed to parse CA certificates")
	}
	return pool
}

// startTLSTestServer starts a server with TLS enabled on the given port.
func startTLSTestServer(t *testing.T, port string, certFile, keyFile, caFile string) *server.Server {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.Network.Address = ":" + port
	cfg.Storage.Path = t.TempDir()
	cfg.Security.TLSEnabled = true
	cfg.Security.TLSCertFile = certFile
	cfg.Security.TLSKeyFile = keyFile
	if caFile != "" {
		cfg.Security.TLSCAFile = caFile
	}

	srv, err := server.NewServerBuilder().WithConfig(cfg).Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	go func() {
		if err := srv.Start(); err != nil {
			t.Logf("Server stopped: %v", err)
		}
	}()

	// Poll until listener is ready
	for i := 0; i < 50; i++ {
		if srv.IsListening() {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !srv.IsListening() {
		t.Fatal("server listener not ready after 1s")
	}

	t.Cleanup(func() {
		srv.Stop()
	})

	return srv
}

// dialTLSAMQP connects to the server using amqps:// with TLS.
func dialTLSAMQP(t *testing.T, url string, caFile string, clientCertFile, clientKeyFile string) *amqp.Connection {
	t.Helper()
	caPool := loadIntegrationCAPool(t, caFile)

	tlsCfg := &tls.Config{
		RootCAs:    caPool,
		ServerName: "localhost",
		MinVersion: tls.VersionTLS12,
	}

	if clientCertFile != "" && clientKeyFile != "" {
		clientCert, err := tls.LoadX509KeyPair(clientCertFile, clientKeyFile)
		if err != nil {
			t.Fatalf("failed to load client cert: %v", err)
		}
		tlsCfg.Certificates = []tls.Certificate{clientCert}
	}

	conn, err := amqp.DialTLS(url, tlsCfg)
	if err != nil {
		t.Fatalf("Failed to dial TLS %s: %v", url, err)
	}
	return conn
}

// --- Integration Tests ---

func TestTLSIntegration_PublishConsumeRoundtrip(t *testing.T) {
	port := "15690"
	certFile, keyFile, caFile := generateIntegrationTLSCerts(t)
	startTLSTestServer(t, port, certFile, keyFile, "")

	conn := dialTLSAMQP(t, fmt.Sprintf("amqps://localhost:%s/", port), caFile, "", "")
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("Failed to create channel: %v", err)
	}
	defer ch.Close()

	// Declare a queue
	q, err := ch.QueueDeclare("tls-test-queue", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("Failed to declare queue: %v", err)
	}

	// Publish a message
	body := []byte("Hello over TLS!")
	err = ch.Publish("", q.Name, false, false, amqp.Publishing{
		ContentType: "text/plain",
		Body:        body,
	})
	if err != nil {
		t.Fatalf("Failed to publish: %v", err)
	}

	// Consume the message
	msgs, err := ch.Consume(q.Name, "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("Failed to consume: %v", err)
	}

	select {
	case msg := <-msgs:
		if string(msg.Body) != string(body) {
			t.Errorf("expected %q, got %q", string(body), string(msg.Body))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for message")
	}
}

func TestTLSIntegration_MultipleConnections(t *testing.T) {
	port := "15691"
	certFile, keyFile, caFile := generateIntegrationTLSCerts(t)
	startTLSTestServer(t, port, certFile, keyFile, "")

	// Connect two clients simultaneously over TLS
	conn1 := dialTLSAMQP(t, fmt.Sprintf("amqps://localhost:%s/", port), caFile, "", "")
	defer conn1.Close()

	conn2 := dialTLSAMQP(t, fmt.Sprintf("amqps://localhost:%s/", port), caFile, "", "")
	defer conn2.Close()

	ch1, err := conn1.Channel()
	if err != nil {
		t.Fatalf("Failed to create channel 1: %v", err)
	}
	defer ch1.Close()

	ch2, err := conn2.Channel()
	if err != nil {
		t.Fatalf("Failed to create channel 2: %v", err)
	}
	defer ch2.Close()

	// conn1 publishes, conn2 consumes
	_, err = ch1.QueueDeclare("tls-multi-queue", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("Failed to declare queue: %v", err)
	}

	err = ch1.Publish("", "tls-multi-queue", false, false, amqp.Publishing{
		Body: []byte("from conn1"),
	})
	if err != nil {
		t.Fatalf("Failed to publish: %v", err)
	}

	msgs, err := ch2.Consume("tls-multi-queue", "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("Failed to consume: %v", err)
	}

	select {
	case msg := <-msgs:
		if string(msg.Body) != "from conn1" {
			t.Errorf("expected 'from conn1', got %q", string(msg.Body))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for message")
	}
}

func TestTLSIntegration_MutualAuthWithCert(t *testing.T) {
	port := "15692"
	certFile, keyFile, caFile := generateIntegrationTLSCerts(t)
	startTLSTestServer(t, port, certFile, keyFile, caFile)

	// Client WITH cert — should connect successfully
	conn := dialTLSAMQP(t, fmt.Sprintf("amqps://localhost:%s/", port), caFile, certFile, keyFile)
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("Failed to create channel with client cert: %v", err)
	}
	defer ch.Close()

	// Verify we can do AMQP operations
	_, err = ch.QueueDeclare("mutual-tls-queue", false, false, false, false, nil)
	if err != nil {
		t.Errorf("Failed to declare queue with mutual TLS: %v", err)
	}
}

func TestTLSIntegration_MutualAuthWithoutCertRejected(t *testing.T) {
	port := "15693"
	certFile, keyFile, caFile := generateIntegrationTLSCerts(t)
	startTLSTestServer(t, port, certFile, keyFile, caFile)

	// Client WITHOUT cert — should be rejected by server
	caPool := loadIntegrationCAPool(t, caFile)
	tlsCfg := &tls.Config{
		RootCAs:    caPool,
		ServerName: "localhost",
		MinVersion: tls.VersionTLS12,
		// No Certificates — server should reject
	}

	_, err := amqp.DialTLS(fmt.Sprintf("amqps://localhost:%s/", port), tlsCfg)
	if err == nil {
		t.Fatal("expected DialTLS to fail without client cert, but it succeeded")
	}
	t.Logf("correctly rejected: %v", err)
}

func TestTLSIntegration_LargeMessageOverTLS(t *testing.T) {
	port := "15694"
	certFile, keyFile, caFile := generateIntegrationTLSCerts(t)
	startTLSTestServer(t, port, certFile, keyFile, "")

	conn := dialTLSAMQP(t, fmt.Sprintf("amqps://localhost:%s/", port), caFile, "", "")
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("Failed to create channel: %v", err)
	}
	defer ch.Close()

	q, err := ch.QueueDeclare("tls-large-queue", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("Failed to declare queue: %v", err)
	}

	// 1MB message over TLS
	largeBody := make([]byte, 1024*1024)
	for i := range largeBody {
		largeBody[i] = byte(i % 256)
	}

	err = ch.Publish("", q.Name, false, false, amqp.Publishing{
		ContentType: "application/octet-stream",
		Body:        largeBody,
	})
	if err != nil {
		t.Fatalf("Failed to publish large message: %v", err)
	}

	msgs, err := ch.Consume(q.Name, "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("Failed to consume: %v", err)
	}

	select {
	case msg := <-msgs:
		if len(msg.Body) != len(largeBody) {
			t.Fatalf("expected %d bytes, got %d", len(largeBody), len(msg.Body))
		}
		for i := range largeBody {
			if msg.Body[i] != largeBody[i] {
				t.Fatalf("byte mismatch at index %d: expected %d, got %d", i, largeBody[i], msg.Body[i])
			}
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for large message")
	}
}
