package server

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

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/interfaces"
)

// ============================================================================
// A1: TLS listener and config validation — spec-driven tests
//
// AMQP 0.9.1 spec: TLS is an out-of-band transport concern. The spec assumes
// a TCP socket; TLS wraps the raw TCP and the AMQP protocol runs on top.
// RabbitMQ uses port 5671 for AMQPS (TLS) and clients connect with amqps://.
//
// Requirements:
//   - loadTLSConfig reads cert/key files and builds *tls.Config
//   - MinVersion must be TLS 1.2 (RFC 8996 deprecated TLS 1.0/1.1)
//   - When TLSCAFile is set → mutual TLS (RequireAndVerifyClientCert)
//   - When TLSCAFile is empty → server-only TLS (NoClientCert)
//   - Missing cert/key files must return error
//   - createTLSListener wraps a TCP listener with TLS
//   - Server.Start() uses TLS listener when Security.TLSEnabled is true
// ============================================================================

// --- Test certificate generation helper ---

// testTLSCerts holds paths to generated cert/key/CA files for testing.
type testTLSCerts struct {
	CertFile string
	KeyFile  string
	CAFile   string
	Dir      string
}

// generateTestTLSCerts generates a self-signed ECDSA certificate and key
// in a temp directory suitable for TLS testing.
func generateTestTLSCerts(t *testing.T) testTLSCerts {
	t.Helper()
	dir := t.TempDir()
	return generateTestTLSCertsInDir(t, dir)
}

// generateTestTLSCertsInDir generates cert/key files in the specified directory.
func generateTestTLSCertsInDir(t *testing.T, dir string) testTLSCerts {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("failed to generate ECDSA key: %v", err)
	}

	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		t.Fatalf("failed to generate serial number: %v", err)
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
		t.Fatalf("failed to create certificate: %v", err)
	}

	certFile := filepath.Join(dir, "cert.pem")
	keyFile := filepath.Join(dir, "key.pem")
	caFile := filepath.Join(dir, "ca.pem")

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	if err := os.WriteFile(certFile, certPEM, 0600); err != nil {
		t.Fatalf("failed to write cert file: %v", err)
	}

	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("failed to marshal ECDSA key: %v", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	if err := os.WriteFile(keyFile, keyPEM, 0600); err != nil {
		t.Fatalf("failed to write key file: %v", err)
	}

	// CA file is the same self-signed cert (it's a CA)
	if err := os.WriteFile(caFile, certPEM, 0600); err != nil {
		t.Fatalf("failed to write CA file: %v", err)
	}

	return testTLSCerts{
		CertFile: certFile,
		KeyFile:  keyFile,
		CAFile:   caFile,
		Dir:      dir,
	}
}

// --- loadTLSConfig tests ---

func TestLoadTLSConfig_ValidCertKey(t *testing.T) {
	certs := generateTestTLSCerts(t)
	secCfg := interfaces.SecurityConfig{
		TLSEnabled:  true,
		TLSCertFile: certs.CertFile,
		TLSKeyFile:  certs.KeyFile,
	}

	tlsCfg, err := loadTLSConfig(secCfg)
	if err != nil {
		t.Fatalf("loadTLSConfig failed: %v", err)
	}
	if tlsCfg == nil {
		t.Fatal("loadTLSConfig returned nil config")
	}
	if len(tlsCfg.Certificates) != 1 {
		t.Errorf("expected 1 certificate, got %d", len(tlsCfg.Certificates))
	}
}

func TestLoadTLSConfig_MinVersionTLS12(t *testing.T) {
	certs := generateTestTLSCerts(t)
	secCfg := interfaces.SecurityConfig{
		TLSEnabled:  true,
		TLSCertFile: certs.CertFile,
		TLSKeyFile:  certs.KeyFile,
	}

	tlsCfg, err := loadTLSConfig(secCfg)
	if err != nil {
		t.Fatalf("loadTLSConfig failed: %v", err)
	}
	if tlsCfg.MinVersion != tls.VersionTLS12 {
		t.Errorf("expected MinVersion TLS 1.2 (0x%04x), got 0x%04x", tls.VersionTLS12, tlsCfg.MinVersion)
	}
}

func TestLoadTLSConfig_ServerOnlyNoClientCert(t *testing.T) {
	certs := generateTestTLSCerts(t)
	secCfg := interfaces.SecurityConfig{
		TLSEnabled:  true,
		TLSCertFile: certs.CertFile,
		TLSKeyFile:  certs.KeyFile,
	}

	tlsCfg, err := loadTLSConfig(secCfg)
	if err != nil {
		t.Fatalf("loadTLSConfig failed: %v", err)
	}
	if tlsCfg.ClientAuth != tls.NoClientCert {
		t.Errorf("expected ClientAuth NoClientCert, got %v", tlsCfg.ClientAuth)
	}
}

func TestLoadTLSConfig_MutualTLS(t *testing.T) {
	certs := generateTestTLSCerts(t)
	secCfg := interfaces.SecurityConfig{
		TLSEnabled:  true,
		TLSCertFile: certs.CertFile,
		TLSKeyFile:  certs.KeyFile,
		TLSCAFile:   certs.CAFile,
	}

	tlsCfg, err := loadTLSConfig(secCfg)
	if err != nil {
		t.Fatalf("loadTLSConfig failed: %v", err)
	}
	if tlsCfg.ClientAuth != tls.RequireAndVerifyClientCert {
		t.Errorf("expected ClientAuth RequireAndVerifyClientCert, got %v", tlsCfg.ClientAuth)
	}
	if tlsCfg.ClientCAs == nil {
		t.Error("expected ClientCAs pool to be set for mutual TLS")
	}
}

func TestLoadTLSConfig_MissingCertFile(t *testing.T) {
	certs := generateTestTLSCerts(t)
	secCfg := interfaces.SecurityConfig{
		TLSEnabled:  true,
		TLSCertFile: filepath.Join(certs.Dir, "nonexistent.pem"),
		TLSKeyFile:  certs.KeyFile,
	}

	_, err := loadTLSConfig(secCfg)
	if err == nil {
		t.Fatal("expected error for missing cert file, got nil")
	}
}

func TestLoadTLSConfig_MissingKeyFile(t *testing.T) {
	certs := generateTestTLSCerts(t)
	secCfg := interfaces.SecurityConfig{
		TLSEnabled:  true,
		TLSCertFile: certs.CertFile,
		TLSKeyFile:  filepath.Join(certs.Dir, "nonexistent.pem"),
	}

	_, err := loadTLSConfig(secCfg)
	if err == nil {
		t.Fatal("expected error for missing key file, got nil")
	}
}

func TestLoadTLSConfig_MissingCAFile(t *testing.T) {
	certs := generateTestTLSCerts(t)
	secCfg := interfaces.SecurityConfig{
		TLSEnabled:  true,
		TLSCertFile: certs.CertFile,
		TLSKeyFile:  certs.KeyFile,
		TLSCAFile:   filepath.Join(certs.Dir, "nonexistent-ca.pem"),
	}

	_, err := loadTLSConfig(secCfg)
	if err == nil {
		t.Fatal("expected error for missing CA file, got nil")
	}
}

func TestLoadTLSConfig_EmptyCertPath(t *testing.T) {
	secCfg := interfaces.SecurityConfig{
		TLSEnabled:  true,
		TLSCertFile: "",
		TLSKeyFile:  "/tmp/key.pem",
	}

	_, err := loadTLSConfig(secCfg)
	if err == nil {
		t.Fatal("expected error for empty cert path, got nil")
	}
}

func TestLoadTLSConfig_EmptyKeyPath(t *testing.T) {
	certs := generateTestTLSCerts(t)
	secCfg := interfaces.SecurityConfig{
		TLSEnabled:  true,
		TLSCertFile: certs.CertFile,
		TLSKeyFile:  "",
	}

	_, err := loadTLSConfig(secCfg)
	if err == nil {
		t.Fatal("expected error for empty key path, got nil")
	}
}

// --- createTLSListener tests ---

func TestCreateTLSListener_ValidConfig(t *testing.T) {
	certs := generateTestTLSCerts(t)
	secCfg := interfaces.SecurityConfig{
		TLSEnabled:  true,
		TLSCertFile: certs.CertFile,
		TLSKeyFile:  certs.KeyFile,
	}

	tlsCfg, err := loadTLSConfig(secCfg)
	if err != nil {
		t.Fatalf("loadTLSConfig failed: %v", err)
	}

	ln, err := createTLSListener("127.0.0.1:0", tlsCfg)
	if err != nil {
		t.Fatalf("createTLSListener failed: %v", err)
	}
	defer ln.Close()

	addr := ln.Addr().(*net.TCPAddr)
	if addr.Port == 0 {
		t.Error("expected non-zero port from listener")
	}
}

func TestCreateTLSListener_TLSHandshake(t *testing.T) {
	certs := generateTestTLSCerts(t)
	secCfg := interfaces.SecurityConfig{
		TLSEnabled:  true,
		TLSCertFile: certs.CertFile,
		TLSKeyFile:  certs.KeyFile,
	}

	tlsCfg, err := loadTLSConfig(secCfg)
	if err != nil {
		t.Fatalf("loadTLSConfig failed: %v", err)
	}

	ln, err := createTLSListener("127.0.0.1:0", tlsCfg)
	if err != nil {
		t.Fatalf("createTLSListener failed: %v", err)
	}
	defer ln.Close()

	addr := ln.Addr().String()

	// Server side: accept, read 1 byte, echo it back
	serverDone := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			serverDone <- fmt.Errorf("accept failed: %v", err)
			return
		}
		defer conn.Close()
		buf := make([]byte, 1)
		if _, err := conn.Read(buf); err != nil {
			serverDone <- fmt.Errorf("server read failed: %v", err)
			return
		}
		// Echo back
		conn.Write(buf)
		serverDone <- nil
	}()

	// Client side: dial with TLS, verify cert against CA pool
	caPool := loadCAPool(t, certs.CAFile)

	clientCfg := &tls.Config{
		RootCAs:    caPool,
		ServerName: "localhost",
		MinVersion: tls.VersionTLS12,
	}

	conn, err := tls.Dial("tcp", addr, clientCfg)
	if err != nil {
		t.Fatalf("TLS dial failed: %v", err)
	}
	defer conn.Close()

	// Verify handshake completed
	state := conn.ConnectionState()
	if !state.HandshakeComplete {
		t.Error("expected handshake to be complete")
	}

	// Write a byte to trigger server read
	if _, err := conn.Write([]byte{0x41}); err != nil {
		t.Fatalf("client write failed: %v", err)
	}

	// Read echo
	buf := make([]byte, 1)
	if _, err := conn.Read(buf); err != nil {
		t.Fatalf("client read failed: %v", err)
	}
	if buf[0] != 0x41 {
		t.Errorf("expected echo byte 0x41, got 0x%02x", buf[0])
	}

	// Wait for server to finish
	select {
	case err := <-serverDone:
		if err != nil {
			t.Fatalf("server error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server timed out")
	}
}

func TestCreateTLSListener_MutualTLSClientCertRequired(t *testing.T) {
	certs := generateTestTLSCerts(t)
	secCfg := interfaces.SecurityConfig{
		TLSEnabled:  true,
		TLSCertFile: certs.CertFile,
		TLSKeyFile:  certs.KeyFile,
		TLSCAFile:   certs.CAFile,
	}

	tlsCfg, err := loadTLSConfig(secCfg)
	if err != nil {
		t.Fatalf("loadTLSConfig failed: %v", err)
	}

	ln, err := createTLSListener("127.0.0.1:0", tlsCfg)
	if err != nil {
		t.Fatalf("createTLSListener failed: %v", err)
	}
	defer ln.Close()

	addr := ln.Addr().String()

	// Server: accept and force handshake
	serverHandshakeErr := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			serverHandshakeErr <- fmt.Errorf("accept: %w", err)
			return
		}
		defer conn.Close()
		// Force server-side handshake
		if err := conn.(*tls.Conn).Handshake(); err != nil {
			serverHandshakeErr <- err
			return
		}
		serverHandshakeErr <- nil
	}()

	// Client WITHOUT cert — handshake should fail
	caPool := loadCAPool(t, certs.CAFile)

	clientCfg := &tls.Config{
		RootCAs:    caPool,
		ServerName: "localhost",
		MinVersion: tls.VersionTLS12,
		// No Certificates — mutual TLS should reject
	}

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("net.Dial failed: %v", err)
	}
	defer conn.Close()

	// Set deadline so handshake doesn't block forever
	conn.SetDeadline(time.Now().Add(10 * time.Second))

	tlsConn := tls.Client(conn, clientCfg)
	clientErr := tlsConn.Handshake()

	// Check server-side result
	var srvErr error
	select {
	case srvErr = <-serverHandshakeErr:
	case <-time.After(10 * time.Second):
		t.Fatal("server handshake timed out")
	}

	t.Logf("server handshake error: %v", srvErr)
	t.Logf("client handshake error: %v", clientErr)

	// At least one side must report failure
	if clientErr == nil && srvErr == nil {
		t.Fatal("expected TLS handshake to fail (client or server) without client cert, but both succeeded")
	}
}

func TestCreateTLSListener_MutualTLSWithClientCert(t *testing.T) {
	certs := generateTestTLSCerts(t)
	secCfg := interfaces.SecurityConfig{
		TLSEnabled:  true,
		TLSCertFile: certs.CertFile,
		TLSKeyFile:  certs.KeyFile,
		TLSCAFile:   certs.CAFile,
	}

	tlsCfg, err := loadTLSConfig(secCfg)
	if err != nil {
		t.Fatalf("loadTLSConfig failed: %v", err)
	}

	ln, err := createTLSListener("127.0.0.1:0", tlsCfg)
	if err != nil {
		t.Fatalf("createTLSListener failed: %v", err)
	}
	defer ln.Close()

	addr := ln.Addr().String()

	// Server: accept and handshake
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		buf := make([]byte, 1)
		conn.Read(buf)
	}()

	// Client WITH cert — should succeed
	caPool := loadCAPool(t, certs.CAFile)

	clientCert, err := tls.LoadX509KeyPair(certs.CertFile, certs.KeyFile)
	if err != nil {
		t.Fatalf("failed to load client cert: %v", err)
	}

	clientCfg := &tls.Config{
		RootCAs:      caPool,
		ServerName:   "localhost",
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{clientCert},
	}

	conn, err := tls.Dial("tcp", addr, clientCfg)
	if err != nil {
		t.Fatalf("TLS dial with client cert failed: %v", err)
	}
	defer conn.Close()

	if err := conn.Handshake(); err != nil {
		t.Fatalf("TLS handshake with client cert failed: %v", err)
	}
}

// --- Server.Start() with TLS integration test ---

// TestServerStart_WithTLS exercises the full Start() path with TLS enabled,
// verifying createListener() routes to TLS (not plain TCP).
func TestServerStart_WithTLS(t *testing.T) {
	certs := generateTestTLSCerts(t)

	cfg := config.DefaultConfig()
	cfg.Network.Address = "127.0.0.1:0"
	cfg.Storage.Path = t.TempDir()
	cfg.Security.TLSEnabled = true
	cfg.Security.TLSCertFile = certs.CertFile
	cfg.Security.TLSKeyFile = certs.KeyFile

	srv, err := NewServerBuilder().WithConfig(cfg).Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	go func() {
		if err := srv.Start(); err != nil {
			t.Logf("Server stopped: %v", err)
		}
	}()
	defer func() {
		srv.Stop()
	}()

	// Poll until listener is ready
	var addr string
	for i := 0; i < 50; i++ {
		srv.Mutex.RLock()
		ln := srv.Listener
		srv.Mutex.RUnlock()
		if ln != nil {
			addr = ln.Addr().String()
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if addr == "" {
		t.Fatal("server listener not ready after 1s")
	}

	// Connect with TLS client — if Start() used plain TCP, this would fail
	caPool := loadCAPool(t, certs.CAFile)
	clientCfg := &tls.Config{
		RootCAs:    caPool,
		ServerName: "localhost",
		MinVersion: tls.VersionTLS12,
	}

	conn, err := tls.Dial("tcp", addr, clientCfg)
	if err != nil {
		t.Fatalf("TLS dial to server failed (Start() may not have used TLS): %v", err)
	}
	defer conn.Close()

	if err := conn.Handshake(); err != nil {
		t.Fatalf("TLS handshake with server failed: %v", err)
	}

	// Send AMQP protocol header over TLS — server should respond
	_, err = conn.Write([]byte("AMQP\x00\x00\x09\x01"))
	if err != nil {
		t.Fatalf("failed to write AMQP header: %v", err)
	}

	buf := make([]byte, 8)
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatalf("failed to read server response: %v", err)
	}
	if n == 0 {
		t.Error("expected server response, got 0 bytes")
	}
}

// TestServerStart_PlainTCPWhenTLSDisabled exercises the full Start() path
// with TLS disabled, verifying createListener() routes to plain TCP.
func TestServerStart_PlainTCPWhenTLSDisabled(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Network.Address = "127.0.0.1:0"
	cfg.Storage.Path = t.TempDir()
	cfg.Security.TLSEnabled = false

	srv, err := NewServerBuilder().WithConfig(cfg).Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	go func() {
		if err := srv.Start(); err != nil {
			t.Logf("Server stopped: %v", err)
		}
	}()
	defer func() {
		srv.Stop()
	}()

	// Poll until listener is ready
	var addr string
	for i := 0; i < 50; i++ {
		srv.Mutex.RLock()
		ln := srv.Listener
		srv.Mutex.RUnlock()
		if ln != nil {
			addr = ln.Addr().String()
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if addr == "" {
		t.Fatal("server listener not ready after 1s")
	}

	// Plain TCP connection should work
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("TCP dial failed: %v", err)
	}
	defer conn.Close()

	_, err = conn.Write([]byte("AMQP\x00\x00\x09\x01"))
	if err != nil {
		t.Fatalf("failed to write AMQP header: %v", err)
	}

	buf := make([]byte, 8)
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatalf("failed to read server response: %v", err)
	}
	if n == 0 {
		t.Error("expected server response, got 0 bytes")
	}
}

// --- Helper ---

// loadCAPool reads a CA file and returns a CertPool containing it.
// Fails the test if the file cannot be read or parsed.
func loadCAPool(t *testing.T, caFile string) *x509.CertPool {
	t.Helper()
	caCertPEM, err := os.ReadFile(caFile)
	if err != nil {
		t.Fatalf("failed to read CA file %s: %v", caFile, err)
	}
	caPool := x509.NewCertPool()
	if !caPool.AppendCertsFromPEM(caCertPEM) {
		t.Fatalf("failed to parse CA certificates from %s", caFile)
	}
	return caPool
}
