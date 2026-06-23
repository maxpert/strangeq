package server

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/config"
)

// ============================================================================
// A2: Builder wiring and CLI flag tests
//
// Tests that WithTLS() and WithTLSMutualAuth() correctly configure the
// server via the builder pattern, and that config.Validate() properly
// validates TLS settings including CA file.
// ============================================================================

// generateBuilderTestCerts creates cert/key/CA files in a temp dir.
func generateBuilderTestCerts(t *testing.T) (certFile, keyFile, caFile string) {
	t.Helper()
	dir := t.TempDir()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	serial, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	template := x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "localhost"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IsCA:                  true,
		BasicConstraintsValid: true,
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
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

func TestBuilderWithTLS(t *testing.T) {
	certFile, keyFile, _ := generateBuilderTestCerts(t)

	cfg := config.DefaultConfig()
	cfg.Storage.Path = t.TempDir()

	srv, err := NewServerBuilder().
		WithConfig(cfg).
		WithTLS(certFile, keyFile).
		Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	if !srv.Config.Security.TLSEnabled {
		t.Error("expected TLSEnabled to be true")
	}
	if srv.Config.Security.TLSCertFile != certFile {
		t.Errorf("expected TLSCertFile %s, got %s", certFile, srv.Config.Security.TLSCertFile)
	}
	if srv.Config.Security.TLSKeyFile != keyFile {
		t.Errorf("expected TLSKeyFile %s, got %s", keyFile, srv.Config.Security.TLSKeyFile)
	}
	if srv.Config.Security.TLSCAFile != "" {
		t.Errorf("expected empty TLSCAFile for server-only TLS, got %s", srv.Config.Security.TLSCAFile)
	}
}

func TestBuilderWithTLSMutualAuth(t *testing.T) {
	certFile, keyFile, caFile := generateBuilderTestCerts(t)

	cfg := config.DefaultConfig()
	cfg.Storage.Path = t.TempDir()

	srv, err := NewServerBuilder().
		WithConfig(cfg).
		WithTLS(certFile, keyFile).
		WithTLSMutualAuth(caFile).
		Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	if !srv.Config.Security.TLSEnabled {
		t.Error("expected TLSEnabled to be true")
	}
	if srv.Config.Security.TLSCAFile != caFile {
		t.Errorf("expected TLSCAFile %s, got %s", caFile, srv.Config.Security.TLSCAFile)
	}
}

func TestBuilderWithoutTLS(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Storage.Path = t.TempDir()

	srv, err := NewServerBuilder().
		WithConfig(cfg).
		Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	if srv.Config.Security.TLSEnabled {
		t.Error("expected TLSEnabled to be false by default")
	}
}

func TestBuilderTLSValidationFailsWithoutCert(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Storage.Path = t.TempDir()
	cfg.Security.TLSEnabled = true
	cfg.Security.TLSCertFile = ""
	cfg.Security.TLSKeyFile = ""

	_, err := NewServerBuilder().
		WithConfig(cfg).
		Build()
	if err == nil {
		t.Fatal("expected Build to fail with TLS enabled but no cert/key")
	}
}

func TestBuilderTLSValidationFailsWithNonExistentCA(t *testing.T) {
	certFile, keyFile, _ := generateBuilderTestCerts(t)

	cfg := config.DefaultConfig()
	cfg.Storage.Path = t.TempDir()
	cfg.Security.TLSEnabled = true
	cfg.Security.TLSCertFile = certFile
	cfg.Security.TLSKeyFile = keyFile
	cfg.Security.TLSCAFile = "/nonexistent/ca.pem"

	_, err := NewServerBuilder().
		WithConfig(cfg).
		Build()
	if err == nil {
		t.Fatal("expected Build to fail with non-existent CA file")
	}
}

func TestConfigValidateTLSWithCAFile(t *testing.T) {
	certFile, keyFile, caFile := generateBuilderTestCerts(t)

	cfg := config.DefaultConfig()
	cfg.Security.TLSEnabled = true
	cfg.Security.TLSCertFile = certFile
	cfg.Security.TLSKeyFile = keyFile
	cfg.Security.TLSCAFile = caFile

	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate failed with valid TLS + CA: %v", err)
	}
}

func TestConfigValidateTLSNonExistentCAFile(t *testing.T) {
	certFile, keyFile, _ := generateBuilderTestCerts(t)

	cfg := config.DefaultConfig()
	cfg.Security.TLSEnabled = true
	cfg.Security.TLSCertFile = certFile
	cfg.Security.TLSKeyFile = keyFile
	cfg.Security.TLSCAFile = "/nonexistent/ca.pem"

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected Validate to fail with non-existent CA file")
	}
}
