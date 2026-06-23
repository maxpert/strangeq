package server

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"

	"github.com/maxpert/amqp-go/interfaces"
)

// loadTLSConfig builds a *tls.Config from SecurityConfig settings.
//
// AMQP 0.9.1 spec: TLS is an out-of-band transport concern. The AMQP
// protocol runs on top of TLS unchanged — no protocol-level modifications.
//
// Configuration:
//   - TLSCertFile/TLSKeyFile: server certificate and private key (required)
//   - TLSCAFile: CA bundle for mutual TLS client verification (optional)
//
// Security defaults:
//   - MinVersion: TLS 1.2 (TLS 1.0/1.1 deprecated per RFC 8996)
//   - ClientAuth: NoClientCert when TLSCAFile is empty (server-only TLS)
//   - ClientAuth: RequireAndVerifyClientCert when TLSCAFile is set (mutual TLS)
func loadTLSConfig(secCfg interfaces.SecurityConfig) (*tls.Config, error) {
	if secCfg.TLSCertFile == "" {
		return nil, fmt.Errorf("TLS cert file path is empty")
	}
	if secCfg.TLSKeyFile == "" {
		return nil, fmt.Errorf("TLS key file path is empty")
	}

	cert, err := tls.LoadX509KeyPair(secCfg.TLSCertFile, secCfg.TLSKeyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load TLS cert/key pair: %w", err)
	}

	tlsCfg := &tls.Config{
		Certificates:             []tls.Certificate{cert},
		MinVersion:               tls.VersionTLS12,
		PreferServerCipherSuites: true,
	}

	if secCfg.TLSCAFile != "" {
		caCert, err := os.ReadFile(secCfg.TLSCAFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read TLS CA file: %w", err)
		}

		caPool := x509.NewCertPool()
		if !caPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to parse TLS CA certificates from %s", secCfg.TLSCAFile)
		}

		tlsCfg.ClientCAs = caPool
		tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
	}

	return tlsCfg, nil
}

// createTLSListener creates a TLS-wrapped TCP listener on the given address.
func createTLSListener(addr string, cfg *tls.Config) (net.Listener, error) {
	ln, err := tls.Listen("tcp", addr, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create TLS listener: %w", err)
	}
	return ln, nil
}
