# TLS/SSL Configuration

AMQP-Go supports TLS encryption for secure client connections (amqps). TLS wraps the raw TCP connection — the AMQP 0.9.1 protocol runs on top unchanged.

## Quick Start

### 1. Generate certificates

```bash
# Generate a self-signed certificate for development
openssl req -x509 -newkey rsa:4096 -keyout key.pem -out cert.pem \
  -days 365 -nodes -subj "/CN=localhost" \
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"
```

### 2. Configure the server

**Via config file:**

```yaml
security:
  tlsenabled: true
  tlscertfile: "/path/to/cert.pem"
  tlskeyfile: "/path/to/key.pem"
```

**Via CLI flags:**

```bash
amqp-server --tls --tls-cert /path/to/cert.pem --tls-key /path/to/key.pem
```

### 3. Connect with a client

```go
import amqp "github.com/rabbitmq/amqp091-go"

conn, err := amqp.DialTLS("amqps://localhost:5671/", &tls.Config{
    RootCAs:    caCertPool,     // pool containing your CA cert
    ServerName: "localhost",
    MinVersion: tls.VersionTLS12,
})
```

## Configuration Reference

| Setting | Config Key | CLI Flag | Default | Description |
|---------|-----------|----------|---------|-------------|
| Enable TLS | `security.tlsenabled` | `--tls` | `false` | Enable TLS listener |
| Certificate | `security.tlscertfile` | `--tls-cert` | `""` | Server certificate (PEM) |
| Private Key | `security.tlskeyfile` | `--tls-key` | `""` | Server private key (PEM) |
| CA File | `security.tlscafile` | `--tls-ca` | `""` | CA bundle for mutual TLS (PEM) |

### Security Defaults

- **Minimum TLS version**: TLS 1.2 (TLS 1.0/1.1 deprecated per RFC 8996)
- **Server cipher preference**: `PreferServerCipherSuites: true` — server selects the most secure mutually-supported cipher
- **Client authentication**: 
  - Without CA file: `NoClientCert` (server-only TLS)
  - With CA file: `RequireAndVerifyClientCert` (mutual TLS)

## Mutual TLS (Client Certificate Verification)

Mutual TLS requires clients to present a certificate signed by the server's CA. This provides an additional layer of authentication beyond SASL.

### Server Configuration

```yaml
security:
  tlsenabled: true
  tlscertfile: "/path/to/server-cert.pem"
  tlskeyfile: "/path/to/server-key.pem"
  tlscafile: "/path/to/ca.pem"    # Setting this enables mutual TLS
```

Or via CLI:

```bash
amqp-server --tls --tls-cert server-cert.pem --tls-key server-key.pem --tls-ca ca.pem
```

### Client Configuration

```go
clientCert, _ := tls.LoadX509KeyPair("client-cert.pem", "client-key.pem")

conn, err := amqp.DialTLS("amqps://localhost:5671/", &tls.Config{
    RootCAs:      caCertPool,
    ServerName:   "localhost",
    MinVersion:   tls.VersionTLS12,
    Certificates: []tls.Certificate{clientCert},
})
```

Without a client certificate, the server rejects the TLS handshake. The
client sees a TLS-level error (not an AMQP protocol error):

```
tls: certificate required
```

## TLS + Authentication

TLS and SASL authentication can be used simultaneously. TLS encrypts the connection while SASL authenticates the user:

```bash
amqp-server --tls --tls-cert cert.pem --tls-key key.pem \
  --auth-enabled --auth-file auth.json
```

Connect with credentials over TLS:

```go
conn, err := amqp.DialTLS("amqps://user:password@localhost:5671/", tlsConfig)
```

## Certificate Generation

### Self-Signed (Development)

```bash
# Generate CA
openssl genrsa -out ca-key.pem 4096
openssl req -x509 -new -key ca-key.pem -out ca.pem -days 365 \
  -subj "/CN=AMQP-Go Test CA"

# Generate server certificate signed by CA
openssl genrsa -out server-key.pem 2048
openssl req -new -key server-key.pem -out server.csr \
  -subj "/CN=localhost"
openssl x509 -req -in server.csr -CA ca.pem -CAkey ca-key.pem \
  -CAcreateserial -out server-cert.pem -days 365 \
  -extfile <(echo "subjectAltName=DNS:localhost,IP:127.0.0.1")

# Generate client certificate (for mutual TLS)
openssl genrsa -out client-key.pem 2048
openssl req -new -key client-key.pem -out client.csr \
  -subj "/CN=amqp-client"
openssl x509 -req -in client.csr -CA ca.pem -CAkey ca-key.pem \
  -CAcreateserial -out client-cert.pem -days 365
```

### Production (Let's Encrypt or Internal CA)

For production, use certificates from a trusted Certificate Authority:

- **Public**: Use Let's Encrypt or a commercial CA
- **Internal**: Use an internal PKI (HashiCorp Vault, cfssl, or OpenSSL CA)
- **Key type**: ECDSA P-256 recommended (smaller keys, faster handshakes)
- **Key size**: RSA 2048 minimum, RSA 4096 or ECDSA P-256 recommended

## Performance Considerations

- **TLS handshake**: Adds ~1-2ms per connection. AMQP connections are long-lived, so this is amortized over the connection lifetime.
- **No per-message overhead**: TLS session is persistent; encryption happens in the kernel/network stack.
- **Certificate type**: ECDSA certificates have smaller signatures and faster verification than RSA.
- **Session resumption**: Go's TLS stack supports session tickets (TLS 1.3) and session IDs (TLS 1.2) for faster reconnection.

## Port Configuration

| Mode | Standard Port | URL Scheme |
|------|--------------|------------|
| Plain TCP | 5672 | `amqp://` |
| TLS | 5671 | `amqps://` |

To use the standard TLS port:

```yaml
network:
  address: ":5671"
  port: 5671
security:
  tlsenabled: true
  tlscertfile: "/path/to/cert.pem"
  tlskeyfile: "/path/to/key.pem"
```
