# TLS/SSL Configuration

StrangeQ supports TLS for AMQP client connections (`amqps://`). TLS replaces
the plaintext listener on the configured address. It does not create a second
listener or change the port automatically.

## Quick Start

This development example uses a self-signed certificate and the standard TLS
port, 5671. It enables transport encryption. SASL authentication remains a
separate setting.

### 1. Generate certificates

Use OpenSSL with `-addext` support:

```bash
umask 077
openssl req -x509 -newkey rsa:4096 -keyout key.pem -out cert.pem \
  -days 365 -nodes -subj "/CN=localhost" \
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"
```

### 2. Configure the server

Save this as `config.yaml`, then run `amqp-server --config config.yaml`:

```yaml
network:
  address: "127.0.0.1:5671"
security:
  tlsenabled: true
  tlscertfile: cert.pem
  tlskeyfile: key.pem
```

Paths are relative to the broker's working directory. The equivalent command is:

```bash
AMQP_NETWORK_PORT=5671 amqp-server --tls --tls-cert cert.pem --tls-key key.pem
```

### 3. Connect with a client

In a Go module with `github.com/rabbitmq/amqp091-go` installed, save this as
`main.go` and run `go run .` from the directory containing `cert.pem`:

```go
package main

import (
    "crypto/tls"
    "crypto/x509"
    "log"
    "os"

    amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
    pem, err := os.ReadFile("cert.pem")
    if err != nil {
        log.Fatal(err)
    }
    roots := x509.NewCertPool()
    if !roots.AppendCertsFromPEM(pem) {
        log.Fatal("cert.pem contains no valid certificates")
    }
    conn, err := amqp.DialTLS("amqps://localhost:5671/", &tls.Config{
        RootCAs:    roots,
        ServerName: "localhost",
        MinVersion: tls.VersionTLS12,
    })
    if err != nil {
        log.Fatal(err)
    }
    defer conn.Close()
    log.Println("Connected over TLS")
}
```

For a CA-issued server certificate, load the issuing CA bundle instead, or use
the system trust store. Keep hostname verification enabled.

## Configuration Reference

| Setting | Config Key | CLI Flag | Default | Description |
|---------|-----------|----------|---------|-------------|
| Enable TLS | `security.tlsenabled` | `--tls` | `false` | Wrap the AMQP listener in TLS |
| Certificate | `security.tlscertfile` | `--tls-cert` | `""` | Server certificate chain (PEM) |
| Private key | `security.tlskeyfile` | `--tls-key` | `""` | Server private key (PEM) |
| CA file | `security.tlscafile` | `--tls-ca` | `""` | Trusted client CAs; enables mutual TLS |

The certificate and key are required when TLS is enabled. The service account
must be able to read them. Restrict private-key permissions to that account or
its service group. Restart the broker to load changed certificates or TLS settings.

### Security Defaults

- Minimum TLS version: TLS 1.2.
- Cipher selection: Go's `crypto/tls` defaults.
- Without a CA file: clients do not need a certificate (`NoClientCert`).
- With a CA file: clients must present a certificate chaining to a trusted client
  CA (`RequireAndVerifyClientCert`).

## Mutual TLS (Client Certificate Verification)

Mutual TLS verifies the client certificate during the TLS handshake. It does not
map the certificate subject to a StrangeQ user. SASL EXTERNAL is not supported.
Configure SASL and authorization separately when you need broker user permissions.

### Server Configuration

```yaml
network:
  address: "127.0.0.1:5671"
security:
  tlsenabled: true
  tlscertfile: server-cert.pem
  tlskeyfile: server-key.pem
  tlscafile: ca.pem
```

Or use:

```bash
AMQP_NETWORK_PORT=5671 amqp-server --tls \
  --tls-cert server-cert.pem --tls-key server-key.pem --tls-ca ca.pem
```

### Client Configuration

In the Go example above, load `ca.pem` into the root pool and add a client
certificate before dialing:

```go
clientCert, err := tls.LoadX509KeyPair("client-cert.pem", "client-key.pem")
if err != nil {
    log.Fatal(err)
}
tlsConfig := &tls.Config{
    RootCAs:      roots,
    ServerName:   "localhost",
    MinVersion:   tls.VersionTLS12,
    Certificates: []tls.Certificate{clientCert},
}
conn, err := amqp.DialTLS("amqps://localhost:5671/", tlsConfig)
```

A missing or untrusted client certificate fails at the TLS layer before AMQP
user authentication. The exact client error depends on the TLS version and client.

## TLS + Authentication

Add these settings to the TLS server configuration:

```yaml
security:
  tlsenabled: true
  tlscertfile: server-cert.pem
  tlskeyfile: server-key.pem
  authenticationenabled: true
  authenticationfilepath: auth.json
  authorizationenabled: true
  authmechanisms: [PLAIN]
```

Keep the `network` section selecting port 5671. Create `auth.json` with bcrypt
hashes and vhost permissions as described in [Authorization](AUTHORIZATION.md).
Start the broker with `amqp-server --config config.yaml`. A client can then use
`amqps://user:password@localhost:5671/` with the same verified TLS configuration.
Percent-encode special characters in URL credentials.

## Certificate Generation

### Self-Signed (Development)

For a local CA and separate server/client certificates, run the following in
Bash or Zsh. The process-substitution syntax (`<(...)`) is not POSIX `sh` syntax.

```bash
umask 077

# CA certificate and private key
openssl genrsa -out ca-key.pem 4096
openssl req -x509 -new -key ca-key.pem -out ca.pem -days 365 \
  -subj "/CN=StrangeQ Test CA" \
  -addext "basicConstraints=critical,CA:TRUE" \
  -addext "keyUsage=critical,keyCertSign,cRLSign"

# Server certificate, including the hostname clients verify
openssl genrsa -out server-key.pem 2048
openssl req -new -key server-key.pem -out server.csr -subj "/CN=localhost"
openssl x509 -req -in server.csr -CA ca.pem -CAkey ca-key.pem \
  -CAcreateserial -out server-cert.pem -days 365 \
  -extfile <(printf '%s\n' 'subjectAltName=DNS:localhost,IP:127.0.0.1' \
    'extendedKeyUsage=serverAuth')

# Client certificate for mutual TLS
openssl genrsa -out client-key.pem 2048
openssl req -new -key client-key.pem -out client.csr -subj "/CN=amqp-client"
openssl x509 -req -in client.csr -CA ca.pem -CAkey ca-key.pem \
  -CAcreateserial -out client-cert.pem -days 365 \
  -extfile <(printf '%s\n' 'extendedKeyUsage=clientAuth')
```

### Production (Let's Encrypt or Internal CA)

Use a certificate from your public or internal CA, with subject alternative names
matching the hostname clients use. Install the certificate chain and private key,
configure client trust, and plan certificate renewal with a broker restart. Keep
the CA signing key separate from the broker and client hosts.

## Performance Considerations

TLS adds handshake and record-encryption work. Reuse long-lived AMQP connections
and measure throughput and connection setup in your deployment. The broker uses
Go's `crypto/tls` in user space. There is no kernel TLS offload on this path.
Session resumption depends on the client configuration and TLS session state.

## Port Configuration

| Mode | Conventional Port | URL Scheme |
|------|-------------------|------------|
| Plain TCP | 5672 | `amqp://` |
| TLS | 5671 | `amqps://` |

StrangeQ defaults to `127.0.0.1:5672` in either mode. Set `network.address` or
`network.port` explicitly to use 5671, as in the examples above. If both fields
specify a port, they must agree. To permit remote clients, replace loopback with
the intended interface and configure firewall and host-access rules.
