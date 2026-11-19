# Quick Start

The examples below use a local broker with authentication disabled. Stop the
broker with Ctrl-C when finished. For a network-facing deployment, configure
[TLS](docs/TLS.md) and [authentication and authorization](docs/AUTHORIZATION.md)
before allowing remote clients.

## Installation

### Pre-built Binary

Download the archive and its `.sha256` file for your OS and architecture.
Release filenames use `darwin` for macOS. Set `archive` to the filename
you downloaded. This example uses Linux on ARM64.
[Release binaries](https://github.com/maxpert/strangeq/releases)

```bash
archive=amqp-server-0.15.0-linux-arm64.tar.gz
sha256sum -c "$archive.sha256" && tar -xzf "$archive" &&
  cd "${archive%.tar.gz}" && ./amqp-server --version
```

On macOS, use `shasum -a 256 -c` in place of `sha256sum -c`. Run the
binary from its directory as `./amqp-server`, or put it on your `PATH`
to use the commands below as written.

### From Source

Install the Go version declared in [go.mod](src/amqp-go/go.mod) or later.
Check out the branch or tag you intend to build before running `go build`.

```bash
git clone https://github.com/maxpert/strangeq.git
cd strangeq/src/amqp-go
go build -o amqp-server ./cmd/amqp-server
sudo mv amqp-server /usr/local/bin/
```

Run the built binary as described above. See the
[clustering guide](docs/CLUSTERING.md#availability) for builds that support
distributed queues.

## Start Server

```bash
# Defaults: persistent storage at ./data, listening on 127.0.0.1:5672,
# authentication off
amqp-server

# Generate example config
amqp-server --generate-config config.yaml

# Run with config file
amqp-server --config config.yaml

# With TLS on the same default port (5672); create certificates first
# See docs/TLS.md for a complete setup on the standard TLS port, 5671.
amqp-server --tls --tls-cert cert.pem --tls-key key.pem

# With telemetry (Prometheus metrics and /health); pprof is off by default
amqp-server --config config.yaml --enable-telemetry
```

### Environment Variables

Environment variables apply with or without `--config`, in the form
`AMQP_<SECTION>_<KEY>` (keys may use underscores). Precedence is
defaults → file → environment → explicitly set flags.

```bash
AMQP_NETWORK_PORT=15672 amqp-server --config config.yaml
AMQP_NETWORK_ADDRESS=0.0.0.0:5672 amqp-server
AMQP_STORAGE_PATH=/var/lib/amqp amqp-server
AMQP_SERVER_LOGLEVEL=debug amqp-server
```

Enabling `security.authenticationenabled` also requires a readable auth file.
see [the authorization guide](docs/AUTHORIZATION.md#auth-file-format).

## Test Connection

### Python

```bash
pip install pika
```

```python
import pika

connection = pika.BlockingConnection(
    pika.ConnectionParameters('localhost')
)
channel = connection.channel()

channel.queue_declare(queue='hello', durable=False)

channel.basic_publish(
    exchange='',
    routing_key='hello',
    body='Hello World!'
)

print(" [x] Sent 'Hello World!'")
connection.close()
```

### Node.js

```bash
npm install amqplib
```

```javascript
const amqp = require('amqplib');

async function main() {
    const connection = await amqp.connect('amqp://localhost');
    const channel = await connection.createConfirmChannel();

    await channel.assertQueue('hello', { durable: false });

    channel.sendToQueue('hello', Buffer.from('Hello World!'));
    await channel.waitForConfirms();
    console.log(" [x] Sent 'Hello World!'");

    await connection.close();
}

main().catch(console.error);
```

### Go

In a new directory, initialize a module and install the client, then save the
example as `main.go` and run `go run .`:

```bash
go mod init example.com/strangeq-hello
go get github.com/rabbitmq/amqp091-go
```

```go
package main

import (
    "log"
    amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
    conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
    if err != nil {
        log.Fatal(err)
    }
    defer conn.Close()

    ch, err := conn.Channel()
    if err != nil {
        log.Fatal(err)
    }
    defer ch.Close()

    q, err := ch.QueueDeclare("hello", false, false, false, false, nil)
    if err != nil {
        log.Fatal(err)
    }

    err = ch.Publish("", q.Name, false, false, amqp.Publishing{
        ContentType: "text/plain",
        Body:        []byte("Hello World!"),
    })
    if err != nil {
        log.Fatal(err)
    }

    log.Println(" [x] Sent 'Hello World!'")
}
```

See [CLIENT_EXAMPLES.md](CLIENT_EXAMPLES.md) for consumers, exchanges, and advanced patterns.

## Configuration

See [config.sample.yaml](src/amqp-go/config.sample.yaml) for the configuration
reference. Paths such as `storage.path` are relative to the working directory
where you start the broker, not the config file location.

```yaml
network:
  address: 127.0.0.1:5672

storage:
  path: ./data

server:
  loglevel: info
```

Omitted keys keep their defaults, so a short file is valid.

## Production Deployment

Follow the [systemd guide](deployment/systemd/README.md) to create the service
account, install configuration and credentials, and start the service. Copying
the unit alone is insufficient. The shipped systemd config enables password
authentication but requires you to add TLS before using it on an untrusted network.

## Troubleshooting

### Port Already in Use

```bash
sudo lsof -i :5672
AMQP_NETWORK_PORT=5673 amqp-server --config config.yaml
```

### Connection Refused

- Check that the broker is running and listening on the host and port used by the client.
- Inspect the terminal or service journal for startup errors. To increase detail,
  set `server.loglevel: debug` and restart the broker.
- Match the client URL scheme to the listener: `amqp://` for plaintext and
  `amqps://` for TLS.
