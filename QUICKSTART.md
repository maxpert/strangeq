# Quick Start

## Installation

### Pre-built Binary

Download from the [releases page](https://github.com/maxpert/strangeq/releases):

```bash
chmod +x amqp-server-*
sudo mv amqp-server-* /usr/local/bin/amqp-server
amqp-server --version
```

### From Source

```bash
git clone https://github.com/maxpert/strangeq.git
cd strangeq/src/amqp-go
go build -o amqp-server ./cmd/amqp-server
sudo mv amqp-server /usr/local/bin/
```

### Go Install

```bash
go install github.com/maxpert/amqp-go/cmd/amqp-server@latest
```

## Start Server

```bash
# Defaults (persistent storage at ./data, port 5672)
amqp-server

# Generate example config
amqp-server --generate-config config.yaml

# Run with config file
amqp-server --config config.yaml

# With TLS
amqp-server --tls --tls-cert cert.pem --tls-key key.pem

# With telemetry (Prometheus + pprof)
amqp-server --config config.yaml --enable-telemetry --telemetry-port 9419
```

### Environment Variables

Override any config setting with `AMQP_<section>_<key>`:

```bash
AMQP_NETWORK_PORT=15672 amqp-server --config config.yaml
AMQP_STORAGE_PATH=/var/lib/amqp amqp-server
AMQP_SERVER_LOGLEVEL=debug amqp-server
```

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

channel.queue_declare(queue='hello')

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
    const channel = await connection.createChannel();

    await channel.assertQueue('hello');

    channel.sendToQueue('hello', Buffer.from('Hello World!'));
    console.log(" [x] Sent 'Hello World!'");

    await connection.close();
}

main().catch(console.error);
```

### Go

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

See `src/amqp-go/config.sample.yaml` for all options with documented defaults.

```yaml
network:
  address: :5672
  port: 5672

storage:
  path: ./data

server:
  loglevel: info
```

## Production Deployment

```bash
sudo cp deployment/systemd/amqp-server.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable amqp-server
sudo systemctl start amqp-server
sudo systemctl status amqp-server
```

## Troubleshooting

### Port Already in Use

```bash
sudo lsof -i :5672
amqp-server --config config.yaml  # change network.port
```

### Connection Refused

- Check server is running: `ps aux | grep amqp-server`
- Check logs: `amqp-server --config config.yaml` (set `server.loglevel: debug`)
