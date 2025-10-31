# StrangeQ - AMQP 0.9.1 Message Broker

A high-performance AMQP 0.9.1 message broker implementation written in Go. Compatible with RabbitMQ clients and tools.

[![Build Status](https://github.com/maxpert/strangeq/workflows/Build%20and%20Test/badge.svg)](https://github.com/maxpert/strangeq/actions)
[![Go Report Card](https://goreportcard.com/badge/github.com/maxpert/strangeq)](https://goreportcard.com/report/github.com/maxpert/strangeq)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Features

- ✅ Full AMQP 0.9.1 protocol implementation
- ✅ Compatible with RabbitMQ clients (Go, Python, Node.js, etc.)
- ✅ Multiple exchange types (direct, fanout, topic, headers)
- ✅ Persistent and non-persistent messages
- ✅ Multiple storage backends (memory, Badger)
- ✅ SASL authentication (PLAIN, ANONYMOUS)
- ✅ Transaction support
- ✅ High performance (3M+ ops/sec in-memory)
- ✅ Prometheus metrics integration

## Installation

### Pre-built Binaries

Download the latest release for your platform from the [releases page](https://github.com/maxpert/strangeq/releases).

```bash
# macOS/Linux
chmod +x amqp-server-*
sudo mv amqp-server-* /usr/local/bin/amqp-server

# Verify installation
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

## Quick Start

### Start Server

```bash
# In-memory mode (development)
amqp-server

# With persistent storage (production)
amqp-server --storage badger --storage-path ./data

# With configuration file
amqp-server --config config.json
```

### Connect from Python

```python
import pika

connection = pika.BlockingConnection(
    pika.ConnectionParameters('localhost')
)
channel = connection.channel()

# Declare queue
channel.queue_declare(queue='hello')

# Publish message
channel.basic_publish(
    exchange='',
    routing_key='hello',
    body='Hello World!'
)

print(" [x] Sent 'Hello World!'")
connection.close()
```

### Connect from Node.js

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

### Connect from Go

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

See [CLIENT_EXAMPLES.md](CLIENT_EXAMPLES.md) for complete examples including consumers, exchanges, and advanced patterns.

## Configuration

### Command Line Options

```bash
amqp-server [options]

Options:
  --port PORT              Server port (default: 5672)
  --storage BACKEND        Storage backend: memory|badger (default: memory)
  --storage-path PATH      Storage directory (default: ./data)
  --config FILE            Configuration file path
  --auth                   Enable authentication
  --auth-file FILE         Authentication file path
  --log-level LEVEL        Log level: debug|info|warn|error (default: info)
  --log-file FILE          Log file path (default: stdout)
```

### Configuration File

Create `config.json`:

```json
{
  "network": {
    "address": ":5672",
    "max_connections": 1000
  },
  "storage": {
    "backend": "badger",
    "path": "./data",
    "persistent": true
  },
  "security": {
    "authentication_enabled": true,
    "authentication_config": {
      "user_file": "users.json"
    }
  },
  "server": {
    "log_level": "info",
    "max_frame_size": 131072
  }
}
```

Run with config:

```bash
amqp-server --config config.json
```

## Performance

Benchmarks on Apple M4 Max (16 cores):

| Operation | Throughput | Latency |
|-----------|-----------|---------|
| Message Publishing (memory) | 2.9M ops/sec | 344 ns/op |
| Exchange Declaration | 3.2M ops/sec | 309 ns/op |
| Queue Declaration | 2.8M ops/sec | 357 ns/op |
| Message Publishing (persistent) | 15K ops/sec | 66 μs/op |

See [docs/BENCHMARKS.md](docs/BENCHMARKS.md) for detailed performance analysis.

## Production Deployment

### Using systemd

```bash
# Copy service file
sudo cp deployment/systemd/amqp-server.service /etc/systemd/system/

# Enable and start
sudo systemctl enable amqp-server
sudo systemctl start amqp-server

# Check status
sudo systemctl status amqp-server
```

### Using Docker (for testing with RabbitMQ)

The repository includes a docker-compose file for testing compatibility with RabbitMQ:

```bash
cd src/amqp-go/protocol
docker-compose up
```

This is used for protocol testing and interoperability verification only.

## Monitoring

StrangeQ exposes Prometheus metrics on port 9419:

```bash
curl http://localhost:9419/metrics
```

Key metrics:
- `amqp_connections_total` - Total active connections
- `amqp_messages_published_total` - Total messages published
- `amqp_messages_delivered_total` - Total messages delivered
- `amqp_messages_acknowledged_total` - Total message acknowledgments

## Documentation

- [Client Examples](CLIENT_EXAMPLES.md) - Complete examples for Python, Node.js, and Go
- [Configuration Guide](docs/CONFIGURATION.md) - Detailed configuration options
- [Authentication Setup](docs/AUTHENTICATION.md) - Setting up user authentication
- [Development Plan](docs/plan.md) - Project roadmap and implementation status
- [Contributing Guidelines](CONTRIBUTING.md) - How to contribute
- [Security Policy](SECURITY.md) - Security best practices

## Protocol Support

StrangeQ implements AMQP 0.9.1 specification with support for:

- Connection management (negotiation, heartbeat, flow control)
- Channel operations (multiple channels per connection)
- Exchange types (direct, fanout, topic, headers)
- Queue operations (declare, bind, purge, delete)
- Message publishing (persistent and non-persistent)
- Message consumption (Basic.Get, Basic.Consume)
- Message acknowledgment (Basic.Ack, Basic.Nack, Basic.Reject)
- Transactions (Tx.Select, Tx.Commit, Tx.Rollback)

## Compatibility

StrangeQ is compatible with all standard AMQP 0.9.1 clients:

- **Python**: `pika`, `aio-pika`
- **Node.js**: `amqplib`
- **Go**: `amqp091-go`, `streadway/amqp`
- **Java**: Spring AMQP, RabbitMQ Java Client
- **Ruby**: `bunny`
- **.NET**: RabbitMQ .NET Client

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

MIT License - see [LICENSE](LICENSE) for details.

## Support

- Report bugs: [GitHub Issues](https://github.com/maxpert/strangeq/issues)
- Security issues: See [SECURITY.md](SECURITY.md)
