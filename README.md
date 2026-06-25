# StrangeQ - AMQP 0.9.1 Message Broker

A high-performance AMQP 0.9.1 message broker implementation written in Go. Compatible with RabbitMQ clients and tools.

[![Build Status](https://github.com/maxpert/strangeq/workflows/Build%20and%20Test/badge.svg)](https://github.com/maxpert/strangeq/actions)
[![Go Report Card](https://goreportcard.com/badge/github.com/maxpert/strangeq)](https://goreportcard.com/report/github.com/maxpert/strangeq)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Features

- ✅ Full AMQP 0.9.1 protocol implementation
- ✅ Compatible with RabbitMQ clients (Go, Python, Node.js, etc.)
- ✅ Multiple exchange types (direct, fanout, topic, headers)
- ✅ Three-tier storage architecture (Ring Buffer → WAL → Segments)
- ✅ Persistent and non-persistent messages with crash recovery
- ✅ TLS/SSL encryption with mutual TLS support
- ✅ SASL authentication (PLAIN, ANONYMOUS)
- ✅ Authorization with RabbitMQ-style configure/write/read permissions per vhost
- ✅ Transaction support (Tx.Select, Tx.Commit, Tx.Rollback)
- ✅ High performance (3M+ ops/sec in-memory, optimized durable path)
- ✅ Prometheus metrics integration
- ✅ Configurable engine tuning (ring buffer size, WAL batch size, spill thresholds)
- ✅ Lock-free hot paths (sync.Map, atomic counters, channel-based fan-in)
- ✅ Linux fdatasync for 10-20% faster WAL fsync

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

# With TLS encryption
amqp-server --tls --tls-cert cert.pem --tls-key key.pem

# With mutual TLS (client cert verification)
amqp-server --tls --tls-cert cert.pem --tls-key key.pem --tls-ca ca.pem

# With authentication and authorization
amqp-server --auth --auth-file auth.json --config config.json

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
  --tls                    Enable TLS encryption
  --tls-cert FILE          TLS certificate file path
  --tls-key FILE           TLS private key file path
  --tls-ca FILE            TLS CA certificate file path (enables mutual TLS)
```

### Configuration File

Create `config.json`:

```json
{
  "network": {
    "address": ":5672",
    "max_connections": 1000,
    "heartbeat_interval_ms": 60000,
    "tcp_keepalive": true
  },
  "storage": {
    "path": "./data",
    "checkpoint_interval_ms": 5000
  },
  "security": {
    "tls_enabled": false,
    "authentication_enabled": false,
    "authorization_enabled": false,
    "default_vhost": "/",
    "auth_mechanisms": ["PLAIN"]
  },
  "server": {
    "log_level": "info",
    "max_frame_size": 131072,
    "max_channels_per_connection": 2047,
    "memory_limit_percent": 60
  },
  "engine": {
    "ring_buffer_size": 65536,
    "spill_threshold_percent": 80,
    "wal_batch_size": 1000,
    "wal_batch_timeout_ms": 5,
    "segment_size": 268435456,
    "consumer_select_timeout_ms": 1,
    "consumer_max_batch_size": 100
  }
}
```

Key engine tuning parameters:
- `ring_buffer_size` — In-memory ring buffer per queue (must be power of 2, default 64K)
- `spill_threshold_percent` — Ring fill % before spilling to WAL (default 80)
- `wal_batch_size` / `wal_batch_timeout_ms` — WAL batching for durable writes (1000 msgs / 5ms)
- `segment_size` — Cold storage segment file size (default 256MB)
- `consumer_select_timeout_ms` — Consumer delivery poll interval (default 1ms)

Run with config:

```bash
amqp-server --config config.json
```

## Performance

Benchmarks on Apple M4 Max (16 cores):

| Operation | Throughput | Latency |
|-----------|-----------|---------|
| Message Publishing (memory) | 2.9M ops/sec | 344 ns/op |
| Multi-queue concurrent publish | 942K ops/sec | 1,062 ns/op |
| Exchange Declaration | 3.2M ops/sec | 309 ns/op |
| Queue Declaration | 2.8M ops/sec | 357 ns/op |
| Message Publishing (persistent) | 15K ops/sec | 66 μs/op |
| WAL read (indexed, with cache) | 89K ops/sec | 11,233 ns/op |
| E2E large message (64KB) | 62K ops/sec | 16,195 ns/op |

### Phase 16 Performance Optimizations

- **Lock-free queue rings** — `sync.Map` for queue lookup, no mutex on hot path
- **Fan-in consumer delivery** — Replaced `reflect.Select` with per-consumer forwarding goroutines → O(1) channel select
- **Batch WAL writes** — Single fsync per batch (fdatasync on Linux for 10-20% speedup)
- **Batch segment checkpoint** — Single mutex + single `file.Write()` per batch
- **Batch segment ACKs** — Channel-batched bitmap updates reduce lock contention
- **Pooled allocations** — `sync.Pool` for WAL done channels, frame buffers; `AppendUint*` for zero-temp serialization
- **Configurable ring buffer** — Size and spill threshold wired from EngineConfig (was hardcoded)
- **WAL file handle cache** — Eliminates open/close syscalls per WAL read; pread for positional I/O

See [docs/BENCHMARKS.md](docs/BENCHMARKS.md) and [PERFORMANCE_PLAN.md](PERFORMANCE_PLAN.md) for detailed performance analysis.

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
- [Authorization Guide](docs/AUTHORIZATION.md) - Permission model and access control
- [TLS Configuration](docs/TLS.md) - TLS encryption and mutual TLS setup
- [Development Plan](docs/plan.md) - Project roadmap and implementation status
- [Performance Plan](PERFORMANCE_PLAN.md) - Phase 16 optimization details and benchmarks
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
- TLS/SSL encryption with mutual TLS authentication
- SASL authentication (PLAIN, ANONYMOUS)
- Authorization with per-vhost configure/write/read permissions

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
