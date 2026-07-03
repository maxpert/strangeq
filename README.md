# StrangeQ - AMQP 0.9.1 Message Broker

A high-performance AMQP 0.9.1 message broker implementation written in Go. Compatible with RabbitMQ clients and tools.

[![Build Status](https://github.com/maxpert/strangeq/workflows/Build%20and%20Test/badge.svg)](https://github.com/maxpert/strangeq/actions)
[![Go Report Card](https://goreportcard.com/badge/github.com/maxpert/strangeq)](https://goreportcard.com/report/github.com/maxpert/strangeq)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Why StrangeQ?

RabbitMQ is the gold standard for AMQP 0.9.1, but it runs on the Erlang VM — a separate runtime with its own scheduling, memory model, and operational overhead. StrangeQ exists for teams that want:

- **Embeddable**: Run it as a library inside your Go application. No separate process, no Erlang runtime, no Docker image required for tests. The benchmark suite itself starts an embedded server in-process.
- **Go-native observability**: Native `net/http/pprof` endpoints for CPU, memory, goroutine, and mutex profiling. Use `go tool pprof` directly — no Erlang tooling required.
- **Single binary deployment**: One static binary with no runtime dependencies. Cross-compile for any Go-supported platform with a single command.
- **AMQP 0.9.1 wire compatibility**: Drop-in replacement for RabbitMQ from any AMQP 0.9.1 client (Go, Python, Node.js, Java, Ruby, C#).
- **Transparent internals**: The entire broker is readable Go source code — no opaque Erlang BEAM abstractions. Debug, profile, and extend with standard Go tooling.

StrangeQ is not a drop-in replacement for RabbitMQ in production. It lacks management UI, clustering, DLX, TTL, priority queues, publisher confirms, and other RabbitMQ features. It is a lightweight, embeddable, high-performance broker for Go applications and development environments.

## Features

- AMQP 0.9.1 protocol compatible with RabbitMQ clients (Go, Python, Node.js, etc.)
- Exchange types: direct, fanout, topic (partial), headers (partial)
- Three-tier storage architecture (Ring Buffer -> WAL -> Segments)
- Persistent and non-persistent messages with crash recovery
- TLS/SSL encryption with mutual TLS support
- SASL authentication (PLAIN, ANONYMOUS)
- Authorization with RabbitMQ-style configure/write/read permissions per vhost
- Transaction protocol handshake (Tx.Select, Tx.Commit, Tx.Rollback)
- 1.07-1.36x faster throughput than RabbitMQ 4.3 on same hardware (see benchmarks below)
- Prometheus metrics and pprof profiling
- Lock-free per-queue AtomicRing with per-slot CAS
- Batch TCP writes and ACK offloading (no fsync head-of-line blocking)

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
# Defaults (in-memory, port 5672)
amqp-server

# Generate example config
amqp-server --generate-config config.yaml

# Run with config file
amqp-server --config config.yaml

# With TLS
amqp-server --tls --tls-cert cert.pem --tls-key key.pem

# With mutual TLS (client cert verification)
amqp-server --tls --tls-cert cert.pem --tls-key key.pem --tls-ca ca.pem

# With telemetry (Prometheus + pprof)
amqp-server --config config.yaml --enable-telemetry --telemetry-port 9419
```

### Environment Variables

Override any configuration setting using `AMQP_<section>_<key>` format:

```bash
AMQP_NETWORK_PORT=15672 amqp-server --config config.yaml
AMQP_STORAGE_PATH=/var/lib/amqp amqp-server
AMQP_SERVER_LOGLEVEL=debug amqp-server
```

### Command Line Options

```
amqp-server [options]

  --config string          Configuration file path (YAML/JSON)
  --generate-config string Generate default config file and exit
  --version                Show version and exit
  --enable-telemetry       Enable Prometheus + pprof endpoint
  --telemetry-port int     Telemetry port (default: 9419)
  --tls                    Enable TLS (amqps)
  --tls-cert string        TLS certificate file (PEM)
  --tls-key string         TLS private key file (PEM)
  --tls-ca string          TLS CA file for mutual TLS client verification (PEM)
```

### Connect from Python

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

See `src/amqp-go/config.sample.yaml` for full configuration with documented defaults.

### Minimal Configuration

```yaml
network:
  address: :5672
  port: 5672

storage:
  path: ./data

server:
  loglevel: info
```

### Production Configuration

```yaml
network:
  address: :5672
  maxconnections: 10000
  tcpkeepalive: true

storage:
  path: /var/lib/amqp

security:
  tlsenabled: true
  tlscertfile: /etc/amqp/cert.pem
  tlskeyfile: /etc/amqp/key.pem
  authenticationenabled: true
  authenticationfilepath: /etc/amqp/auth.json

server:
  loglevel: info
  logfile: /var/log/amqp-server.log
  pidfile: /var/run/amqp-server.pid
  daemonize: true
  memorylimitpercent: 60

engine:
  ringbuffersize: 262144
  walbatchsize: 1000
  walbatchtimeout: 10ms
```

Key engine tuning parameters:
- `ringbuffersize` -- In-memory ring buffer per queue (must be power of 2, default 64K)
- `spillthresholdpercent` -- Ring fill % before spilling to WAL (default 80)
- `walbatchsize` / `walbatchtimeoutms` -- WAL batching for durable writes (1000 msgs / 10ms)
- `segmentsize` -- Cold storage segment file size (default 1 GB)
- `consumermaxbatchsize` -- Max messages to deliver per consumer per poll (default 100)

## Performance

### Head-to-Head vs RabbitMQ 4.3

Same machine, same Go client ([amqp091-go](https://github.com/rabbitmq/amqp091-go)), 12-byte messages, durable queue, transient messages, 1 publisher + 1 consumer. RabbitMQ ran in a Docker container (`rabbitmq:4.3-management`, ARM64) on the same host. StrangeQ ran natively.

| Benchmark | Broker | Median (ns/op) | msg/s | B/op | allocs/op | Ratio |
|-----------|--------|----------------|-------|------|-----------|-------|
| Auto-ack | StrangeQ | 3,463 | 289K | 1,371 | 32 | 1.14x |
| Auto-ack | RabbitMQ | 3,931 | 254K | | | |
| Manual ack | StrangeQ | 3,412 | 293K | 1,451 | 34 | 1.36x |
| Manual ack | RabbitMQ | 4,641 | 215K | | | |
| Multi-ack 1000 | StrangeQ | 3,831 | 261K | 1,418 | 33 | 1.07x |
| Multi-ack 1000 | RabbitMQ | 4,099 | 244K | | | |

RabbitMQ `B/op` and `allocs/op` are not shown because StrangeQ's include both client and embedded server (same process), while RabbitMQ's would include only the Go client — the two are not directly comparable.

### Running the Benchmarks

```bash
cd src/amqp-go

# Benchmark StrangeQ (embedded server starts automatically)
go test . -run='^$' -bench="BenchmarkVersus" -benchmem -benchtime=50000x -count=20

# Benchmark RabbitMQ (must be running on localhost:5672)
docker run -d -p 5672:5672 --name rabbitmq-bench rabbitmq:4.3-management
AMQP_TARGET=rabbitmq go test . -run='^$' -bench="BenchmarkVersus" -benchmem -benchtime=50000x -count=5

# E2E benchmarks (publish-only, roundtrip, multi-queue, multi-connection)
go test . -run='^$' -bench="BenchmarkE2E" -benchmem -benchtime=3s -count=3
```

## Feature Comparison vs RabbitMQ

### AMQP Protocol Methods

| Method | StrangeQ | RabbitMQ |
|--------|----------|----------|
| `connection.start` / `start-ok` | Yes | Yes |
| `connection.tune` / `tune-ok` | Yes | Yes |
| `connection.open` / `open-ok` | Yes | Yes |
| `connection.close` / `close-ok` | Yes | Yes |
| `connection.secure` / `secure-ok` | No | Yes |
| `channel.open` / `open-ok` | Yes | Yes |
| `channel.close` / `close-ok` | Yes | Yes |
| `channel.flow` / `flow-ok` | No | Yes |
| `exchange.declare` / `declare-ok` | Yes | Yes |
| `exchange.delete` / `delete-ok` | Yes | Yes |
| `exchange.bind` / `bind-ok` | No | Yes |
| `exchange.unbind` / `unbind-ok` | No | Yes |
| `queue.declare` / `declare-ok` | Yes | Yes |
| `queue.delete` / `delete-ok` | Yes | Yes |
| `queue.bind` / `bind-ok` | Yes | Yes |
| `queue.unbind` / `unbind-ok` | Yes | Yes |
| `queue.purge` / `purge-ok` | No | Yes |
| `basic.qos` / `qos-ok` | Yes (per-channel; global QoS not enforced) | Yes |
| `basic.consume` / `consume-ok` | Yes | Yes |
| `basic.cancel` / `cancel-ok` | Yes | Yes |
| `basic.publish` | Yes | Yes |
| `basic.ack` | Yes | Yes |
| `basic.nack` | Yes | Yes |
| `basic.reject` | Yes | Yes |
| `basic.get` / `get-ok` / `get-empty` | Partial (always returns `get-empty`) | Yes |
| `basic.deliver` | Yes | Yes |
| `basic.return` | No | Yes |
| `basic.recover` / `recover-ok` | No | Yes |
| `tx.select` / `commit` / `rollback` | Partial (protocol handshake works; publishes not buffered for atomicity) | Yes |
| `confirm.select` / `select-ok` | No | Yes |

### Exchange Types

| Type | StrangeQ | RabbitMQ |
|------|----------|----------|
| `direct` | Yes | Yes |
| `fanout` | Yes | Yes |
| `topic` | Partial (`*` wildcard not implemented; exact match and `#` work) | Yes |
| `headers` | Partial (only `x-match: all`; `x-match: any` not implemented) | Yes |
| `match` | No | Yes (plugin) |

### Security

| Feature | StrangeQ | RabbitMQ |
|---------|----------|----------|
| TLS | Yes | Yes |
| Mutual TLS (mTLS) | Yes | Yes |
| SASL PLAIN | Yes | Yes |
| SASL ANONYMOUS | Yes | Yes |
| SASL EXTERNAL | No | Yes |
| File-based authentication | Yes (bcrypt) | Yes |
| Per-vhost authorization | Yes (RabbitMQ-style regex) | Yes |
| Loopback restriction | Yes | Yes |

### Persistence & Reliability

| Feature | StrangeQ | RabbitMQ |
|---------|----------|----------|
| Durable queues | Yes | Yes |
| Durable exchanges | Yes | Yes |
| Persistent messages (delivery mode 2) | Yes | Yes |
| WAL (write-ahead log) | Yes | Yes |
| Crash recovery | Yes | Yes |
| Bindings persistence | Yes | Yes |

### Special Features

| Feature | StrangeQ | RabbitMQ |
|---------|----------|----------|
| Dead-letter exchange (DLX) | No | Yes |
| Message TTL | No | Yes |
| Queue TTL (`x-expires`) | No | Yes |
| Priority queues (`x-max-priority`) | No | Yes |
| Lazy queues (`x-queue-mode`) | No | Yes |
| Exclusive queues | Partial (flag stored; not enforced on connection close) | Yes |
| Auto-delete queues | Partial (flag stored; last-consumer-leave not enforced) | Yes |
| Passive declare | Partial (bypasses reserved-name check; no NOT_FOUND for missing entities) | Yes |
| Publisher confirms | No | Yes |
| Mandatory / `basic.return` | No | Yes |

### Management & Observability

| Feature | StrangeQ | RabbitMQ |
|---------|----------|----------|
| Prometheus metrics | Yes | Yes (via plugin) |
| pprof profiling | Yes | No (Erlang) |
| Management UI | No | Yes (plugin) |
| CLI management | No | Yes (rabbitmqctl) |

## Architecture

### Storage Engine

- **Ring Buffer**: `[]atomic.Pointer[Message]` with per-slot CAS -- zero allocations on hot path
- **Tag-to-Seq Map**: `map[uint64]uint64` with `sync.RWMutex` -- no per-tag `*uint64` boxing (replaces `sync.Map`)
- **WAL Spill**: When ring buffer fills, messages spill to write-ahead log (batched fsync)
- **WAL Skip**: In-memory transient messages skip WAL/segment ack on delete -- eliminates 1 alloc + 2 channel sends per ack
- **Segment Storage**: 1 GB segments for cold message storage
- **Per-Queue Isolation**: Each queue has its own ring buffer, WAL cursor, and consumer dispatch -- zero cross-queue contention

### Connection Handling

- **Reader/Processor Separation**: TCP reader and frame processor in separate goroutines
- **ACK Offloading**: Per-connection `ackProcessor` goroutine handles all ACK/NACK/reject frames -- eliminates fsync head-of-line blocking on the frame processor
- **Blocking AckQueue**: ACK frames are sent to AckQueue with `conn.Done` fallback -- preserves ordering, prevents frame-processor stalls
- **Batched TCP Writes**: All deliveries in a batch are serialized into a single buffer and sent with one `Write()` call -- reduces syscalls and lock acquisitions
- **Direct-Write Serialization**: Frame bytes written directly into batch buffer via `AppendFrame` / `AppendShortString` -- eliminates intermediate `*Frame`, `*BasicDeliverMethod`, `*ContentHeader` allocations
- **Object Pooling**: `*Frame`, `*BasicPublishMethod`, `*PendingMessage`, `*ContentHeader` pooled via `sync.Pool` -- 6 fewer heap allocs per publish
- **Consumer Dirty Flag**: Delivery loop only re-scans consumers when the consumer set changes -- avoids per-iteration map scan
- **Lock-Free AckCursor**: Per-consumer `sync.Map` + atomic CAS for `minAckCursor` -- no mutex contention between delivery and ack goroutines
- **Flow Control**: RabbitMQ-style memory pressure handling

### Crash Recovery

1. Validate storage integrity
2. Recover durable exchanges and queues
3. Rebuild bindings
4. Load persistent messages directly into ring buffer
5. Restore pending acknowledgments

Recovery is incremental and happens in milliseconds for typical workloads.

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

Enable telemetry to expose Prometheus metrics and pprof profiling:

```bash
amqp-server --config config.yaml --enable-telemetry --telemetry-port 9419
```

### Prometheus Metrics

```bash
curl http://localhost:9419/metrics
```

Available metrics:
- Message throughput (published, consumed, confirmed)
- Connection and channel counts
- Queue depths and message rates
- Memory usage and GC statistics
- WAL and ring buffer utilization

### Profiling

```bash
# CPU profile (30 seconds)
curl -o cpu.prof http://localhost:9419/debug/pprof/profile?seconds=30
go tool pprof -http=:8080 cpu.prof

# Memory profile
curl -o mem.prof http://localhost:9419/debug/pprof/heap
go tool pprof -http=:8080 mem.prof

# Goroutine profile
curl -o goroutine.prof http://localhost:9419/debug/pprof/goroutine
go tool pprof -http=:8080 goroutine.prof

# Mutex contention
curl -o mutex.prof http://localhost:9419/debug/pprof/mutex
go tool pprof -http=:8080 mutex.prof
```

## Development

### Running Tests

```bash
cd src/amqp-go

# Run all tests
go test ./...

# Run with race detector
go test -race ./...

# Run with coverage
go test -cover ./...

# Run benchmarks
go test -bench=. -benchmem -benchtime=5s ./storage
```

### Building

```bash
cd src/amqp-go

# Build server
go build -o amqp-server ./cmd/amqp-server

# Build with optimizations
go build -ldflags="-s -w" -o amqp-server ./cmd/amqp-server

# Cross-compile for Linux
GOOS=linux GOARCH=amd64 go build -o amqp-server-linux ./cmd/amqp-server
```

### Code Structure

```
src/amqp-go/
├── cmd/amqp-server/     # Server entry point
├── config/              # Configuration management
├── protocol/            # AMQP protocol implementation
├── server/              # Server and connection handling
├── storage/             # Storage engine (AtomicRing, WAL, segments)
├── interfaces/          # Interface definitions
├── broker/              # Message routing and delivery
├── auth/                # Authentication & authorization
├── metrics/             # Prometheus metrics
└── benchmark/           # Performance testing tools
```

## Compatibility

Compatible with any AMQP 0.9.1 client library:

- **Go**: [github.com/rabbitmq/amqp091-go](https://github.com/rabbitmq/amqp091-go)
- **Python**: [pika](https://pika.readthedocs.io/), `aio-pika`
- **Node.js**: [amqplib](https://www.npmjs.com/package/amqplib)
- **Java**: [RabbitMQ Java Client](https://www.rabbitmq.com/java-client.html), Spring AMQP
- **Ruby**: `bunny`
- **C#**: [RabbitMQ .NET Client](https://www.rabbitmq.com/dotnet.html)

## Documentation

- [Client Examples](CLIENT_EXAMPLES.md) - Complete examples for Python, Node.js, and Go
- [Authorization Guide](docs/AUTHORIZATION.md) - Permission model and access control
- [TLS Configuration](docs/TLS.md) - TLS encryption and mutual TLS setup
- [Contributing Guidelines](CONTRIBUTING.md) - How to contribute
- [Security Policy](SECURITY.md) - Security best practices

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

MIT License - see [LICENSE](LICENSE) for details.

## Support

- Report bugs: [GitHub Issues](https://github.com/maxpert/strangeq/issues)
- Security issues: See [SECURITY.md](SECURITY.md)
