# StrangeQ — AMQP 0.9.1 Message Broker

A high-performance AMQP 0.9.1 message broker written in Go. Compatible with RabbitMQ clients and tools.

[![Build Status](https://github.com/maxpert/strangeq/workflows/Build%20and%20Test/badge.svg)](https://github.com/maxpert/strangeq/actions)
[![Go Report Card](https://goreportcard.com/badge/github.com/maxpert/strangeq)](https://goreportcard.com/report/github.com/maxpert/strangeq)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Why StrangeQ?

RabbitMQ is the gold standard for AMQP 0.9.1, but it runs on the Erlang VM — a separate runtime with its own scheduling, memory model, and operational overhead. StrangeQ exists for teams that want:

- **Embeddable**: Run it as a library inside your Go application. No separate process, no Erlang runtime, no Docker image required for tests.
- **Go-native observability**: Native `net/http/pprof` endpoints for CPU, memory, goroutine, and mutex profiling. Use `go tool pprof` directly — no Erlang tooling required.
- **Single binary deployment**: One static binary with no runtime dependencies. Cross-compile for any Go-supported platform with a single command.
- **AMQP 0.9.1 wire compatibility**: Drop-in replacement for RabbitMQ from any AMQP 0.9.1 client (Go, Python, Node.js, Java, Ruby, C#).
- **Transparent internals**: The entire broker is readable Go source code. Debug, profile, and extend with standard Go tooling.

## Features

- AMQP 0.9.1 protocol compatible with RabbitMQ clients (Go, Python, Node.js, Java, Ruby, C#)
- Exchange types: direct, fanout, topic (with `*` and `#` wildcards), headers (with `x-match: all` and `any`)
- Exchange-to-exchange bindings with cycle detection
- Three-tier storage architecture (ring buffer → WAL → segments)
- Persistent and non-persistent messages with crash recovery
- TLS/SSL encryption with mutual TLS support
- SASL authentication (PLAIN, ANONYMOUS)
- Authorization with RabbitMQ-style configure/write/read permissions per vhost
- Publisher confirms (`confirm.select`)
- Transactions (`tx.select` / `tx.commit` / `tx.rollback`) with publish and ack buffering
- `basic.get` with synchronous message retrieval and ack tracking
- `basic.return` for unroutable mandatory messages
- `basic.recover` for redelivering unacknowledged messages
- `queue.purge` for removing all messages from a queue
- `channel.flow` / `channel.flow-ok` for start/stop of content frames
- Message & queue TTL — `x-message-ttl`, per-message `expiration`, and `x-expires` idle-queue expiry
- Dead-letter exchanges — `x-dead-letter-exchange` / `x-dead-letter-routing-key` with full `x-death` / `x-first-death-*` headers
- Queue length limits — `x-max-length` / `x-max-length-bytes` with `x-overflow` (`drop-head`, `reject-publish`)
- Resource alarms — `connection.blocked` / `connection.unblocked` driven by memory (RSS) and free-disk watermarks
- Exclusive and auto-delete queues with automatic cleanup (exclusive queues deleted on owning-connection close; auto-delete queues deleted when the last consumer leaves)
- Measured ahead of RabbitMQ 4.3 on durable end-to-end throughput across every tested workload — from 1.1x (64 KB bodies) to 3.7x (durable + publisher confirms) in a Docker ARM64-native head-to-head (see [Performance](#performance))
- Prometheus metrics and pprof profiling
- Lock-free per-queue ring buffer with per-slot CAS
- Batch TCP writes and ACK offloading

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
- `ringbuffersize` — In-memory ring buffer per queue (must be power of 2, default 64K)
- `spillthresholdpercent` — Ring fill % before spilling to WAL (default 80)
- `walbatchsize` / `walbatchtimeoutms` — WAL batching for durable writes (1000 msgs / 10ms)
- `segmentsize` — Cold storage segment file size (default 1 GB)
- `consumermaxbatchsize` — Max messages to deliver per consumer per poll (default 100)

## Performance

All numbers below are **end-to-end consumed msg/s** — every published message is also delivered and acknowledged, so the figures reflect full round-trip delivery, not publish acceptance. Both brokers are driven by the same client ([`cmd/perftest`](src/amqp-go/cmd/perftest), built on [amqp091-go](https://github.com/rabbitmq/amqp091-go)) under identical settings.

**Methodology**
- Single machine: Apple Silicon, 16 cores.
- Both brokers run in [OrbStack](https://orbstack.dev/) **ARM64-native containers** (not emulated), with **in-container storage** (no host bind mount) so disk behavior is comparable.
- RabbitMQ 4.3 vs StrangeQ, 20-second runs, 1 KB message bodies unless noted, consumer prefetch 100, manual acks.
- **Durable queues** (see the caveat below). Measured 2026-07-10.

### Head-to-Head vs RabbitMQ 4.3 (durable)

> **Why durable-only?** RabbitMQ 4.3 rejects transient (non-durable) non-exclusive queues by default (`541 transient_nonexcl_queues is deprecated`). The only apples-to-apples comparison is therefore on durable queues, and that is what is reported here.

| Workload (durable, 1 KB body unless noted) | RabbitMQ 4.3 | StrangeQ | StrangeQ advantage |
|---|--:|--:|--:|
| Durable + publisher confirms, 10 pub / 10 con | 28,616 msg/s | 104,986 msg/s | 3.67x |
| Durable, 1 pub / 1 con | 85,353 msg/s | 127,711 msg/s | 1.50x |
| Durable, 30 pub / 30 con | ~31,000 msg/s | ~109,000 msg/s | ~3.5x |
| Durable, 64 KB body, 1 pub / 1 con | 22,027 msg/s | 25,177 msg/s | 1.14x |

The 64 KB figure is a median of 3 runs (individual runs ranged from 0.99x to 1.15x). All other figures are steady-state medians of a 20-second run.

### Crash safety

Confirmed durable messages survive `kill -9` and are recovered on restart. The invariant is **confirm ⇒ fsynced ⇒ recoverable**: a publish is confirmed only after its WAL batch is fsynced, so every confirmed message is present after a crash.

### Transient (non-durable) throughput — reported standalone

StrangeQ also serves transient queues at high throughput. Because RabbitMQ 4.3 refuses transient non-exclusive queues, this is **not** a head-to-head figure and is reported on its own: roughly **140K msg/s** for 1 pub / 1 con, 1 KB bodies, host-native (no container).

### Reproducing

```bash
cd src/amqp-go

# Durable + publisher confirms, 10 producers / 10 consumers, 1 KB, 20s
go run ./cmd/perftest -url amqp://guest:guest@localhost:5672/ \
  -durable -confirm -producers 10 -consumers 10 -size 1024 -duration 20s

# Durable, single producer / single consumer
go run ./cmd/perftest -url amqp://guest:guest@localhost:5672/ \
  -durable -producers 1 -consumers 1 -size 1024 -duration 20s

# Durable, 64 KB bodies
go run ./cmd/perftest -url amqp://guest:guest@localhost:5672/ \
  -durable -producers 1 -consumers 1 -size 65536 -duration 20s
```

Point `-url` at either broker (both speak AMQP 0.9.1). To run RabbitMQ 4.3 the same way:

```bash
docker run -d --name rabbitmq-bench -p 5672:5672 rabbitmq:4.3-management
```

A Go microbenchmark (`BenchmarkVersus`) also exists for allocation/latency profiling of the hot path.

## Feature Comparison vs RabbitMQ

### AMQP Protocol Methods

| Method | StrangeQ | RabbitMQ |
|--------|----------|----------|
| `connection.start` / `start-ok` | Yes | Yes |
| `connection.tune` / `tune-ok` | Yes | Yes |
| `connection.open` / `open-ok` | Yes | Yes |
| `connection.close` / `close-ok` | Yes | Yes |
| `connection.blocked` / `unblocked` | Yes (memory/disk alarms) | Yes |
| `connection.secure` / `secure-ok` | No | Yes |
| `channel.open` / `open-ok` | Yes | Yes |
| `channel.close` / `close-ok` | Yes | Yes |
| `channel.flow` / `flow-ok` | Yes | Yes |
| `exchange.declare` / `declare-ok` | Yes | Yes |
| `exchange.delete` / `delete-ok` | Yes | Yes |
| `exchange.bind` / `bind-ok` | Yes | Yes |
| `exchange.unbind` / `unbind-ok` | Yes | Yes |
| `queue.declare` / `declare-ok` | Yes | Yes |
| `queue.delete` / `delete-ok` | Yes | Yes |
| `queue.bind` / `bind-ok` | Yes | Yes |
| `queue.unbind` / `unbind-ok` | Yes | Yes |
| `queue.purge` / `purge-ok` | Yes | Yes |
| `basic.qos` / `qos-ok` | Yes (per-channel; global QoS not enforced) | Yes |
| `basic.consume` / `consume-ok` | Yes | Yes |
| `basic.cancel` / `cancel-ok` | Yes | Yes |
| `basic.publish` | Yes | Yes |
| `basic.ack` | Yes | Yes |
| `basic.nack` | Yes | Yes |
| `basic.reject` | Yes | Yes |
| `basic.get` / `get-ok` / `get-empty` | Yes | Yes |
| `basic.deliver` | Yes | Yes |
| `basic.return` | Yes | Yes |
| `basic.recover` / `recover-ok` | Yes | Yes |
| `tx.select` / `commit` / `rollback` | Yes | Yes |
| `confirm.select` / `select-ok` | Yes | Yes |

### Exchange Types

| Type | StrangeQ | RabbitMQ |
|------|----------|----------|
| `direct` | Yes | Yes |
| `fanout` | Yes | Yes |
| `topic` | Yes (`*` single-word, `#` zero-or-more words) | Yes |
| `headers` | Yes (`x-match: all` and `any`) | Yes |

### Security

| Feature | StrangeQ | RabbitMQ |
|---------|----------|----------|
| TLS | Yes | Yes |
| Mutual TLS (mTLS) | Yes | Yes |
| SASL PLAIN | Yes | Yes |
| SASL ANONYMOUS | Yes (opt-in; disabled by default) | Yes |
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

### RabbitMQ Extensions (queue `x-arguments`)

| Extension | StrangeQ | Notes |
|-----------|----------|-------|
| `x-message-ttl` (per-queue) | Yes | Message expires after N ms in the queue |
| Per-message `expiration` | Yes | Effective TTL = min(per-message, per-queue) |
| `x-expires` (queue TTL) | Yes | Idle queue auto-deletes after N ms with no consumers/get/redeclare |
| `x-dead-letter-exchange` | Yes | Rejected / expired / evicted messages re-routed through the normal router |
| `x-dead-letter-routing-key` | Yes | Overrides the routing key on dead-letter (else the original key is kept) |
| `x-death` / `x-first-death-*` headers | Yes | Full array-of-tables with `count`/`reason`/`queue`/`time`/`exchange`/`routing-keys` aggregation |
| `x-max-length` | Yes | Cap on ready message count |
| `x-max-length-bytes` | Yes | Cap on ready message bytes |
| `x-overflow` | Yes | `drop-head` (evict oldest → DLX) or `reject-publish` (nack/return/drop the incoming message) |
| `x-last-death-*` headers | No | Deferred; only `x-first-death-*` + the `x-death` array are emitted |
| `x-max-priority` (priority queues) | No | See Not Yet Supported |

Dead-letter `reason` values match RabbitMQ: `rejected` (nack/reject with requeue=false), `expired` (TTL), `maxlen` (drop-head eviction). Cycle detection matches RabbitMQ too — a `rejected` loop is not broken (it cycles), while `expired`/`maxlen` are dropped on a queue-name match.

### Behavioral Divergences from RabbitMQ

StrangeQ is wire-compatible, but a few observable behaviors intentionally differ. All are verified against RabbitMQ 4.3 by the conformance suite (`make conformance-rabbitmq`):

- **`mandatory` + full `reject-publish` queue**: StrangeQ returns the message via `basic.return` (reply code 312); RabbitMQ does not return a routable-but-full message.
- **`x-message-ttl: 0` / `expiration: 0` with no ready consumer**: the message is dropped. StrangeQ has no publish-time direct hand-off to a waiting consumer, so a zero-TTL message cannot be delivered-before-expiry.
- **Malformed / non-numeric per-message `expiration`**: treated as "no TTL" (lenient) rather than a channel error.
- **`reject-publish` inside a transaction**: not enforced (a tx-published message to a full reject-publish queue is accepted); `drop-head` inside a transaction *is* enforced.

### Not Yet Supported

The following RabbitMQ features are not implemented:

- Clustering and high availability
- Priority queues (`x-max-priority`)
- Lazy queues (`x-queue-mode`)
- Consumer priorities (`x-priority`)
- `connection.secure` / `secure-ok` (multi-step challenge-response auth)
- SASL EXTERNAL
- Global prefetch (`basic.qos` with `global=true`) — accepted but applied per-consumer, not enforced globally
- Prefetch size (`basic.qos` with `prefetch_size > 0`) — rejected with a `540 not implemented` channel error rather than silently ignored
- Management UI and CLI management tools

## Architecture

### Storage Engine

- **Ring Buffer**: Per-queue lock-free ring with per-slot CAS — zero allocations on the hot path
- **WAL Spill**: When the ring buffer fills, messages spill to a write-ahead log with batched fsync
- **Segment Storage**: Cold message storage in segment files
- **Per-Queue Isolation**: Each queue has its own ring buffer, WAL cursor, and consumer dispatch — no cross-queue contention

### Connection Handling

- **Reader/Processor Separation**: TCP reader and frame processor run in separate goroutines
- **ACK Offloading**: A dedicated goroutine handles ACK/NACK/reject frames — eliminates head-of-line blocking on the frame processor
- **Batched TCP Writes**: Deliveries in a batch are serialized into a single buffer and sent with one write — reduces syscalls and lock acquisitions
- **Direct-Write Serialization**: Frame bytes are written directly into the batch buffer — eliminates intermediate object allocations
- **Object Pooling**: Frames, publish methods, pending messages, and content headers are pooled via `sync.Pool`
- **Flow Control**: `channel.flow` gates delivery forwarding with a single atomic load — zero overhead when flow is active

### Crash Recovery

1. Validate storage integrity
2. Recover durable exchanges and queues
3. Rebuild bindings
4. Load persistent messages into ring buffers
5. Restore pending acknowledgments

Recovery is incremental and completes in milliseconds for typical workloads.

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

### Using Docker

The repository includes a docker-compose file for protocol interoperability testing with RabbitMQ:

```bash
cd src/amqp-go/protocol
docker-compose up
```

## Monitoring

Enable telemetry to expose Prometheus metrics and pprof profiling:

```bash
amqp-server --config config.yaml --enable-telemetry --telemetry-port 9419
```

### Prometheus Metrics

```bash
curl http://localhost:9419/metrics
```

Available metrics include message throughput, connection and channel counts, queue depths, memory usage, and storage utilization.

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
├── storage/             # Storage engine (ring buffer, WAL, segments)
├── interfaces/          # Interface definitions
├── broker/              # Message routing and delivery
├── auth/                # Authentication & authorization
├── metrics/             # Prometheus metrics
└── transaction/         # Transaction manager
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

- [Client Examples](CLIENT_EXAMPLES.md) — Complete examples for Python, Node.js, and Go
- [Authorization Guide](docs/AUTHORIZATION.md) — Permission model and access control
- [TLS Configuration](docs/TLS.md) — TLS encryption and mutual TLS setup
- [Contributing Guidelines](CONTRIBUTING.md) — How to contribute
- [Security Policy](SECURITY.md) — Security best practices

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

MIT License — see [LICENSE](LICENSE) for details.

## Support

- Report bugs: [GitHub Issues](https://github.com/maxpert/strangeq/issues)
- Security issues: See [SECURITY.md](SECURITY.md)
