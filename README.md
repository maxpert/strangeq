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
- Higher durable end-to-end throughput than RabbitMQ 4.3 Classic queues on 64 KB messages with 10 producers / 10 consumers (1.58x), with lower peak memory (see [Performance](#performance))
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

The Go module path (`github.com/maxpert/amqp-go`) does not match the
repository path (`github.com/maxpert/strangeq`), so `go install ...@latest` is
not supported. Build from source as shown above.

## Quick Start

### Start Server

```bash
# Defaults: persistent storage in ./data, listening on 127.0.0.1:5672,
# authentication off
amqp-server

# Generate example config
amqp-server --generate-config config.yaml

# Run with config file
amqp-server --config config.yaml

# With TLS
amqp-server --tls --tls-cert cert.pem --tls-key key.pem

# With mutual TLS (client cert verification)
amqp-server --tls --tls-cert cert.pem --tls-key key.pem --tls-ca ca.pem

# With telemetry (Prometheus metrics and /health)
amqp-server --config config.yaml --enable-telemetry

# Telemetry also with pprof (heap/goroutine dumps)
amqp-server --config config.yaml --enable-telemetry --enable-pprof
```

### Environment Variables

Environment variables apply with or without `--config`. The format is
`AMQP_<SECTION>_<KEY>`; keys may use underscores. Precedence is
defaults → file → environment → explicitly set flags.

```bash
AMQP_NETWORK_PORT=15672 amqp-server --config config.yaml
AMQP_NETWORK_ADDRESS=0.0.0.0:5672 amqp-server
AMQP_STORAGE_PATH=/var/lib/amqp amqp-server
AMQP_SECURITY_AUTHENTICATION_ENABLED=true amqp-server
AMQP_SERVER_LOGLEVEL=debug amqp-server
```

`AMQP_NETWORK_PORT` rewrites the port in `network.address`. List-valued keys
can only be set from the environment as a single element; use the config file
for lists.

### Command Line Options

```
amqp-server [options]

  --config string           Configuration file path (YAML/JSON)
  --generate-config string  Generate default config file (mode 0600) and exit
  --version                 Show version and exit
  --enable-telemetry        Enable telemetry HTTP endpoint (/metrics, /health)
  --telemetry-port int      Telemetry port (default from config: 9419)
  --telemetry-address string Telemetry bind address (default from config: 127.0.0.1)
  --enable-pprof            Enable pprof endpoints (off by default)
  --healthcheck             Probe the local /health endpoint and exit 0/1
  --unsafe-recovery         Start despite unrecoverable artifacts (DATA LOSS)
  --tls                     Enable TLS (amqps)
  --tls-cert string         TLS certificate file (PEM)
  --tls-key string          TLS private key file (PEM)
  --tls-ca string           TLS CA file for mutual TLS client verification (PEM)
```

Flags that are not passed keep the value from the file or environment; flag
defaults are documentation only.

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
  address: 127.0.0.1:5672

storage:
  path: ./data

server:
  loglevel: info
```

This loads on top of the defaults; omitted keys keep their default values. A
minimal file no longer fails validation.

### Production Configuration

```yaml
network:
  address: "0.0.0.0:5672"
  maxconnections: 10000
  tcpkeepalive: true

storage:
  path: /var/lib/amqp
  fsync: true

security:
  tlsenabled: true
  tlscertfile: /etc/amqp/cert.pem
  tlskeyfile: /etc/amqp/key.pem
  authenticationenabled: true
  authenticationfilepath: /etc/amqp/auth.json
  allowedhosts: ["10.0.0.0/8", "localhost"]

resourcealarms:
  enabled: true
  memoryhighwatermark: 0.8

telemetry:
  enabled: true
  address: 127.0.0.1
  port: 9419

server:
  loglevel: info

engine:
  ringbuffersize: 262144
  walbatchsize: 1000
```

Run it under systemd (see `deployment/systemd/`) rather than `daemonize: true`
and a PID file; the unit handles restart, environment, and logging.

Key engine tuning parameters:
- `ringbuffersize` — In-memory ring buffer per queue (must be power of 2, default 64K)
- `spillthresholdpercent` — Ring fill % before spilling to WAL (default 80)
- `walbatchsize` — Messages per WAL batch (default 1000)
- `walbatchtimeoutms` — Retained for compatibility; WAL flushing is drain-then-flush, not timer-gated
- `walfilesize` — Size of each WAL file (default 512 MB)
- `segmentsize` — Cold storage segment file size (default 256 MB)
- `consumermaxbatchsize` — Max messages to deliver per consumer per poll (default 100)

## Performance

Throughput is measured as **end-to-end delivered messages per second (msg/s)**: every published message is delivered and acknowledged. Both brokers are tested with the same client ([`cmd/perftest`](src/amqp-go/cmd/perftest), built on [amqp091-go](https://github.com/rabbitmq/amqp091-go)) and identical settings.

**Common settings**
- Single Apple Silicon machine with 16 cores.
- Durable queues, publisher confirms, fsync enabled, consumer prefetch 100, and manual acknowledgements.
- No message loss or unconfirmed publishes.

### Latest benchmark (2026-10-25)

Each workload used a 10-second warmup followed by a 30-second measurement. Results are median throughput.

| Message size | Producers / consumers | StrangeQ | RabbitMQ 4.3 Classic | StrangeQ / RabbitMQ |
|---|--:|--:|--:|--:|
| 1 KB | 1 / 1 | 116,219 msg/s | — | — |
| 1 KB | 10 / 10 | 157,232 msg/s | — | — |
| 64 KB | 1 / 1 | 30,214 msg/s | — | — |
| 64 KB | 10 / 10 | 32,899 msg/s | 20,892 msg/s | **1.58x** |

Memory usage for the 64 KB workload with 10 producers and 10 consumers:

| Memory (RSS) | StrangeQ | RabbitMQ 4.3 Classic | StrangeQ / RabbitMQ |
|---|--:|--:|--:|
| Steady state | 1,027 MB | 1,110 MB | 0.93x |
| Peak | 1,029 MB | 1,597 MB | **0.65x** |

Median publisher-confirm latency for this workload was **118.3 ms** for StrangeQ and **33.2 ms** for RabbitMQ Classic.

### Multi-queue scaling (durable + publisher confirms)

3 producers / 3 consumers per queue, 1 KB bodies, one perftest process per queue. Aggregate consumed msg/s, zero loss, evenly balanced across queues.

| Queues | StrangeQ aggregate | vs single queue |
|---|--:|--:|
| 1 | 142,159 msg/s | — |
| 2 | 174,339 msg/s | 1.20x |
| 4 | 191,109 msg/s | 1.32x |

### Crash safety

Confirmed durable messages survive `kill -9` and are recovered on restart. The invariant is **confirm ⇒ fsynced ⇒ recoverable**: a publish is confirmed only after its WAL batch is fsynced, so every confirmed message is present after a crash.

This holds with `storage.fsync: true` (the default). With `fsync: false`,
confirms are sent before data reaches stable storage and a crash or power loss
can lose confirmed messages.

### Reproducing

```bash
cd src/amqp-go

# 10 producers / 10 consumers, 64 KB bodies, 30s
go run ./cmd/perftest -url amqp://guest:guest@localhost:5672/ \
  -durable -confirm -producers 10 -consumers 10 -size 65536 -duration 30s
```

Point `-url` at either broker (both speak AMQP 0.9.1). To run RabbitMQ 4.3 the same way:

```bash
docker run -d --name rabbitmq-bench --platform linux/arm64 -p 5672:5672 rabbitmq:4.3-management
```

A Go microbenchmark (`BenchmarkVersus`) also exists for allocation and latency profiling of the hot path.

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
- **Vectored Delivery**: Message bodies of 32 KB or larger are delivered with scatter-gather I/O (`writev`) — the body is sent directly from its buffer with no intermediate copy
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
# Install the unit and the shipped config (see deployment/systemd/README.md
# for the auth file and directory setup)
sudo cp deployment/systemd/amqp-server.service /etc/systemd/system/
sudo install -o root -g amqp -m 0640 deployment/systemd/config.yaml /etc/amqp/config.yaml

# Enable and start
sudo systemctl daemon-reload
sudo systemctl enable amqp-server
sudo systemctl start amqp-server

# Check status
sudo systemctl status amqp-server
```

### Using Docker

```bash
make docker-build VERSION=0.1.0
docker run --rm -p 5672:5672 -p 9419:9419 -v amqp-data:/var/lib/amqp amqp-go:0.1.0
```

The image binds `0.0.0.0` with authentication off by default (it prints the
startup security warning). To enable authentication, mount an auth file and set
`AMQP_SECURITY_AUTHENTICATIONENABLED=true` and
`AMQP_SECURITY_AUTHENTICATIONFILEPATH=/etc/amqp/auth.json`.
The image's `HEALTHCHECK` uses `amqp-server --healthcheck`, which probes the
same telemetry address/port from the environment.

For protocol interoperability testing with RabbitMQ, the repository also
includes a docker-compose file:

```bash
cd src/amqp-go/protocol
docker-compose up
```

## Monitoring

Enable telemetry to expose Prometheus metrics and readiness:

```bash
amqp-server --config config.yaml --enable-telemetry
```

Telemetry binds `127.0.0.1:9419` by default. Use `--telemetry-address` (or
`telemetry.address`) to change the bind address. `/health` is a readiness
probe: it returns 503 until recovery has completed and the AMQP listener is
bound, and 503 during shutdown. During a resource alarm it still returns 200
with `OK (resource alarm active: memory,disk)` so consumers keep draining.

### Prometheus Metrics

```bash
curl http://localhost:9419/metrics
```

Available metrics include message throughput, connection and channel counts, queue depths, memory usage, and storage utilization.

### Profiling

pprof is disabled unless `--enable-pprof` (or `telemetry.pprofenabled: true`) is
set. When enabled, heap and goroutine dumps are unauthenticated, so keep
telemetry on loopback or behind a firewall.

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
