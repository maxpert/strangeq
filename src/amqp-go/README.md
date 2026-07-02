# AMQP-Go Server

High-performance AMQP 0.9.1 server implementation in Go, compatible with RabbitMQ clients.

## Features

- **AMQP 0.9.1 Protocol**: Compatible with RabbitMQ clients (amqp091-go, pika, amqplib, etc.)
- **High Performance**: 1.14-1.32x faster than RabbitMQ 4.3 on same hardware, same Go client (single-queue, 1 pub + 1 consumer, 3 ack modes)
- **Lock-Free Architecture**: Per-queue AtomicRing with per-slot CAS — zero contention across queues
- **Three-Tier Storage**: In-memory ring buffer → WAL spill → segment files (automatic tiering)
- **Batch TCP Writes**: All deliveries in a batch serialized into a single buffer, single `Write()` call
- **ACK Offloading**: Per-connection ACK processor goroutine eliminates fsync head-of-line blocking
- **TLS Support**: Secure connections with mutual TLS (mTLS) support
- **Authorization**: RabbitMQ-style per-vhost configure/write/read regex permissions
- **Crash Recovery**: Fast recovery with WAL replay and metadata integrity checks

## Quick Start

### Installation

```bash
# Clone repository
git clone https://github.com/maxpert/amqp-go.git
cd amqp-go

# Build server
go build -o amqp-server ./cmd/amqp-server

# Run server with defaults
./amqp-server
```

### Configuration

```bash
# Generate example configuration
./amqp-server --generate-config config.yaml

# Edit configuration
nano config.yaml

# Start server with config
./amqp-server --config config.yaml
```

### Environment Variables

Override any configuration setting using environment variables:

```bash
# Override network port
AMQP_NETWORK_PORT=15672 ./amqp-server --config config.yaml

# Override storage path
AMQP_STORAGE_PATH=/var/lib/amqp ./amqp-server

# Override log level
AMQP_SERVER_LOGLEVEL=debug ./amqp-server
```

Environment variable format: `AMQP_<section>_<key>` (e.g., `AMQP_NETWORK_PORT`)

## Command Line Options

```
amqp-server [options]

Options:
  --config string
        Configuration file path (YAML/JSON)

  --generate-config string
        Generate default config file and exit (e.g., config.yaml)

  --version
        Show version and exit

  --enable-telemetry
        Enable telemetry endpoint (Prometheus + pprof profiling)

  --telemetry-port int
        Telemetry HTTP server port (default: 9419)
```

### Examples

```bash
# Show version
./amqp-server --version

# Generate config
./amqp-server --generate-config config.yaml

# Run with config file
./amqp-server --config config.yaml

# Run with telemetry
./amqp-server --config config.yaml --enable-telemetry

# View metrics
curl http://localhost:9419/metrics

# Profile CPU for 30 seconds
curl -o cpu.prof http://localhost:9419/debug/pprof/profile?seconds=30
go tool pprof -http=:8080 cpu.prof
```

## Configuration

See `config.sample.yaml` for full configuration documentation with detailed comments.

### Minimal Configuration

```yaml
network:
  address: :5672
  port: 5672

storage:
  backend: badger
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
  backend: badger
  path: /var/lib/amqp
  persistent: true
  offsetcheckpointinterval: 5s

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
  ringbuffersize: 262144  # 256K messages
  walbatchsize: 1000
  walbatchtimeout: 10ms
```

## Performance

### Head-to-Head vs RabbitMQ 4.3

Same machine (Apple M4 Max, 16-core ARM64), same Go client ([amqp091-go](https://github.com/rabbitmq/amqp091-go)), 12-byte messages (RabbitMQ PerfTest default), durable queue, transient messages, 1 publisher + 1 consumer, 50K iterations × 5 runs each. RabbitMQ ran in a Docker container (`rabbitmq:4.3-management`, ARM64) on the same host. StrangeQ ran natively.

| Benchmark | Broker | Run 1 (ns/op) | Run 2 (ns/op) | Run 3 (ns/op) | Run 4 (ns/op) | Run 5 (ns/op) | Median (ns/op) | msg/s | Ratio |
|-----------|--------|---------------|---------------|---------------|---------------|---------------|----------------|-------|-------|
| Auto-ack | StrangeQ | 2,894 | 3,413 | 4,071 | 3,180 | 3,166 | 3,180 | 314K | 1.14x |
| Auto-ack | RabbitMQ | 3,918 | 3,635 | 3,559 | 3,638 | 3,712 | 3,638 | 275K | |
| Manual ack | StrangeQ | 3,047 | 3,196 | 3,052 | 3,325 | 3,437 | 3,196 | 313K | 1.32x |
| Manual ack | RabbitMQ | 4,520 | 6,842 | 4,226 | 4,032 | 4,104 | 4,226 | 237K | |
| Multi-ack 1000 | StrangeQ | 4,038 | 3,578 | 3,174 | 2,820 | 2,674 | 3,174 | 315K | 1.26x |
| Multi-ack 1000 | RabbitMQ | 3,943 | 3,976 | 3,999 | 4,058 | 9,583 | 3,999 | 250K | |

### Running the Benchmarks

```bash
# Benchmark StrangeQ (embedded server starts automatically)
go test . -run='^$' -bench="BenchmarkVersus" -benchmem -benchtime=50000x -count=3

# Benchmark RabbitMQ (must be running on localhost:5672)
docker run -d -p 5672:5672 --name rabbitmq-bench rabbitmq:4.3-management
AMQP_TARGET=rabbitmq go test . -run='^$' -bench="BenchmarkVersus" -benchmem -benchtime=50000x -count=3

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

- **Ring Buffer**: `[]atomic.Pointer[Message]` with per-slot CAS — zero allocations on hot path
- **WAL Spill**: When ring buffer fills, messages spill to write-ahead log (batched fsync)
- **Segment Storage**: 1 GB segments for cold message storage
- **Per-Queue Isolation**: Each queue has its own ring buffer, WAL cursor, and consumer dispatch — zero cross-queue contention

### Connection Handling

- **Reader/Processor Separation**: TCP reader and frame processor in separate goroutines
- **ACK Offloading**: Per-connection `ackProcessor` goroutine handles all ACK/NACK/reject frames — eliminates fsync head-of-line blocking on the frame processor
- **Batched TCP Writes**: All deliveries in a batch are serialized into a single buffer and sent with one `Write()` call — reduces syscalls and lock acquisitions
- **Consumer Dirty Flag**: Delivery loop only re-scans consumers when the consumer set changes — avoids per-iteration map scan
- **Flow Control**: RabbitMQ-style memory pressure handling

### Crash Recovery

1. Validate storage integrity
2. Recover durable exchanges and queues
3. Rebuild bindings
4. Load persistent messages directly into ring buffer
5. Restore pending acknowledgments

Recovery is incremental and happens in milliseconds for typical workloads.

## Development

### Running Tests

```bash
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
# Build server
go build -o amqp-server ./cmd/amqp-server

# Build with optimizations
go build -ldflags="-s -w" -o amqp-server ./cmd/amqp-server

# Cross-compile for Linux
GOOS=linux GOARCH=amd64 go build -o amqp-server-linux ./cmd/amqp-server
```

### Code Structure

```
.
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

## Client Compatibility

Compatible with any AMQP 0.9.1 client library:

- **Go**: [github.com/rabbitmq/amqp091-go](https://github.com/rabbitmq/amqp091-go)
- **Python**: [pika](https://pika.readthedocs.io/)
- **Node.js**: [amqplib](https://www.npmjs.com/package/amqplib)
- **Java**: [RabbitMQ Java Client](https://www.rabbitmq.com/java-client.html)
- **C#**: [RabbitMQ .NET Client](https://www.rabbitmq.com/dotnet.html)

## Monitoring

Enable telemetry to expose Prometheus metrics and pprof profiling:

```bash
./amqp-server --config config.yaml --enable-telemetry --telemetry-port 9419
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

## License

[Add your license here]

## Contributing

[Add contributing guidelines here]

## Support

[Add support information here]
