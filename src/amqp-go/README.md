# AMQP-Go Server

High-performance AMQP 0.9.1 server implementation in Go, compatible with RabbitMQ clients.

## Features

- **AMQP 0.9.1 Protocol**: Full compatibility with RabbitMQ clients
- **High Performance**: 1.7-1.8x faster than RabbitMQ (110-120K msg/s on Apple M4 Max)
- **Persistent Storage**: Write-Ahead Log (WAL) with batch synchronous writes
- **Lock-Free Architecture**: Per-queue isolation eliminates global contention
- **Memory Efficient**: Optimized 256K ring buffer with zero disk reads for hot messages
- **Zero TCP Blocking**: Reader/processor separation prevents fsync from blocking connections
- **TLS Support**: Secure connections with certificate-based authentication
- **Crash Recovery**: Fast recovery (476K msg/s) with metadata integrity checks

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

Benchmarked on Apple M4 Max with 20 producers / 20 consumers:

| Metric | Value |
|--------|-------|
| **Throughput** | 110,000-120,000 msg/s |
| **Peak Bursts** | 230,000+ msg/s |
| **vs RabbitMQ** | **1.7-1.8x faster** |
| **Recovery Rate** | 476,000 msg/s |
| **Ring Buffer Hits** | 100% (zero disk reads) |

See `benchmark/README.md` for detailed performance testing guide.

## Architecture

### Storage Engine

- **Ring Buffer**: 256K message circular buffer (6.5 MB per queue)
- **Write-Ahead Log**: Batch synchronous writes with fsync per batch
- **Segment Storage**: 1 GB segments for cold message storage
- **Lock-Free Design**: Per-queue isolation using sync.Map

### Connection Handling

- **Reader/Processor Separation**: TCP reader and frame processor in separate goroutines
- **Frame Queue**: 10K buffered channel prevents fsync from blocking socket reads
- **Zero TCP Blocking**: WAL writes never block connection reads
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

# Run with coverage
go test -cover ./...

# Run benchmarks
go test -bench=. -benchtime=5s ./storage
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
├── storage/             # Storage engine (ring buffer, WAL, segments)
├── interfaces/          # Interface definitions
├── broker/              # Message routing and delivery
├── metrics/             # Prometheus metrics
├── benchmark/           # Performance testing tools
└── examples/            # Example configurations
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
