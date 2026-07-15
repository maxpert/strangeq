# StrangeQ Performance Benchmarks

## Quick Start

```bash
cd src/amqp-go
go run ./cmd/amqp-server &

# 1 producer / 1 consumer, 1 KB, 20s
go run ./cmd/perftest -url amqp://guest:guest@localhost:5672/ \
  -durable -confirm -producers 1 -consumers 1 -size 1024 -duration 20s

# 10 producers / 10 consumers
go run ./cmd/perftest -url amqp://guest:guest@localhost:5672/ \
  -durable -confirm -producers 10 -consumers 10 -size 1024 -duration 20s
```

## Command Line Options

| Option | Default | Description |
|--------|---------|-------------|
| `-url` | `amqp://guest:guest@localhost:5672/` | AMQP connection URI |
| `-producers` | 1 | Number of producer connections |
| `-consumers` | 1 | Number of consumer connections |
| `-duration` | 30s | Test duration |
| `-rate` | 0 | Publishing rate limit (msg/s, 0 = unlimited) |
| `-size` | 1024 | Message size in bytes |
| `-durable` | false | Durable queue and persistent messages |
| `-confirm` | false | Enable publisher confirms |
| `-queue` | perftest | Queue name |
| `-auto-ack` | false | Auto-acknowledge messages |
| `-prefetch` | 100 | Consumer prefetch count |

## Output

Real-time per-second:
```
published: 108234 msg/s, consumed: 107981 msg/s, confirmed: 108234
```

Final results include total throughput and consumer latency percentiles (min, p50, p75, p95, p99, max).

## Head-to-Head vs RabbitMQ 4.3

Measured 2026-07-14. StrangeQ host-native, RabbitMQ 4.3 in OrbStack ARM64 container. 20-second runs, durable queues, publisher confirms, fsync enabled, 1 KB messages.

| Workload | RabbitMQ 4.3 | StrangeQ | Advantage |
|---|--:|--:|--:|
| 1 pub / 1 con | 64,532 msg/s | 108,000 msg/s | 1.67x |
| 10 pub / 10 con | 25,381 msg/s | 113,667 msg/s | 4.48x |
| 30 pub / 30 con | 24,232 msg/s | 106,906 msg/s | 4.41x |
| 64 KB, 1 pub / 1 con | 21,157 msg/s | 23,217 msg/s | 1.10x |

## Running Against RabbitMQ

```bash
docker run -d --name rabbitmq-bench --platform linux/arm64 -p 5672:5672 rabbitmq:4.3-management
go run ./cmd/perftest -url amqp://guest:guest@localhost:5672/ -durable -confirm -producers 10 -consumers 10 -duration 20s
```
