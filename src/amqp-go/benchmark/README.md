# StrangeQ Performance Benchmarks

`cmd/perftest` is the load client behind the numbers in the repository
[Performance section](../../README.md#performance). It is built on
[amqp091-go](https://github.com/rabbitmq/amqp091-go): producers and consumers
run on separate connections, and consumers use manual acknowledgement with a
QoS prefetch.

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
| `-drain-timeout` | 30s | Max time to drain the consumer backlog after the publish window before counting the remainder as lost |
| `-size` | 1024 | Message size in bytes |
| `-durable` | false | Durable queue and persistent messages |
| `-confirm` | false | Publisher confirms; the run FAILS if any publish is never confirmed |
| `-confirm-grace` | 15s | How long to wait after the run for outstanding confirms to drain |
| `-outstanding` | 1000 | Maximum unconfirmed publishes in flight per producer (0 = unbounded) |
| `-queue` | `perf-test` | Queue name |
| `-exchange` | (empty) | Exchange name (default = default exchange) |
| `-prefetch` | 100 | Consumer prefetch count |

## Output

Real-time per-second, then a summary:

```
[Ns]  sent: N/s  recv: N/s  lat p50=… p95=… p99=…
=== SUMMARY (Ns) ===
Published : N total  |  N avg/s
Consumed  : N in window  |  N avg/s
Delivered : N total  (after drain)
Lost      : N (N.NN%)
Confirmed : N total  |  N avg/s  (unconfirmed: N, nacked: N)
Consumer latency  p50=…  p75=…  p95=…  p99=…  p99.9=…
Confirm latency   p50=…  p75=…  p95=…  p99=…  p99.9=…
```

`Consumed` is the in-window receive rate (the headline throughput);
`Delivered` counts the messages drained after the publish window.

## Head-to-head vs RabbitMQ

The authoritative StrangeQ-vs-RabbitMQ matrix, its methodology, validity gates,
and caveats live in the repository
[Performance section](../../README.md#performance). The client and its flags are
identical for both brokers.

## Running Against RabbitMQ

```bash
docker run -d --name rabbitmq-bench --platform linux/arm64 -p 5672:5672 rabbitmq:4.4-management
go run ./cmd/perftest -url amqp://guest:guest@localhost:5672/ -durable -confirm -producers 10 -consumers 10 -duration 20s
```
