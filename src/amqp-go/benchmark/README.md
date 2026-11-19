# StrangeQ Performance Benchmarks

`cmd/perftest` measures one broker endpoint. It uses
[amqp091-go](https://github.com/rabbitmq/amqp091-go). Producers and consumers run on
separate connections, and consumers use manual acknowledgement with a QoS prefetch. Use
this guide for a quick single-endpoint workload. For controlled single-node Classic and
three-voter quorum comparisons, use the [comparison runner](CLUSTER_COMPARISON.md),
which records topology, durability, payload profile, and paired evidence.

## Quick Start

```bash
cd src/amqp-go
go run ./cmd/amqp-server
```

Wait for the broker to listen, then run these commands in a second terminal from
`src/amqp-go`.

```bash
# 1 producer / 1 consumer, 1 KiB, 20 seconds
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

The command prints per-second rates followed by a summary.

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

`Consumed` is the in-window receive rate (the headline throughput). `Delivered` counts
all deliveries, including the drain after the publish window. `Lost` is the published
count minus deliveries seen by the drain deadline. It can include a backlog still
waiting at that deadline. It is not proof of durable message loss. Confirm mode fails on
negative or unresolved confirms, but it does not establish restart or failover safety.
Check logs and require positive publish and delivery counts before accepting a run.

## Head-to-head vs RabbitMQ

The [comparison guide](CLUSTER_COMPARISON.md) separates single-node local-journal and
replicated-cluster measurements and explains how to reproduce a controlled trial. The
command below is a quick workload against one RabbitMQ endpoint.

## Running Against RabbitMQ

Use host port 5673 so the local StrangeQ process can remain on 5672. Run from
`src/amqp-go`. Wait for RabbitMQ to finish startup before starting the workload.

```bash
docker run -d --rm --name rabbitmq-bench -p 127.0.0.1:5673:5672 rabbitmq:4.4.0-management
go run ./cmd/perftest -url amqp://guest:guest@localhost:5673/ \
  -durable -confirm -producers 10 -consumers 10 -duration 20s
docker stop rabbitmq-bench
```

For a nonempty `-exchange`, declare the exchange and bind `-queue` to it with the queue
name as routing key before running. The command declares its queue but does not declare
exchanges or bindings.

Use an empty benchmark queue for each trial. This command reuses the named queue and
does not isolate broker storage or collect paired resource evidence.
