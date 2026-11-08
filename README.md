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
- Exchange types: direct, fanout, topic (with `*` and `#` wildcards), headers (with `x-match: all`, `any`, `all-with-x`, `any-with-x`)
- Exchange-to-exchange bindings with cycle detection
- Three-tier storage architecture (in-memory hot cache → WAL (durable) → segments (cold fallback))
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
- Peak RSS is lower than RabbitMQ's best-measured configuration in every benchmark cell (12-83% less). Throughput beats RabbitMQ's best configuration on all five workloads in `none` mode and, except for a statistical tie on 64 KiB 1p/1c, in the default `barrier` mode; the power-loss-durable `flush` mode wins the 1 KiB workloads and trades 14-22% of 64 KiB throughput for the stronger guarantee. See [Performance](#performance)
- Prometheus metrics and pprof profiling
- Management control plane — a RabbitMQ-compatible REST API plus an embedded single-binary web UI (go:embed, no CDN), including a Metrics tab over the live Prometheus matrix
- Byte-bounded per-queue hot cache with its own eviction budget
- Batch TCP writes and ACK offloading

## Installation

### Pre-built Binaries

Download the latest release for your platform from the [releases page](https://github.com/maxpert/strangeq/releases). Each platform ships as a tarball (`amqp-server-<version>-<os>-<arch>.tar.gz`) that contains the `amqp-server` binary, `config.sample.yaml`, `README.txt`, and `LICENSE`, plus a `.sha256` checksum.

```bash
# macOS/Linux (replace <version>, <os>, <arch> with your release)
tar -xzf amqp-server-<version>-<os>-<arch>.tar.gz
cd amqp-server-<version>-<os>-<arch>/
sudo mv amqp-server /usr/local/bin/

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

# With the management control plane (REST API + web UI at http://127.0.0.1:15672)
amqp-server --config config.yaml --management
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
  --management              Enable the management control plane (REST API + web UI)
  --management-address string  Management API bind address (default from config: 127.0.0.1)
  --management-port int     Management API port (default from config: 15672)
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
  durability: barrier

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
  maxqueuebacklog: 209715
  walbatchsize: 1000
```

Run it under systemd (see `deployment/systemd/`) rather than `daemonize: true`
and a PID file; the unit handles restart, environment, and logging.

Key engine tuning parameters:
- `maxqueuebacklog` — Per-queue publisher backpressure depth high-water mark in messages (default 52428; 0 = default). The hot-cache byte budget usually binds first.
- `walbatchsize` — Messages per WAL batch (default 1000)
- `walbatchtimeoutms` — Retained for compatibility; WAL sync is drain-then-sync, not timer-gated
- `walfilesize` — Size of each WAL file (default 512 MB)
- `segmentsize` — Cold storage segment file size (code default 64 MB; the shipped `config.sample.yaml` sets 256 MB)
- `walcleanupcheckintervalms` — WAL reclaim sweep interval (code default and shipped sample: 1000 ms)
- `consumermaxbatchsize` — Max messages to deliver per consumer per poll (default 100)

## Performance

Throughput is measured as **end-to-end delivered messages per second (msg/s)**: every published message is delivered and acknowledged, and `lost=0`. Both brokers are driven by the same client ([`cmd/perftest`](src/amqp-go/cmd/perftest), built on [amqp091-go](https://github.com/rabbitmq/amqp091-go)) with identical flags. Shipped release is `v0.13.0`. The `none` and `barrier` StrangeQ columns are from the release benchmark matrix (5 stall-free repetitions per cell, profiling-free harness, zero invalid cells); the `flush` rows are separate full-flush reference measurements — see **Repetitions** below.

### How to read this

**Contract (both brokers).** One 16-core Apple Silicon machine (M4 Max) with an APFS data volume; one broker runs at a time. Durable queue (RabbitMQ Classic; StrangeQ durable), persistent messages, publisher confirms, consumer prefetch 100, an outstanding window of 1000, and manual acknowledgements. A 10 s warm-up is followed by a 30 s measured window; throughput is the median per-second receive rate over seconds 6–30.

**Repetitions and stalled cells.** Every `none` and `barrier` figure is the **median of 5 stall-free repetitions** from the release benchmark matrix. The `flush` rows are reference measurements from separate full-flush runs on an earlier tree (the 1 KiB rows predate the barrier-everywhere change and carry fewer repetitions); they are indicative of the power-loss mode's cost and are **not** part of the gated matrix. A repetition whose consumer throughput collapses part-way through the measured window is excluded even when it passes the harness gate: the gate checks counts (`lost=0`, `published == confirmed == delivered`, queue depth back to 0), not the shape of the per-second series, so a mid-window stall can pass it. Stalled repetitions are detected separately and re-run; none remain in the matrix below.

**Validity.** Each cell is a complete run behind a gate: `lost=0`, `unconfirmed=0`, `nacked=0`, `published == confirmed == delivered`, the queue drains to empty, the queue type is verified (`classic`), the broker binary sha256 matches, and no process panicked. Every cell in the matrix cleared the gate.

**RabbitMQ reference.** The RabbitMQ columns are RabbitMQ 4.4 Classic at its **best-measured configuration for that workload** — its default where tuning does not help, and the best of the tuning arms where it does. The `none`/`barrier` StrangeQ columns are medians from the release benchmark session; the RabbitMQ figures are the reference figures. Ratios across the two sessions carry a few percent of uncertainty; `none`-vs-`barrier` comparisons are same-session and exact, while any comparison involving a `flush` row is across separate campaigns and carries a few percent of uncertainty.

**WAL size.** StrangeQ ran with its shipped default WAL file size of **512 MiB** (`walfilesize: 536870912`), matching the shipped sample. WAL file size is the knob behind the 1 KiB peaks: a smaller WAL buys a lower peak on the small-message rows and costs 64 KiB 10p/10c throughput.

### Durability modes

`storage.durability` controls how confirmed messages reach the drive:

- **`none`**: confirm on write-completion, no device sync. Fastest mode.
  Survives: process crash (kill -9 / panic). Does NOT survive: OS crash, power loss.
  219 MB RSS on F1 (vs RabbitMQ 259).

- **`barrier`** (default): `fcntl(F_BARRIERFSYNC)` per confirm batch — orders data
  into the drive's cache without waiting for it to drain to media.
  Confirmed data survives OS crash (kernel panic, power button). Power loss may
  lose the last ~seconds of confirmed data sitting in the drive's volatile cache.
  > This is **stronger** than RabbitMQ 4.4 Classic, not equal to it: a source
  > audit of RabbitMQ v4.4.0 shows Classic queues issue **no** device sync on the
  > confirm path at all — a Classic confirm is a page-cache write (their confirms
  > are fast because they never wait for a device flush; 436 µs confirm p50), so
  > it survives only a broker-process crash, RabbitMQ's own stated contract
  > ("crash of this rabbit node"). `barrier` additionally survives an OS crash.

- **`flush`**: `fcntl(F_FULLFSYNC)` per confirm batch — waits for the drive to
  flush its write cache to media. Confirmed data survives power loss.
  ~10-30% slower than barrier on Apple Silicon; equivalent on Linux.

### Performance vs RabbitMQ 4.4 Classic (Go 1.27.3, Apple Silicon)

`msg/s` is the window median; RSS is peak resident set size. RabbitMQ is 4.4 Classic at its best-measured configuration for each workload.

| Workload | Durability | SQ thr | RMQ thr | Margin | SQ RSS | RMQ RSS | Margin |
|---|---|---|---|---|---|---|---|
| F1 (1K, 1p/1c) | none | 139,732 | 96,318 | +45% | 219 MB | 259 MB | −15% |
| | barrier | 137,200 | 96,318 | +42% | 227 MB | 259 MB | −12% |
| | flush | 123,990 | 96,318 | +29% | 43 MB | 259 MB | −83% |
| F2 (1K, 10p/10c) | none | 162,823 | 86,262 | +89% | 257 MB | 397 MB | −35% |
| | barrier | 160,455 | 86,262 | +86% | 262 MB | 397 MB | −34% |
| | flush | 115,715 | 86,262 | +34% | 261 MB | 397 MB | −34% |
| F3 (64K, 1p/1c) | none | 36,570 | 35,463 | +3.1% | 251 MB | 1,195 MB | −79% |
| | barrier | 35,415 | 35,463 | −0.1% (tie) | 262 MB | 1,195 MB | −78% |
| | flush | 27,632 | 35,463 | −22% | 322 MB | 1,195 MB | −73% |
| F4 (64K, 10p/10c) | none | 40,926 | 36,864 | +11% | 665 MB | 2,020 MB | −67% |
| | barrier | 44,206 | 36,864 | +20% | 656 MB | 2,020 MB | −68% |
| | flush | 31,858 | 36,864 | −14% | 922 MB | 2,020 MB | −54% |
| F5 (1K transient) | none | 142,096 | 45,339 | +214% | 333 MB | 539 MB | −38% |
| | barrier | 137,798 | 45,339 | +204% | 330 MB | 539 MB | −39% |
| | flush | 94,666 | 45,339 | +109% | 365 MB | 539 MB | −32% |

F5 is 1 KiB transient messages at 32 producers / 32 consumers; worker rows share the workload label of the row above them.

- **F4 `barrier` beats our own `none` mode.** The self-sizing batches plus the cheap barrier produce a genuinely faster pipeline than the classic write-and-go path on this workload (44,206 vs 40,926 msg/s).
- **F3 `barrier` is a statistical tie with RabbitMQ best** (−0.1%), sitting at 96.8% of our own `none` wall. The `none` wall itself rose to 36,570 (+3.1% over RabbitMQ) — the two-phase reclaim fix lifted both modes.
- **RSS wins are clean across every cell:** the 64 KiB rows use 2-5× less memory than RabbitMQ's peak, and every 1 KiB row is also lower (12-39%).

**Workload shape.** The 1 KiB rows are CPU- and dispatch-bound: StrangeQ's engine parallelises queue dispatch across the producer/consumer fan-out where RabbitMQ Classic serialises a single queue, so added connections raise throughput. The 64 KiB rows are limited by durable write bandwidth, which added connections do not raise.

### Confirm semantics

The three modes differ only in what a publisher confirm waits for. `none` confirms once the write reaches the page cache. `barrier` orders each confirm batch into the drive's cache and confirms when that ordering call returns — the drive is not asked to drain the batch to media. `flush` waits for the drive to report its write cache flushed before the confirm goes out. Because one sync releases every confirm in its batch, the per-message sync cost falls as the batch grows.

### Memory

Peak RSS is lower than RabbitMQ's best configuration in **every cell**, by 12-83%. The 1 KiB rows are where it is most decisive: F1 `flush` peaks at 43 MB against RabbitMQ's 259 MB, and F1 `none`/`barrier` at 219/227 MB. On 64 KiB 10p/10c the `flush` arm carries the highest StrangeQ peak (922 MB), still 54% below RabbitMQ's 2,020 MB.

### What we do not claim

- **We do not beat RabbitMQ on 64 KiB throughput in `flush` mode.** F3 is −22% and F4 is −14% against RabbitMQ's best configuration for those rows; the power-loss guarantee is the trade.
- **We do not claim a throughput win on 64 KiB 1p/1c in `barrier` mode.** F3 is a statistical tie (−0.1%); `none` is +3.1%.
- **We do not claim steady-state RSS separates cleanly** — this document reports peak RSS, not steady-state RSS.
- **We do not claim compression**, on these all-zero bodies or as a general lever.
- **We do not claim a blanket parallelism win** — it applies to the CPU/dispatch-bound 1 KiB rows, not the durable-write-bound 64 KiB rows.

### Known gaps

Two throughput cells are losses, both in the strongest mode: **F3 — 64 KiB, 1p/1c, `flush`: 27,632 msg/s, −22%** and **F4 — 64 KiB, 10p/10c, `flush`: 31,858 msg/s, −14%**, both against RabbitMQ's best configuration for the row. F3 `barrier` is a statistical tie (−0.1%); every other throughput cell wins, and peak RSS wins in all 15 cells.

### Reproducing

```bash
cd src/amqp-go

# 64 KiB, 10 producers / 10 consumers, durable + confirms, 30s
go run ./cmd/perftest -url amqp://guest:guest@localhost:5672/ \
  -durable -confirm -producers 10 -consumers 10 -size 65536 \
  -duration 30s -outstanding 1000 -prefetch 100
```

Point `-url` at either broker (both speak AMQP 0.9.1). Each cell's throughput and per-second window are printed by the client; run a 10 s warm-up first, or compare only the stable portion of the window. Inspect the per-second series for a mid-window stall and re-run that cell — the balance gate (`published == confirmed == delivered`, depth 0) does not detect a stall on its own. To run RabbitMQ 4.4 the same way:

```bash
docker run -d --name rabbitmq-bench --platform linux/arm64 -p 5672:5672 rabbitmq:4.4-management
```

The strongest durability guarantee depends on the platform's device-sync primitive, so run on the same kind of storage you intend to deploy against. A Go microbenchmark (`BenchmarkVersus`) also exists for allocation and latency profiling of the hot path.

Per-repetition throughput and RSS figures were collected with the same `cmd/perftest` client shown above; run any cell repeatedly to reproduce the medians.

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
| `headers` | Yes (`x-match: all`, `any`, `all-with-x`, `any-with-x`) | Yes |

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
- Management CLI tools (the REST API and web UI are available; see [docs/MANAGEMENT.md](docs/MANAGEMENT.md))

## Architecture

### Storage Engine

- **Hot Cache**: Byte-bounded per-queue in-memory cache with its own eviction budget
- **WAL Spill**: Durable messages are written to the write-ahead log with a batched group-commit durability barrier; a transient message the hot cache cannot admit spills to the WAL too.
- **Segment Storage**: Cold message storage in segment files, read after the WAL on the cold path
- **Per-Queue Isolation**: Each queue has its own hot cache, WAL cursor, and consumer dispatch — no cross-queue contention

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
4. Load persistent messages into the hot cache
5. Restore pending acknowledgments

Recovery is incremental (recovery time is not measured in this document).

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

## Management & Observability

Two optional HTTP surfaces can be enabled independently of the broker listener:

- **Management control plane** — a RabbitMQ-4.4-management-subset REST API plus an
  embedded single-page web UI, served from the same binary (assets are `go:embed`-ed; no
  separate folder, build step, or CDN). Off by default and loopback-only by default.

  ```bash
  amqp-server --config config.yaml --management
  # Management API listening on http://127.0.0.1:15672
  # open http://127.0.0.1:15672/ in a browser
  ```

  See [docs/MANAGEMENT.md](docs/MANAGEMENT.md) for enabling it, the startup refusal matrix,
  the auth model (capabilities, sessions + CSRF, loopback/host ACLs, rate limits), the full
  REST API reference, the UI screens, and the honest divergences from RabbitMQ.

- **Telemetry** — Prometheus metrics, readiness, and optional pprof.

  ```bash
  amqp-server --config config.yaml --enable-telemetry
  ```

  See [docs/METRICS.md](docs/METRICS.md) for the serving endpoints, the full emitted
  `amqp_*` metric reference, alerting queries, and what is deliberately not emitted.

The management UI includes a **Metrics tab** that renders the same Prometheus matrix the
telemetry listener exposes (a 45-tile vital-stats panel, a searchable family browser, and a
raw view) from a same-origin, auth-gated `GET /api/metrics`.

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

The endpoint exposes the full `amqp_*` family set — message throughput and depth,
connections, channels, consumers, queues and exchanges, publish/delivery latency,
back-pressure and parking, WAL/segment durability and recovery, resource alarms, shutdown,
panics, and (when the management surface is enabled) the `amqp_management_*` families. The
complete reference, with types, labels and meanings, plus ready-to-use alerting queries, is
in [docs/METRICS.md](docs/METRICS.md).

The management UI's **Metrics tab** (`/metrics`) renders the same matrix in the browser
through a same-origin, auth-gated `GET /api/metrics`, so no separate dashboard is required.
When telemetry is disabled that route answers `501` and the screen shows a truthful notice.

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
├── storage/             # Storage engine (hot cache, WAL, segments)
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
- [Management Control Plane](docs/MANAGEMENT.md) — REST API, embedded web UI, auth model, and divergences
- [Telemetry & Metrics](docs/METRICS.md) — Serving telemetry, the full metric reference, and alerting
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
