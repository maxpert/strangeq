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
- Lower peak memory than RabbitMQ 4.4 Classic on every measured workload (0.23x–0.82x of stock; 0.18x–0.55x of a tuned RabbitMQ) — our strongest and most stable advantage — plus higher durable throughput on the 1 KiB workloads. We do **not** beat RabbitMQ on 64 KiB durable throughput; see [Performance](#performance)
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

Throughput is measured as **end-to-end delivered messages per second (msg/s)**: every published message is delivered and acknowledged, and `lost=0`. Both brokers are driven by the same client ([`cmd/perftest`](src/amqp-go/cmd/perftest), built on [amqp091-go](https://github.com/rabbitmq/amqp091-go)) with identical flags.

### How to read this

**Contract (both brokers).** One 16-core Apple Silicon machine (M4 Max) with an APFS data volume; one broker runs at a time. Durable queue (RabbitMQ Classic; StrangeQ durable), persistent messages, publisher confirms, consumer prefetch 100, an outstanding window of 1000, manual acknowledgements. A 10 s warm-up is followed by a 30 s measured window; throughput is the median per-second receive rate over seconds 6–30.

**Validity.** Each cell is a complete run behind a gate: `lost=0`, `unconfirmed=0`, `nacked=0`, `published == confirmed`, the queue drains to empty, the queue type is verified (`classic`), the broker binary sha256 matches, and no process panicked. The pre-cell external load must clear the harness quiet gate; cells that fail any gate are retried up to three times, and cell order is rotated and interleaved across arms and repetitions.

**Two RabbitMQ references.** Every comparison is made against RabbitMQ both at its **default** ("stock") configuration and at its **best-measured configuration for that workload**. The best configuration is workload-dependent — the tuned config on the 1 KiB rows, the socket-tuned config on 64 KiB 10p/10c, and on 64 KiB 1p/1c effectively the default, because every RabbitMQ tuning knob either lowers that row or lands within rep-spread noise. Both references are reported so it is always clear which one a ratio uses.

**WAL size.** The benchmark ran StrangeQ with a **64 MiB** WAL file size (`walfilesize: 67108864`). A 1 GiB option is planned but not shipped (see [Planned, not shipped](#planned-not-shipped)).

**Ranges.** Where a workload shows a range, that is **session-to-session variation of the same arm** — the same configuration re-run in separate sessions — not rep-to-rep spread inside one session.

### Throughput

`msg/s`, window median. "Best config" is RabbitMQ's best measured configuration for that row, named in the row. All figures are RabbitMQ 4.4 Classic.

| Workload | RabbitMQ stock | RabbitMQ best config | StrangeQ | vs stock | vs best | Verdict |
|---|---:|---:|---:|---:|---:|---|
| F1 — 1 KiB, 1p/1c | 93,783 | 97,996 (tuned) | 112,080 | 1.20x | 1.14x | **Win** |
| F2 — 1 KiB, 10p/10c | 75,360 | 87,902 (tuned) | 135,548 | 1.80x | 1.54x | **Win** |
| F3 — 64 KiB, 1p/1c | 34,475–35,037 | 35,322 (stats-only) | 26,142–26,306 | ~0.75x | ~0.74x | **Loss** |
| F4 — 64 KiB, 10p/10c | 20,480–21,052 | 37,674 (socket-tuned + client pin) | 30,204–31,008 | ~1.44–1.51x | ~0.80–0.82x | Win vs stock, **loss vs best** |
| F5 — 1 KiB, 32p/32c | 45,534 | not measured | 125,357 | 2.75x | — | Win (older session) |

- **F3 is a loss, and it is a durability exception rather than merely a slow row** — the two brokers are honouring different contracts. See [Confirm latency and the durability difference](#confirm-latency-and-the-durability-difference). Every F3 tuning arm makes RabbitMQ worse or is within noise: stock 34,475–35,037; stats-only 35,322 (+0.8% = noise); TCP-buffer-only 28,670–28,818; full tuned 28,928–29,601; aggressive (queue-index embedding + busy-wait) 18,322. RabbitMQ's best F3 configuration is its default.
- **F4 flips with the reference.** Stock RabbitMQ collapses to ~20.5k under 10p/10c contention; its socket-buffer tuning recovers that row to ~37.1k–37.7k (TCP-only 37,100–37,332; +client socket pin 37,674; full tuned 36,821–37,050; stats-only 21,868). StrangeQ's ~30.2k–31.0k beats stock by ~1.5x and **loses to the tuned reference by ~0.82x**. We report both.
- **F5 is from an older candidate session** and was not re-measured with the current build; treat its ratio as historical.

**Where the wins come from.** StrangeQ's engine is co-designed with its storage and dispatch path, so CPU and queue-dispatch work parallelise across the producer/consumer fan-out where RabbitMQ Classic serialises a queue. That is where the win lives: the CPU/dispatch-bound 1 KiB rows (F2 1.54x, F5 2.75x). On the 64 KiB rows the wall is the **durable write bandwidth**, not dispatch, and parallelism buys nothing there. We do not claim a blanket parallelism win.

**Sockets are not our bottleneck.** Sweeping StrangeQ's network read/write buffers across kernel-auto, 192 KiB, 256 KiB (shipped), 512 KiB and 1 MiB moves F4 by ~1.7% and F3 by ~0.1% — it does not move. The equivalent pin on RabbitMQ moves **its** F4 by +79.5% and its F3 by −17.9%: stock RabbitMQ was sitting on unhelpful socket-buffer defaults; we were not. StrangeQ's best F4 arm is the kernel-auto setting (30,722 msg/s); the shipped 256 KiB gives 30,204, and pinning the client's sockets too gives 30,379 — all inside one ~2% band.

### Memory

Peak resident set size is StrangeQ's strongest and most stable advantage. On **every** workload, StrangeQ's peak RSS is **0.23x–0.82x of stock RabbitMQ's**, and it is lower still against RabbitMQ's tuned configuration (**0.18x–0.55x**). Tuning *costs* RabbitMQ memory on every row.

Peak RSS, MB:

| Workload | StrangeQ | RabbitMQ stock | vs stock | RabbitMQ tuned | vs tuned |
|---|---:|---:|---:|---:|---:|
| F1 — 1 KiB, 1p/1c | 89 | 262 | 0.34x | 508 | 0.18x |
| F2 — 1 KiB, 10p/10c | 348 | 423 | 0.82x | 1,015 | 0.34x |
| F3 — 64 KiB, 1p/1c | 256–329 | 1,125 | 0.23x–0.29x | 952–1,365 | 0.19x–0.35x |
| F4 — 64 KiB, 10p/10c | 920–996 | 1,596 | 0.58x–0.62x | 1,815–2,079 | 0.44x–0.55x |
| F5 — 1 KiB, 32p/32c | 425 | 555 | 0.77x | not measured | — |

**Which metric separates.** The clean, stable separation is on **peak** RSS. **Idle** RSS is further apart and just as stable: StrangeQ holds ~20 MB on every row against RabbitMQ's 162–187 MB. Resident bytes per in-flight message also separate on the 64 KiB rows (F3 410 vs 859 B/msg; F4 1,021 vs 2,234 B/msg — against the tuned config those become 1,381 and 1,506 B/msg, so the gap widens). **Steady-state RSS is noisy across repetitions**, so we do **not** claim that all four metrics separate cleanly; where steady is close (1 KiB 10p/10c: StrangeQ 347 MB vs stock 398 MB) we say so rather than round it up to a clean win. Peak (and idle) are the claims worth defending.

StrangeQ's resident memory is driven by in-flight message data (message size × outstanding window), not connection count — its idle RSS is ~20 MB whether there are 2 connections or 64.

### Confirm latency and the durability difference

Publisher-confirm latency (p50) under load, current build unless noted:

| Workload | StrangeQ | RabbitMQ Classic (stock) |
|---|---:|---:|
| F1 — 1 KiB, 1p/1c | ~7.3 ms | ~8.4 ms |
| F2 — 1 KiB, 10p/10c | ~69.6 ms | ~125.8 ms |
| F3 — 64 KiB, 1p/1c | ~27.5–30.9 ms | **~432–459 µs** |
| F4 — 64 KiB, 10p/10c | ~155 ms | ~33 ms |

Confirm latency under load reflects the client window and the batch cadence as much as the disk, so on its own it is not a durability ranking. **F3 is the row that isolates the difference**, because the single producer's window is the whole story there. (On F4, stock RabbitMQ confirms in ~33 ms only because its throughput has collapsed to ~20.5k; the socket-tuned RabbitMQ that *wins* F4 confirms in ~137 ms, close to StrangeQ's ~155 ms.)

On F3 the two brokers are confirming under **different durability contracts**:

- **StrangeQ confirms only after `fcntl(F_FULLFSYNC)` returns** on the WAL batch — a full drive-cache flush. The measured durable write bandwidth of this machine is ~2.3 GiB/s, so a 64 KiB message costs ~28 µs of forced flush, independent of batch size. The commit cycle is ~18 ms for ~450 messages, of which the fsync alone is ~13 ms. A confirmed StrangeQ message is on stable storage.
- **RabbitMQ Classic confirms from the page cache.** Its own documentation says classic queues are "suitable for use cases where data safety is not a priority … If data safety is a priority, the recommendation is to use quorum queues"; that "a RabbitMQ node can lose persistent messages if it fails before said messages are written to disk"; and that the message store "persists messages to disk in batches after an interval (a few hundred milliseconds)". Its measured confirm p50 of ~432–459 µs — against that documented few-hundred-millisecond flush interval — is the evidence that Classic confirms before the flush completes.

The F3 row therefore compares **two different durability contracts**: StrangeQ's acknowledgements survive power loss; Classic's confirmed-but-unflushed messages do not. We report the row rather than hide it, and we do not weaken the flush to win it.

**Group commit is already in place.** ~450 messages share one flush, and a single flush releases every confirm in the batch. Larger batches are not a lever: the flush is bandwidth-bound (the per-byte cost is constant), and the ingest path is already ~96% utilised refilling the client window. With `fsync: false` StrangeQ reaches **35,288 msg/s** on F3 — **parity** with stock RabbitMQ (34,475–35,037), not a win, because both are then bounded by the same device write bandwidth. That is the honest disclosure: the durable contract is what costs the ~0.75x, and removing it buys exactly parity, no more.

**Compression is not a lever.** The benchmark builds every body from an all-zero buffer, which would compress about 1000:1; claiming a win from that would be benchmark gaming. On real payloads the LZ4-class CPU cost is a wash or a loss for the 64 KiB rows.

### Durability modes

| `storage.fsync` | Publisher-confirm semantics | F3 (64 KiB, 1p/1c) |
|---|---|---|
| `true` (default) | A publish is confirmed only after its WAL batch's `F_FULLFSYNC` returns. The invariant is **confirm ⇒ fsynced ⇒ recoverable**: confirmed durable messages survive `kill -9` and power loss. | 26,142–26,306 msg/s |
| `false` | Confirms are sent before data reaches stable storage. **A crash or power loss can lose confirmed messages.** Faster, and exactly as durable as RabbitMQ Classic's page-cache confirms on this workload. | 35,288 msg/s (older 64 MiB session) |

The `fsync: false` figure is a diagnostic run, not a shipped default.

### What we do not claim

- **We do not beat RabbitMQ on 64 KiB durable throughput.** F3 is ~0.75x and F4 is ~0.82x against RabbitMQ's best configuration. F3 is a durability exception, not a tuning problem.
- **We do not claim F4 as a win against a socket-tuned RabbitMQ.** It is a win against stock (~1.5x) and a loss against the socket-tuned reference (~0.82x).
- **F5 is from an older session** and was not re-measured with the current build.
- **The 1 GiB WAL is not shipped.** The published tables use the 64 MiB benchmark setting.
- **We do not claim steady-state RSS separates cleanly on every row** (peak and idle do).
- **We do not claim compression**, on these all-zero bodies or as a general lever.
- **We do not claim a blanket parallelism win** — it applies to the CPU/dispatch-bound 1 KiB rows, not the durable-write-bound 64 KiB rows.

### Planned, not shipped

A **1 GiB WAL file size**, measured in a controlled A/B, gives F3 **25,098 → 28,378 msg/s (+13.1%)** and drops WAL rolls from ~18.2/s to ~1.7/s. It is not in the build reported above: the shipped sample config defaults to 512 MiB, and the benchmark ran at 64 MiB. It will be exposed as a configurable option.

### Reproducing

```bash
cd src/amqp-go

# 64 KiB, 10 producers / 10 consumers, durable + confirms, 30s
go run ./cmd/perftest -url amqp://guest:guest@localhost:5672/ \
  -durable -confirm -producers 10 -consumers 10 -size 65536 \
  -duration 30s -outstanding 1000 -prefetch 100
```

Point `-url` at either broker (both speak AMQP 0.9.1). Each cell's throughput and per-second window are printed by the client; run a 10 s warm-up first, or compare only the stable portion of the window. To run RabbitMQ 4.4 the same way:

```bash
docker run -d --name rabbitmq-bench --platform linux/arm64 -p 5672:5672 rabbitmq:4.4-management
```

The durability figures depend on the platform's full-flush primitive (`F_FULLFSYNC` on macOS), so run on the same kind of storage you intend to deploy against.

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
