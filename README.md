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
- Memory-efficient by design: ~20 MB idle RSS on every workload against RabbitMQ 4.4 Classic's 164–175 MB. Peak RSS is lower than RabbitMQ's best-measured configuration on 9 of 10 cells across both durability modes; the exception is 1 KiB, 1p/1c at `fsync:false` (265 MB vs 259 MB, +2.3%). Throughput beats RabbitMQ's best-measured configuration on all five workloads at `fsync:false` (+1.5% to +220%); at `fsync:true` two 64 KiB rows are losses (F3 0.81x, F4 0.88x against best; F4 still wins against stock at 1.57x). See [Performance](#performance)
- Prometheus metrics and pprof profiling
- Lock-free per-queue ring buffer with per-slot CAS
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
- `segmentsize` — Cold storage segment file size (code default 64 MB; the shipped `config.sample.yaml` sets 256 MB)
- `walcleanupcheckintervalms` — WAL reclaim sweep interval (code default and shipped sample: 1000 ms)
- `consumermaxbatchsize` — Max messages to deliver per consumer per poll (default 100)

## Performance

Throughput is measured as **end-to-end delivered messages per second (msg/s)**: every published message is delivered and acknowledged, and `lost=0`. Both brokers are driven by the same client ([`cmd/perftest`](src/amqp-go/cmd/perftest), built on [amqp091-go](https://github.com/rabbitmq/amqp091-go)) with identical flags. Shipped `main` is commit `8e3a240`.

### How to read this

**Contract (both brokers).** One 16-core Apple Silicon machine (M4 Max) with an APFS data volume; one broker runs at a time. Durable queue (RabbitMQ Classic; StrangeQ durable), persistent messages, publisher confirms, consumer prefetch 100, an outstanding window of 1000, manual acknowledgements. A 10 s warm-up is followed by a 30 s measured window; throughput is the median per-second receive rate over seconds 6–30.

**Repetitions and stalled cells.** Every figure is the **median of 6 stall-free repetitions**. A repetition whose consumer throughput collapses part-way through the measured window is excluded even when it passes the harness gate: the gate checks counts (`lost=0`, `published == confirmed == delivered`, queue depth back to 0), not the shape of the per-second series, so a mid-window stall can pass it. Stalled repetitions are detected separately and re-run; across this campaign 2.8–3.8% of cells were affected and replaced. See [Reproducing](#reproducing).

**Validity.** Each cell is a complete run behind a gate: `lost=0`, `unconfirmed=0`, `nacked=0`, `published == confirmed == delivered`, the queue drains to empty, the queue type is verified (`classic`), the broker binary sha256 matches, and no process panicked. The pre-cell external load must clear the harness quiet gate; cells that fail any gate are retried up to three times, and cell order is rotated and interleaved across arms and repetitions.

**Two RabbitMQ references.** Every comparison is made against RabbitMQ both at its **default** ("stock") configuration and at its **best-measured configuration for that workload**. The best configuration is workload-dependent — configuration tuning on the 1 KiB rows and on 64 KiB 10p/10c, and on the 64 KiB 1p/1c row the default, because every RabbitMQ tuning knob lowers that row. Tuning helps RabbitMQ at 10 connections and hurts it at 32 (F5). Both references are reported so it is always clear which one a ratio uses.

**Two durability modes (StrangeQ).** StrangeQ is reported with the WAL group-commit fsync barrier **on** (`storage.fsync: true`, the shipped default: a publish is confirmed only after its WAL batch has been forced to media with `F_FULLFSYNC`) and **off** (`storage.fsync: false`: confirms are sent before the data is stable). RabbitMQ Classic does not fsync persistent messages on the publish path — its message store "no longer fsyncs; it only flushes data to disk" (since RabbitMQ 3.13; only compaction and a clean shutdown sync) — so the `fsync: false` rows are the like-for-like comparison in durability behaviour, and the `fsync: true` rows are a stronger contract. Both are reported in full.

**Two measurement sessions.** The StrangeQ columns are medians from one session measured **2026-11-06**; the RabbitMQ columns are the reference figures from the **2026-11-03** campaign, in which both brokers ran together in one session. Ratios across the two sessions carry a few percent of uncertainty; comparisons **within** the StrangeQ columns are same-session and exact.

**WAL size.** StrangeQ ran with its shipped default WAL file size of **512 MiB** (`walfilesize: 536870912`); the benchmark config matches the shipped sample. A same-binary A/B (only `walfilesize` differs) puts 512 MiB vs 64 MiB at **−3.0% on F3 and +5.7% on F4**, and the larger file costs **~80–140 MB of peak RSS** per row — most visible on the 1 KiB rows, where the baseline is small. That trade is set out under [Memory](#memory).

**Storage ceiling.** Measured with no broker running, this machine's bare `write` + `F_FULLFSYNC` ceiling is **~2.23 GB/s** into media; the 64 KiB 10p/10c reference of 36,864 msg/s is 2.42 GB/s of body bytes.

**Two supporting experiments** are labelled where they appear: the socket-buffer sweep and the WAL-size A/B. Both are same-binary, same-session A/Bs from the 2026-11-03 reference session.

### Throughput

`msg/s`, window median. "Best config" is RabbitMQ's best measured configuration for that row, named in the row. All RabbitMQ figures are RabbitMQ 4.4 Classic; see [How to read this](#how-to-read-this) for the cross-session caveat. StrangeQ is shown in both durability modes, and the verdict column gives each mode separately.

| Workload | RabbitMQ stock | RabbitMQ best config | StrangeQ `fsync:false` | vs stock | vs best | StrangeQ `fsync:true` | vs stock | vs best | Verdict |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| F1 — 1 KiB, 1p/1c | 95,216 | 96,318 (conf-tuned) | **138,330** | 1.45x | 1.44x | **119,491** | 1.25x | 1.24x | `false`: **WIN** +43.6%; `true`: **WIN** +24.1% |
| F2 — 1 KiB, 10p/10c | 76,164 | 86,262 (conf-tuned) | **165,929** | 2.18x | 1.92x | **115,530** | 1.52x | 1.34x | `false`: **WIN** +92.4%; `true`: **WIN** +33.9% |
| F3 — 64 KiB, 1p/1c | 35,463 | 35,463 (stock) | **35,992** | 1.01x | 1.01x | 28,558 | 0.81x | 0.81x | `false`: **WIN** +1.5%; `true`: **LOSS** −19.5% |
| F4 — 64 KiB, 10p/10c | 20,652 | 36,864 (conf-tuned) | **42,820** | 2.07x | 1.16x | 32,427 | 1.57x | 0.88x | `false`: **WIN** +16.2%; `true`: **LOSS** −12.0% vs best, **WIN** +57.0% vs stock |
| F5 — 1 KiB, 32p/32c | 45,339 | 45,339 (stock) | **145,137** | 3.20x | 3.20x | **102,711** | 2.27x | 2.27x | `false`: **WIN** +220%; `true`: **WIN** +126.6% |

- **F1 — 1 KiB, 1p/1c.** `fsync:false` 138,330 (+43.6% vs best); `fsync:true` 119,491 (+24.1%).
- **F2 — 1 KiB, 10p/10c.** `fsync:false` 165,929 (+92.4%); `fsync:true` 115,530 (+33.9%).
- **F3 — 64 KiB, 1p/1c.** `fsync:false` 35,992 (+1.5%). `fsync:true` 28,558 (−19.5%). RabbitMQ's best configuration for this row is its default (35,463); every tuning arm lowers it.
- **F4 — 64 KiB, 10p/10c.** `fsync:false` 42,820 (+16.2% vs best). `fsync:true` 32,427 is +57.0% over stock (20,652) and −12.0% against the tuned reference (36,864).
- **F5 — 1 KiB, 32p/32c.** `fsync:false` 145,137 (+220%); `fsync:true` 102,711 (+126.6%). RabbitMQ's best configuration here is also its default (45,339).

**Workload shape.** The 1 KiB rows are CPU- and dispatch-bound: StrangeQ's engine parallelises queue dispatch across the producer/consumer fan-out where RabbitMQ Classic serialises a single queue, so added connections raise throughput. The 64 KiB rows are limited by durable write bandwidth, which added connections do not raise. That is the shape of the two modes' divergence: small on the 1 KiB rows, large on the 64 KiB rows.

**Socket buffers.** A controlled sweep of StrangeQ's accepted-socket read/write buffers (kernel-auto, 192 KiB, 256 KiB shipped, 512 KiB, 1 MiB; separate 64 MiB-WAL session, 28 cells) moves F4 by ~1.7% and F3 by ~0.1%. The equivalent pin on RabbitMQ moves its F4 by +79.5% and its F3 by −17.9%: stock RabbitMQ was sitting on unhelpful socket-buffer defaults, while StrangeQ already pins 256 KiB, larger than the 192 KiB RabbitMQ needs. No client-side socket pinning is used anywhere in the table above. This sweep is a 2026-11-03 reference-session A/B.

### Memory

Idle RSS is measured with no load. Idle RSS, MB (median of reps, 2026-11-03 reference session):

| Workload | StrangeQ | RabbitMQ stock | RabbitMQ best config |
|---|---:|---:|---:|
| F1 — 1 KiB, 1p/1c | 19.8 | 167 | 173 |
| F2 — 1 KiB, 10p/10c | 19.8 | 170 | 171 |
| F3 — 64 KiB, 1p/1c | 19.8 | 168 | 164 |
| F4 — 64 KiB, 10p/10c | 19.8 | 169 | 168 |
| F5 — 1 KiB, 32p/32c | 19.9 | 172 | 164 |

Idle RSS holds at ~20 MB for StrangeQ on every workload (2 to 64 connections), against 164–175 MB for RabbitMQ, and does not depend on the fsync mode.

Peak RSS, MB (StrangeQ medians from the 2026-11-06 session, against the 2026-11-03 RabbitMQ reference):

| Workload | RabbitMQ stock | RabbitMQ best config | StrangeQ `fsync:false` | vs stock | vs best | StrangeQ `fsync:true` | vs stock | vs best | Verdict |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| F1 — 1 KiB, 1p/1c | 258 | 259 (conf-tuned) | 265 | 1.03x | 1.02x | **40** | 0.16x | 0.15x | `false`: **LOSS** +2.3%; `true`: **WIN** −84.6% |
| F2 — 1 KiB, 10p/10c | 424 | 397 (conf-tuned) | **308** | 0.73x | 0.78x | **301** | 0.71x | 0.76x | `false`: **WIN** −22.4%; `true`: **WIN** −24.2% |
| F3 — 64 KiB, 1p/1c | 1,195 | 1,195 (stock) | **377** | 0.32x | 0.32x | **330** | 0.28x | 0.28x | `false`: **WIN** −68.5%; `true`: **WIN** −72.4% |
| F4 — 64 KiB, 10p/10c | 1,810 | 2,020 (conf-tuned) | **956** | 0.53x | 0.47x | **991** | 0.55x | 0.49x | `false`: **WIN** −52.7%; `true`: **WIN** −50.9% |
| F5 — 1 KiB, 32p/32c | 539 | 539 (stock) | **372** | 0.69x | 0.69x | **384** | 0.71x | 0.71x | `false`: **WIN** −31.0%; `true`: **WIN** −28.8% |

Peak RSS is lower than RabbitMQ's best configuration on **9 of the 10 cells** across the two modes. The one exception is **F1 — 1 KiB, 1p/1c at `fsync:false`: 265 MB against 259 MB (+2.3%)**. The `fsync:true` arm of the same row is 40 MB (0.15x).

**Fixes already measured take the `fsync:false` F1 cell to 228 MB (−12.0% vs RabbitMQ), verified in 6 of 6 reps.** They are held back because they cost ~2.6% on 64 KiB 1p/1c `fsync:false` throughput (+1.5% → −1.2% vs best) and land once that regression is removed.

**WAL file size is the knob behind the 1 KiB peaks.** A same-binary A/B (512 MiB vs 64 MiB, only `walfilesize` differs; 2026-11-03 reference session) shows both sides of the trade:

| Row | Peak @ 512 MiB | Peak @ 64 MiB | Throughput @ 512 MiB | Throughput @ 64 MiB |
|---|---:|---:|---:|---:|
| F3 — 64 KiB, 1p/1c | 394 MB | 313 MB | 26,068 msg/s | 26,866 msg/s (−3.0%) |
| F4 — 64 KiB, 10p/10c | 1,095 MB | 994 MB | 31,974 msg/s | 30,236 msg/s (+5.7%) |

In earlier 64 MiB sessions the F1 peak was ~90 MB (0.35x of stock); at the shipped 512 MiB it is ~265 MB in the `fsync:false` arm (1.03x of stock). The choice is one config value wide: a smaller WAL buys a lower peak on the small-message rows and costs F4 throughput; the shipped default is 512 MiB and `walfilesize` moves it.

**What this checkpoint cost.** Two measured regressions are folded into the numbers above and are reported rather than smoothed away:

- The shipped **1-second WAL reclaim interval** (`walcleanupcheckintervalms: 1000`, matching the code default) costs **−0.7% to −1.9% throughput** versus the previous 5-minute interval, in exchange for **−22% to −78% peak RSS**. The reclaim latency is itself a memory bound, because an unreclaimed file keeps its per-file inventory alive.
- The **S1 WAL-bookkeeping cut** (the WAL index is bounded by the in-flight set, and the per-shard index maps shrink once the unacked set drains) costs **≈ −0.8% (F1) to −3.9% (F5) confirmed/s on the `fsync:true` rows**, while cutting **120–158 MB of peak RSS on the 1 KiB rows**.

**Steady-state RSS is noisy across repetitions**, so **idle RSS** and (on the 64 KiB rows) **peak RSS** are the memory measures worth treating as separated; this document does not claim that every metric separates cleanly. StrangeQ's resident set is driven by in-flight message data and the WAL, not connection count.

### Confirm latency and the durability difference

Publisher-confirm latency (p50) under load, 2026-11-03 reference session, StrangeQ in `fsync:true`:

| Workload | StrangeQ | RabbitMQ stock | RabbitMQ best config |
|---|---:|---:|---:|
| F1 — 1 KiB, 1p/1c | ~6.7 ms | ~8.2 ms | ~8.2 ms |
| F2 — 1 KiB, 10p/10c | ~83 ms | ~124 ms | ~111 ms |
| F3 — 64 KiB, 1p/1c | ~25.9 ms | **~423 µs** | ~423 µs (stock) |
| F4 — 64 KiB, 10p/10c | ~151 ms | ~33 ms | ~135 ms |
| F5 — 1 KiB, 32p/32c | ~340 ms | ~736 ms | ~736 ms (stock) |

Confirm latency under load reflects the client window and the batch cadence as much as the disk, so on its own it is not a durability ranking. **F3 is the row that isolates the difference**, because the single producer's window is the whole story there. (On F4, stock RabbitMQ confirms in ~33 ms only because its throughput has collapsed to ~20.7k; the conf-tuned RabbitMQ that wins F4 confirms in ~135 ms, close to StrangeQ's ~151 ms.)

The two brokers confirm under different contracts. StrangeQ `fsync:true` confirms only after `fcntl(F_FULLFSYNC)` returns on the WAL batch — a full drive-cache flush; the machine's measured durable write bandwidth is ~2.3 GiB/s, so a 64 KiB message costs ~28 µs of forced flush. RabbitMQ Classic does not fsync persistent messages on the publish path (see [How to read this](#how-to-read-this)), and its measured F3 confirm p50 of **~423 µs** reflects that. StrangeQ `fsync:false` confirms from the page cache, as RabbitMQ Classic does.

**Group commit.** ~450 messages share one flush, and a single flush releases every confirm in the batch. With `fsync:false` StrangeQ reaches **35,992 msg/s** on F3 (+1.5% vs RabbitMQ's 35,463) and **42,820 msg/s** on F4 (+16.2% vs the tuned reference 36,864). With `fsync:true` it is 28,558 on F3 (−19.5%) and 32,427 on F4 (−12.0% vs best, +57.0% vs stock).

**Compression is not a lever.** The benchmark builds every body from an all-zero buffer, which would compress about 1000:1; claiming a win from that would be benchmark gaming. On real payloads the LZ4-class CPU cost is a wash or a loss for the 64 KiB rows.

### Durability modes

| `storage.fsync` | Publisher-confirm semantics | F3 throughput | F3 confirm p50 | F4 throughput |
|---|---|---:|---:|---:|
| `true` (default) | A publish is confirmed only after its WAL batch's `F_FULLFSYNC` returns. The invariant is **confirm ⇒ fsynced ⇒ recoverable**: confirmed durable messages survive `kill -9` and power loss. | 28,558 msg/s | ~25.9 ms | 32,427 msg/s |
| `false` | Confirms are sent before data reaches stable storage. **A crash or power loss can lose confirmed messages.** | 35,992 msg/s | ~322 µs | 42,820 msg/s |

Both modes are measured on the same build and the same WAL size. The F3/F4 throughput columns are the 2026-11-06 medians; the confirm-p50 columns are from the 2026-11-03 reference session. The `fsync: false` mode is not the shipped default.

### What we do not claim

- **We do not beat RabbitMQ on 64 KiB throughput in `fsync:true` mode.** F3 is 0.81x and F4 is 0.88x against RabbitMQ's best configuration for those rows. With `fsync:false`, F3 is +1.5% and F4 is +16.2%.
- **We do not claim F4 as a win against a configuration-tuned RabbitMQ in `fsync:true` mode.** It is a win against stock (1.57x) and a loss against the tuned reference (0.88x).
- **We do not claim a peak-RSS win on every cell.** With `fsync:false`, F1 — 1 KiB, 1p/1c peaks above both references (265 MB; 1.03x stock, 1.02x best). With `fsync:true`, peak RSS is lower than both references on every row (0.15x–0.78x on the 1 KiB rows, 0.28x–0.49x on the 64 KiB rows).
- **We do not claim steady-state RSS separates cleanly** (idle RSS and the 64 KiB peaks do).
- **We do not claim compression**, on these all-zero bodies or as a general lever.
- **We do not claim a blanket parallelism win** — it applies to the CPU/dispatch-bound 1 KiB rows, not the durable-write-bound 64 KiB rows.

### Known gaps

Three cells are losses in the current tables:

- **F3 — 64 KiB, 1p/1c, `fsync:true` throughput: 28,558 msg/s, −19.5%** against RabbitMQ's best (35,463).
- **F4 — 64 KiB, 10p/10c, `fsync:true` throughput: 32,427 msg/s, −12.0%** against the configuration-tuned RabbitMQ (36,864), against +57.0% versus stock.
- **F1 — 1 KiB, 1p/1c, `fsync:false` peak RSS: 265 MB, +2.3%** against RabbitMQ's best (259 MB). The same row at `fsync:true` is 40 MB (0.15x).

The fixes that take the F1 `fsync:false` cell to 228 MB (−12.0%, 6/6 reps) are already measured and are held back by the ~2.6% cost they carry on 64 KiB 1p/1c `fsync:false` throughput, as noted under [Memory](#memory).

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
