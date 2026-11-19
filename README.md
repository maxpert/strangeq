# StrangeQ

StrangeQ is an AMQP 0.9.1 message broker written in Go. Run a single binary,
connect with RabbitMQ client libraries, and optionally replicate queues with Raft.

[![Build Status](https://github.com/maxpert/strangeq/workflows/Build%20and%20Test/badge.svg)](https://github.com/maxpert/strangeq/actions)
[![Go Report Card](https://goreportcard.com/badge/github.com/maxpert/strangeq)](https://goreportcard.com/report/github.com/maxpert/strangeq)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

## Quick start

Download the archive for your OS and architecture, verify its checksum, and
extract it. [Release binaries](https://github.com/maxpert/strangeq/releases)

Run this from the extracted directory to start a standalone broker. If you
plan to add peers later, use the [single-node Raft setup](#local-raft-journal)
with a fresh data directory.

```sh
./amqp-server
```

The broker listens on `127.0.0.1:5672` and stores data in `./data`, with
authentication disabled. The [client examples](CLIENT_EXAMPLES.md) show how
to publish and consume. Configure [authentication](docs/AUTHORIZATION.md)
and [TLS](docs/TLS.md) for remote access.

## Clustering

The v0.15.0 release predates distributed queues.
[Cluster version requirements](docs/CLUSTERING.md#availability)

Clients connect to any enrolled broker using AMQP. StrangeQ routes queue
operations to the owning Raft shard. Three voting replicas per queue can
continue committing through one replica outage.

Start with the [local Raft journal](#local-raft-journal) below, then
[activate peers and add nodes](docs/CLUSTERING.md#pilot-start-locally-then-activate-peers).
Preserve the source's generated identity and data directory, add authenticated
peer settings, and send SIGHUP. Start joiners normally with fresh directories,
unique node IDs, the source's lineage, and the original source as their seed.
Never initialize a joiner. Keep `local: true` on the original source.

The [cluster guide](docs/CLUSTERING.md) also covers initializing an addressed
seed, peer TLS, placement, quorum, and recovery. Use fresh stores for the shared
segmented journal. Existing standalone and older Raft stores are not imported.
Cluster mode supports routing, confirms, acknowledgments, requeue, and
[at-least-once dead-lettering](docs/DEAD_LETTERING.md#replicated-queues), with
[feature limits](docs/CLUSTERING.md#behavior-and-limits). Administration currently
uses configuration, logs, telemetry, and AMQP. See
[cluster administration status](docs/MANAGEMENT.md#cluster-administration).

### Historical normal workloads

StrangeQ `f02275a` and RabbitMQ 4.4.0 **quorum queues** ran on the same eight-core
ARM64 Linux VM with three voters per queue, persistent high-entropy messages,
and manual acknowledgments. Figures are medians of three alternating pairs.
RSS is the peak simultaneous sum of the three broker processes.

| Workload | Broker | Confirms/s | Deliveries/s | Peak cluster RSS |
|---|---|---:|---:|---:|
| 1 KiB, 1 producer / 1 consumer, 1 queue | StrangeQ | 27,813 | 27,816 | 151 MiB |
| | RabbitMQ quorum | 26,035 | 26,034 | 636 MiB |
| 64 KiB, 10 producers / 10 consumers, 10 queues | StrangeQ | 4,147 | 4,150 | 618 MiB |
| | RabbitMQ quorum | 2,982 | 2,982 | 1,748 MiB |

These historical results use their recorded revision and settings.
[Methodology and reproduction](src/amqp-go/benchmark/CLUSTER_COMPARISON.md#recorded-arm64-runs)

### Active dead-lettering

Three alternating pairs compared StrangeQ `bf52074` with RabbitMQ 4.4.0 quorum
queues using at-least-once dead-lettering on the same ARM64 Linux VM. Ten
producers and ten source consumers moved persistent 64 KiB high-entropy messages
through ten source queues into ten DLQs, each with three replicas.

| Broker | Confirms/s | DLQ deliveries/s | Peak cluster RSS |
|---|---:|---:|---:|
| StrangeQ | 1,915 | 1,921 | 627 MiB |
| RabbitMQ quorum | 1,663 | 1,673 | 1,882 MiB |

Figures are medians. All messages reconciled after draining, with no duplicates
observed. These short runs do not measure crash recovery or failover.
[Methodology and per-run evidence](src/amqp-go/benchmark/CLUSTER_COMPARISON.md#replicated-dead-lettering-runs)

## Single-node

### Local Raft journal

Save this as `local.yaml` and start with a fresh storage directory.

```yaml
network:
  address: "127.0.0.1:5672"
storage:
  path: "./local-data"
  durability: barrier
cluster:
  local: true
  shards: 32
```

```sh
./amqp-server --config local.yaml
```

Startup creates a one-voter journal without an initialization command or peer
listener. Preserve the printed lineage, node ID, and directory for restarts and
[later peer activation](docs/CLUSTERING.md#pilot-start-locally-then-activate-peers).
It has cluster-mode feature limits. The default `amqp-server` startup uses the
legacy standalone engine, whose data is not migrated by this configuration.

### Configuration and durability

Use `./amqp-server --generate-config config.yaml` for a full configuration, then
start with `./amqp-server --config config.yaml`. Files overlay defaults, environment
variables override files, and explicit flags override both. See the
[configuration reference](src/amqp-go/config.sample.yaml).

The default `barrier` profile uses synchronous durability. The local journal
also permits `storage.durability: process`, which protects confirms against a
broker-process kill while the OS kernel remains alive. It does not promise
OS-crash or power-loss recovery. Peer activation permanently upgrades that
journal to full durability. See [durability details](docs/CLUSTERING.md#pilot-start-locally-then-activate-peers).

### Performance against RabbitMQ Classic

The local journal and RabbitMQ Classic 4.4.0 ran on the same native ARM64 Linux
VM and scratch filesystem. StrangeQ used **process durability**, 32 shards,
two journal streams, range sync, and consumer assignments. Both used persistent
high-entropy messages, confirm window 64, and prefetch 100. Each 32-second run
excluded five seconds of warmup. RSS covers one broker process.

| Workload | Broker | Confirms/s | Deliveries/s | Peak broker RSS |
|---|---|---:|---:|---:|
| 64 KiB, 10 producers / 10 consumers, 10 queues | StrangeQ | 37,156 | 37,153 | 384 MiB |
| | RabbitMQ Classic | 34,475 | 34,473 | 1,201 MiB |
| 1 KiB, 1 producer / 1 consumer, 1 queue | StrangeQ | 129,215 | 129,215 | 42 MiB |
| | RabbitMQ Classic | 58,458 | 58,458 | 175 MiB |

The 64 KiB figures average two pairs with broker order reversed. The 1 KiB
figures are from one matched pair.
[Per-pair results and reproduction instructions](src/amqp-go/benchmark/CLUSTER_COMPARISON.md#single-node-classic-runs)

## Documentation and support

[Installation and source builds](QUICKSTART.md#installation) · [Protocol limits](CLIENT_EXAMPLES.md#protocol-limits) ·
[Management UI and REST API](docs/MANAGEMENT.md) ·
[Metrics, readiness, and profiling](docs/METRICS.md) · [systemd deployment](deployment/systemd/README.md) ·
[Release notes](CHANGELOG.md) · [Contributing](CONTRIBUTING.md)

Report bugs through [GitHub Issues](https://github.com/maxpert/strangeq/issues)
and vulnerabilities through the [security policy](SECURITY.md).

## License

[MIT](LICENSE).
