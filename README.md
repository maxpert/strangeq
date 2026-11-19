# StrangeQ

StrangeQ is an AMQP 0.9.1 message broker written in Go. Run it as a single
binary, connect with RabbitMQ client libraries, and use optional Raft replication
to distribute queues across brokers.

[![Build Status](https://github.com/maxpert/strangeq/workflows/Build%20and%20Test/badge.svg)](https://github.com/maxpert/strangeq/actions)
[![Go Report Card](https://goreportcard.com/badge/github.com/maxpert/strangeq)](https://goreportcard.com/report/github.com/maxpert/strangeq)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

## Features

- Direct, fanout, topic, and headers exchanges, including exchange-to-exchange bindings.
- Durable queues, persistent messages, publisher confirms, manual acknowledgments,
  redelivery, and crash recovery.
- Standalone queue TTL, [dead-letter exchanges](docs/DEAD_LETTERING.md), queue
  length limits, and transactions.
- TLS and mutual TLS, file-based authentication, and per-vhost permissions.
- Optional replicated queues accessible through any enrolled broker, with
  [at-least-once dead-lettering](docs/DEAD_LETTERING.md#replicated-queues) for durable queues.
- Prometheus metrics, readiness checks, and optional Go pprof profiling.
- A standalone management web UI and RabbitMQ-compatible REST API subset, served
  by the broker binary.

See [Compatibility and limits](#compatibility-and-limits) before migrating an
application or enabling cluster mode.

## Installation

### Release binaries

Download a macOS or Linux archive and its checksum from
[Releases](https://github.com/maxpert/strangeq/releases). Verify the checksum,
extract the archive, and run this command from the extracted directory.

```sh
./amqp-server --version
```

Each archive includes the binary, a sample configuration, and the license. The
examples below assume `amqp-server` is on your `PATH`; use `./amqp-server` if
running from the extracted directory.

### Build from source

Requires Go 1.27.3 or later.

```sh
git clone https://github.com/maxpert/strangeq.git
cd strangeq/src/amqp-go
go build -o amqp-server ./cmd/amqp-server
./amqp-server --version
```

## Quick start

Start a local standalone broker:

```sh
amqp-server
```

It listens on `127.0.0.1:5672`, stores data in `./data`, and has authentication
disabled. Configure [authentication](docs/AUTHORIZATION.md) and
[TLS](docs/TLS.md) before making the listener accessible to other hosts.

In another terminal, install the Python client:

```sh
python3 -m venv .venv
. .venv/bin/activate
python -m pip install pika
```

Publish a persistent message with a confirm, then retrieve and acknowledge it:

```python
import pika

with pika.BlockingConnection(pika.ConnectionParameters("127.0.0.1")) as connection:
    channel = connection.channel()
    channel.queue_declare(queue="hello", durable=True)
    channel.confirm_delivery()
    channel.basic_publish(
        exchange="",
        routing_key="hello",
        body=b"Hello from StrangeQ!",
        properties=pika.BasicProperties(delivery_mode=2),
        mandatory=True,
    )

    method, properties, body = channel.basic_get(queue="hello", auto_ack=False)
    if method is None:
        raise RuntimeError("No message available")
    print(body.decode())
    channel.basic_ack(method.delivery_tag)
```

[Client examples](CLIENT_EXAMPLES.md) cover publishers, consumers, and routing
with Python, Node.js, and Go.

## Configuration and operation

Generate a configuration and start the broker with it:

```sh
amqp-server --generate-config config.yaml
amqp-server --config config.yaml
```

Configuration files overlay the defaults. Environment variables override the
file; explicitly supplied command-line flags override both. For example:

```sh
AMQP_STORAGE_PATH=/var/lib/amqp amqp-server --config config.yaml
```

Use the [sample configuration](src/amqp-go/config.sample.yaml) for the available
settings, including storage durability, resource alarms, and connection limits.
Run `amqp-server --help` for command-line options.

### Management and telemetry

For a standalone broker, configure [management authentication](docs/MANAGEMENT.md)
before enabling the UI and API at `http://127.0.0.1:15672`.

```sh
amqp-server --config config.yaml --management
```

Enable Prometheus metrics and readiness at `http://127.0.0.1:9419`:

```sh
amqp-server --config config.yaml --enable-telemetry
curl http://127.0.0.1:9419/health
curl http://127.0.0.1:9419/metrics
```

Add `--enable-pprof` to enable profiling on the telemetry listener. See the
[management guide](docs/MANAGEMENT.md) for API coverage and access controls, and
the [metrics guide](docs/METRICS.md) for endpoints, metric definitions, and alerts.

### Deployment

The [systemd deployment guide](deployment/systemd/README.md) covers installation,
service configuration, credentials, and persistent directories.

To build and run the Docker image from a repository checkout:

```sh
make docker-build VERSION=local
docker run --rm -p 127.0.0.1:5672:5672 -p 127.0.0.1:9419:9419 \
  -v amqp-data:/var/lib/amqp amqp-go:local
```

The image enables telemetry and binds its listeners to all container interfaces
with authentication disabled. The example exposes those ports on host loopback.
For remote access, mount your configuration and credentials and enable
[authentication](docs/AUTHORIZATION.md) and [TLS](docs/TLS.md).

## Clustering

Cluster mode is opt-in and uses separate peer listeners and persistent
directories. Clients connect to any enrolled broker using AMQP; the broker
routes queue operations to the owning Raft shard. A queue with three voting
replicas can continue committing through one replica outage.

Follow the [cluster setup guide](docs/CLUSTERING.md) to initialize a cluster,
enroll brokers, configure peer TLS, and understand quorum and recovery. See
[how clustered messages are committed and read](docs/CLUSTERING.md#how-clustered-messages-are-committed-and-read)
for the publish and delivery paths. Cluster mode has a smaller feature set than
standalone mode; its limits are listed below and in the guide.

The segmented shared journal is **experimental and disabled by default**. Enable
it only when provisioning fresh cluster stores. Its configuration and restart
requirements are documented in the [experimental journal guide](docs/CLUSTERING.md#experimental-shared-journal).

## Performance

These same-host benchmarks use three voting replicas per queue on an eight-core
ARM64 Linux VM. Results describe the measured workloads and storage settings.

### Active dead-lettering

Three paired, order-alternating runs compared StrangeQ `bf52074` using the
experimental segmented journal with RabbitMQ 4.4.0 quorum queues and at-least-once
dead-lettering. Ten producers sent 64 KiB persistent messages to ten source
queues. Ten source consumers rejected them into ten dead-letter queues.

| Broker | Confirms/s | DLQ deliveries/s | Peak cluster RSS |
|---|---:|---:|---:|
| StrangeQ | 1,915 | 1,921 | 627 MiB |
| RabbitMQ | 1,663 | 1,673 | 1,882 MiB |

Figures are medians. StrangeQ had higher confirm and DLQ delivery rates and lower
peak cluster RSS in all three pairs. Every accepted message reconciled after
draining, with no duplicates observed. In separate same-cluster controls, enabling
an idle dead-letter policy changed confirm and delivery throughput by less than
0.2%. [Methodology and per-run evidence](src/amqp-go/benchmark/CLUSTER_COMPARISON.md#replicated-dead-lettering-runs)

### Historical normal workloads

Three paired runs per workload compared StrangeQ `f02275a` using the experimental
segmented journal with RabbitMQ 4.4.0 quorum queues. The runs used three voting
replicas on the same ARM64 Linux host and high-entropy payloads. Figures are
medians across runs, and peak RSS measures the highest simultaneous sum for the
three broker processes. [Recorded methodology](src/amqp-go/benchmark/CLUSTER_COMPARISON.md#recorded-arm64-runs)

| Workload | Broker | Confirms/s | Deliveries/s | Peak cluster RSS |
|---|---|---:|---:|---:|
| 1 KiB, 1 producer / 1 consumer, 1 queue | StrangeQ | 27,813 | 27,816 | 151 MiB |
| | RabbitMQ | 26,035 | 26,034 | 636 MiB |
| 64 KiB, 10 producers / 10 consumers, 10 queues | StrangeQ | 4,147 | 4,150 | 618 MiB |
| | RabbitMQ | 2,982 | 2,982 | 1,748 MiB |

StrangeQ had higher confirm and delivery rates and lower peak cluster RSS in all
six measured pairs.

For local load testing, the repository also provides a Go client and
[benchmark instructions](src/amqp-go/benchmark/README.md).

## Compatibility and limits

StrangeQ supports AMQP 0.9.1 clients such as `amqp091-go`, `pika`, and `amqplib`.
It implements a subset of RabbitMQ's extensions and management API.

- Priority queues, consumer priorities, lazy queues, SASL EXTERNAL, and multi-step
  SASL challenge-response are unsupported.
- Global prefetch is accepted but applied per consumer; a nonzero prefetch size
  is rejected.
- A mandatory publish to a full `reject-publish` queue produces `basic.return`.
  A transaction can publish to that queue without enforcing `reject-publish`.
- Zero-TTL messages expire without a publish-time handoff to a waiting consumer.
  Malformed per-message expiration values are treated as no TTL.
- Cluster mode supports ordinary queues, exchange routing, publish, get, consume,
  acknowledgments, and requeue in the default vhost. Durable replicated queues
  support [at-least-once dead-lettering](docs/DEAD_LETTERING.md#replicated-queues).
  Transactions, exclusive and auto-delete queues, other queue policies, named
  vhosts, and the management HTTP API are unavailable in cluster mode.
- A cluster publish routed to multiple queues commits separately on each
  destination shard; an error can leave a partial fanout. Retrying a publish
  after an unknown outcome can produce duplicates.

See the [cluster guide](docs/CLUSTERING.md#behavior-and-limits) and
[management compatibility reference](docs/MANAGEMENT.md) for further details.

## Documentation

- [Client examples](CLIENT_EXAMPLES.md)
- [Configuration reference](src/amqp-go/config.sample.yaml)
- [Cluster setup and limits](docs/CLUSTERING.md)
- [Authentication and authorization](docs/AUTHORIZATION.md)
- [TLS and mutual TLS](docs/TLS.md)
- [Management UI and REST API](docs/MANAGEMENT.md)
- [Telemetry, profiling, and metrics](docs/METRICS.md)
- [systemd deployment](deployment/systemd/README.md)
- [Release notes](CHANGELOG.md)
- [Security policy](SECURITY.md)

## Contributing and support

See [Contributing](CONTRIBUTING.md) for development setup and test commands.
Report bugs and feature requests through
[GitHub Issues](https://github.com/maxpert/strangeq/issues). Follow the
[security policy](SECURITY.md) to report a vulnerability.

## License

[MIT](LICENSE).
