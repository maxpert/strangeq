# Distributed queues

Cluster mode is opt-in. Start with one original voter or provision up to three
original voters together. Each node keeps its own persistent directory. The
application and peer listeners are independent. Clients use the same AMQP port
and can connect to any enrolled broker.

## Start with one node

Generate a shared peer credential and distribute it to each host as a secret file.

```sh
umask 077
openssl rand -base64 32 | tr '+/' '-_' | tr -d '=\n' > peer.token
```

Save this local test configuration as `node-1.yaml`.

```yaml
network:
  address: "127.0.0.1:5672"
storage:
  path: "./node-1-data"
cluster:
  id: "example"
  node_id: 1
  peer_address: "127.0.0.1:7661"
  seeds: ["1@127.0.0.1:7661"]
  token_file: "./peer.token"
  allow_loopback_plaintext: true
```

Run `amqp-server --config node-1.yaml --initialize-cluster` once. It provisions
the original voter's disk and exits. Then start it with
`amqp-server --config node-1.yaml`. If using multiple original voters, provision
all original voter disks before starting any of their servers.

For traffic between hosts, omit `allow_loopback_plaintext` and set
`peer_cert_file` and `peer_key_file` to a certificate and key for the advertised
peer address. Set `peer_ca_file` if the certificate uses a private CA.
`peer_listen` can override the bind address while keeping the advertised address.
Every peer holding the cluster credential is trusted to participate in consensus.
Keep peer clocks synchronized for request authentication and delivery leases.

## Add a node

Keep the cluster ID, original `seeds`, shard count, and token the same. Set a new
positive `node_id`, a reachable `peer_address`, and its own storage directory.
For a second broker on the same machine, save this as `node-2.yaml`.

```yaml
network:
  address: "127.0.0.1:5673"
storage:
  path: "./node-2-data"
cluster:
  id: "example"
  node_id: 2
  peer_address: "127.0.0.1:7662"
  seeds: ["1@127.0.0.1:7661"]
  token_file: "./peer.token"
  allow_loopback_plaintext: true
```

Start it with `amqp-server --config node-2.yaml`. The broker enrolls on startup.
You can declare a queue and publish with confirms on port 5672, then consume
and acknowledge using the same queue name on port 5673. The receiving broker
routes each operation to the owning shard.

If the original seed is unavailable after the catalog has three voters, set
`cluster.discovery` to the peer addresses of reachable enrolled brokers. The
immutable `seeds` still identify the original configuration.

The membership catalog retains its first three voters after it reaches three.
Online enrollment does not replace those voters. Shards place replicas and
preferred leaders across eligible peers. A joining replica catches up as a
learner before voting. As placement converges, retired stores are removed after
persisting a replica tombstone. Never use initialization to replace a lost
original voter disk. Replacement needs a new node identity, and recovery of a
lost catalog quorum is a separate operator operation.

When initializing a seed with a fresh directory, StrangeQ probes the other
original seeds and configured discovery addresses for a running cluster. A
reachable enrolled peer with the same cluster credentials blocks initialization.
Unreachable peers leave the probe inconclusive. Use initialization only for a
known-new cluster and preserve the durable identity of an existing voter.

## Configuration reference

| Key under `cluster` | Default | Purpose |
|---|---|---|
| `id` | Empty | Enables cluster mode and identifies the cluster. Use the same ID on every broker. |
| `node_id` | Required | Unique positive node ID. Keep it stable across restarts with the same data directory. |
| `peer_address` | Required | Advertised peer `host:port`. Every peer must be able to reach it. |
| `peer_listen` | `peer_address` | Local bind address for peer traffic. |
| `seeds` | Required | One to three original voters as `node_id@host:port`. Copy the same list to every broker. |
| `discovery` | Empty | Additional peer `host:port` addresses for discovering enrolled brokers. |
| `shards` | `32` | Fixed data shard count from 1 to 1024. Use the same count on every broker. |
| `token_file` | Required | File containing the shared 32-byte token encoded as unpadded base64url. |
| `peer_cert_file`, `peer_key_file` | Empty | PEM certificate and private key for peer TLS. Set both for multi-host deployments. |
| `peer_ca_file` | System trust roots | PEM trust roots for a private CA. |
| `allow_loopback_plaintext` | `false` | Allows peer traffic without TLS on numeric loopback addresses for local tests. |
| `experimental_raft_journal` | `false` | Uses one shared append journal per node. Select this when provisioning a fresh cluster storage directory. |
| `experimental_raft_range_sync` | `false` | Enables Linux synchronous range writes for eligible journal appends. Requires `experimental_raft_journal`; other platforms retain normal sync. |
| `experimental_ack_and_claim` | `false` | Combines manual acknowledgments and delivery refill in a fresh journal cluster. Enable it on every broker when provisioning. |
| `experimental_assignments` | `false` | Uses committed consumer assignments in a fresh journal cluster. Enable it on every broker when provisioning. |
| `experimental_replica_bodies` | `false` | Lets a broker reconstruct eligible remote assignment bodies from its own durable journal. Requires `experimental_assignments`. |
| `experimental_journal_body_compression` | `false` | Losslessly packs eligible 32–128 KiB bodies. Requires `experimental_assignments`. |
| `experimental_journal_small_body_compression` | `false` | Extends body compression down to 1 KiB in a fresh cluster. Requires `experimental_journal_body_compression`. |

Cluster state lives under `storage.path/cluster`. Relative paths resolve from
the working directory. Configure AMQP client authentication and encryption
separately from peer TLS. See [TLS](TLS.md) and [Authorization](AUTHORIZATION.md).

### Experimental shared journal

Set `cluster.experimental_raft_journal: true` on a fresh cluster storage
directory to try the shared journal. Keep the setting on subsequent restarts.
The selected backend is part of the durable node identity, and a backend
mismatch stops startup. The default bbolt backend continues to open existing
cluster stores.

The journal uses one active file for the catalog and data shards and syncs
Raft updates before releasing durability-dependent responses. A durable
checkpoint allows the previous snapshot blob to be reclaimed. The active
journal caps at 8 GiB and retained snapshot blobs at another 8 GiB per node.
A write that exceeds a cap fails the affected storage path. Use disposable
test clusters for this option while segment reclamation and migration are
pending.

Set `cluster.experimental_raft_range_sync: true` to try Linux
`pwritev2(RWF_SYNC)` for nonempty durable append batches with no earlier dirty
bytes. The normal journal path handles earlier dirty data and empty barriers.
An unsupported syscall or operation (`ENOSYS` or `EOPNOTSUPP`) falls back to
write-and-sync only when no bytes were written. Short writes and other errors
still stop the writer. Other platforms retain normal sync. This option is
off by default and can change on restart with the same journal; it changes
neither the disk format nor peer protocol and need not match across brokers.

Set `cluster.experimental_ack_and_claim: true` together with the journal
setting on every broker in a fresh test cluster to enable combined manual ACK
and delivery refill. Keep both settings on restart. This mode uses a distinct
storage marker and peer protocol identity so earlier brokers cannot open its
journals or join its Raft groups.

Enable small-body compression on every broker when provisioning a fresh
cluster. Keep the setting on restart. The mode has its own disk identity and
peer protocol. Packing requires a saving of at least 25%. AMQP clients receive
the original bytes, queue quotas use the original size, and publisher confirms
follow the journal's existing durability requirements.

For a cluster already using experimental assignments, set
`cluster.experimental_replica_bodies: true` to try reconstructing 32–128 KiB
assignment bodies from the receiving broker's journal. This setting can change
on restart with the same assignment journal. Older assignment peers and
unavailable references use full assignment pages with the original delivery
cursor. If local body verification fails, the broker logs the fault and stops
its affected shard replica before retrying through a healthy leader.

## Readiness and quorum

With [telemetry](METRICS.md) enabled, `/health` includes the broker's cluster
enrollment status. A ready broker can still encounter a shard without quorum.
Use an AMQP publish with confirms to verify that a particular queue can accept
writes. If enrollment stalls, check reachability on the advertised peer port,
the original seed list, the shared token, and the peer certificate and CA.

Restart a broker with its existing configuration and persistent directory. If
a shard loses quorum, restore access to enough of its existing voters before
expecting new commits.

## Queue placement and metadata

Every broker hashes the queue name to the same shard ID. The replicated
catalog records broker identities, addresses, and each shard's committed
replica membership. Brokers keep a routing view in
`storage.path/cluster/catalog-cache`. Each shard elects its own leader, and
brokers update their leader hints as they route requests.

StrangeQ embeds the [etcd Raft library](https://github.com/etcd-io/raft).
With its default backend, StrangeQ persists each group's log, term, vote, and
snapshots in a local bbolt `raft.db`, including a separate database for the
catalog on its replicas. With either backend, the broker persists Raft updates
before sending replication acknowledgments or reporting a committed queue
operation.

## Behavior and limits

- One voter commits to its local durable store. With two voters, both must
  respond. A three-voter shard can commit through one voter outage.
- A confirm follows a committed, durably replicated publish. A connection or
  leadership failure can leave the outcome unknown. AMQP publishers that retry
  after an unknown outcome should account for duplicate messages. Unsettled
  deliveries are requeued on session release or lease expiry.
- The default 32 shard count is fixed for a cluster's lifetime. A single strict
  FIFO queue runs on one shard. Multiple queues allow compute and storage work
  to spread across more brokers. The supported configuration range is 1–1024
  shards.
- Each data shard has a 48 MiB live payload budget and bounds on queues,
  messages, and sessions. Snapshots prune committed log prefixes and the disk
  store reuses their pages. A database file can retain its physical high-water
  size until offline compaction.
- Run the same StrangeQ build on every broker. The current build reads legacy
  JSON queue snapshots and writes CBOR queue snapshots. Earlier builds cannot
  install or reopen the new snapshots.
- Cluster mode supports ordinary queue declarations, publish, get, consume,
  ack, nack, requeue, and exchange routing in the default virtual host.
  A publish routed to several queues commits separately on each destination
  shard, so an error can leave a partial fanout.
- Cluster mode rejects transactions, exclusive or auto-delete queues, queue
  policies, named virtual hosts, and the management HTTP API. Use a separate
  empty storage directory for a new cluster. Existing standalone stores are not
  imported.
