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

Use a configuration like this for a local test.

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
Start with the ordinary command and let it enroll. If the original seed is
unavailable after the catalog has three voters, set `cluster.discovery` to the
peer addresses of reachable enrolled brokers. The immutable `seeds` still
identify the original configuration.

The membership catalog retains its first three voters after it reaches three.
Online enrollment does not replace those voters. Shards place replicas and
preferred leaders across eligible peers. A joining replica catches up as a
learner before voting. As placement converges, retired stores are removed after
persisting a replica tombstone. Never use initialization to replace a lost
original voter disk. Replacement needs a new node identity, and recovery of a
lost catalog quorum is a separate operator operation.

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
- Cluster mode supports ordinary queue declarations, publish, get, consume,
  ack, nack, requeue, and exchange routing in the default virtual host.
  A publish routed to several queues commits separately on each destination
  shard, so an error can leave a partial fanout.
- Cluster mode rejects transactions, exclusive or auto-delete queues, queue
  policies, named virtual hosts, and the management HTTP API. Use a separate
  empty storage directory for a new cluster. Existing standalone stores are not
  imported.
