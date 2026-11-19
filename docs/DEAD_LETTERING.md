# Dead-letter exchanges

Standalone queues can route rejected, expired, or evicted messages to a
dead-letter exchange. Set `x-dead-letter-exchange` on the source queue and bind
the destination queues to that exchange. Set `x-dead-letter-routing-key` to
override the original routing key.

## At-least-once transfers

The default strategy is `at-most-once`. To persist pending transfers and retry
when a destination is unavailable, declare a durable source queue with these
arguments.

| Queue argument | Value |
| --- | --- |
| `x-dead-letter-exchange` | Name of the dead-letter exchange |
| `x-dead-letter-strategy` | `at-least-once` |
| `x-overflow` | `reject-publish` |

Use durable destination queues. On a standalone broker, set
`storage.durability` to `barrier` or `flush`. Restart the broker with its
existing data directory to recover pending transfers.

StrangeQ persists a pending transfer before settling the source delivery. It
waits for a route and retries destinations that are unavailable, full, unbound,
or unable to store the message. Once the first route resolves, the transfer
keeps that set of destination names. StrangeQ rechecks routing on retry and
records successful delivery to each destination. Pending transfers occupy disk
space until they finish.

A crash between storing a destination copy and recording its completion can
deliver a duplicate. On standalone brokers, recovery can also replay a
persistent source message, and background expiry works from the head of the
source queue.

## Replicated queues

Declare the source and destinations as durable queues with the same arguments
shown above. Cluster mode supports at-least-once dead-lettering for
`basic.reject` and `basic.nack` with `requeue=false`, including multiple
negative acknowledgments. Clustered TTL and queue-length policies are
unavailable. Upgrade all brokers together. Cluster stores opened by this
build require brokers that support replicated dead-lettering.

A rejection commits the pending transfer in the source shard. Its current
leader resumes transfers after failover or restart, including rejections of
messages originally published as transient. A same-shard target and its
completion commit together. For targets on other shards, a failure between
destination commit and the source recording completion can lead to a duplicate.

Admission on a dead-letter source reserves space for the transfer and one
same-shard copy. Pending transfers continue to count against the source
shard's capacity until all planned destinations commit. Missing exchanges,
missing bindings, and unavailable or full targets can delay completion. An
exchange name of `""` selects the default exchange. To use it, make the routing
key name the destination queue.

See the [cluster setup and limits](CLUSTERING.md#behavior-and-limits).
