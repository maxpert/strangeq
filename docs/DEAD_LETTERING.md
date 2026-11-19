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

Use durable destination queues. Set `storage.durability` to `barrier` or
`flush`. Restart the broker with its existing data directory to recover pending
transfers.

StrangeQ persists a pending transfer before settling the source delivery. It
waits for a route and retries destinations that are unavailable, full, unbound,
or unable to store the message. Once the first route resolves, the transfer
keeps that set of destination names. StrangeQ rechecks routing on retry and
records successful delivery to each destination. Pending transfers occupy disk
space until they finish.

A crash between storing a destination copy and recording its completion can
deliver a duplicate. Recovery can also replay a persistent source message.
Consumers should account for duplicate deliveries. Background expiry for this
strategy works from the head of the source queue.

For replicated queues, see the [cluster mode limits](CLUSTERING.md#behavior-and-limits).
