package config

import "github.com/maxpert/amqp-go/storage"

// PublishInflightBudget resolves Network.PublishInflightBudgetBytes to the
// effective WS-I Phase B publish-inflight lease budget G in bytes:
//
//	 0 → 4 × storage.WALBatchSealBytesDefault (256 MiB — a batch in fsync, a
//	     staging batch, and slack; tied to the WAL byte target, not the machine)
//	−1 → disabled (the limiter grants everything, allocation-free)
//	>0 → the explicit budget (Validate requires ≥ 1 MiB)
//
// G is never re-tuned after the fact.
func (c *AMQPConfig) PublishInflightBudget() int64 {
	switch v := c.Network.PublishInflightBudgetBytes; {
	case v == 0:
		return 4 * storage.WALBatchSealBytesDefault
	case v < 0:
		return -1
	default:
		return v
	}
}
