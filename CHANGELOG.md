# Changelog

## [Unreleased]

## [0.1.0] - TBD

### Added
- Full AMQP 0.9.1 protocol implementation
- Exchange types: direct, fanout, topic, headers
- Exchange-to-exchange bindings with cycle detection
- Three-tier storage architecture (ring buffer, WAL, segments)
- Persistent and non-persistent messages with crash recovery
- TLS/SSL encryption with mutual TLS support
- SASL authentication (PLAIN, ANONYMOUS)
- Per-vhost authorization (RabbitMQ-style configure/write/read permissions)
- Publisher confirms (`confirm.select`)
- Transactions (`tx.select` / `tx.commit` / `tx.rollback`)
- `basic.get`, `basic.return`, `basic.recover`
- Message and queue TTL (`x-message-ttl`, per-message `expiration`, `x-expires`)
- Dead-letter exchanges with full `x-death` / `x-first-death-*` headers
- Queue length limits (`x-max-length`, `x-max-length-bytes`, `x-overflow`)
- Resource alarms (memory and disk watermarks, `connection.blocked`/`unblocked`)
- Prometheus metrics and pprof profiling
- Lock-free per-queue ring buffer with per-slot CAS
- Batch TCP writes and ACK offloading
- Vectored delivery for large messages (writev)

### Performance
- 108,000 msg/s durable (1 pub / 1 con, 1 KB, publisher confirms, fsync)
- 113,667 msg/s durable (10 pub / 10 con)
- 1.10x to 4.48x faster than RabbitMQ 4.3 on durable workloads

[Unreleased]: https://github.com/maxpert/strangeq/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/maxpert/strangeq/releases/tag/v0.1.0
