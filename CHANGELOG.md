# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- GitHub Actions workflow for automated multi-platform releases
- Comprehensive build automation for macOS (arm64, amd64), Linux (amd64, arm64, 386)
- Issue and PR templates for better contribution workflow

## [0.1.0] - TBD

### Added
- Full AMQP 0.9.1 protocol implementation
- Connection and channel management
- Exchange operations (direct, fanout, topic, headers)
- Queue operations with durability support
- Message publishing and consumption
- Transaction support (tx.select, tx.commit, tx.rollback)
- Multiple storage backends:
  - In-memory storage for development
  - Badger-based persistent storage
- Authentication framework:
  - SASL PLAIN mechanism
  - SASL ANONYMOUS mechanism
  - File-based authentication with bcrypt
- Prometheus metrics integration
- Comprehensive configuration system
- Builder pattern API for server construction
- Recovery and durability features
- Performance optimizations:
  - Buffer pooling (sync.Pool)
  - Optimized frame operations
  - Zero-allocation hot paths
- Extensive test coverage:
  - Protocol compliance tests
  - RabbitMQ interoperability tests
  - Fuzz testing
  - Performance benchmarks

### Performance
- Memory storage: 3M+ operations/sec
- Message throughput: Up to 3.6 GB/s
- Frame processing: 24-65 million ops/sec
- Protocol operations: 15-140 ns/op

### Documentation
- Complete README with examples
- Configuration guide
- Authentication setup guide
- Metrics and monitoring guide
- Performance documentation
- RabbitMQ test documentation

### Infrastructure
- Modular package structure
- Interface-based design
- Comprehensive error handling
- Graceful shutdown support
- PID file management
- Daemon mode support

[Unreleased]: https://github.com/maxpert/amqp-go/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/maxpert/amqp-go/releases/tag/v0.1.0

