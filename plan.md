# Plan: Implement AMQP 0.9.1 Server in Go

## Goal
Create a Go package `github.com/maxpert/amqp-go` that implements an AMQP 0.9.1 server based on the specification: https://www.rabbitmq.com/resources/specs/amqp0-9-1.extended.xml

## Current Status (Updated: 2025-01-10)
**Phase 3 - Message Publishing and Consumption: MAJOR PROGRESS COMPLETED** ✅

### Key Achievements:
- ✅ **Core AMQP functionality working**: Message publishing and consumption implemented
- ✅ **Race condition resolved**: Messages published before consumers register are properly queued
- ✅ **Message routing operational**: Default exchange behavior and direct queue delivery working
- ✅ **Full message transmission**: Server correctly processes and delivers complete message content
- ✅ **Consumer management**: Proper consumer registration, delivery loops, and acknowledgments
- ✅ **Broker architecture**: Comprehensive message queuing and delivery system implemented
- ✅ **Protocol methods**: `basic.publish`, `basic.consume`, `basic.ack`, etc. fully implemented

### Current Focus:
- 🔧 **Protocol compatibility**: Resolving frame format issues with standard AMQP clients
- 🔧 **Connection cleanup**: Fixing channel/connection closure protocol handling

**Next Phase:** Complete Phase 3 client compatibility, then move to Phase 4 (Persistence)

## Phases

### Phase 1: Project Setup and Core Protocol Foundation
- [x] Initialize Go module `github.com/maxpert/amqp-go`
- [x] Define core data structures (Connection, Channel, Exchange, Queue, Message) matching the spec
- [x] Implement binary frame reader/writer based on the spec (using `encoding/binary`)
- [x] Parse the XML spec to auto-generate protocol constants/methods (optional but helpful)
- [x] Implement basic TCP server accepting connections on standard AMQP ports (5672, 5671)
- [x] Handle the initial handshake (connection negotiation, protocol headers)
- [x] Implement the `connection.*` and `channel.*` methods (open, close, basic flow)
- [x] Simple in-memory storage for connections and channels (using `sync.Map` or similar)
- [x] Write unit tests for core protocol parsing and connection/channel management
- [x] Implement proper logging using zap logger

### Phase 1.5: Housekeeping and Deployment Preparation
- [x] Create build script in root directory
- [x] Set up bin directory for binary output
- [x] Clean up current directory from junk files (logs, binaries, temp files)
- [x] Add command-line flags for log levels and output files
- [x] Add daemonize flag for server deployment
- [x] Create proper server binary with configuration options

### Phase 2: Exchange and Queue Management
- [x] Implement `exchange.declare` and `exchange.delete` methods
- [x] Implement `queue.declare`, `queue.bind`, `queue.unbind`, `queue.delete` methods
- [x] Implement core exchange types (direct, fanout, topic, headers) with routing logic
- [x] Basic in-memory queue implementation (FIFO)
- [x] Basic binding mechanism
- [x] Write tests for exchange and queue operations

### Phase 3: Message Publishing and Consumption
- [x] Implement `basic.publish` method (accepting message content and routing)
- [x] Implement `basic.consume`, `basic.cancel`, `basic.get` methods
- [x] Implement message delivery logic (to consumers)
- [x] Implement basic acknowledgments (`basic.ack`, `basic.nack`, `basic.reject`)
- [x] Handle pre-fetching and flow control
- [x] Write tests for publish/consume with acknowledgments
- [x] Fix race condition for messages published before consumers register
- [x] Implement broker message queuing and delivery system
- [x] Create consumer delivery loop with proper message routing
- [ ] Fix protocol compatibility issues during connection cleanup
- [ ] Complete integration test passes with standard AMQP clients

### Phase 4: Persistence and Reliability
- [ ] Choose and integrate a storage backend (e.g., bbolt, badger) for messages and metadata
- [ ] Implement message persistence (durability, persistence flags)
- [ ] Implement durable exchanges and queues
- [ ] Ensure message durability across server restarts
- [ ] Write integration tests covering persistence scenarios

### Phase 5: Advanced Features and Security
- [ ] Implement transactions (optional for 0.9.1 but part of spec)
- [ ] Implement access control (vhosts, permissions, authentication - maybe start with simple PLAIN)
- [ ] Implement TLS/SSL support
- [ ] Implement basic monitoring/management (optional stats)
- [ ] Write tests for security and advanced features

### Phase 6: Testing and Refinement
- [ ] Comprehensive integration tests using AMQP clients (streadway/amqp, rabbitmq/amqp091-go)
- [ ] Fuzz testing for protocol parsing
- [ ] Performance benchmarking
- [ ] Stress test with multiple concurrent clients
- [ ] Documentation (Go doc, README, examples)
- [ ] Prepare for release (versioning, release notes)

## Outstanding Questions

1.  **Persistence Strategy:** Which embedded database/library should be used for initial implementation? `bbolt` or `badger`?
A. Pick the most well maintained and high performing

2.  **Spec Parsing:** Should the XML spec be parsed at runtime to generate constants/methods, or should structures be manually defined based on the spec?
A. Follow what makes performance better

3.  **Concurrency Model:** How should connections/channels be managed using goroutines and channels? What are the locking strategies for shared state (exchanges, queues)?
A. Pick memory optimized high performance strategy

4.  **Client Compatibility:** Which existing AMQP Go clients will be used for testing compatibility throughout development?
A. Pick the most well maintained

5.  **Authentication:** What is the initial scope for authentication (e.g., hardcoded users, file-based, plugin system)?
A. Filebased

6.  **TLS/SSL:** How will TLS certificates be configured (file paths, options)?
A. file paths

7.  **Clustering/Federation:** Is clustering a goal for the initial release, or a future phase?
A. Keep it simple but write code structured for future phases keep it extensible as much as possible

8.  **AMQP 1.0:** Is there any intention to support AMQP 1.0, or strictly 0.9.1?
A. In future yes

9.  **Performance Targets:** Are there specific performance benchmarks or goals to aim for (e.g., messages/second, concurrent connections)?
A. Messages/second

10. **Logging:** What logging library and approach will be used?
A. Pick no overhead performance libraries like zerolog. Search for best ones available

## Dependencies
- Standard Go libraries (`net`, `encoding/binary`, `sync`, `context`, etc.)
- Potential embedded DB library (e.g., `go.etcd.io/bbolt`, `github.com/dgraph-io/badger`)
- Optional: XML parsing library if spec parsing is automated
- Test dependencies: Standard `testing` package, potentially `testify` for assertions
- Client libraries for testing (e.g., `github.com/streadway/amqp`, `github.com/rabbitmq/amqp091-go`)

## Initial Steps
1.  Create the repository and initial module structure.
2.  Download and examine the XML spec file.
3.  Decide on persistence and spec parsing strategies (Q1, Q2).
4.  Start with Phase 1: Project setup and core protocol structures.