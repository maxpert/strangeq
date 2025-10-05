# AMQP-Go Server

A high-performance AMQP 0.9.1 server implementation written in Go, providing full message broker functionality with both in-memory and persistent storage backends.

## Features

### Core AMQP 0.9.1 Support
- **Connection Management**: Full connection negotiation, heartbeat, and lifecycle management
- **Channel Management**: Multi-channel support with proper flow control
- **Exchange Operations**: Support for direct, fanout, topic, and headers exchanges
- **Queue Operations**: Durable and non-durable queues with auto-delete support
- **Message Routing**: Complete routing logic for all exchange types with binding support
- **Message Publishing**: Reliable message publishing with persistence options
- **Message Consumption**: Basic.Get and Basic.Consume with acknowledgment support
- **Transaction Support**: Full AMQP transaction support with commit/rollback

### Advanced Features
- **Persistence**: Badger-based persistent storage for messages, metadata, and acknowledgments
- **Durability**: Message durability with DeliveryMode=2 support (AMQP compliant)
- **Recovery**: Automatic recovery of durable exchanges, queues, and pending acknowledgments on startup
- **Atomic Operations**: Transaction-based atomic operations across all storage components
- **Storage Validation**: Integrity validation and auto-repair of storage on startup
- **TTL Support**: Configurable message time-to-live with automatic cleanup
- **High Concurrency**: Optimized for concurrent operations with minimal locking

### Storage Backends
- **Memory**: High-performance in-memory storage for development and testing
- **Badger**: Embedded key-value store for production persistence with excellent performance

## Performance Benchmarks

Performance tests conducted on Apple M4 Max (16 cores):

### Memory Storage Performance
```
Operation                           | Operations/sec | ns/op    | Allocations
-----------------------------------|----------------|----------|-------------
Exchange Declaration               | 3,233,268      | 309.2    | 317 B/op
Queue Declaration                  | 2,798,751      | 357.2    | 297 B/op  
Message Publishing                 | 2,903,255      | 344.4    | 700 B/op
```

### Badger Storage Performance (Persistent)
```
Operation                           | Operations/sec | ns/op      | Allocations
-----------------------------------|----------------|------------|-------------
Durable Exchange Declaration       | 68             | 14.69ms    | 27.6 MB/op
Durable Queue Declaration          | 34             | 29.60ms    | 52.9 MB/op
Persistent Message Publishing      | 15,108         | 66.18μs    | 110 KB/op
```

### Message Throughput by Size
```
Message Size    | Throughput      | Operations/sec | ns/op
----------------|-----------------|----------------|--------
100 B          | 346.49 MB/s     | 3,633,119      | 275.2
1 KB           | 3,460.79 MB/s   | 3,544,018      | 282.2
10 KB          | 34,068.12 MB/s  | 3,488,372      | 286.7
100 KB         | 342,967.97 MB/s | 3,512,281      | 284.7
1 MB           | 3,596,523.99 MB/s| 3,597,122     | 278.0
```

### Concurrent Operations
```
Operation                        | Operations/sec | ns/op    | Allocations
--------------------------------|----------------|----------|-------------
Concurrent Publishing           | 2,471,473      | 404.6    | 736 B/op
Concurrent Consumer Registration| 573,065        | 1745     | 2.1 KB/op
```

### Exchange Type Performance
```
Exchange Type   | Operations/sec | ns/op  | Allocations
----------------|----------------|--------|-------------
Direct          | 2,759,358      | 362.4  | 703 B/op
Fanout          | 2,712,042      | 368.7  | 703 B/op
Topic           | 2,737,362      | 365.3  | 702 B/op
Headers         | 1,976,379      | 505.9  | 992 B/op
```

### Storage Operations
```
Operation          | Operations/sec | ns/op    | Allocations
-------------------|----------------|----------|-------------
Message Storage    | 185,197        | 5,398    | 2.0 KB/op
Pending Ack Storage| 181,234        | 5,518    | 3.0 KB/op
Atomic Operations  | 137,996        | 7,248    | 3.1 KB/op
```

## Supported AMQP Commands

### Connection Class
- `Connection.Start` - Connection negotiation initiation
- `Connection.StartOk` - Connection parameters response
- `Connection.Tune` - Connection tuning parameters
- `Connection.TuneOk` - Tuning parameters acknowledgment
- `Connection.Open` - Virtual host connection
- `Connection.OpenOk` - Connection confirmation
- `Connection.Close` - Connection termination
- `Connection.CloseOk` - Close acknowledgment

### Channel Class
- `Channel.Open` - Channel creation
- `Channel.OpenOk` - Channel creation confirmation
- `Channel.Close` - Channel termination
- `Channel.CloseOk` - Channel close acknowledgment
- `Channel.Flow` - Flow control
- `Channel.FlowOk` - Flow control acknowledgment

### Exchange Class
- `Exchange.Declare` - Exchange creation/configuration
- `Exchange.DeclareOk` - Exchange declaration confirmation
- `Exchange.Delete` - Exchange deletion
- `Exchange.DeleteOk` - Exchange deletion confirmation
- `Exchange.Bind` - Exchange to exchange binding
- `Exchange.BindOk` - Exchange binding confirmation
- `Exchange.Unbind` - Exchange unbinding
- `Exchange.UnbindOk` - Exchange unbinding confirmation

### Queue Class
- `Queue.Declare` - Queue creation/configuration
- `Queue.DeclareOk` - Queue declaration confirmation
- `Queue.Bind` - Queue to exchange binding
- `Queue.BindOk` - Queue binding confirmation
- `Queue.Purge` - Queue message purging
- `Queue.PurgeOk` - Queue purge confirmation
- `Queue.Delete` - Queue deletion
- `Queue.DeleteOk` - Queue deletion confirmation
- `Queue.Unbind` - Queue unbinding
- `Queue.UnbindOk` - Queue unbinding confirmation

### Basic Class
- `Basic.Qos` - Quality of service settings
- `Basic.QosOk` - QoS confirmation
- `Basic.Consume` - Consumer registration
- `Basic.ConsumeOk` - Consumer registration confirmation
- `Basic.Cancel` - Consumer cancellation
- `Basic.CancelOk` - Consumer cancellation confirmation
- `Basic.Publish` - Message publishing
- `Basic.Return` - Unroutable message return
- `Basic.Deliver` - Message delivery to consumer
- `Basic.Get` - Single message retrieval
- `Basic.GetOk` - Message retrieval confirmation
- `Basic.GetEmpty` - No message available response
- `Basic.Ack` - Message acknowledgment
- `Basic.Reject` - Message rejection
- `Basic.RecoverAsync` - Asynchronous recovery
- `Basic.Recover` - Message recovery
- `Basic.RecoverOk` - Recovery confirmation
- `Basic.Nack` - Negative acknowledgment

### Transaction Class
- `Tx.Select` - Transaction mode selection
- `Tx.SelectOk` - Transaction mode confirmation
- `Tx.Commit` - Transaction commit
- `Tx.CommitOk` - Commit confirmation
- `Tx.Rollback` - Transaction rollback
- `Tx.RollbackOk` - Rollback confirmation

## Installation

```bash
go get github.com/maxpert/amqp-go
```

## Quick Start

### Basic Server Setup

```go
package main

import (
    "log"
    "github.com/maxpert/amqp-go/server"
    "github.com/maxpert/amqp-go/config"
)

func main() {
    // Create server with memory storage
    amqpServer := server.NewServerBuilder().
        WithConfig(config.DefaultConfig()).
        Build()
    
    log.Println("Starting AMQP server on :5672")
    if err := amqpServer.ListenAndServe(":5672"); err != nil {
        log.Fatal(err)
    }
}
```

### Persistent Storage Setup

```go
package main

import (
    "log"
    "github.com/maxpert/amqp-go/server"
    "github.com/maxpert/amqp-go/config"
)

func main() {
    cfg := config.DefaultConfig()
    cfg.Storage.Backend = "badger"
    cfg.Storage.Path = "./amqp-data"
    cfg.Storage.MessageTTL = 86400 // 24 hours
    
    amqpServer := server.NewServerBuilder().
        WithConfig(cfg).
        Build()
    
    log.Println("Starting AMQP server with persistent storage")
    if err := amqpServer.ListenAndServe(":5672"); err != nil {
        log.Fatal(err)
    }
}
```

### Transaction Support

AMQP-Go provides complete AMQP 0.9.1 transaction support with a pluggable architecture for extensibility.

```go
package main

import (
    "fmt"
    "log"
    
    "github.com/maxpert/amqp-go/server"
    "github.com/maxpert/amqp-go/config"
    "github.com/maxpert/amqp-go/protocol"
    "github.com/maxpert/amqp-go/transaction"
)

func main() {
    // Create server with transaction support
    cfg := config.DefaultConfig()
    cfg.Storage.Backend = "memory" // or "badger" for persistence
    
    amqpServer, err := server.NewServerBuilder().
        WithConfig(cfg).
        WithZapLogger("info").
        Build()
    if err != nil {
        log.Fatalf("Failed to create server: %v", err)
    }
    
    // Set up broker resources
    err = setupBrokerResources(amqpServer)
    if err != nil {
        log.Fatalf("Failed to setup resources: %v", err)
    }
    
    // Demonstrate transaction operations
    demoTransactions(amqpServer)
}

func setupBrokerResources(server *server.Server) error {
    // Create exchange
    err := server.Broker.DeclareExchange("demo.exchange", "direct", true, false, false, nil)
    if err != nil {
        return err
    }
    
    // Create queue and bind to exchange
    _, err = server.Broker.DeclareQueue("demo.queue", true, false, false, nil)
    if err != nil {
        return err
    }
    
    return server.Broker.BindQueue("demo.queue", "demo.exchange", "demo.key", nil)
}

func demoTransactions(server *server.Server) {
    tm := server.TransactionManager
    channelID := uint16(1)
    
    // Start transaction
    err := tm.Select(channelID)
    if err != nil {
        log.Printf("Failed to start transaction: %v", err)
        return
    }
    fmt.Println("✅ Transaction started")
    
    // Add operations to transaction
    message := &protocol.Message{
        Exchange:     "demo.exchange",
        RoutingKey:   "demo.key", 
        Body:         []byte("Transactional message"),
        DeliveryMode: 2, // Persistent
    }
    
    publishOp := transaction.NewPublishOperation("demo.exchange", "demo.key", message)
    err = tm.AddOperation(channelID, publishOp)
    if err != nil {
        log.Printf("Failed to add operation: %v", err)
        return
    }
    fmt.Println("✅ Added publish operation to transaction")
    
    // Commit transaction
    err = tm.Commit(channelID)
    if err != nil {
        log.Printf("Transaction commit failed: %v", err)
        return
    }
    fmt.Println("✅ Transaction committed successfully!")
    
    // Get transaction statistics
    stats := tm.GetTransactionStats()
    fmt.Printf("📊 Statistics: Commits=%d, Rollbacks=%d, Active=%d\n", 
        stats.TotalCommits, stats.TotalRollbacks, stats.ActiveTransactions)
}
```

### Transaction Features

- **AMQP 0.9.1 Compliance**: Full support for `tx.select`, `tx.commit`, and `tx.rollback`
- **Pluggable Architecture**: Extensible transaction system with interface-based design
- **Channel Isolation**: Transactions are isolated per channel for concurrent operations
- **Operation Types**: Support for Publish, Ack, Nack, and Reject operations
- **Atomic Execution**: All operations in a transaction succeed or fail together
- **Statistics Tracking**: Comprehensive transaction metrics and monitoring
- **Thread-Safe**: Concurrent transaction operations with proper synchronization
- **Storage Integration**: Works with both memory and persistent storage backends

### Transaction Operation Types

```go
// Publishing messages transactionally
publishOp := transaction.NewPublishOperation(exchange, routingKey, message)
tm.AddOperation(channelID, publishOp)

// Acknowledging messages transactionally  
ackOp := transaction.NewAckOperation(queueName, deliveryTag, false)
tm.AddOperation(channelID, ackOp)

// Negative acknowledgment transactionally
nackOp := transaction.NewNackOperation(queueName, deliveryTag, false, true)
tm.AddOperation(channelID, nackOp)

// Rejecting messages transactionally
rejectOp := transaction.NewRejectOperation(queueName, deliveryTag, false)
tm.AddOperation(channelID, rejectOp)
```

### Advanced Transaction Configuration

```go
// Create transaction manager with atomic storage
cfg := config.DefaultConfig()
cfg.Storage.Backend = "badger"
cfg.Storage.Path = "./transaction-data"

server, err := server.NewServerBuilder().
    WithConfig(cfg).
    Build()

// The transaction manager is automatically configured with:
// - Atomic storage support for ACID compliance
// - Unified broker executor for operation execution
// - Statistics tracking for monitoring
// - Channel-based isolation for concurrency
```

## Configuration

### Storage Configuration

```go
type StorageConfig struct {
    Backend    string `json:"backend"`      // "memory" or "badger"
    Path       string `json:"path"`         // Storage directory for Badger
    MessageTTL int    `json:"message_ttl"`  // Message TTL in seconds
}
```

### Server Configuration

```go
type AMQPConfig struct {
    Server     ServerConfig  `json:"server"`
    Storage    StorageConfig `json:"storage"`
    
    // Connection settings
    MaxChannels     int `json:"max_channels"`
    MaxFrameSize    int `json:"max_frame_size"`
    HeartbeatDelay  int `json:"heartbeat_delay"`
    
    // Performance tuning
    ReadBufferSize  int `json:"read_buffer_size"`
    WriteBufferSize int `json:"write_buffer_size"`
}
```

## Testing

### Run Unit Tests
```bash
go test ./...
```

### Run Integration Tests
```bash
go test ./tests/
```

### Run Performance Benchmarks
```bash
go test -bench=. -benchmem ./benchmarks/
```

### Run Specific Benchmark Categories
```bash
# Memory storage benchmarks
go test -bench=BenchmarkMemoryStorage -benchmem ./benchmarks/

# Badger storage benchmarks  
go test -bench=BenchmarkBadgerStorage -benchmem ./benchmarks/

# Concurrent operation benchmarks
go test -bench=BenchmarkConcurrentOperations -benchmem ./benchmarks/

# Message size performance
go test -bench=BenchmarkMessageSizes -benchmem ./benchmarks/

# Exchange type performance
go test -bench=BenchmarkExchangeTypes -benchmem ./benchmarks/

# Storage operation performance
go test -bench=BenchmarkStorageOperations -benchmem ./benchmarks/
```

## Architecture

### Storage Layer
- **Interfaces**: Abstracted storage interfaces for all components
- **Memory Implementation**: High-performance in-memory storage with concurrent access
- **Badger Implementation**: Persistent storage with embedded key-value database
- **Composite Storage**: Unified storage interface combining all storage types
- **Transaction Support**: Atomic operations across multiple storage backends

### Broker Layer
- **Storage Broker**: Persistent message broker with durability support
- **Memory Broker**: High-performance in-memory message broker
- **Unified Interface**: Common interface for all broker implementations

### Protocol Layer
- **Frame Processing**: Binary AMQP frame encoding/decoding
- **Method Handling**: Complete AMQP method implementation
- **Connection Management**: Connection lifecycle and heartbeat handling
- **Channel Management**: Multi-channel support with proper isolation

### Recovery System
- **Storage Validation**: Integrity checking and corruption repair
- **Entity Recovery**: Automatic restoration of durable exchanges and queues
- **Acknowledgment Recovery**: Restoration of pending acknowledgments
- **Statistics**: Comprehensive recovery statistics and error reporting

## Development

### Building
```bash
go build ./...
```

### Running Examples

The `examples/` directory contains various demonstration programs:

```bash
# Run basic transaction demo
go run examples/transaction_demo.go

# Run complete transaction flow demo with broker setup
go run examples/transaction_complete_demo.go
```

### Code Formatting
```bash
go fmt ./...
```

### Linting
```bash
golangci-lint run
```

### Project Structure
```
├── benchmarks/          # Performance benchmarks
├── broker/             # Message broker implementations
├── config/             # Configuration management
├── examples/           # Usage examples and demos
├── interfaces/         # Interface definitions
├── protocol/           # AMQP protocol implementation
├── server/             # Server implementation
├── storage/            # Storage backend implementations
├── tests/              # Integration tests
└── transaction/        # Transaction system implementation
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Run the full test suite
6. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Performance Notes

- **Memory Backend**: Optimal for development, testing, and high-throughput scenarios where persistence isn't required
- **Badger Backend**: Recommended for production deployments requiring message persistence and durability
- **Concurrent Performance**: Server is optimized for high-concurrency workloads with minimal lock contention
- **Message Size**: Performance scales well with message size up to 1MB+
- **Storage Operations**: Atomic operations provide consistency guarantees with acceptable performance overhead

## Acknowledgments

- Built following the official AMQP 0.9.1 specification
- Uses Badger embedded database for high-performance persistence
- Designed with Go concurrency patterns for optimal performance