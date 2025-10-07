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
- **Authentication**: SASL authentication with PLAIN and ANONYMOUS mechanisms
- **Security**: File-based authentication with bcrypt password hashing

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

AMQP-Go uses a comprehensive configuration system with four main sections: Network, Storage, Security, and Server. Configuration can be created programmatically using builders or loaded from JSON files.

### Default Configuration

```go
import "github.com/maxpert/amqp-go/config"

// Create server with default configuration
cfg := config.DefaultConfig()
server, err := server.NewServerBuilder().
    WithConfig(cfg).
    Build()
```

### Configuration Builder Pattern

```go
import (
    "time"
    "github.com/maxpert/amqp-go/config"
)

// Build configuration using fluent API
cfg, err := config.NewConfigBuilder().
    // Network configuration
    WithAddress(":5672").
    WithPort(5672).
    WithMaxConnections(1000).
    WithHeartbeat(60 * time.Second).
    WithBufferSizes(8192, 8192).
    
    // Storage configuration
    WithBadgerStorage("./amqp-data").
    WithCacheSize(64 * 1024 * 1024).
    WithSyncWrites(false).
    
    // Security configuration
    WithTLS("server.crt", "server.key").
    WithFileAuthentication("users.json").
    WithAuthorization(true).
    
    // Server configuration  
    WithLogging("info", "amqp.log").
    WithProtocolLimits(2047, 131072, 16777216).
    WithTimeouts(60*time.Second, 30*time.Second, 5*time.Minute).
    
    Build()
```

### Network Configuration

Controls network behavior, connections, and TCP settings.

```go
type NetworkConfig struct {
    // Basic network settings
    Address               string        // Server bind address (default: ":5672")
    Port                 int           // Server port (default: 5672)
    MaxConnections       int           // Max concurrent connections (default: 1000)
    
    // Timing and keepalive
    ConnectionTimeout    time.Duration // Connection timeout (default: 30s)
    HeartbeatInterval    time.Duration // Heartbeat interval (default: 60s)
    TCPKeepAlive         bool          // Enable TCP keepalive (default: true)
    TCPKeepAliveInterval time.Duration // Keepalive interval (default: 30s)
    
    // Performance buffers
    ReadBufferSize       int           // Read buffer size (default: 8192)
    WriteBufferSize      int           // Write buffer size (default: 8192)
}
```

**Configuration Examples:**

```go
// High-performance configuration
builder.WithBufferSizes(32768, 32768).
    WithMaxConnections(5000).
    WithHeartbeat(30 * time.Second)

// Low-resource configuration  
builder.WithBufferSizes(4096, 4096).
    WithMaxConnections(100).
    WithHeartbeat(120 * time.Second)
```

### Storage Configuration

Controls persistence, caching, and storage backend behavior.

```go
type StorageConfig struct {
    // Backend selection
    Backend      string                 // "memory", "badger" (default: "memory")
    Path         string                 // Storage directory for persistent backends
    Options      map[string]interface{} // Backend-specific options
    
    // Persistence settings
    Persistent   bool                   // Enable persistence (auto-set by backend)
    SyncWrites   bool                   // Synchronous writes (default: false)
    MessageTTL   int64                  // Message TTL in seconds (0 = no TTL)
    
    // Performance settings
    CacheSize    int64                  // Cache size in bytes (default: 64MB)
    MaxOpenFiles int                    // Max open files (default: 100)
    CompactionAge time.Duration         // Compaction age (default: 24h)
}
```

**Storage Backend Examples:**

```go
// Memory storage (development/testing)
builder.WithMemoryStorage()

// Badger storage (production)
builder.WithBadgerStorage("./data").
    WithCacheSize(128 * 1024 * 1024). // 128MB cache
    WithSyncWrites(true)               // Ensure durability

// Custom storage options
builder.WithBadgerStorage("./data").
    WithStorageOptions(map[string]interface{}{
        "ValueLogFileSize":   1<<28, // 256MB value log files
        "NumMemtables":       4,     // Number of memtables
        "NumLevelZeroTables": 2,     // Level 0 tables
    })
```

### Security Configuration

Controls TLS, authentication, and authorization.

```go
type SecurityConfig struct {
    // TLS configuration
    TLSEnabled  bool   // Enable TLS (default: false)
    TLSCertFile string // TLS certificate file path
    TLSKeyFile  string // TLS private key file path
    TLSCAFile   string // TLS CA certificate file path

    // Authentication
    AuthenticationEnabled  bool                   // Enable authentication (default: false)
    AuthenticationBackend  string                 // "file", "ldap", "database" (default: "file")
    AuthenticationConfig   map[string]interface{} // Backend-specific auth config
    AuthenticationFilePath string                 // Path to auth file for file backend (default: "./auth.json")
    AuthMechanisms         []string               // Enabled SASL mechanisms (default: ["PLAIN"])

    // Authorization
    AuthorizationEnabled bool     // Enable authorization (default: false)
    DefaultVHost        string    // Default virtual host (default: "/")

    // Access control lists
    AllowedUsers  []string // Allowed usernames
    BlockedUsers  []string // Blocked usernames
    AllowedHosts  []string // Allowed client IP addresses
    BlockedHosts  []string // Blocked client IP addresses
}
```

#### Authentication

AMQP-Go supports SASL authentication with pluggable mechanisms. Currently supported mechanisms:

**PLAIN Mechanism (RFC 4616)**
- Standard username/password authentication
- Credentials transmitted in `\0username\0password` format
- Requires TLS in production to protect credentials
- Default mechanism for secure environments

**ANONYMOUS Mechanism (RFC 4505)**
- Guest user authentication for development/testing
- No credentials required - creates guest user with minimal permissions
- Should be disabled in production environments
- Useful for local development and testing

**File-Based Authentication Backend**

The file authentication backend uses JSON files with bcrypt-hashed passwords:

```json
{
  "users": [
    {
      "username": "admin",
      "password_hash": "$2a$10$...",
      "permissions": [
        {
          "resource": ".*",
          "action": "configure",
          "pattern": ".*"
        },
        {
          "resource": ".*",
          "action": "write",
          "pattern": ".*"
        },
        {
          "resource": ".*",
          "action": "read",
          "pattern": ".*"
        }
      ],
      "groups": ["admin"],
      "metadata": {
        "created": "2024-10-07",
        "description": "Administrator account"
      }
    }
  ]
}
```

**Creating Password Hashes:**

```go
import "golang.org/x/crypto/bcrypt"

// Generate bcrypt hash for password
hash, err := bcrypt.GenerateFromPassword([]byte("password"), bcrypt.DefaultCost)
if err != nil {
    log.Fatal(err)
}
fmt.Printf("Password hash: %s\n", string(hash))
```

**Security Examples:**

```go
// Enable PLAIN authentication with TLS
cfg := config.DefaultConfig()
cfg.Security.TLSEnabled = true
cfg.Security.TLSCertFile = "server.crt"
cfg.Security.TLSKeyFile = "server.key"
cfg.Security.AuthenticationEnabled = true
cfg.Security.AuthenticationBackend = "file"
cfg.Security.AuthenticationFilePath = "./users.json"
cfg.Security.AuthMechanisms = []string{"PLAIN"}

// Development with ANONYMOUS (no auth required)
cfg := config.DefaultConfig()
cfg.Security.AuthenticationEnabled = true
cfg.Security.AuthMechanisms = []string{"ANONYMOUS"}

// Multiple mechanisms (PLAIN with ANONYMOUS fallback)
cfg := config.DefaultConfig()
cfg.Security.AuthenticationEnabled = true
cfg.Security.AuthenticationFilePath = "./users.json"
cfg.Security.AuthMechanisms = []string{"PLAIN", "ANONYMOUS"}

// Access control with authentication
cfg.Security.AuthenticationEnabled = true
cfg.Security.AuthorizationEnabled = true
cfg.Security.AllowedUsers = []string{"admin", "producer"}
cfg.Security.BlockedUsers = []string{"guest"}
```

**Authentication Integration:**

```go
package main

import (
    "log"
    "github.com/maxpert/amqp-go/auth"
    "github.com/maxpert/amqp-go/server"
    "github.com/maxpert/amqp-go/config"
)

func main() {
    // Create configuration with authentication
    cfg := config.DefaultConfig()
    cfg.Security.AuthenticationEnabled = true
    cfg.Security.AuthenticationFilePath = "./auth.json"
    cfg.Security.AuthMechanisms = []string{"PLAIN"}

    // Create file authenticator
    authenticator, err := auth.NewFileAuthenticator(cfg.Security.AuthenticationFilePath)
    if err != nil {
        log.Fatalf("Failed to create authenticator: %v", err)
    }

    // Create mechanism registry
    registry := auth.DefaultRegistry() // Includes PLAIN and ANONYMOUS

    // Create server with authentication
    srv := server.NewServerBuilder().
        WithConfig(cfg).
        WithAuthenticator(authenticator).
        WithMechanismRegistry(server.NewMechanismRegistryAdapter(registry)).
        Build()

    log.Println("Starting authenticated AMQP server")
    if err := srv.ListenAndServe(":5672"); err != nil {
        log.Fatal(err)
    }
}
```

**Default Auth File:**

If the auth file doesn't exist, AMQP-Go automatically creates one with a default guest user:
- Username: `guest`
- Password: `guest`
- Permissions: Full read/write/configure access
- Should be changed immediately in production!

### Server Configuration

Controls server behavior, limits, and operational settings.

```go
type ServerConfig struct {
    // Server identification
    Name      string // Server name (default: "amqp-go-server")
    Version   string // AMQP version (default: "0.9.1")
    Product   string // Product name (default: "AMQP-Go")
    Platform  string // Platform (default: "Go")
    Copyright string // Copyright notice
    
    // Operational settings
    LogLevel  string // Log level: debug, info, warn, error (default: "info")
    LogFile   string // Log file path (empty = stdout)
    PidFile   string // PID file path
    Daemonize bool   // Run as daemon (default: false)
    
    // Protocol limits
    MaxChannelsPerConnection int   // Max channels per connection (default: 2047)
    MaxFrameSize            int   // Max frame size bytes (default: 131072)
    MaxMessageSize          int64 // Max message size bytes (default: 16777216)
    
    // Timeouts and cleanup
    ChannelTimeout  time.Duration // Channel timeout (default: 60s)
    MessageTimeout  time.Duration // Message timeout (default: 30s)
    CleanupInterval time.Duration // Cleanup interval (default: 5m)
}
```

**Server Configuration Examples:**

```go
// Production server
builder.WithServerInfo(
    "production-amqp",     // name
    "0.9.1",              // version
    "MyCompany-AMQP",     // product
    "Go/Linux",           // platform
    "© 2024 MyCompany",   // copyright
).
WithLogging("warn", "/var/log/amqp.log").
WithDaemonize(true, "/var/run/amqp.pid").
WithProtocolLimits(4095, 262144, 33554432). // Double limits
WithTimeouts(120*time.Second, 60*time.Second, 10*time.Minute)

// Development server
builder.WithLogging("debug", "").  // Debug to stdout
    WithProtocolLimits(1024, 65536, 8388608) // Smaller limits
```

### JSON Configuration Files

Configuration can be loaded from and saved to JSON files:

```go
// Load from file
cfg := &config.AMQPConfig{}
err := cfg.Load("config.json")

// Save to file
err = cfg.Save("config.json")
```

**Example config.json:**

```json
{
  "network": {
    "address": ":5672",
    "port": 5672,
    "max_connections": 1000,
    "connection_timeout": "30s",
    "heartbeat_interval": "60s",
    "tcp_keep_alive": true,
    "tcp_keep_alive_interval": "30s",
    "read_buffer_size": 8192,
    "write_buffer_size": 8192
  },
  "storage": {
    "backend": "badger",
    "path": "./amqp-data",
    "persistent": true,
    "sync_writes": false,
    "message_ttl": 0,
    "cache_size": 67108864,
    "max_open_files": 100,
    "compaction_age": "24h"
  },
  "security": {
    "tls_enabled": true,
    "tls_cert_file": "server.crt",
    "tls_key_file": "server.key",
    "authentication_enabled": true,
    "authentication_backend": "file",
    "authentication_config": {
      "user_file": "users.json"
    },
    "authorization_enabled": true,
    "default_vhost": "/",
    "allowed_users": ["admin", "producer"],
    "blocked_users": ["guest"]
  },
  "server": {
    "name": "amqp-go-server",
    "version": "0.9.1",
    "product": "AMQP-Go",
    "platform": "Go",
    "copyright": "Maxpert AMQP-Go Server",
    "log_level": "info",
    "log_file": "",
    "max_channels_per_connection": 2047,
    "max_frame_size": 131072,
    "max_message_size": 16777216,
    "channel_timeout": "60s",
    "message_timeout": "30s",
    "cleanup_interval": "300s"
  }
}
```

### Configuration Validation

All configurations are automatically validated:

```go
cfg := config.DefaultConfig()
err := cfg.Validate() // Returns validation errors

// Common validation errors:
// - Invalid port numbers (<=0 or >65535)
// - Missing TLS files when TLS is enabled  
// - Invalid storage backend types
// - Missing storage paths for persistent backends
// - Invalid timeout or buffer size values
```

### Environment-Specific Configurations

```go
// Development configuration
devConfig := config.NewConfigBuilder().
    WithMemoryStorage().
    WithLogging("debug", "").
    WithMaxConnections(50).
    BuildUnsafe()

// Staging configuration  
stagingConfig := config.NewConfigBuilder().
    WithBadgerStorage("./staging-data").
    WithLogging("info", "staging.log").
    WithTLS("staging.crt", "staging.key").
    WithMaxConnections(500).
    BuildUnsafe()

// Production configuration
prodConfig := config.NewConfigBuilder().
    WithBadgerStorage("/var/lib/amqp").
    WithLogging("warn", "/var/log/amqp.log").
    WithTLS("prod.crt", "prod.key").
    WithFileAuthentication("/etc/amqp/users.json").
    WithMaxConnections(5000).
    WithSyncWrites(true).
    WithDaemonize(true, "/var/run/amqp.pid").
    BuildUnsafe()
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

## Monitoring and Metrics

AMQP-Go provides comprehensive Prometheus metrics for monitoring server health, performance, and operational statistics. The metrics system integrates with the Prometheus ecosystem and can be visualized using Grafana dashboards.

### Metrics Overview

The server exposes the following metric categories:

**Connection Metrics:**
- `amqp_connections_total` - Current number of active connections (Gauge)
- `amqp_connections_created_total` - Total connections created since startup (Counter)
- `amqp_connections_closed_total` - Total connections closed (Counter)

**Channel Metrics:**
- `amqp_channels_total` - Current number of active channels (Gauge)
- `amqp_channels_created_total` - Total channels created (Counter)
- `amqp_channels_closed_total` - Total channels closed (Counter)

**Queue Metrics:**
- `amqp_queues_declared_total` - Total queues declared (Counter)
- `amqp_queues_deleted_total` - Total queues deleted (Counter)
- `amqp_queue_messages_ready` - Messages ready for delivery per queue (GaugeVec with queue, vhost labels)
- `amqp_queue_messages_unacked` - Unacknowledged messages per queue (GaugeVec with queue, vhost labels)
- `amqp_queue_consumers` - Number of consumers per queue (GaugeVec with queue, vhost labels)

**Exchange Metrics:**
- `amqp_exchanges_declared_total` - Total exchanges declared (Counter)
- `amqp_exchanges_deleted_total` - Total exchanges deleted (Counter)

**Message Metrics:**
- `amqp_messages_published_total` - Total messages published (Counter)
- `amqp_messages_published_bytes_total` - Total bytes published (Counter)
- `amqp_messages_delivered_total` - Total messages delivered (Counter)
- `amqp_messages_delivered_bytes_total` - Total bytes delivered (Counter)
- `amqp_messages_acknowledged_total` - Total messages acknowledged (Counter)
- `amqp_messages_redelivered_total` - Total messages redelivered (Counter)
- `amqp_messages_rejected_total` - Total messages rejected (Counter)
- `amqp_messages_unroutable_total` - Total unroutable messages (Counter)

**Consumer Metrics:**
- `amqp_consumers_total` - Current number of active consumers (Gauge)

**Transaction Metrics:**
- `amqp_transactions_started_total` - Total transactions started (Counter)
- `amqp_transactions_committed_total` - Total transactions committed (Counter)
- `amqp_transactions_rolledback_total` - Total transactions rolled back (Counter)

**Server Metrics:**
- `amqp_server_uptime_seconds` - Server uptime in seconds (Gauge)

### Enabling Metrics

#### Basic Metrics Setup

```go
package main

import (
    "context"
    "log"
    "time"

    "github.com/maxpert/amqp-go/config"
    "github.com/maxpert/amqp-go/metrics"
    "github.com/maxpert/amqp-go/server"
)

func main() {
    // Create configuration
    cfg := config.DefaultConfig()
    cfg.Network.Address = ":5672"

    // Create metrics collector
    metricsCollector := metrics.NewCollector("amqp")

    // Create metrics HTTP server (port 9419 - standard AMQP exporter port)
    metricsServer := metrics.NewServer(9419)

    // Start metrics server
    go func() {
        log.Printf("Starting Prometheus metrics server on :%d", metricsServer.Port())
        log.Printf("Metrics available at http://localhost:%d/metrics", metricsServer.Port())
        if err := metricsServer.Start(); err != nil {
            log.Printf("Metrics server error: %v", err)
        }
    }()

    // Create AMQP server with metrics
    amqpServer := server.NewServer(cfg.Network.Address)
    amqpServer.Config = cfg
    amqpServer.MetricsCollector = metricsCollector

    // Update server uptime periodically
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    go func() {
        ticker := time.NewTicker(10 * time.Second)
        defer ticker.Stop()

        for {
            select {
            case <-ticker.C:
                uptime := time.Since(amqpServer.StartTime).Seconds()
                metricsCollector.UpdateServerUptime(uptime)
            case <-ctx.Done():
                return
            }
        }
    }()

    // Start AMQP server
    log.Printf("Starting AMQP server on %s", cfg.Network.Address)
    if err := amqpServer.Start(); err != nil {
        log.Fatalf("AMQP server error: %v", err)
    }
}
```

#### Custom Metrics Port

```go
// Use custom port for metrics endpoint
metricsServer := metrics.NewServer(8080)

// Or use the default AMQP exporter port (9419)
metricsServer := metrics.NewServer(0) // Uses default 9419
```

### Metrics Endpoints

The metrics server provides several HTTP endpoints:

- `GET /metrics` - Prometheus metrics in text format
- `GET /health` - Health check endpoint (returns 200 OK)
- `GET /` - HTML documentation page with endpoint links

### Prometheus Configuration

Add the AMQP-Go server as a scrape target in your `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: 'amqp-go'
    static_configs:
      - targets: ['localhost:9419']
    scrape_interval: 15s
    scrape_timeout: 10s
```

### Grafana Dashboard

Create a Grafana dashboard with the following sample queries:

**Connection Rate:**
```promql
rate(amqp_connections_created_total[5m])
```

**Message Throughput:**
```promql
rate(amqp_messages_published_total[5m])
```

**Queue Depth:**
```promql
amqp_queue_messages_ready
```

**Consumer Count by Queue:**
```promql
sum by (queue) (amqp_queue_consumers)
```

**Transaction Success Rate:**
```promql
rate(amqp_transactions_committed_total[5m]) /
(rate(amqp_transactions_committed_total[5m]) + rate(amqp_transactions_rolledback_total[5m]))
```

**Average Message Size:**
```promql
rate(amqp_messages_published_bytes_total[5m]) / rate(amqp_messages_published_total[5m])
```

### Alerting Rules

Example Prometheus alerting rules:

```yaml
groups:
  - name: amqp_alerts
    rules:
      - alert: HighConnectionRate
        expr: rate(amqp_connections_created_total[1m]) > 100
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High connection creation rate detected"

      - alert: QueueBacklog
        expr: amqp_queue_messages_ready > 10000
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "Queue {{ $labels.queue }} has {{ $value }} messages ready"

      - alert: HighTransactionRollbackRate
        expr: rate(amqp_transactions_rolledback_total[5m]) > rate(amqp_transactions_committed_total[5m])
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "Transaction rollback rate exceeds commit rate"

      - alert: NoConsumers
        expr: amqp_queue_messages_ready > 0 and amqp_queue_consumers == 0
        for: 15m
        labels:
          severity: warning
        annotations:
          summary: "Queue {{ $labels.queue }} has messages but no consumers"
```

### Metrics Integration

The metrics collector integrates automatically with server operations:

```go
// Metrics are automatically recorded by the server when:
// - Connections are created/closed
// - Channels are opened/closed
// - Queues are declared/deleted
// - Exchanges are declared/deleted
// - Messages are published/delivered/acknowledged
// - Transactions are started/committed/rolled back

// You can also manually record metrics:
metricsCollector.RecordMessagePublished(messageSize)
metricsCollector.RecordQueueDeclared()
metricsCollector.UpdateQueueMetrics("my-queue", "/", readyCount, unackedCount, consumerCount)
```

### Disabling Metrics

To run the server without metrics collection:

```go
// Don't set MetricsCollector field - server will use NoOpMetricsCollector
amqpServer := server.NewServer(cfg.Network.Address)
amqpServer.Config = cfg
// MetricsCollector not set - no metrics collected
```

### Performance Impact

The Prometheus metrics system has minimal performance impact:
- Metrics use atomic operations for thread-safe updates
- No additional allocations for counter/gauge updates
- GaugeVec metrics use label-based indexing for O(1) lookups
- Metrics scraping is handled by separate HTTP server on different port

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