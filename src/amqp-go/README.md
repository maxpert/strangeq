# AMQP-Go Server

A Go implementation of an AMQP 0.9.1 server based on the official specification.

## Current Status

This is an early implementation focusing on Phase 1 and Phase 1.5 of our development plan:

- [x] Project setup and module initialization
- [x] Core data structures (Connection, Channel, Exchange, Queue, Message)
- [x] Binary frame reader/writer implementation
- [x] Basic TCP server accepting connections
- [x] Protocol header verification
- [x] Initial connection handshake (connection.start, connection.start-ok, connection.tune, connection.tune-ok, connection.open, connection.open-ok)
- [x] Channel management (channel.open, channel.open-ok)
- [x] Simple in-memory storage for connections and channels
- [x] Unit tests for core protocol parsing and connection/channel management
- [x] Proper logging implementation using zap
- [x] Build script for easy compilation
- [x] Binary output to bin directory
- [x] Command-line flags for configuration (address, log level, log file, daemonize)
- [x] Proper server binary with configuration options

## Architecture

The server is organized into several packages:

- `protocol/`: Contains frame encoding/decoding, method definitions, and core data structures
- Root directory: Contains the main server implementation and entry point

## Usage

To run the server:

```bash
go run .
```

The server will start listening on port 5672 (the standard AMQP port).

## Development

This project is in early development. We're following the AMQP 0.9.1 specification closely and implementing features incrementally according to our development plan.

## License

MIT