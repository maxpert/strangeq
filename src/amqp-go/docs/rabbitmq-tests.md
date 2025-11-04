# RabbitMQ Interoperability Tests

This document describes the RabbitMQ interoperability tests for the AMQP 0.9.1 protocol implementation.

## Overview

The RabbitMQ interoperability tests (`rabbitmq_interop_test.go`) validate that our AMQP protocol implementation correctly interacts with a real RabbitMQ server. These tests verify:

- Protocol header exchange
- Connection handshake (Start, StartOK, Tune, TuneOK, Open, OpenOK)
- Channel operations (opening channels)
- Queue operations (declaring queues)
- Exchange operations (declaring exchanges)
- Message publishing (Basic.Publish with content headers and body)
- Message consumption (Basic.Get)
- Heartbeat frames
- Multiple concurrent channels

## Prerequisites

To run these tests, you need:

1. **RabbitMQ Server**: A running RabbitMQ instance
2. **Network Access**: Ability to connect to the RabbitMQ server
3. **Credentials**: Valid username and password (default: guest/guest)

## Quick Start with Docker

The easiest way to run these tests is using Docker:

```bash
# Start RabbitMQ with management interface
docker run -d --name rabbitmq-test \
  -p 5672:5672 \
  -p 15672:15672 \
  rabbitmq:3-management

# Wait for RabbitMQ to be ready (about 10 seconds)
sleep 10

# Run the tests
go test -v -run TestRabbitMQ

# Stop and remove the container when done
docker stop rabbitmq-test
docker rm rabbitmq-test
```

## Configuration

The tests use environment variables for configuration:

| Variable | Description | Default |
|----------|-------------|---------|
| `RABBITMQ_HOST` | RabbitMQ hostname | `localhost` |
| `RABBITMQ_PORT` | RabbitMQ port | `5672` |
| `RABBITMQ_USER` | Username for authentication | `guest` |
| `RABBITMQ_PASS` | Password for authentication | `guest` |
| `RABBITMQ_VHOST` | Virtual host to connect to | `/` |
| `SKIP_RABBITMQ_TESTS` | Set to `1` to skip these tests | (empty) |

### Example with Custom Configuration

```bash
# Test against a remote RabbitMQ server
RABBITMQ_HOST=rabbitmq.example.com \
RABBITMQ_PORT=5672 \
RABBITMQ_USER=testuser \
RABBITMQ_PASS=testpass \
go test -v -run TestRabbitMQ
```

### Skipping Tests

If you don't have RabbitMQ available or want to skip these tests:

```bash
# Skip RabbitMQ interop tests
SKIP_RABBITMQ_TESTS=1 go test -v
```

The tests will also automatically skip if they cannot connect to RabbitMQ.

## Test Coverage

The test suite includes the following tests:

### 1. TestRabbitMQ_ProtocolHeader
Tests the initial protocol header exchange (`AMQP\0\0\9\1`).

### 2. TestRabbitMQ_ConnectionHandshake
Tests the complete connection handshake:
- Send protocol header
- Receive Connection.Start
- Send Connection.StartOK with PLAIN authentication
- Receive Connection.Tune
- Send Connection.TuneOK
- Send Connection.Open
- Receive Connection.OpenOK

### 3. TestRabbitMQ_ChannelOpen
Tests opening a channel after establishing a connection.

### 4. TestRabbitMQ_QueueDeclare
Tests declaring a queue:
- Opens a channel
- Declares an exclusive, auto-delete queue
- Verifies Queue.DeclareOK response

### 5. TestRabbitMQ_ExchangeDeclare
Tests declaring an exchange:
- Opens a channel
- Declares a direct exchange
- Verifies Exchange.DeclareOK response

### 6. TestRabbitMQ_BasicPublish
Tests publishing a message:
- Opens a channel
- Declares a queue
- Publishes a message with content header and body frames
- Verifies message is accepted

### 7. TestRabbitMQ_BasicGet
Tests retrieving a message:
- Opens a channel
- Declares a queue
- Publishes a message
- Retrieves the message using Basic.Get
- Verifies message body matches

### 8. TestRabbitMQ_Heartbeat
Tests sending heartbeat frames.

### 9. TestRabbitMQ_MultipleChannels
Tests opening multiple channels concurrently.

## Running Specific Tests

You can run individual tests using the `-run` flag:

```bash
# Run only the connection handshake test
go test -v -run TestRabbitMQ_ConnectionHandshake

# Run only publishing and consumption tests
go test -v -run "TestRabbitMQ_(BasicPublish|BasicGet)"
```

## Debugging

### Enable Verbose Output

```bash
# Run tests with verbose output
go test -v -run TestRabbitMQ
```

### Check RabbitMQ Logs

If tests are failing, check the RabbitMQ server logs:

```bash
# For Docker
docker logs rabbitmq-test

# For system installation (varies by OS)
# Ubuntu/Debian
sudo tail -f /var/log/rabbitmq/rabbit@*.log

# macOS (Homebrew)
tail -f /usr/local/var/log/rabbitmq/rabbit@*.log
```

### RabbitMQ Management Interface

If using the management plugin, you can monitor connections and channels:

```
http://localhost:15672
Username: guest
Password: guest
```

## CI/CD Integration

For automated testing in CI/CD pipelines:

```yaml
# Example GitHub Actions workflow
- name: Start RabbitMQ
  run: |
    docker run -d --name rabbitmq-test -p 5672:5672 rabbitmq:3
    sleep 10

- name: Run RabbitMQ interop tests
  run: go test -v -run TestRabbitMQ
  working-directory: ./src/amqp-go/protocol

- name: Stop RabbitMQ
  run: docker stop rabbitmq-test
```

## Troubleshooting

### Connection Refused

If you see `connection refused` errors:
1. Verify RabbitMQ is running: `docker ps` or `systemctl status rabbitmq-server`
2. Check the port is correct (default: 5672)
3. Ensure no firewall is blocking the connection

### Authentication Failed

If you see authentication errors:
1. Verify credentials are correct (default: guest/guest)
2. Note that the `guest` user can only connect from localhost by default
3. For remote connections, create a new user with appropriate permissions

### Timeout Errors

If tests timeout:
1. Increase the test timeout: `go test -timeout 30s -run TestRabbitMQ`
2. Check if RabbitMQ is overloaded or slow to respond
3. Verify network latency is acceptable

## Implementation Notes

These tests use the low-level protocol package directly, without any high-level client abstractions. They:

- Manually construct AMQP frames using the protocol types
- Serialize methods using the Serialize() functions
- Parse responses by reading frame payloads
- Handle all AMQP handshake steps explicitly

This approach validates that the protocol implementation is correct at the lowest level and compatible with RabbitMQ's AMQP 0.9.1 implementation.
