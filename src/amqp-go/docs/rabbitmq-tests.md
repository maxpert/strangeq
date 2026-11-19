# RabbitMQ Interoperability Tests

This document describes the RabbitMQ interoperability tests for the AMQP 0.9.1 protocol implementation.

## Overview

The [RabbitMQ interoperability tests](../protocol/rabbitmq_interop_test.go)
exercise low-level AMQP frame serialization and parsing against a real RabbitMQ
server. They do not start StrangeQ or establish full broker conformance. They cover

- Protocol header exchange
- Connection handshake (Start, StartOK, Tune, TuneOK, Open, OpenOK)
- Channel operations (opening channels)
- Queue operations (declaring queues)
- Exchange operations (declaring exchanges)
- Message publishing (Basic.Publish with content headers and body)
- Message consumption (Basic.Get)
- Heartbeat frames
- Multiple channels on one connection

## Prerequisites

To run these tests, you need:

1. **RabbitMQ Server**: A running RabbitMQ instance
2. **Network Access**: Ability to connect to the RabbitMQ server
3. **Credentials**: Valid username and password (default: guest/guest)

## Quick Start with Docker

Run these commands from the Go module root (`src/amqp-go`). Choose host ports
that do not conflict with a local StrangeQ or RabbitMQ instance. The Docker
example binds only to loopback and creates an explicit test user.

```bash
# Start RabbitMQ with management interface
docker run -d --name rabbitmq-test \
  -p 127.0.0.1:5672:5672 \
  -p 127.0.0.1:15672:15672 \
  -e RABBITMQ_DEFAULT_USER=interop \
  -e RABBITMQ_DEFAULT_PASS=local-test-only \
  rabbitmq:4-management

# Wait up to two minutes for the broker, failing if it never becomes ready.
ready=false
for attempt in $(seq 1 60); do
  if docker exec rabbitmq-test rabbitmq-diagnostics -q check_running; then
    ready=true
    break
  fi
  sleep 2
done
if [ "$ready" != true ]; then
  docker logs rabbitmq-test
  exit 1
fi

# Disable test-result caching so this run actually contacts RabbitMQ.
RABBITMQ_USER=interop RABBITMQ_PASS=local-test-only \
  go test -count=1 -v -run TestRabbitMQ ./protocol/

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
go test -count=1 -v -run TestRabbitMQ ./protocol/
```

### Skipping Tests

If you don't have RabbitMQ available or want to skip these tests:

```bash
# Skip RabbitMQ interop tests
SKIP_RABBITMQ_TESTS=1 go test -count=1 -v ./protocol/
```

The tests also skip when TCP connection establishment fails. A green `go test`
exit status therefore does not prove these checks ran. Inspect verbose output
for `SKIP`, especially in CI. The tests use plaintext TCP, so use an isolated
test network rather than production credentials. For the separate broker
conformance suite and its `AMQP_URL` setting, see
[Contributing](../../../CONTRIBUTING.md#amqp-compliance).

## Test Coverage

The test suite includes the following tests:

### 1. TestRabbitMQ_ProtocolHeader
Tests the initial protocol header bytes (`41 4d 51 50 00 00 09 01` in hexadecimal).

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
- Checks that the frames can be written without an immediate socket error

This test does not request a publisher confirm or read a server acknowledgement.
`TestRabbitMQ_BasicGet` provides the message round-trip check.

### 7. TestRabbitMQ_BasicGet
Tests retrieving a message:
- Opens a channel
- Declares a queue
- Publishes a message
- Retrieves the message using Basic.Get
- Verifies message body matches

### 8. TestRabbitMQ_Heartbeat
Tests writing a heartbeat frame without an immediate socket error. It does not
check heartbeat timeout enforcement or server liveness after the write.

### 9. TestRabbitMQ_MultipleChannels
Opens three channels sequentially on one connection and checks each response.

## Running Specific Tests

You can run individual tests using the `-run` flag:

```bash
# Run only the connection handshake test
go test -count=1 -v -run TestRabbitMQ_ConnectionHandshake ./protocol/

# Run only publishing and consumption tests
go test -count=1 -v -run "TestRabbitMQ_(BasicPublish|BasicGet)" ./protocol/
```

## Debugging

### Enable Verbose Output

```bash
# Run tests with verbose output
go test -count=1 -v -run TestRabbitMQ ./protocol/
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
Username: interop
Password: local-test-only
```

## CI/CD Integration

In CI, check RabbitMQ readiness before starting `go test`, use `-count=1`, and
inspect output for skipped tests. This example is a step list for a workflow
with checkout and Go setup already configured.

```yaml
- name: Start RabbitMQ
  run: |
    docker run -d --name rabbitmq-test -p 127.0.0.1:5672:5672 \
      -e RABBITMQ_DEFAULT_USER=interop \
      -e RABBITMQ_DEFAULT_PASS=local-test-only rabbitmq:4-management
    for attempt in $(seq 1 60); do
      if docker exec rabbitmq-test rabbitmq-diagnostics -q check_running; then
        exit 0
      fi
      sleep 2
    done
    docker logs rabbitmq-test
    exit 1

- name: Run RabbitMQ interop tests
  run: go test -count=1 -v -run TestRabbitMQ ./protocol/
  working-directory: ./src/amqp-go
  env:
    RABBITMQ_USER: interop
    RABBITMQ_PASS: local-test-only

- name: Stop RabbitMQ
  if: always()
  run: docker rm -f rabbitmq-test
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
1. Set an overall test timeout with `go test -count=1 -timeout 2m -run TestRabbitMQ ./protocol/`.
   This does not change the internal five-second dial or ten-second connection deadlines.
2. Check if RabbitMQ is overloaded or slow to respond
3. Verify network latency is acceptable

## Implementation Notes

These tests use the low-level protocol package directly, without any high-level client abstractions. They:

- Manually construct AMQP frames using the protocol types
- Serialize methods using the Serialize() functions
- Parse responses by reading frame payloads
- Handle all AMQP handshake steps explicitly

These checks cover the listed frame exchanges, not the entire AMQP 0.9.1
specification. Use the separate conformance suite for broker behavior.
