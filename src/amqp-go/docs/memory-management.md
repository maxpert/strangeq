# Memory Management Configuration

StrangeQ AMQP server implements RabbitMQ-style memory management with dynamic memory thresholds and publisher flow control.

## Overview

The server monitors memory usage and triggers flow control when approaching memory limits, blocking publishers without closing connections. This ensures connection stability while preventing out-of-memory conditions.

## Configuration Options

### CLI Flags

#### `--memory-limit-percent` (Default: 60)

Sets the memory threshold as a percentage of total system RAM (0-100).

**Usage:**
```bash
# Use 80% of system RAM
amqp-server --memory-limit-percent 80

# Use 40% of system RAM (conservative)
amqp-server --memory-limit-percent 40

# Default (60%, same as RabbitMQ)
amqp-server
```

**When to use:**
- **60%** (default): Balanced for general workloads, matches RabbitMQ default
- **40-50%**: Conservative setting for memory-constrained environments
- **70-80%**: Aggressive setting for dedicated message broker servers
- **Set to 0**: Use absolute memory limit instead (see `--memory-limit-bytes`)

#### `--memory-limit-bytes` (Default: 0)

Sets an absolute memory limit in bytes. When non-zero, this overrides `--memory-limit-percent`.

**Usage:**
```bash
# Set 4GB absolute limit
amqp-server --memory-limit-bytes 4GB

# Set 512MB limit
amqp-server --memory-limit-bytes 512MB

# Set 2GB limit
amqp-server --memory-limit-bytes 2147483648  # in bytes

# Default (0 = use percentage)
amqp-server
```

**Supported formats:**
- Bytes: `2147483648`
- Kilobytes: `512KB`
- Megabytes: `512MB`
- Gigabytes: `4GB`

**When to use:**
- **Container environments**: Set explicit limits matching container memory limits
- **Testing**: Set precise limits for reproducible benchmarks
- **Multi-tenant**: Set guaranteed memory per server instance

### Configuration File

Memory settings can also be configured via JSON config file:

```json
{
  "server": {
    "memory_limit_percent": 60,
    "memory_limit_bytes": 0
  }
}
```

**Usage:**
```bash
amqp-server --config /path/to/config.json
```

## Memory Threshold Calculation

The server calculates the memory threshold using this logic:

1. If `memory_limit_bytes > 0`: Use absolute limit
2. Else: Use `memory_limit_percent` of system RAM
3. Minimum threshold: 1GB (enforced even on small systems)

**Example calculations:**

| System RAM | Percent | Calculated Threshold |
|------------|---------|---------------------|
| 16GB | 60% | 9.6GB |
| 32GB | 60% | 19.2GB |
| 8GB | 60% | 4.8GB |
| 4GB | 60% | 2.4GB |
| 2GB | 60% | 1GB (minimum enforced) |

## Flow Control Behavior

When memory usage exceeds the threshold:

1. **Publishers are blocked**: New messages are rejected or delayed
2. **Connections stay open**: No connection drops (unlike connection-level blocking)
3. **Warning logged**: Server logs memory pressure warnings
4. **Consumers continue**: Message consumption continues to drain queues

**Memory alarm triggers:**
- **90%+ of threshold**: Memory alarm activated, publisher blocking begins
- **<80% of threshold**: Memory alarm clears (hysteresis prevents flapping)

## Architecture: RabbitMQ-Style Mailboxes

StrangeQ uses a three-mailbox architecture inspired by RabbitMQ's Erlang design:

### Mailbox Types

1. **Heartbeat Mailbox**
   - Handles: Heartbeat frames
   - Initial capacity: 100 frames
   - Purpose: Ensures heartbeats are never starved

2. **Channel Mailbox**
   - Handles: Method, header, body frames
   - Initial capacity: 100,000 frames
   - Purpose: High-throughput data processing

3. **Connection Mailbox**
   - Handles: Connection control frames
   - Initial capacity: 100 frames
   - Purpose: Separate control plane

### Unbounded Queue Design

All mailboxes use unbounded queues (Workiva/go-datastructures/queue):

- **Never blocks the TCP reader**: Reader can always enqueue frames
- **Dynamic memory growth**: Queues grow under load rather than blocking
- **Memory tracking**: Each frame ~1KB estimated for memory alarms
- **Per-mailbox goroutines**: Independent processing prevents head-of-line blocking

## Monitoring Memory Usage

### Check Current Configuration

```bash
# View current memory settings
amqp-server --version
amqp-server --help | grep memory
```

### Monitor at Runtime

The server logs memory warnings when threshold is exceeded:

```
WARN High memory usage detected but allowing publish to maintain connection stability
  connection_id=conn-12345
  channel_id=1
  exchange=my-exchange
```

### Memory Pressure Indicators

Watch for these signs in logs:
- Frequent "High memory usage" warnings
- Publisher rate limiting
- Growing queue depths

## Best Practices

### Development

```bash
# Relaxed limits for local development
amqp-server --memory-limit-percent 80
```

### Production

```bash
# RabbitMQ-compatible defaults
amqp-server --memory-limit-percent 60

# Or use absolute limits in containers
amqp-server --memory-limit-bytes 4GB
```

### Testing/Benchmarking

```bash
# Fixed limits for reproducibility
amqp-server --memory-limit-bytes 2GB
```

### High-Throughput Workloads

```bash
# More aggressive memory usage
amqp-server --memory-limit-percent 75
```

## Troubleshooting

### "High memory usage" warnings appear frequently

**Symptoms:**
- Logs show repeated memory warnings
- Publisher rate is throttled

**Solutions:**
1. Increase memory limit: `--memory-limit-percent 70`
2. Add more consumers to drain queues faster
3. Use smaller message sizes
4. Implement application-level rate limiting

### Server crashes with OOM

**Symptoms:**
- Server process killed by OS
- "killed: 9" or "OOM" in system logs

**Solutions:**
1. Set **lower** memory limit: `--memory-limit-percent 50`
2. Reduce prefetch count to limit queued messages
3. Increase system RAM
4. Use absolute limit in containers: `--memory-limit-bytes 3GB` (for 4GB container)

### Memory alarm never clears

**Symptoms:**
- Memory stays above 90% threshold
- Publishers permanently blocked

**Solutions:**
1. Check consumers are actively draining queues
2. Verify no queue buildup: `./perftest` with more consumers
3. Restart server if queues are empty but memory high
4. Check for memory leaks (use Go profiling tools)

## Comparison with RabbitMQ

| Feature | RabbitMQ | StrangeQ |
|---------|----------|----------|
| Default threshold | 60% RAM | 60% RAM |
| Memory alarm | Yes | Yes |
| Publisher blocking | Yes | Yes |
| Connection drops | No | No |
| Configuration | `vm_memory_high_watermark` | `--memory-limit-percent` |
| Absolute limits | `vm_memory_high_watermark.absolute` | `--memory-limit-bytes` |

StrangeQ's implementation closely matches RabbitMQ's behavior for compatibility and predictability.

## Performance Impact

Memory management has minimal performance impact:

- **Memory estimation**: Simple atomic counter increment/decrement
- **Threshold check**: Single comparison per enqueue
- **Flow control**: Logs warning, no connection teardown
- **Overhead**: < 1% of overall message processing time

See [benchmark results](../benchmark/README.md#performance-comparison-vs-rabbitmq) for detailed performance data.
