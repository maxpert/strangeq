# StrangeQ Performance Benchmarks

This directory contains RabbitMQ-compatible performance benchmarks that measure throughput (msg/s) and latency (percentiles) using actual AMQP client connections.

## Quick Start

### Build the benchmark tool

```bash
cd src/amqp-go/benchmark
go build -o perftest perftest.go
```

### Start StrangeQ server

```bash
# In another terminal
cd src/amqp-go

# Option 1: Use default configuration
go run ./cmd/amqp-server

# Option 2: Generate and use custom config
go run ./cmd/amqp-server --generate-config config.yaml
# Edit config.yaml as needed
go run ./cmd/amqp-server --config config.yaml
```

### Run benchmarks

```bash
# Basic test - 1 producer, 1 consumer, 30 seconds
./perftest

# High throughput - multiple producers/consumers
./perftest -producers 10 -consumers 10 -duration 60s

# Persistent messages
./perftest -persistent -producers 5 -consumers 5

# Large messages
./perftest -size 10240 -producers 5 -consumers 5

# Rate limited
./perftest -rate 10000 -producers 1 -consumers 1
```

## Command Line Options

| Option | Default | Description |
|--------|---------|-------------|
| `-uri` | `amqp://guest:guest@localhost:5672/` | AMQP connection URI |
| `-producers` | `1` | Number of producer connections |
| `-consumers` | `1` | Number of consumer connections |
| `-duration` | `30s` | Test duration |
| `-rate` | `0` | Publishing rate limit (msg/s, 0 = unlimited) |
| `-size` | `1024` | Message size in bytes |
| `-persistent` | `false` | Use persistent messages (DeliveryMode=2) |
| `-queue` | `perftest` | Queue name |
| `-auto-ack` | `false` | Auto-acknowledge messages |
| `-prefetch` | `1` | Consumer prefetch count |

## Output Metrics

The benchmark reports metrics in RabbitMQ-standard format:

### Real-time (every second)
```
published: 15234 msg/s, consumed: 15189 msg/s, confirmed: 15234
```

### Final Results
```
=== Performance Test Results ===
Duration: 30.00s

Throughput:
  Published: 456789 messages (15226 msg/s)
  Consumed:  456234 messages (15208 msg/s)
  Confirmed: 456789 messages

Consumer Latency:
  min:    145.2µs
  median: 892.4µs
  75th:   1.234ms
  95th:   2.456ms
  99th:   5.123ms
  max:    12.34ms
```

## Example Scenarios

### Scenario 1: Single Producer/Consumer (In-Memory)

Test basic throughput with minimal overhead:

```bash
./perftest -duration 30s
```

Expected results on Apple M4 Max:
- Throughput: 100,000+ msg/s sustained
- Peak throughput: 200,000+ msg/s
- Latency p50: ~1-2ms

Note: With optimized ring buffer (256K) and reader/processor separation for zero TCP blocking

### Scenario 2: Multiple Producers/Consumers (In-Memory)

Test concurrent operations (recommended configuration):

```bash
./perftest -producers 20 -consumers 20 -duration 60s
```

Expected results on Apple M4 Max:
- Throughput: 110,000-120,000 msg/s sustained
- Peak throughput: 230,000+ msg/s bursts
- Stable performance with zero connection drops
- Ring buffer utilization: 0% WAL reads (all in-memory)

### Scenario 3: Persistent Messages (Badger Storage)

Test with disk persistence (WAL-based durability):

```bash
# Generate config with custom storage path
amqp-server --generate-config config.yaml
# Edit config.yaml to set storage.path: ./bench-data
amqp-server --config config.yaml

# Run benchmark
./perftest -persistent -producers 20 -consumers 20 -duration 60s
```

Expected results on Apple M4 Max:
- Throughput: 175,000-231,000 msg/s publishing
- Consumption: 87,500-117,000 msg/s
- Batch synchronous WAL writes (fsync per batch)
- Recovery: 10,000 messages in 21ms (476,000 msg/s recovery rate)

### Scenario 4: Large Messages

Test with 100KB messages:

```bash
./perftest -size 102400 -producers 3 -consumers 3 -duration 30s
```

### Scenario 5: Rate Limited Publishing

Test with controlled publishing rate:

```bash
./perftest -rate 10000 -producers 5 -consumers 5 -duration 60s
```

## Performance Comparison vs RabbitMQ

Head-to-head benchmark comparing StrangeQ with RabbitMQ 3.13 under identical conditions:

**Test Configuration:**
- **Test**: 20 producers, 20 consumers (single queue stress test)
- **Message Size**: 1KB (realistic application payload)
- **Duration**: 60 seconds
- **Prefetch**: 100 (high throughput setting)
- **Auto-ack**: false (realistic reliability)
- **Platform**: Apple M4 Max
- **Date**: November 2025
- **RabbitMQ**: 3.13 (Docker official image)
- **StrangeQ**: Latest with reader/processor separation & optimized ring buffer

### Benchmark Results

| Server | Published (msg/s) | Consumed (msg/s) | Notes |
|--------|------------------|------------------|-------|
| **RabbitMQ 3.13** | 69,665 | 64,952 | Baseline |
| **StrangeQ (Latest)** | **114,178** | **107,572** | **1.7x faster** |

### Analysis

**Publishing Performance:**
- StrangeQ: **114,178 msg/s** (1.7x faster than RabbitMQ)
- Peak bursts: 220,000+ msg/s
- Zero connection drops under sustained load
- Zero metadata disk I/O (all cached in memory)
- Batch synchronous WAL writes with fsync per batch

**Consumer Performance:**
- StrangeQ: 107,572 msg/s (1.7x faster than RabbitMQ)
- RabbitMQ: 65K msg/s
- **Balanced throughput** with reader/processor separation
- Zero TCP blocking during WAL fsync operations
- O(1) binding lookups for message routing

**Metadata Caching:**
- All exchanges, queues, and bindings cached in memory
- Lock-free concurrent access with sync.Map
- O(1) routing decisions (previously O(n) disk scans)
- Zero disk reads for routing metadata
- CPU profile shows no metadata bottlenecks

**Stability:**
- Both servers maintained stable connections
- No memory issues or connection drops
- Sustained performance over full 60 second test
- StrangeQ: 0% WAL reads (100% ring buffer hits)

### Key Findings

1. **Overall Throughput**: StrangeQ is **1.7-1.8x faster** than RabbitMQ for both publishing and consumption
2. **Balanced Performance**: Publishing and consumption rates are now balanced (no queue buildup)
3. **Memory Efficiency**: Ring buffer optimization (256K) keeps all messages in memory with zero disk reads
4. **Zero TCP Blocking**: Reader/processor separation prevents fsync from blocking TCP socket reads

### Architecture Benefits

StrangeQ's superior performance comes from:

1. **Metadata Caching**: All routing metadata cached in memory with O(1) lock-free lookups, eliminating disk I/O on message routing hot path
2. **Reader/Processor Separation**: TCP reader and frame processor run in separate goroutines with 10K frame buffer, preventing fsync from blocking socket reads
3. **Lock-Free Per-Queue Isolation**: Using `sync.Map` eliminates global lock contention
4. **Optimized Ring Buffer**: 64K message ring buffer (6.5 MB per queue) eliminates disk I/O for hot messages
5. **Batch Synchronous WAL**: Groups fsync operations for durability without individual write latency
6. **Dynamic Memory Management**: RabbitMQ-style 60% RAM threshold with proper flow control
7. **Connection Stability**: Blocks publishers during memory pressure without closing connections
8. **CBOR Serialization**: 2-3x faster metadata persistence compared to JSON

## Comparison with RabbitMQ

This benchmark tool follows the same metrics as RabbitMQ PerfTest:
- Throughput in msg/s (not ops/sec)
- Latency percentiles in milliseconds
- Real AMQP protocol overhead included
- Publisher confirms tracked
- Consumer acknowledgments tracked

## Running Against RabbitMQ

You can run the same benchmarks against RabbitMQ for comparison:

```bash
# Start RabbitMQ
docker run -d --name rabbitmq -p 5672:5672 rabbitmq:3-management

# Run benchmark
./perftest -uri amqp://guest:guest@localhost:5672/
```

## Continuous Benchmarking

To track performance over time:

```bash
# Run baseline
./perftest -duration 60s > baseline.txt

# After changes
./perftest -duration 60s > results.txt

# Compare
diff baseline.txt results.txt
```

## Performance Tips

For maximum throughput:

1. **Disable persistence** for in-memory testing
2. **Increase producers/consumers** to utilize all CPU cores
3. **Use auto-ack** if you don't need reliability
4. **Increase prefetch** for better consumer throughput
5. **Use larger messages** to reduce per-message overhead

For realistic production testing:

1. **Enable persistence** with `-persistent`
2. **Use manual acks** (default)
3. **Set reasonable prefetch** (10-50)
4. **Test with your actual message sizes**
5. **Include network latency** by testing remote connections

## Benchmarking Best Practices

1. **Warm up**: Run for at least 10 seconds before measuring
2. **Sustained load**: Test for 30-60 seconds minimum
3. **Isolated environment**: Run on dedicated hardware
4. **Baseline**: Compare against RabbitMQ on same hardware
5. **Reproducible**: Use same parameters for all tests
6. **Multiple runs**: Run 3-5 times and average results

## Troubleshooting

### Low throughput?

- Check CPU usage: `top` or `htop`
- Increase producers/consumers
- Verify network is not the bottleneck
- Check if disk I/O is saturated (with persistent messages)

### High latency?

- Reduce prefetch count
- Check if server is overloaded
- Verify storage backend performance
- Look for GC pauses in server logs

### Connection errors?

- Check server is running and accepting connections
- Verify URI is correct
- Check firewall settings
- Ensure enough file descriptors available

## Contributing Benchmarks

When adding new benchmark scenarios, please:

1. Document the scenario purpose
2. Include expected results
3. Use standard metrics (msg/s, latency percentiles)
4. Compare with RabbitMQ if applicable
