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
go run ./cmd/amqp-server
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
- Throughput: ~15,000 msg/s
- Latency p50: <1ms

### Scenario 2: Multiple Producers/Consumers (In-Memory)

Test concurrent operations:

```bash
./perftest -producers 10 -consumers 10 -duration 60s
```

Expected results:
- Throughput: ~100,000 msg/s
- Latency p50: <2ms

### Scenario 3: Persistent Messages (Badger Storage)

Test with disk persistence:

```bash
# Start server with Badger storage
amqp-server --storage badger --storage-path ./bench-data

# Run benchmark
./perftest -persistent -producers 5 -consumers 5 -duration 60s
```

Expected results:
- Throughput: ~15,000 msg/s (limited by disk I/O)
- Latency p50: ~2-5ms

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
