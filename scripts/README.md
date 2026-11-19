# StrangeQ Monitoring Scripts

This directory contains a local development launcher for the AMQP server,
Prometheus, Grafana, and a load generator. It is not a production deployment.
The launcher exposes telemetry on all interfaces, and Docker publishes Grafana
and Prometheus ports on all interfaces with initial Grafana credentials
`admin`/`admin`. Restrict access to a trusted development host.

Run commands below from the repository's `scripts` directory unless stated
otherwise. Requirements: Docker with a running daemon, the `docker-compose`
command (the scripts do not call `docker compose`), `curl`, `lsof`, and Go matching
[src/amqp-go/go.mod](../src/amqp-go/go.mod) if the binaries need building.

## Quick Start

Launch everything (server + monitoring + load test) with one command:

```bash
./launch-with-monitoring.sh
```

This will:

1. Start Prometheus and Grafana in Docker containers
2. Start the AMQP server with telemetry enabled
3. Run a performance test with 10 producers and 10 consumers
4. Tail `/tmp/perftest.log` and print dashboard links

The launcher builds missing `src/amqp-go/bin/amqp-server` and `bin/perftest`
binaries. It does not rebuild existing ones. Stop it with Ctrl-C to stop the
broker and load generator it started. Prometheus and Grafana remain running
until you run `./stop-monitoring.sh`. The broker uses persistent `src/amqp-go/data`
storage, so runs can reuse earlier data.

## Custom Configuration

You can override the default settings using environment variables:

```bash
# Custom load test configuration
PUBLISHERS=20 CONSUMERS=20 DURATION=600s ./launch-with-monitoring.sh

# Custom AMQP port: both values are needed by the current script
AMQP_PORT=5673 AMQP_NETWORK_PORT=5673 ./launch-with-monitoring.sh

# Combine multiple settings
PUBLISHERS=50 CONSUMERS=50 DURATION=1800s ./launch-with-monitoring.sh
```

### Available Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PUBLISHERS` | 10 | Number of producer goroutines |
| `CONSUMERS` | 10 | Number of consumer goroutines |
| `RATE` | 1000 | Displayed only: `perftest` has no rate limit, so this value does not throttle the run |
| `DURATION` | 300s | Test duration (e.g., 60s, 5m, 1h) |
| `AMQP_PORT` | 5672 | Load-generator URL and port precheck; does not set the broker listener |
| `AMQP_NETWORK_PORT` | 5672 | Broker port override; set it to match a custom `AMQP_PORT` |
| `TELEMETRY_PORT` | 9419 | Broker telemetry port; Prometheus target remains fixed at 9419 |

If changing `TELEMETRY_PORT`, also update the target in
[`monitoring/prometheus.yml`](monitoring/prometheus.yml) and restart Prometheus.
`RATE` does not throttle traffic even though one launcher status line labels it
as msg/s. The load generator runs without a rate limit.

## Available Scripts

- **`launch-with-monitoring.sh`** - Launch everything with monitoring (main script)
- **`stop-monitoring.sh`** - Stop the monitoring stack (Prometheus + Grafana)

## Manual Usage

### Start Monitoring Stack Only

```bash
docker-compose -f monitoring/docker-compose.yml up -d
```

### Start AMQP Server Manually

```bash
cd ../src/amqp-go
go build -o bin/amqp-server ./cmd/amqp-server
# Docker Prometheus reaches this host listener through host.docker.internal.
./bin/amqp-server --enable-telemetry --telemetry-address 0.0.0.0
```

### Run Performance Test Manually

Open another terminal at the `scripts` directory:

```bash
cd ../src/amqp-go
go build -o bin/perftest ./cmd/perftest
./bin/perftest -url amqp://guest:guest@localhost:5672/ \
  -producers 10 -consumers 10 -duration 300s
```

See [src/amqp-go/benchmark/README.md](../src/amqp-go/benchmark/README.md) for the
full flag list (`perftest` has no rate-limit flag).

### Stop Monitoring

From the `scripts` directory:

```bash
./stop-monitoring.sh
```

## Accessing Dashboards

### Grafana Dashboard
- **URL**: [Grafana](http://localhost:3000)
- **Username**: `admin`
- **Initial password**: `admin` (persisted Grafana data may contain a changed password)
- **Dashboard**: "AMQP Server - StrangeQ Monitoring" (auto-loaded)
- **Features**:
  - Auto-refreshes every 5 seconds
  - Pre-configured panels for all key metrics
  - Latency percentiles (p50, p95, p99)
  - Storage and disk metrics
  - Memory and resource utilization
  - Error tracking

### Prometheus
- **URL**: [Prometheus](http://localhost:9090)
- **Use**: Query and explore metrics
- **Examples**:
  ```promql
  # Message publish rate
  rate(amqp_messages_published_total[1m])
  
  # 99th percentile latency
  histogram_quantile(0.99, rate(amqp_message_publish_duration_seconds_bucket[5m]))
  
  # Memory usage
  amqp_memory_used_bytes / (1024*1024*1024)
  ```

### Raw Metrics Endpoint
- **URL**: [Raw metrics](http://localhost:9419/metrics)
- **Format**: Prometheus text format
- **Use**: Direct access to all metrics

## Dashboard Panels

The Grafana dashboard includes:

### Overview Row
- Server uptime
- Active connections
- Total queues
- Publish rate (msg/s)
- Delivery rate (msg/s)
- Disk free space

### Latency & Performance
- Publish latency percentiles (p50, p95, p99)
- Delivery latency percentiles (p50, p95, p99)

### Storage & Disk
- Disk usage (used/free)
- WAL file size
- WAL fsync latency (p99)

### Memory & Resources
- Memory usage (total and heap)
- Goroutine count
- File descriptor usage

### Message Flow & Queues
- Message throughput (publish/delivery/ack rates)
- Queue depth by queue (ready + unacked messages)

### Errors & Health
- Connection and channel errors by type
- Storage errors (WAL writes, segment reads)
- Dropped messages by queue and reason

## Viewing Logs

```bash
# Server logs
tail -f /tmp/amqp-server.log

# Performance test logs
tail -f /tmp/perftest.log

# Docker container logs
docker-compose -f monitoring/docker-compose.yml logs -f prometheus grafana
```

## Troubleshooting

### Port Already in Use

If you get a "port already in use" error:

```bash
# Check what's using the port
lsof -i :5672  # AMQP
lsof -i :9090  # Prometheus
lsof -i :3000  # Grafana
lsof -i :9419  # Telemetry

# Stop a conflicting service using its normal shutdown command, or choose
# a different AMQP port for both the broker and the load generator.
AMQP_PORT=5673 AMQP_NETWORK_PORT=5673 ./launch-with-monitoring.sh
```

### Docker Issues

```bash
# Check Docker status
docker ps

# Restart Docker containers
docker-compose -f monitoring/docker-compose.yml restart

# View container logs
docker-compose -f monitoring/docker-compose.yml logs -f

# Remove stored metrics and Grafana settings only when intentionally resetting:
# docker-compose -f monitoring/docker-compose.yml down -v
```

### Server Won't Start

The launcher treats any HTTP response as readiness, including `/health` 503,
and proceeds after its wait loop expires. Check readiness explicitly if clients
cannot connect:

```bash
curl --fail http://localhost:9419/health
```

Check the logs for errors:

```bash
cat /tmp/amqp-server.log
```

Common issues:
- Port already in use
- Permission denied on data directory
- Missing configuration file

### Metrics Not Showing in Grafana

1. **Check if server is running with telemetry:**
   ```bash
   curl --fail http://localhost:9419/metrics
   ```

2. **Check Prometheus is scraping:**
   - Go to http://localhost:9090/targets
   - Look for `amqp-server` job
   - Status should be "UP"

3. **Check Grafana datasource:**
   - Go to http://localhost:3000/datasources
   - Verify Prometheus datasource exists
   - Click "Test" to verify connection

4. **Verify dashboard is loaded:**
   - Go to http://localhost:3000/dashboards
   - Look for "AMQP Server - StrangeQ Monitoring"

### Performance Test Not Running

Check the perftest logs:

```bash
cat /tmp/perftest.log
```

Common issues:
- AMQP server not reachable
- Authentication failure
- Resource limits exceeded

## Architecture

```
┌─────────────────┐
│  AMQP Server    │
│  Port 5672      │
│                 │
│  Telemetry      │
│  Port 9419      │◄──────┐
└─────────────────┘       │
                          │
┌─────────────────┐       │ Scrape
│  Prometheus     │───────┘ every 5s
│  Port 9090      │
│                 │
│  - Time series  │
│  - Alerting     │
└────────┬────────┘
         │
         │ Query
         │
┌────────▼────────┐
│  Grafana        │
│  Port 3000      │
│                 │
│  - Dashboard    │
│  - Visualization│
└─────────────────┘
```

## Files in This Directory

```
scripts/
├── launch-with-monitoring.sh     # Main launch script
├── stop-monitoring.sh            # Stop monitoring stack
├── README.md                     # This file
└── monitoring/
    ├── docker-compose.yml        # Docker services definition
    ├── prometheus.yml            # Prometheus configuration
    ├── grafana-datasources/
    │   └── datasource.yml        # Grafana datasource config
    └── grafana-dashboards/
        ├── dashboard.yml         # Dashboard provisioning config
        └── amqp-dashboard.json   # Dashboard definition
```

## Tips

1. **First Time Setup**: On first run, Grafana will create volumes for data persistence. Your dashboards and settings will survive container restarts.

2. **Customizing Dashboards**: You can edit dashboards in Grafana UI. To save changes permanently, export the JSON and update `monitoring/grafana-dashboards/amqp-dashboard.json`.

3. **Long-Running Tests**: For tests longer than 15 minutes, adjust the Grafana time range (top-right corner) or use absolute time ranges.

4. **Multiple Servers**: To monitor multiple AMQP servers, edit `monitoring/prometheus.yml` to add more scrape targets.

5. **Alerting**: Set up alerting rules in Prometheus for critical metrics like disk space, error rates, or latency thresholds.

## Next Steps

- Customize the dashboard to your needs
- Set up Prometheus alerting rules
- Integrate with PagerDuty/Slack for alerts
- Add custom metrics for your use case
- Export metrics to long-term storage (e.g., Thanos)

## Support

For issues or questions:
- Check the logs (server, perftest, docker)
- Review Prometheus targets: http://localhost:9090/targets
- Verify Grafana datasource connection
- Ensure Docker containers are running: `docker ps`

