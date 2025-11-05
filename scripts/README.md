# StrangeQ Monitoring Scripts

This directory contains scripts to launch the AMQP server with full observability using Prometheus and Grafana.

## Quick Start

Launch everything (server + monitoring + load test) with one command:

```bash
./launch-with-monitoring.sh
```

This will:
1. ✅ Start Prometheus and Grafana in Docker containers
2. ✅ Start the AMQP server with telemetry enabled
3. ✅ Run a performance test with 10 publishers and 10 consumers at 1000 msg/s
4. ✅ Display real-time progress and provide dashboard links

## Custom Configuration

You can override the default settings using environment variables:

```bash
# Custom load test configuration
PUBLISHERS=20 CONSUMERS=20 RATE=2000 DURATION=600s ./launch-with-monitoring.sh

# Custom ports
AMQP_PORT=5673 TELEMETRY_PORT=9420 ./launch-with-monitoring.sh

# Combine multiple settings
PUBLISHERS=50 CONSUMERS=50 RATE=5000 DURATION=1800s ./launch-with-monitoring.sh
```

### Available Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PUBLISHERS` | 10 | Number of publisher goroutines |
| `CONSUMERS` | 10 | Number of consumer goroutines |
| `RATE` | 1000 | Target message rate (msg/s) |
| `DURATION` | 300s | Test duration (e.g., 60s, 5m, 1h) |
| `AMQP_PORT` | 5672 | AMQP server port |
| `TELEMETRY_PORT` | 9419 | Prometheus metrics port |

## Available Scripts

- **`launch-with-monitoring.sh`** - Launch everything with monitoring (main script)
- **`stop-monitoring.sh`** - Stop the monitoring stack (Prometheus + Grafana)

## Manual Usage

### Start Monitoring Stack Only

```bash
cd monitoring
docker-compose up -d
```

### Start AMQP Server Manually

```bash
cd ../src/amqp-go
./bin/amqp-server --enable-telemetry
```

### Run Performance Test Manually

```bash
cd ../src/amqp-go/benchmark
./perftest -publishers 10 -consumers 10 -rate 1000 -duration 300s
```

### Stop Monitoring

```bash
./stop-monitoring.sh
```

## Accessing Dashboards

### Grafana Dashboard
- **URL**: http://localhost:3000
- **Username**: `admin`
- **Password**: `admin`
- **Dashboard**: "AMQP Server - StrangeQ Monitoring" (auto-loaded)
- **Features**:
  - Auto-refreshes every 5 seconds
  - Pre-configured panels for all key metrics
  - Latency percentiles (p50, p95, p99)
  - Storage and disk metrics
  - Memory and resource utilization
  - Error tracking

### Prometheus
- **URL**: http://localhost:9090
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
- **URL**: http://localhost:9419/metrics
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
cd monitoring
docker-compose logs -f prometheus
docker-compose logs -f grafana
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

# Kill the process
kill -9 <PID>

# Or use a different port
AMQP_PORT=5673 TELEMETRY_PORT=9420 ./launch-with-monitoring.sh
```

### Docker Issues

```bash
# Check Docker status
docker ps

# Restart Docker containers
cd monitoring
docker-compose restart

# Clean restart (removes data volumes)
docker-compose down -v
docker-compose up -d

# View container logs
docker-compose logs -f
```

### Server Won't Start

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
   curl http://localhost:9419/metrics
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

2. **Customizing Dashboards**: You can edit dashboards in Grafana UI. To save changes permanently, export the JSON and update `grafana-dashboards/amqp-dashboard.json`.

3. **Long-Running Tests**: For tests longer than 15 minutes, adjust the Grafana time range (top-right corner) or use absolute time ranges.

4. **Multiple Servers**: To monitor multiple AMQP servers, edit `prometheus.yml` to add more scrape targets.

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

