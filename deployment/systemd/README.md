# systemd Deployment

This directory contains systemd service files for running AMQP-Go as a system service.

## Installation

### 1. Create System User

```bash
sudo useradd --system --no-create-home --shell /bin/false amqp
```

### 2. Create Directories

```bash
# Data directory
sudo mkdir -p /var/lib/amqp
sudo chown amqp:amqp /var/lib/amqp
sudo chmod 750 /var/lib/amqp

# Configuration directory
sudo mkdir -p /etc/amqp
sudo chown root:amqp /etc/amqp
sudo chmod 750 /etc/amqp

# Log directory
sudo mkdir -p /var/log/amqp
sudo chown amqp:amqp /var/log/amqp
sudo chmod 750 /var/log/amqp

# Runtime directory
sudo mkdir -p /run/amqp
sudo chown amqp:amqp /run/amqp
sudo chmod 755 /run/amqp
```

### 3. Install Binary

```bash
# Download or build the binary
sudo cp amqp-server /usr/local/bin/
sudo chmod 755 /usr/local/bin/amqp-server
```

### 4. Create Configuration

```bash
sudo cat > /etc/amqp/config.json << 'EOF'
{
  "network": {
    "address": ":5672",
    "port": 5672,
    "max_connections": 1000,
    "heartbeat_interval": "60s"
  },
  "storage": {
    "backend": "badger",
    "path": "/var/lib/amqp/data",
    "sync_writes": true,
    "cache_size": 67108864
  },
  "security": {
    "tls_enabled": false,
    "authentication_enabled": false
  },
  "server": {
    "log_level": "info",
    "log_file": "/var/log/amqp/server.log",
    "pid_file": "/run/amqp/amqp-server.pid"
  }
}
EOF

sudo chown root:amqp /etc/amqp/config.json
sudo chmod 640 /etc/amqp/config.json
```

### 5. Install Service File

```bash
sudo cp amqp-server.service /etc/systemd/system/
sudo chmod 644 /etc/systemd/system/amqp-server.service
```

### 6. Enable and Start Service

```bash
# Reload systemd
sudo systemctl daemon-reload

# Enable service to start on boot
sudo systemctl enable amqp-server

# Start service
sudo systemctl start amqp-server

# Check status
sudo systemctl status amqp-server
```

## Managing the Service

### Start/Stop/Restart

```bash
# Start
sudo systemctl start amqp-server

# Stop
sudo systemctl stop amqp-server

# Restart
sudo systemctl restart amqp-server

# Reload configuration (graceful)
sudo systemctl reload amqp-server
```

### Check Status

```bash
# Service status
sudo systemctl status amqp-server

# Is service running?
sudo systemctl is-active amqp-server

# Is service enabled?
sudo systemctl is-enabled amqp-server
```

### View Logs

```bash
# Follow logs
sudo journalctl -u amqp-server -f

# Last 100 lines
sudo journalctl -u amqp-server -n 100

# Since boot
sudo journalctl -u amqp-server -b

# Today's logs
sudo journalctl -u amqp-server --since today

# Logs from specific time
sudo journalctl -u amqp-server --since "2024-01-01 00:00:00"
```

### Debugging

```bash
# Check service file syntax
systemd-analyze verify /etc/systemd/system/amqp-server.service

# Show service configuration
systemctl cat amqp-server

# Show service dependencies
systemctl list-dependencies amqp-server

# Show environment variables
systemctl show-environment

# Run service in foreground (for debugging)
sudo -u amqp /usr/local/bin/amqp-server --config /etc/amqp/config.json
```

## Log Rotation

Create `/etc/logrotate.d/amqp-server`:

```bash
sudo cat > /etc/logrotate.d/amqp-server << 'EOF'
/var/log/amqp/server.log {
    daily
    rotate 14
    compress
    delaycompress
    notifempty
    create 0640 amqp amqp
    sharedscripts
    postrotate
        systemctl reload amqp-server > /dev/null 2>&1 || true
    endscript
}
EOF
```

## Security Hardening

The provided service file includes several security hardening options:

- `NoNewPrivileges=true` - Prevents privilege escalation
- `PrivateTmp=true` - Uses private /tmp directory
- `ProtectSystem=strict` - Makes most of filesystem read-only
- `ProtectHome=true` - Makes /home inaccessible
- `ReadWritePaths=...` - Only specified paths are writable

### Additional Hardening (Optional)

Add these to `[Service]` section for more security:

```ini
# Network
PrivateNetwork=false
RestrictAddressFamilies=AF_INET AF_INET6

# System calls
SystemCallFilter=@system-service
SystemCallErrorNumber=EPERM

# Capabilities
CapabilityBoundingSet=CAP_NET_BIND_SERVICE
AmbientCapabilities=CAP_NET_BIND_SERVICE
```

## Firewall Configuration

### UFW (Ubuntu/Debian)

```bash
sudo ufw allow 5672/tcp comment 'AMQP-Go'
```

### firewalld (RHEL/CentOS)

```bash
sudo firewall-cmd --permanent --add-port=5672/tcp
sudo firewall-cmd --reload
```

### iptables

```bash
sudo iptables -A INPUT -p tcp --dport 5672 -j ACCEPT
sudo iptables-save > /etc/iptables/rules.v4
```

## Monitoring

### Systemd Status

```bash
# Create status check script
cat > /usr/local/bin/amqp-status.sh << 'EOF'
#!/bin/bash
systemctl is-active --quiet amqp-server
exit $?
EOF

chmod +x /usr/local/bin/amqp-status.sh

# Add to crontab for monitoring
# */5 * * * * /usr/local/bin/amqp-status.sh || mail -s "AMQP Down" admin@example.com
```

### Metrics Endpoint

If metrics are enabled, expose via nginx:

```nginx
location /metrics {
    proxy_pass http://localhost:9419/metrics;
    allow 10.0.0.0/8;
    deny all;
}
```

## Troubleshooting

### Service Won't Start

```bash
# Check logs
sudo journalctl -u amqp-server -n 50

# Verify configuration
/usr/local/bin/amqp-server --config /etc/amqp/config.json --help

# Check file permissions
ls -la /etc/amqp/
ls -la /var/lib/amqp/
ls -la /var/log/amqp/
```

### Permission Errors

```bash
# Reset ownership
sudo chown -R amqp:amqp /var/lib/amqp
sudo chown -R amqp:amqp /var/log/amqp
sudo chown -R amqp:amqp /run/amqp
```

### Port Binding Issues

```bash
# Check if port is in use
sudo lsof -i :5672
sudo netstat -tlnp | grep 5672

# Grant bind capability (if needed for port <1024)
sudo setcap 'cap_net_bind_service=+ep' /usr/local/bin/amqp-server
```

## Uninstallation

```bash
# Stop and disable service
sudo systemctl stop amqp-server
sudo systemctl disable amqp-server

# Remove service file
sudo rm /etc/systemd/system/amqp-server.service
sudo systemctl daemon-reload

# Remove binary
sudo rm /usr/local/bin/amqp-server

# Remove data (optional - backup first!)
# sudo rm -rf /var/lib/amqp
# sudo rm -rf /etc/amqp
# sudo rm -rf /var/log/amqp

# Remove user
# sudo userdel amqp
```

