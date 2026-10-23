# systemd Deployment

This directory contains systemd service files for running StrangeQ (AMQP 0.9.1)
as a system service. Logs go to the journal; there is no log file or PID file to
manage.

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
```

### 3. Install Binary

```bash
# Download or build the binary
sudo cp amqp-server /usr/local/bin/
sudo chmod 755 /usr/local/bin/amqp-server
```

### 4. Install Configuration and Auth File

```bash
# Shipped production config: binds 0.0.0.0, enables authentication, enables
# loopback-only telemetry.
sudo install -o root -g amqp -m 0640 config.yaml /etc/amqp/config.yaml

# Optional environment overrides (the unit loads this file only if present).
sudo install -o root -g amqp -m 0640 amqp-server.env.example /etc/amqp/amqp-server.env

# Create the auth file referenced by the config. Generate a bcrypt hash:
HASH=$(htpasswd -bnBC 10 "" 'change-me' | tr -d ':\n')
sudo install -o root -g amqp -m 0640 /dev/null /etc/amqp/auth.json
sudo tee /etc/amqp/auth.json >/dev/null <<EOF
{
  "users": [
    {
      "username": "guest",
      "password_hash": "$HASH",
      "vhost_permissions": [
        {"vhost": "/", "permission": {"configure": ".*", "write": ".*", "read": ".*"}}
      ],
      "tags": ["administrator"]
    }
  ]
}
EOF
```

Replace `change-me` before installing. The hash is quoted as a JSON string;
bcrypt hashes contain no characters that need escaping.

The broker only re-reads `auth.json` on `systemctl reload` (SIGHUP). Other
settings require a restart.

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

# Re-read the auth file (users/permissions). Other settings need a restart.
sudo systemctl reload amqp-server
```

`reload` sends SIGHUP. The broker reloads `/etc/amqp/auth.json` when the
configured authenticator supports it and never exits on SIGHUP.

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
sudo -u amqp /usr/local/bin/amqp-server --config /etc/amqp/config.yaml
```

## Log Retention

The unit sends stdout/stderr to the journal, so log rotation is journald's job:

```bash
sudo mkdir -p /etc/systemd/journald.conf.d
sudo tee /etc/systemd/journald.conf.d/amqp-server.conf >/dev/null <<'EOF'
[Journal]
SystemMaxUse=2G
SystemMaxFileSize=100M
EOF
sudo systemctl restart systemd-journald
```

If you configure `server.logfile` instead, the broker does not reopen the file
on SIGHUP. Use logrotate with `copytruncate`, or use the journal (recommended).

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

The shipped config binds telemetry to `127.0.0.1:9419` (loopback only), so
scrape it from the host or proxy it locally. `/health` reports readiness: 503
while recovery runs, during shutdown, and 503 when the AMQP listener is not
bound. pprof is disabled unless `--enable-pprof` is passed.

If metrics must be reachable from another host, expose them via nginx (keep
the broker's telemetry listener on loopback):

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

# Verify the binary version and list flags
/usr/local/bin/amqp-server --version
/usr/local/bin/amqp-server -h

# Check that the config loads (start in the foreground; Ctrl-C to stop)
sudo -u amqp /usr/local/bin/amqp-server --config /etc/amqp/config.yaml

# Check file permissions
ls -la /etc/amqp/
ls -la /var/lib/amqp/
```

### Permission Errors

```bash
# Reset ownership
sudo chown -R amqp:amqp /var/lib/amqp
sudo chown root:amqp /etc/amqp/config.yaml /etc/amqp/auth.json
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

# Remove user
# sudo userdel amqp
```

