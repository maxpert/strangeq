# systemd Deployment

This directory contains systemd service files for running StrangeQ (AMQP 0.9.1)
as a system service. Logs go to the journal; there is no log file or PID file to
manage.

## Installation

These instructions target a Linux host with systemd. Run the file-copy commands
from this repository's `deployment/systemd` directory. Hash generation requires `htpasswd` (typically `apache2-utils` on
Debian/Ubuntu or `httpd-tools` on RHEL-family systems).

The shipped config listens on all interfaces with password authentication, but
without TLS or resource authorization. Before exposing the listener on an
untrusted network, add [TLS](../../docs/TLS.md), set
`security.authorizationenabled: true` to enforce the auth-file permissions, and
restrict network access.

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

Download or build the binary using [Quick Start](../../QUICKSTART.md). Replace
`/absolute/path/to/amqp-server` below with its actual location.

```bash
sudo install -o root -g root -m 0755 /absolute/path/to/amqp-server /usr/local/bin/amqp-server
```

### 4. Install Configuration and Auth File

```bash
# Shipped production config: binds 0.0.0.0, enables authentication, enables
# loopback-only telemetry.
sudo install -o root -g amqp -m 0640 config.yaml /etc/amqp/config.yaml

# Optional environment overrides (the unit loads this file only if present).
sudo install -o root -g amqp -m 0640 amqp-server.env.example /etc/amqp/amqp-server.env

# Create the auth file referenced by the config. Generate a bcrypt hash:
# This prompts for the password instead of putting it in shell history.
HASH=$(htpasswd -nBC 10 admin | cut -d: -f2)
sudo install -o root -g amqp -m 0640 /dev/null /etc/amqp/auth.json
sudo tee /etc/amqp/auth.json >/dev/null <<EOF
{
  "users": [
    {
      "username": "admin",
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

Choose a strong password at the prompt and verify that `HASH` is nonempty before
creating the auth file. The hash is inserted as a JSON string. Bcrypt hashes
contain no characters that need JSON escaping. Do not use the placeholder
credentials from client examples for this account.

The example grants full access to `/`. For application accounts, use only the
permissions they need. See [Authorization](../../docs/AUTHORIZATION.md).

The broker reads `auth.json` on startup and re-reads it on `systemctl reload`
(SIGHUP). Existing AMQP connections keep their user snapshot. Reconnect clients
when changing their permissions. Other settings require a restart.

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

# Show the optional environment-file path
systemctl show amqp-server --property=EnvironmentFiles

# Stop the service before running another broker on the same data directory
sudo systemctl stop amqp-server
# Use the same working directory as the unit
(cd /var/lib/amqp && sudo -u amqp /usr/local/bin/amqp-server --config /etc/amqp/config.yaml)
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

These journald limits apply to the host journal, not just this service. If you
configure `server.logfile` instead, choose a path writable under the unit's
`ReadWritePaths` restriction. SIGHUP does not reopen that file. Use the journal
for managed retention. A `copytruncate` rotation can lose log lines.

## Security Hardening

The provided service file includes several security hardening options:

- `NoNewPrivileges=true` - Prevents privilege escalation
- `PrivateTmp=true` - Uses private /tmp directory
- `ProtectSystem=strict` - Makes most of filesystem read-only
- `ProtectHome=true` - Makes /home inaccessible
- `ReadWritePaths=/var/lib/amqp` - Allows service data writes under that path
- `UMask=0027` - Removes world access and group-write permission from new files

### Additional Hardening (Optional)

Review these optional restrictions against your host and workload before adding
them through `sudo systemctl edit amqp-server`. Apply changes with
`sudo systemctl daemon-reload` and a service restart:

```ini
# Network families used by the broker
[Service]
RestrictAddressFamilies=AF_INET AF_INET6

# System calls
SystemCallFilter=@system-service
SystemCallErrorNumber=EPERM

# No capabilities are needed for ports 5672 and 5671
CapabilityBoundingSet=
AmbientCapabilities=
```

## Firewall Configuration

Permit only the client network that needs access. The examples below use
`10.0.0.0/8`. Replace it with your client CIDR and use the configured AMQP port
(5671 if you selected the conventional TLS port). Choose the tool your host uses.

### UFW (Ubuntu/Debian)

```bash
sudo ufw allow from 10.0.0.0/8 to any port 5672 proto tcp comment 'StrangeQ'
```

### firewalld (RHEL/CentOS)

```bash
sudo firewall-cmd --permanent --add-rich-rule='rule family="ipv4" source address="10.0.0.0/8" port port="5672" protocol="tcp" accept'
sudo firewall-cmd --reload
```

### iptables

```bash
sudo iptables -A INPUT -s 10.0.0.0/8 -p tcp --dport 5672 -j ACCEPT
# On hosts with iptables-persistent and /etc/iptables installed:
sudo iptables-save | sudo tee /etc/iptables/rules.v4 >/dev/null
```

## Monitoring

### Systemd Status

Use `systemctl is-active --quiet amqp-server` in a host-monitoring check. A zero
exit status means systemd considers the service active. Use `/health` to check
broker readiness.

### Metrics Endpoint

The shipped config binds telemetry to `127.0.0.1:9419` (loopback only), so
scrape it from the host or proxy it locally. `/health` reports readiness: 503
while recovery runs, during shutdown, and 503 when the AMQP listener is not
bound. pprof is disabled by default. It can be enabled by configuration,
environment, or `--enable-pprof`.

If metrics must be reachable from another host, expose them via nginx (keep
the broker's telemetry listener on loopback):

```nginx
location /metrics {
    proxy_pass http://127.0.0.1:9419/metrics;
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

# Check that the config loads (stop the service first; Ctrl-C to stop)
sudo systemctl stop amqp-server
(cd /var/lib/amqp && sudo -u amqp /usr/local/bin/amqp-server --config /etc/amqp/config.yaml)

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

# Ports 5672 and 5671 do not require a bind capability.
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

