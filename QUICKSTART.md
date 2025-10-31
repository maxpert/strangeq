# Quick Start Guide

Get StrangeQ up and running in 5 minutes!

## Installation

### Option 1: Download Pre-built Binary (Recommended)

1. Go to the [Releases page](https://github.com/maxpert/strangeq/releases)
2. Download the binary for your platform:
   - macOS ARM64: `amqp-server-darwin-arm64`
   - macOS Intel: `amqp-server-darwin-amd64`
   - Linux AMD64: `amqp-server-linux-amd64`
   - Linux ARM64: `amqp-server-linux-arm64`
   - Linux 32-bit: `amqp-server-linux-386`

3. Verify the checksum:
   ```bash
   # Linux/macOS
   sha256sum -c amqp-server-YOUR-PLATFORM.sha256

   # macOS (alternative)
   shasum -a 256 -c amqp-server-YOUR-PLATFORM.sha256
   ```

4. Make it executable (Linux/macOS):
   ```bash
   chmod +x amqp-server-YOUR-PLATFORM
   sudo mv amqp-server-YOUR-PLATFORM /usr/local/bin/amqp-server
   ```

### Option 2: Build from Source

```bash
git clone https://github.com/maxpert/strangeq.git
cd strangeq/src/amqp-go
go build -o amqp-server ./cmd/amqp-server
sudo mv amqp-server /usr/local/bin/
```

### Option 3: Go Install

```bash
go install github.com/maxpert/amqp-go/cmd/amqp-server@latest
```

## Start Server

### In-Memory Mode (Development)

```bash
amqp-server
```

The server will start on port 5672 with in-memory storage.

### Persistent Storage (Production)

```bash
amqp-server --storage badger --storage-path ./data
```

### Common Options

```bash
# Custom port
amqp-server --port 5673

# Enable debug logging
amqp-server --log-level debug

# With authentication
amqp-server --auth --auth-file users.json

# All together
amqp-server \
  --storage badger \
  --storage-path /var/lib/amqp \
  --auth --auth-file /etc/amqp/users.json \
  --log-level info \
  --log-file /var/log/amqp/server.log
```

## Test Connection

Choose your language and try a quick test:

### Python

```bash
pip install pika
```

```python
import pika

connection = pika.BlockingConnection(
    pika.ConnectionParameters('localhost')
)
channel = connection.channel()

channel.queue_declare(queue='hello')

channel.basic_publish(
    exchange='',
    routing_key='hello',
    body='Hello World!'
)

print(" [x] Sent 'Hello World!'")
connection.close()
```

### Node.js

```bash
npm install amqplib
```

```javascript
const amqp = require('amqplib');

async function main() {
    const connection = await amqp.connect('amqp://localhost');
    const channel = await connection.createChannel();

    await channel.assertQueue('hello', { durable: false });

    channel.sendToQueue('hello', Buffer.from('Hello World!'));
    console.log(" [x] Sent 'Hello World!'");

    await connection.close();
}

main().catch(console.error);
```

### Go

```bash
go get github.com/rabbitmq/amqp091-go
```

```go
package main

import (
    "log"
    amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
    conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
    if err != nil {
        log.Fatal(err)
    }
    defer conn.Close()

    ch, err := conn.Channel()
    if err != nil {
        log.Fatal(err)
    }
    defer ch.Close()

    q, err := ch.QueueDeclare("hello", false, false, false, false, nil)
    if err != nil {
        log.Fatal(err)
    }

    err = ch.Publish("", q.Name, false, false, amqp.Publishing{
        ContentType: "text/plain",
        Body:        []byte("Hello World!"),
    })
    if err != nil {
        log.Fatal(err)
    }

    log.Println(" [x] Sent 'Hello World!'")
}
```

## Configuration File

Create `config.json`:

```json
{
  "network": {
    "address": ":5672",
    "max_connections": 1000
  },
  "storage": {
    "backend": "badger",
    "path": "./data"
  },
  "security": {
    "authentication_enabled": false
  },
  "server": {
    "log_level": "info"
  }
}
```

Run with config:

```bash
amqp-server --config config.json
```

## Production Deployment

### Linux (systemd)

See `deployment/systemd/amqp-server.service` for a systemd service file.

```bash
# Copy service file
sudo cp deployment/systemd/amqp-server.service /etc/systemd/system/

# Reload systemd
sudo systemctl daemon-reload

# Enable service
sudo systemctl enable amqp-server

# Start service
sudo systemctl start amqp-server

# Check status
sudo systemctl status amqp-server

# View logs
sudo journalctl -u amqp-server -f
```

## Monitoring

Access Prometheus metrics:

```bash
# If metrics server is enabled
curl http://localhost:9419/metrics
```

## Next Steps

- Read [CLIENT_EXAMPLES.md](CLIENT_EXAMPLES.md) for complete examples with consumers, exchanges, and patterns
- Review [README.md](README.md) for configuration options
- Check [SECURITY.md](SECURITY.md) for authentication setup
- See [CONTRIBUTING.md](CONTRIBUTING.md) to contribute

## Troubleshooting

### Port Already in Use

```bash
# Find process using port 5672
sudo lsof -i :5672
# or
sudo netstat -tlnp | grep 5672

# Kill the process or use a different port
amqp-server --port 5673
```

### Permission Denied

```bash
# Linux: Allow binding to port 5672
sudo setcap 'cap_net_bind_service=+ep' /usr/local/bin/amqp-server

# Or run on a higher port (>1024)
amqp-server --port 15672
```

### Connection Refused

- Check server is running: `ps aux | grep amqp-server`
- Check firewall: `sudo ufw status`
- Check logs: `amqp-server --log-level debug`

## Help

```bash
amqp-server --help
```

For more detailed documentation, see [README.md](README.md).
