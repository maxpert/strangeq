# Quick Start Guide

Get AMQP-Go up and running in 5 minutes!

## Installation

### Option 1: Download Pre-built Binary (Recommended)

1. Go to the [Releases page](https://github.com/YOUR-ORG/strangeq/releases)
2. Download the binary for your platform:
   - macOS ARM64: `amqp-server-darwin-arm64`
   - macOS Intel: `amqp-server-darwin-amd64`
   - Linux AMD64: `amqp-server-linux-amd64`
   - Linux ARM64: `amqp-server-linux-arm64`
   - Linux 32-bit: `amqp-server-linux-386`
   - Windows 64-bit: `amqp-server-windows-amd64.exe`
   - Windows 32-bit: `amqp-server-windows-386.exe`

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
git clone https://github.com/YOUR-ORG/strangeq.git
cd strangeq/src/amqp-go
go build -o amqp-server ./cmd/amqp-server
sudo mv amqp-server /usr/local/bin/
```

### Option 3: Go Install

```bash
go install github.com/maxpert/amqp-go/cmd/amqp-server@latest
```

## Basic Usage

### Start Server (In-Memory)

```bash
amqp-server
```

The server will start on port 5672 with in-memory storage.

### Start Server (Persistent Storage)

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

# With TLS
amqp-server --tls --tls-cert server.crt --tls-key server.key

# All together
amqp-server \
  --storage badger \
  --storage-path /var/lib/amqp \
  --auth --auth-file /etc/amqp/users.json \
  --tls --tls-cert /etc/amqp/certs/server.crt --tls-key /etc/amqp/certs/server.key \
  --log-level info \
  --log-file /var/log/amqp/server.log
```

## First Steps

### 1. Test with Python

```python
import pika

# Connect to server
connection = pika.BlockingConnection(
    pika.ConnectionParameters('localhost')
)
channel = connection.channel()

# Declare queue
channel.queue_declare(queue='hello')

# Publish message
channel.basic_publish(
    exchange='',
    routing_key='hello',
    body='Hello World!'
)

print(" [x] Sent 'Hello World!'")

# Consume message
def callback(ch, method, properties, body):
    print(f" [x] Received {body}")

channel.basic_consume(
    queue='hello',
    on_message_callback=callback,
    auto_ack=True
)

print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()

connection.close()
```

### 2. Test with Go

```go
package main

import (
    "log"
    amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
    // Connect to server
    conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
    if err != nil {
        log.Fatal(err)
    }
    defer conn.Close()

    // Create channel
    ch, err := conn.Channel()
    if err != nil {
        log.Fatal(err)
    }
    defer ch.Close()

    // Declare queue
    q, err := ch.QueueDeclare(
        "hello", // name
        false,   // durable
        false,   // delete when unused
        false,   // exclusive
        false,   // no-wait
        nil,     // arguments
    )
    if err != nil {
        log.Fatal(err)
    }

    // Publish message
    err = ch.Publish(
        "",     // exchange
        q.Name, // routing key
        false,  // mandatory
        false,  // immediate
        amqp.Publishing{
            ContentType: "text/plain",
            Body:        []byte("Hello World!"),
        },
    )
    if err != nil {
        log.Fatal(err)
    }

    log.Println("Message sent!")
}
```

### 3. Test with Node.js

```javascript
const amqp = require('amqplib');

async function main() {
    // Connect to server
    const connection = await amqp.connect('amqp://localhost');
    const channel = await connection.createChannel();

    // Declare queue
    const queue = 'hello';
    await channel.assertQueue(queue, { durable: false });

    // Publish message
    channel.sendToQueue(queue, Buffer.from('Hello World!'));
    console.log(" [x] Sent 'Hello World!'");

    // Consume message
    channel.consume(queue, (msg) => {
        console.log(" [x] Received:", msg.content.toString());
    }, { noAck: true });
}

main().catch(console.error);
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

## Setting up Authentication

1. Create `users.json`:

```json
{
  "users": [
    {
      "username": "admin",
      "password_hash": "$2a$10$YOUR_BCRYPT_HASH_HERE",
      "permissions": [
        {
          "resource": ".*",
          "action": "configure",
          "pattern": ".*"
        },
        {
          "resource": ".*",
          "action": "write",
          "pattern": ".*"
        },
        {
          "resource": ".*",
          "action": "read",
          "pattern": ".*"
        }
      ]
    }
  ]
}
```

2. Generate password hash:

```go
package main

import (
    "fmt"
    "log"
    "golang.org/x/crypto/bcrypt"
)

func main() {
    password := "your-password"
    hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println(string(hash))
}
```

3. Start server with authentication:

```bash
amqp-server --auth --auth-file users.json
```

4. Connect with credentials:

```python
# Python
credentials = pika.PlainCredentials('admin', 'your-password')
connection = pika.BlockingConnection(
    pika.ConnectionParameters('localhost', credentials=credentials)
)
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

- Read the [Configuration Guide](README.md#configuration)
- Set up [Authentication](README.md#authentication)
- Configure [TLS/SSL](README.md#security-configuration)
- Enable [Metrics](README.md#monitoring-and-metrics)
- Review [Security Best Practices](SECURITY.md)

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

