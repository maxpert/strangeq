AMQP-Go Server - Quick Start Guide
====================================

Thank you for downloading AMQP-Go Server!

## Quick Start

1. Extract this package:
   tar -xzf amqp-server-*.tar.gz
   cd amqp-server-*/

2. Start the server (in-memory mode, no persistence):
   ./amqp-server

3. Or with custom config:
   cp config.sample.json config.json
   # Edit config.json as needed
   ./amqp-server -config config.json

## Default Configuration

The server starts with these defaults:
- Address: localhost:5672 (AMQP default port)
- Storage: In-memory (data lost on restart)
- Authentication: Disabled (open access)
- TLS: Disabled

## Common Configuration Changes

### Enable Persistence (Recommended for Production)

Edit config.json:
{
  "storage": {
    "backend": "badger",    # Change from "memory" to "badger"
    "path": "./data",       # Data directory
    "persistent": true      # Enable persistence
  }
}

### Enable Authentication

Edit config.json:
{
  "security": {
    "authentication_enabled": true,
    "authentication_backend": "file",
    "authentication_config": {
      "user_file": "users.json"
    }
  }
}

Create users.json:
{
  "users": [
    {
      "username": "admin",
      "password_hash": "$2a$10$...",  # Use bcrypt hash
      "tags": ["administrator"]
    }
  ]
}

### Run as Daemon (Background Process)

./amqp-server -config config.json -daemonize -log /var/log/amqp-server.log

### Enable TLS

Edit config.json:
{
  "security": {
    "tls_enabled": true,
    "tls_cert_file": "/path/to/server.crt",
    "tls_key_file": "/path/to/server.key"
  }
}

## Command Line Options

Usage: ./amqp-server [options]

Options:
  -config string
        Path to configuration file
  -addr string
        Server address (default ":5672")
  -storage string
        Storage backend: memory or badger (default "memory")
  -data-dir string
        Data directory path (default "./data")
  -log-level string
        Log level: debug, info, warn, error (default "info")
  -daemonize
        Run as daemon (background process)
  -log string
        Log file path (for daemon mode)
  -pid string
        PID file path
  -version
        Show version information

## Testing the Server

Using rabbitmq-amqp091-go client:
  go get github.com/rabbitmq/amqp091-go

Example client code:
  conn, err := amqp.Dial("amqp://localhost:5672/")
  // ... use connection

## Documentation

Full documentation: https://github.com/maxpert/strangeq
Report issues: https://github.com/maxpert/strangeq/issues

## Performance Tips

1. Use Badger storage for persistence
2. Disable sync_writes for better write performance (less durability)
3. Increase cache_size for better read performance
4. Adjust max_connections based on your workload
5. Enable TCP keep-alive for long-lived connections

## License

See LICENSE file for details.

## Support

For questions and support:
- GitHub Issues: https://github.com/maxpert/strangeq/issues
- Documentation: https://github.com/maxpert/strangeq

Enjoy using AMQP-Go Server!
