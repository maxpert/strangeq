StrangeQ AMQP 0.9.1 Server - Quick Start Guide
==============================================

Thank you for downloading StrangeQ (module: github.com/maxpert/amqp-go).

## Quick Start

1. Extract this package:
   tar -xzf amqp-server-*.tar.gz
   cd amqp-server-*/

2. Start the server:
   ./amqp-server

   Defaults: persistent storage in ./data, listening on 127.0.0.1:5672,
   authentication disabled (loopback only). The first start creates ./data.

3. With the shipped sample configuration:
   cp config.sample.yaml config.yaml
   ./amqp-server --config config.yaml

## Defaults

- Address: 127.0.0.1:5672 (loopback only)
- Storage: persistent, ./data (fsync on, CRC on)
- Authentication: disabled
- TLS: disabled
- Telemetry: disabled

Binding to a non-loopback address with authentication disabled logs a loud
SECURITY WARNING at startup. Set security.authenticationenabled: true before
exposing the broker to a network.

## Environment variables

Every setting can be overridden with AMQP_<SECTION>_<KEY>, with or without
--config. Precedence: defaults -> file -> env -> explicit flags.

  AMQP_NETWORK_ADDRESS=0.0.0.0:5672 ./amqp-server
  AMQP_STORAGE_PATH=/var/lib/amqp ./amqp-server --config config.yaml
  AMQP_SECURITY_AUTHENTICATIONENABLED=true ./amqp-server

## Command Line Options

Usage: ./amqp-server [options]

Options:
  --config string
        Configuration file path (YAML/JSON)
  --version
        Show version, commit, build date and Go version
  --generate-config string
        Generate a default config file (mode 0600) and exit
  --tls
        Enable TLS (amqps)
  --tls-cert string
        Path to TLS certificate file (PEM)
  --tls-key string
        Path to TLS private key file (PEM)
  --tls-ca string
        Path to TLS CA file for mutual TLS client verification (PEM)
  --unsafe-recovery
        DATA-LOSS SWITCH: start even when recovery cannot complete safely
  --enable-telemetry
        Enable telemetry HTTP endpoint (/metrics, /health)
  --telemetry-port int
        Telemetry HTTP port (default from config: 9419)
  --telemetry-address string
        Telemetry bind address (default from config: 127.0.0.1)
  --enable-pprof
        Enable pprof profiling endpoints (loopback only; off by default)
  --healthcheck
        Probe the local /health endpoint and exit 0/1 (container use)
  -h
        List all flags

## Configuration notes

- Keys are case-insensitive and accept snake_case (authentication_enabled)
  or concatenated (authenticationenabled) spellings.
- Unknown keys are reported at startup and ignored.
- security.allowedhosts/blockedhosts accept IPs, CIDR ranges or "localhost";
  hostnames are rejected (reverse DNS is not used). A blocked entry beats an
  allowed entry. User lists apply only when authentication is enabled.
- The per-peer failed-auth limiter is security.authfailurelimit (default 5 per
  60 s); set 0 to disable.

## Health and shutdown

With telemetry enabled, GET /health returns 200 only after recovery completed
and the AMQP listener is bound; it returns 503 while starting and immediately
after SIGTERM. SIGTERM triggers a graceful shutdown with a 30 s budget.

## Documentation

Full documentation: https://github.com/maxpert/strangeq
Report issues: https://github.com/maxpert/strangeq/issues

## License

See LICENSE file for details.
