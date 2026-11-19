# Security Policy

## Supported Versions

Use the latest published release. This repository does not state a maintenance
window or backport policy for older releases. Consult the
[release history](CHANGELOG.md) for available fixes.

## Reporting a Vulnerability

Please do **not** open a public issue for security problems.

Use [GitHub's private vulnerability reporting](https://github.com/maxpert/strangeq/security/advisories/new)
if it is enabled for this repository. No monitored security email address is
published here. If private reporting is unavailable, contact a repository
maintainer privately without disclosing exploit details in a public issue.

Include affected version (`amqp-server --version`), configuration (redact secrets), reproduction steps, and impact.
We aim to acknowledge reports within 48 hours. Fixes are released as patch
versions and announced through a GitHub Security Advisory.

## Security Best Practices

### Authentication
- Enable authentication in production
- Use PLAIN mechanism with TLS to protect credentials
- Never use ANONYMOUS mechanism in production
- Rotate passwords regularly
- Use strong passwords; the broker verifies them with bcrypt (`bcrypt.CompareHashAndPassword`). The cost factor comes from the stored hash — the shipped systemd example generates cost-10 hashes.

### Network Security
- Enable TLS for all production deployments
- Use valid, trusted certificates
- Restrict network access using firewalls

### Configuration Security
- Restrict configuration, auth-file, and private-key access to their owner or
  the service group (for example, 0600 or root-owned 0640 with group `amqp`)
- Never commit credentials to version control
- Keep password hashes in the protected auth file. Load private keys from
  protected files or your deployment secret manager

### Storage Security
- Protect storage directories with appropriate file permissions
- Regular backups of persistent storage

### Monitoring
- Enable Prometheus metrics
- Set up alerts for unusual activity
- Log authentication failures

### Example Secure Configuration

```yaml
network:
  address: "0.0.0.0:5672"
  maxconnections: 10000

storage:
  path: /var/lib/amqp/data
  durability: barrier

security:
  tlsenabled: true
  tlscertfile: /etc/amqp/certs/server.crt
  tlskeyfile: /etc/amqp/certs/server.key
  authenticationenabled: true
  authenticationfilepath: /etc/amqp/auth.json
  authorizationenabled: true
  authmechanisms: [PLAIN]
  allowedhosts: ["10.0.0.0/8", "localhost"]
  blockedusers: []
  authfailurelimit: 5
  authfailurewindowms: 60000

telemetry:
  enabled: true
  address: 127.0.0.1
  port: 9419
  pprofenabled: false

server:
  loglevel: warn
```

Create `/etc/amqp/auth.json` with bcrypt hashes and per-vhost permissions before
starting the broker. See [Authorization](docs/AUTHORIZATION.md). The example
keeps TLS on port 5672, so clients use `amqps://HOST:5672/`. For certificate
setup and port selection, see [TLS configuration](docs/TLS.md).

## Known Limitations

1. **Rate limiting**: Basic per-IPv4/per-IPv6-/64 failed-authentication limiting only (`security.authfailurelimit`); there is no general AMQP request-rate limiting
2. **SASL EXTERNAL**: Not supported. Mutual TLS verifies client certificates at
   the transport layer. It does not map certificate identities to broker users.
   Enable SASL authentication and authorization separately. Management connection
   `secure`/`ssl` flags report TLS transport, not user authorization.
3. **Audit logging**: Standalone management mutations can emit structured audit
   events through the broker logger. This is not a durable audit trail, and
   there is no general AMQP operation audit trail.

## Security Checklist

- [ ] TLS enabled with valid certificates
- [ ] Authentication and authorization enabled (PLAIN with TLS)
- [ ] ANONYMOUS mechanism disabled
- [ ] Strong passwords used
- [ ] Configuration files have restricted permissions
- [ ] Storage directories protected
- [ ] Firewall rules configured
- [ ] Monitoring and alerting enabled
- [ ] Telemetry bound to loopback; pprof disabled
- [ ] Connection access control (`allowedhosts`/`allowedusers`) reviewed
