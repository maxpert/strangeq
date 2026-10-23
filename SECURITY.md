# Security Policy

## Supported Versions

| Version | Supported |
| ------- | ---------- |
| 0.1.x   | Yes        |
| < 0.1   | No         |

## Reporting a Vulnerability

Please do **not** open a public issue for security problems.

Preferred: use GitHub's private vulnerability reporting —
https://github.com/maxpert/strangeq/security/advisories/new
(Repository maintainers: enable it under Settings → Code security → Private vulnerability reporting.)

Alternative: email <!-- MAINTAINER: replace with a real, monitored address before release --> `SECURITY-CONTACT-TBD`.

Include affected version (`amqp-server --version`), configuration (redact secrets), reproduction steps, and impact.
We aim to acknowledge within 48 hours. Fixes are released as patch versions and announced via a GitHub Security Advisory.

## Security Best Practices

### Authentication
- Enable authentication in production
- Use PLAIN mechanism with TLS to protect credentials
- Never use ANONYMOUS mechanism in production
- Rotate passwords regularly
- Use strong passwords (bcrypt hashed with high cost)

### Network Security
- Enable TLS for all production deployments
- Use valid, trusted certificates
- Restrict network access using firewalls

### Configuration Security
- Store configuration files with restricted permissions (0600)
- Never commit credentials to version control
- Use environment variables or secret management for sensitive data

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
  fsync: true

security:
  tlsenabled: true
  tlscertfile: /etc/amqp/certs/server.crt
  tlskeyfile: /etc/amqp/certs/server.key
  authenticationenabled: true
  authenticationfilepath: /etc/amqp/auth.json
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

## Known Limitations

1. **Rate limiting**: Basic per-IPv4/per-IPv6-/64 failed-authentication limiting only (`security.authfailurelimit`); there is no general request-rate limiting
2. **SASL EXTERNAL**: Not supported (certificate-based auth via mTLS is supported)
3. **Audit logging**: Not implemented

## Security Checklist

- [ ] TLS enabled with valid certificates
- [ ] Authentication enabled (PLAIN with TLS)
- [ ] ANONYMOUS mechanism disabled
- [ ] Strong passwords used
- [ ] Configuration files have restricted permissions
- [ ] Storage directories protected
- [ ] Firewall rules configured
- [ ] Monitoring and alerting enabled
- [ ] Telemetry bound to loopback; pprof disabled
- [ ] Connection access control (`allowedhosts`/`allowedusers`) reviewed
