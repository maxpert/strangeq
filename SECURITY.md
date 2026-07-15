# Security Policy

## Supported Versions

| Version | Supported |
| ------- | ---------- |
| 0.1.x   | Yes        |
| < 0.1   | No         |

## Reporting a Vulnerability

Do not open a public issue. Email the maintainers with details, reproduction steps, and potential impact. You will receive acknowledgment within 48 hours. Once a fix is available, a security advisory is published and patched versions are released.

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
  address: ":5672"
  maxconnections: 10000

storage:
  path: /var/lib/amqp/data

security:
  tlsenabled: true
  tlscertfile: /etc/amqp/certs/server.crt
  tlskeyfile: /etc/amqp/certs/server.key
  authenticationenabled: true
  authenticationfilepath: /etc/amqp/auth.json

server:
  loglevel: warn
```

## Known Limitations

1. **Rate limiting**: Not implemented
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
