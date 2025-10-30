# Security Policy

## Supported Versions

We release patches for security vulnerabilities for the following versions:

| Version | Supported          |
| ------- | ------------------ |
| 0.1.x   | :white_check_mark: |
| < 0.1   | :x:                |

## Reporting a Vulnerability

We take the security of AMQP-Go seriously. If you discover a security vulnerability, please follow these steps:

### Please Do Not
- Open a public issue
- Discuss the vulnerability in public forums
- Exploit the vulnerability

### Please Do
1. **Email the maintainers** with details about the vulnerability
   - Include a description of the vulnerability
   - Steps to reproduce
   - Potential impact
   - Any suggested fixes (if available)

2. **Wait for acknowledgment** - We will acknowledge receipt within 48 hours

3. **Allow time for a fix** - We will work to address the issue as quickly as possible

## What to Expect

1. **Acknowledgment**: We will confirm receipt of your report within 48 hours
2. **Assessment**: We will assess the vulnerability and determine its severity
3. **Fix Development**: We will work on a fix and keep you updated on progress
4. **Disclosure**: Once a fix is available, we will:
   - Release a security advisory
   - Credit you for the discovery (unless you prefer to remain anonymous)
   - Release patched versions

## Security Best Practices

When deploying AMQP-Go in production:

### Authentication
- **Always enable authentication** in production environments
- Use **PLAIN mechanism with TLS** to protect credentials
- **Never use ANONYMOUS** mechanism in production
- Rotate passwords regularly
- Use strong passwords (bcrypt hashed with high cost)

### Network Security
- **Enable TLS/SSL** for all production deployments
- Use valid, trusted certificates
- Keep certificates up to date
- Restrict network access using firewalls
- Use VPNs or private networks when possible

### Configuration Security
- Store configuration files with restricted permissions (0600)
- Never commit credentials to version control
- Use environment variables or secret management for sensitive data
- Keep authentication files outside the web root

### Storage Security
- Protect storage directories with appropriate file permissions
- Regular backups of persistent storage
- Encrypt storage at rest if handling sensitive data
- Monitor storage for unauthorized access

### Monitoring
- Enable and monitor Prometheus metrics
- Set up alerts for unusual activity
- Log authentication failures
- Monitor connection patterns
- Track resource usage

### Updates
- Keep AMQP-Go updated to the latest version
- Subscribe to security advisories
- Test updates in staging before production
- Have a rollback plan

### Example Secure Configuration

```json
{
  "network": {
    "address": ":5672",
    "max_connections": 1000,
    "connection_timeout": "30s"
  },
  "storage": {
    "backend": "badger",
    "path": "/var/lib/amqp/data",
    "sync_writes": true
  },
  "security": {
    "tls_enabled": true,
    "tls_cert_file": "/etc/amqp/certs/server.crt",
    "tls_key_file": "/etc/amqp/certs/server.key",
    "authentication_enabled": true,
    "authentication_backend": "file",
    "authentication_config": {
      "user_file": "/etc/amqp/users.json"
    },
    "authorization_enabled": true,
    "allowed_hosts": ["10.0.0.0/8", "192.168.0.0/16"]
  },
  "server": {
    "log_level": "warn",
    "log_file": "/var/log/amqp/server.log"
  }
}
```

## Known Security Considerations

### Current Limitations
1. **Authorization**: Fine-grained authorization is not yet fully implemented
2. **Certificate Validation**: mTLS (mutual TLS) is not yet supported
3. **Rate Limiting**: Built-in rate limiting is not yet implemented

### Planned Improvements
- Full authorization system with ACLs
- Mutual TLS authentication
- Rate limiting and connection throttling
- Audit logging
- LDAP/Active Directory integration

## Security Checklist for Production

- [ ] TLS enabled with valid certificates
- [ ] Authentication enabled (PLAIN with TLS)
- [ ] ANONYMOUS mechanism disabled
- [ ] Strong passwords used (12+ characters)
- [ ] Configuration files have restricted permissions
- [ ] Storage directories protected
- [ ] Firewall rules configured
- [ ] Monitoring and alerting enabled
- [ ] Regular backups configured
- [ ] Update plan in place
- [ ] Incident response plan documented

## Additional Resources

- [AMQP Security Considerations](https://www.amqp.org/security)
- [OWASP Top 10](https://owasp.org/www-project-top-ten/)
- [CWE/SANS Top 25](https://www.sans.org/top25-software-errors/)

## Contact

For security issues, please contact the maintainers directly rather than using public channels.

