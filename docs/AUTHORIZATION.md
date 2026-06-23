# Authorization

StrangeQ implements RabbitMQ-compatible authorization using per-user, per-vhost permission triples.

## Permission Model

Each user has a set of **virtual host permissions**, one per vhost. Each vhost permission is a triple of regular expressions:

| Permission | Operations |
|---|---|
| **configure** | Creating or destroying resources (declare/delete exchange/queue) |
| **write** | Injecting messages into a resource (publish, bind queue to exchange) |
| **read** | Retrieving messages from a resource (consume, get, bind exchange) |

Each regex pattern is matched against the resource name. Patterns are **unanchored** (Go regexp default) — use `^...$` to restrict matches. An empty pattern denies all operations for that action.

### Operation → Permission Mapping

| AMQP Operation | configure | write | read |
|----------------|-----------|-------|------|
| exchange.declare | exchange | | |
| exchange.delete | exchange | | |
| exchange.bind | | exchange (dest) | exchange (src) |
| exchange.unbind | | exchange (dest) | exchange (src) |
| queue.declare | queue | | |
| queue.delete | queue | | |
| queue.bind | | queue | exchange |
| queue.unbind | | queue | exchange |
| basic.publish | | exchange | |
| basic.consume | | | queue |
| basic.get | | | queue |
| queue.purge | | | queue |

### Default Exchange

The AMQP default exchange (empty name) is mapped to `amq.default` for permission checks. This matches RabbitMQ behavior.

### Reserved Names

Exchange and queue names starting with `amq.` are reserved for pre-declared and standardized resources. Clients may declare `amq.*` names only with the **passive** flag (checking existence) or if the resource already exists. Non-passive declaration of `amq.*` names returns **403 (access-refused)**.

Server-generated queue names use the `amq.gen.` prefix and are exempt from this restriction.

## Auth File Format

Authorization is configured via the auth file (JSON). Each user has `vhost_permissions` with a permission triple per vhost:

```json
{
  "users": [
    {
      "username": "admin",
      "password_hash": "$2a$10$...",
      "vhost_permissions": [
        {
          "vhost": "/",
          "permission": {
            "configure": ".*",
            "write": ".*",
            "read": ".*"
          }
        }
      ],
      "tags": ["administrator"]
    },
    {
      "username": "producer",
      "password_hash": "$2a$10$...",
      "vhost_permissions": [
        {
          "vhost": "/",
          "permission": {
            "configure": "^$",
            "write": ".*",
            "read": "^$"
          }
        }
      ]
    },
    {
      "username": "consumer",
      "password_hash": "$2a$10$...",
      "vhost_permissions": [
        {
          "vhost": "/",
          "permission": {
            "configure": "^$",
            "write": "^$",
            "read": ".*"
          }
        }
      ]
    },
    {
      "username": "guest",
      "password_hash": "$2a$10$...",
      "vhost_permissions": [
        {
          "vhost": "/",
          "permission": {
            "configure": ".*",
            "write": ".*",
            "read": ".*"
          }
        }
      ],
      "tags": ["administrator"],
      "loopback_only": true
    }
  ]
}
```

### Loopback Restriction

Users with `"loopback_only": true` can only connect from localhost (loopback interface). This is enabled by default for the `guest` user, matching RabbitMQ behavior. Remote connections from loopback-restricted users are refused with **403 (access-refused)**.

### Backward Compatibility

The old `permissions` field (resource/action/pattern) is automatically migrated to `vhost_permissions` on load. Each legacy action (`configure`/`write`/`read`) is mapped to the corresponding permission field using its pattern. Missing actions default to `^$` (deny all). A warning is logged during migration.

## Configuration

Enable authorization in the config file:

```yaml
security:
  authentication_enabled: true
  authorization_enabled: true
  auth_mechanisms:
    - PLAIN
```

Or via command-line / environment variables:

```bash
amqp-server --config config.yaml
# AMQP_SECURITY_AUTHENTICATION_ENABLED=true
# AMQP_SECURITY_AUTHORIZATION_ENABLED=true
```

## Error Handling

Authorization failures are **channel-level soft errors** (403 access-refused). The server sends a `channel.close` frame with reply code 403 and closes the channel. The connection remains alive — only the channel is closed. The client can open a new channel and continue.

Vhost access denial at `connection.open` is a **connection-level error** (402 invalid-path). The server sends `connection.close` and terminates the connection.
