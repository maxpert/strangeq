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

Enable authorization in the config file (canonical concatenated keys):

```yaml
security:
  authenticationenabled: true
  authorizationenabled: true
  authmechanisms:
    - PLAIN
```

Snake_case spellings (`authentication_enabled`, `authorization_enabled`,
`auth_mechanisms`) are also accepted since v0.1.0; both bind to the same
fields. Unknown keys are reported at startup rather than silently ignored.

Environment variables apply with or without `--config`:

```bash
AMQP_SECURITY_AUTHENTICATION_ENABLED=true \
AMQP_SECURITY_AUTHORIZATION_ENABLED=true \
  amqp-server --config config.yaml
```

## Connection access control

Four lists restrict who may connect. Empty lists mean "no restriction".

```yaml
security:
  allowedhosts: ["10.0.0.0/8", "2001:db8::/32", "localhost"]
  blockedhosts: ["10.9.9.9"]
  allowedusers: ["app"]
  blockedusers: ["mallory"]
```

- **Host entries** are IP literals, CIDR prefixes, or the literal `localhost`
  (= `127.0.0.0/8` plus `::1/128`). Hostnames are rejected at startup:
  matching them would require reverse DNS on the accept path.
- **Precedence:** a blocked match wins; otherwise, when `allowedhosts` is
  non-empty, the peer must match it. `allowedhosts` has no implicit loopback
  exemption — add `localhost` or `127.0.0.0/8` if local clients should connect.
- **IPv4-mapped IPv6** peers (`::ffff:10.0.0.5`) match IPv4 entries.
- **Host lists apply even when authentication is disabled.** They are network
  policy, not identity policy.
- **User lists apply only when authentication is enabled**, because without it
  every connection is the unverified `guest` identity. Usernames are matched
  exactly (case-sensitive). A refusal sends the same `Authentication failed`
  text as a wrong password, so it does not reveal whether the username exists.
- A non-TCP peer (for example a test `net.Pipe`) is allowed only when both host
  lists are empty; otherwise it is refused (fail closed).

### Failed-authentication limiting

After `security.authfailurelimit` failures in a window, further attempts from
that peer are refused before any bcrypt work:

```yaml
security:
  authfailurelimit: 5
  authfailurewindowms: 60000
```

The budget is per IPv4 address, and per IPv6 `/64` prefix (a single host
usually owns an entire /64, so per-address keys are easy to evade). Set
`authfailurelimit: 0` to disable. Behind a TCP proxy every client appears as
the proxy's address and shares one budget — raise the limit or disable it in
that topology.

Refusals are counted in `amqp_auth_failures_total{reason}` with one of
`host_blocked`, `host_not_allowed`, `user_blocked`, `user_not_allowed`,
`rate_limited`, `bad_credentials`, or `unsupported_mechanism`.


## Error Handling

Authorization failures are **channel-level soft errors** (403 access-refused). The server sends a `channel.close` frame with reply code 403 and closes the channel. The connection remains alive — only the channel is closed. The client can open a new channel and continue.

Vhost access denial at `connection.open` is a **connection-level error** (402 invalid-path). The server sends `connection.close` and terminates the connection.
