# Phase 15: TLS + Authorization Implementation Plan

## Spec Foundation

### What the AMQP 0.9.1 spec says
- **No `access` class** — RabbitMQ removed it; the deprecated `ticket` field (must be zero, ignored) is all that remains
- **No TLS in the wire spec** — TLS is an out-of-band transport concern; the spec assumes a TCP socket
- **SASL auth** — `connection.start`/`start-ok`/`secure`/`secure-ok` negotiate identity
- **Vhost authorization** — `connection.open` rule `separation` (MUST enforce full vhost isolation) + rule `security` (SHOULD verify client has permission to access the vhost); error `invalid-path` (402) for unknown vhost
- **Reserved names** — `exchange.declare`/`queue.declare` rule `reserved` → `access-refused` (403) for `amq.` prefix on non-passive declare
- **Error codes** — 402 (invalid-path), 403 (access-refused), 530 (not-allowed)

### What RabbitMQ's de-facto model adds (the standard all clients expect)
- **Per-user, per-vhost permission triple**: `configure` / `write` / `read` as regex patterns
- **Operation → permission mapping**:
  | AMQP Operation | configure | write | read |
    |----------------|-----------|-------|------|
    | exchange.declare | exchange | | |
    | exchange.delete | exchange | | |
    | queue.declare | queue | | |
    | queue.delete | queue | | |
    | queue.bind | | queue | exchange |
    | queue.unbind | | queue | exchange |
    | exchange.bind | | exchange (dest) | exchange (src) |
    | basic.publish | | exchange | |
    | basic.consume | | | queue |
    | basic.get | | | queue |
    | queue.purge | | | queue |
- **Loopback restriction** — default `guest` user can only connect from localhost
- **EXTERNAL SASL mechanism** — x.509 certificate-based auth via TLS
- **Default exchange** — blank name mapped to `amq.default` for permission checks

### Current codebase gaps
- `SecurityConfig` has TLS fields but `Server.Start()` always uses `net.Listen("tcp")` — no TLS path
- `Authenticator.Authorize()` exists but is a stub (`return nil` = allow all)
- `Permission` struct has `Resource/Action/Pattern` but doesn't model the configure/write/read triple
- `processConnectionOpen` only allows `/` — no per-user vhost permission check
- No authorization checks in any handler before performing operations
- No reserved `amq.` name enforcement
- No loopback restriction for guest user

---

## Part A: TLS Support

### Commit A1: TLS listener and config validation

**Files to modify:**
- `server/server.go` — `Start()`: create `tls.Listener` when `Security.TLSEnabled`
- `server/tls.go` (new) — `createTLSListener(addr, cfg)`, `loadTLSConfig(securityCfg)` helpers

**Design:**
- `loadTLSConfig()` reads cert/key files, builds `tls.Config` with min version TLS 1.2
- When `TLSCAFile` is set → mutual TLS: `ClientAuth: tls.RequireAndVerifyClientCert`
- When `TLSCAFile` is empty → server-only TLS: `ClientAuth: tls.NoClientCert`
- `Start()` checks `s.Config.Security.TLSEnabled` and calls `createTLSListener` instead of `net.Listen`
- Everything downstream (`handleConnection`, frame reading) is unchanged — TLS wraps the raw TCP, AMQP protocol runs on top

**Tests (`server/tls_test.go`):**
- `TestLoadTLSConfig` — valid cert/key produces working `*tls.Config`
- `TestLoadTLSConfigMutualTLS` — CA file set → `ClientAuth == RequireAndVerifyClientCert`
- `TestLoadTLSConfigMissingCert` — missing cert file returns error
- `TestLoadTLSConfigMissingKey` — missing key file returns error
- `TestTLSCertGeneration` — helper to generate self-signed test certs in `t.TempDir()`

### Commit A2: Builder wiring and CLI flags

**Files to modify:**
- `server/builder.go` — `Build()`: log TLS status, validate TLS config in config.Validate
- `config/config.go` — `Validate()`: already checks cert/key exist (lines 163-175), add CA file check if set
- `cmd/amqp-server/main.go` — add `--tls`, `--tls-cert`, `--tls-key`, `--tls-ca` flags

**Tests:**
- `TestBuilderWithTLS` — `WithTLS()` + `Build()` produces server with TLS enabled
- `TestConfigValidateTLS` — TLS enabled without cert → error; with non-existent cert → error; with valid paths → OK
- Integration: `TestTLSConnection` in `tls_integration_test.go` — start server with self-signed cert, connect with `amqp091-go` using `tls.Dial`, verify basic publish/consume works

### Commit A3: TLS integration test and documentation

**Files to add:**
- `tls_integration_test.go` (package main, top-level) — full end-to-end TLS test

**Tests:**
- `TestTLSConnection` — start TLS server, connect with `amqp.Dial("amqps://...")`, declare queue, publish, consume, verify roundtrip
- `TestTLSConnectionAuth` — TLS + authentication enabled simultaneously
- `TestTLSMutualAuth` — server requires client cert; client with cert connects OK, client without cert rejected

**Documentation:**
- `docs/TLS.md` (new) — how to generate certs, configure TLS, enable mutual TLS, connect from clients

---

## Part B: Authorization

### Commit B1: Permission model redesign

**Files to modify:**
- `interfaces/server.go` — redesign `Permission` and `Operation` to match RabbitMQ model

**New types:**
```go
// Permission represents a per-vhost permission triple (RabbitMQ model).
// Each field is a regex pattern matched against resource names.
type Permission struct {
    Configure string `json:"configure"` // regex for configure ops (declare/delete)
    Write     string `json:"write"`     // regex for write ops (publish/bind queue to exchange)
    Read      string `json:"read"`      // regex for read ops (consume/get/bind exchange)
}

// VHostPermission ties a permission triple to a specific vhost.
type VHostPermission struct {
    VHost      string `json:"vhost"`
    Permission Permission `json:"permission"`
}

// Operation represents an authorization check request.
type Operation struct {
    Action     OperationAction // configure, write, read
    ResourceType ResourceType   // exchange, queue, vhost
    Resource   string           // resource name (exchange/queue name, or vhost name)
    VHost      string           // virtual host context
}

type OperationAction string
const (
    ActionConfigure OperationAction = "configure"
    ActionWrite     OperationAction = "write"
    ActionRead      OperationAction = "read"
)

type ResourceType string
const (
    ResourceExchange ResourceType = "exchange"
    ResourceQueue    ResourceType = "queue"
    ResourceVHost    ResourceType = "vhost"
)
```

- `User` struct updated: `VHostPermissions []VHostPermission` replaces `Permissions []Permission`
- `Authenticator.Authorize()` signature unchanged — still `(user *User, operation Operation) error`

**Tests (`auth/permission_test.go`):**
- `TestPermissionMatches` — regex matching for configure/write/read patterns
- `TestVHostPermissionLookup` — finding permissions for a specific vhost

### Commit B2: Implement Authorize() in FileAuthenticator

**Files to modify:**
- `auth/file_auth.go` — implement `Authorize()` with full RabbitMQ permission table

**Auth file format (backward-compatible migration):**
```json
{
  "users": [
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

**Authorize() logic:**
1. Find user's `VHostPermission` for `operation.VHost` — if none, return `AccessRefused`
2. Select the regex pattern based on `operation.Action` (configure/write/read)
3. Map default exchange: blank name → `amq.default` for matching
4. `regexp.MatchString(pattern, resourceName)` — if no match, return `AccessRefused`
5. If matched, return `nil` (allowed)

**Backward compatibility:**
- If old `permissions` field present (Resource/Action/Pattern), migrate to `vhost_permissions` with `.*` for all on vhost `/`
- `UserEntry` struct: add `VHostPermissions`, `Tags`, `LoopbackOnly`; keep old `Permissions` for migration

**Tests (`auth/file_auth_test.go`):**
- `TestAuthorizeConfigure` — exchange.declare matches configure regex
- `TestAuthorizeWrite` — basic.publish matches write regex
- `TestAuthorizeRead` — basic.consume matches read regex
- `TestAuthorizeNoVHostPermission` — user has no permission for vhost → refused
- `TestAuthorizeDefaultExchange` — blank exchange name mapped to `amq.default`
- `TestAuthorizeRegexPartial` — `^user1-.*` only matches user1's queues, not user2's
- `TestAuthorizeBackwardCompat` — old format permissions still work (migrated to allow-all)

### Commit B3: Vhost access check at connection.open

**Files to modify:**
- `server/connection_handlers.go` — `processConnectionOpen()`: check user has permission for vhost

**Logic:**
1. Parse `connection.open` virtual-host field
2. If `AuthorizationEnabled` and `Authenticator` is set:
   - Call `Authorize(user, Operation{Action: ActionConfigure, ResourceType: ResourceVHost, Resource: vhost, VHost: vhost})`
   - Actually: check if user has *any* VHostPermission entry for this vhost (presence = allowed)
   - If no permission → `sendConnectionClose(conn, 402, "INVALID_PATH", "access to vhost refused")` (per RabbitMQ behavior)
3. Loopback check: if user has `LoopbackOnly=true`, verify `conn.Conn.RemoteAddr()` is loopback
   - If not loopback → `sendConnectionClose(conn, 403, "ACCESS_REFUSED", "user can only connect via localhost")`
4. Set `conn.Vhost` and continue

**Store authenticated user on connection:**
- Add `User *interfaces.User` field to `protocol.Connection` (or `server`-level tracking)
- Set it during `handleConnectionStartOK` after successful authentication

**Tests (`server/authz_test.go`):**
- `TestVHostAccessAllowed` — user with permission for `/` can connect to `/`
- `TestVHostAccessRefused` — user without permission for `/myvhost` rejected with 402
- `TestLoopbackGuestAllowed` — guest from localhost connects OK
- `TestLoopbackGuestRefused` — guest from non-localhost rejected with 403

### Commit B4: Per-operation authorization checks in handlers

**Files to modify:**
- `server/exchange_handlers.go` — add authz checks in `handleExchangeDeclare`, `handleExchangeDelete`
- `server/queue_handlers.go` — add authz checks in `handleQueueDeclare`, `handleQueueDelete`, `handleQueueBind`, `handleQueueUnbind`
- `server/basic_handlers.go` — add authz checks in `handleBasicPublish`, `handleBasicConsume`, `handleBasicGet`

**New helper:**
```go
// server/authz.go (new)
func (s *Server) authorize(conn *protocol.Connection, op interfaces.Operation) error
```
- If `AuthorizationEnabled` is false or `Authenticator` is nil → allow (return nil)
- Look up `conn.User`; if nil → allow (auth disabled)
- Call `s.Authenticator.Authorize(conn.User, op)`
- On failure → return `*errors.AMQPError` with code 403 (access-refused)

**Per-handler checks:**
| Handler | Operation | On failure |
|---------|-----------|------------|
| `handleExchangeDeclare` | `{ActionConfigure, ResourceExchange, exchangeName}` | channel.close 403 |
| `handleExchangeDelete` | `{ActionConfigure, ResourceExchange, exchangeName}` | channel.close 403 |
| `handleQueueDeclare` | `{ActionConfigure, ResourceQueue, queueName}` | channel.close 403 |
| `handleQueueDelete` | `{ActionConfigure, ResourceQueue, queueName}` | channel.close 403 |
| `handleQueueBind` | `{ActionWrite, ResourceQueue, queueName}` + `{ActionRead, ResourceExchange, exchangeName}` | channel.close 403 |
| `handleQueueUnbind` | `{ActionWrite, ResourceQueue, queueName}` + `{ActionRead, ResourceExchange, exchangeName}` | channel.close 403 |
| `handleBasicPublish` | `{ActionWrite, ResourceExchange, exchangeName}` | channel.close 403 |
| `handleBasicConsume` | `{ActionRead, ResourceQueue, queueName}` | channel.close 403 |
| `handleBasicGet` | `{ActionRead, ResourceQueue, queueName}` | channel.close 403 |

**Error handling:** Authorization failures are **channel-level soft errors** (403 access-refused) — send `channel.close` with reply code 403, not connection.close. The connection stays alive; only the channel is closed.

**Reserved `amq.` name enforcement (spec rule):**
- In `handleExchangeDeclare`: if exchange name starts with `amq.` and `passive` is false → `access-refused` (403)
- In `handleQueueDeclare`: if queue name starts with `amq.` and `passive` is false → `access-refused` (403)
- Passive declares (passive=true) of `amq.` names are allowed (just checking existence)

**Tests (`server/authz_test.go`):**
- `TestAuthorizeExchangeDeclare` — user with configure permission declares OK; without → 403
- `TestAuthorizeExchangeDelete` — user with configure permission deletes OK; without → 403
- `TestAuthorizeQueueDeclare` — user with configure permission declares OK; without → 403
- `TestAuthorizeQueueDelete` — user with configure permission deletes OK; without → 403
- `TestAuthorizeQueueBind` — user needs write on queue AND read on exchange; missing either → 403
- `TestAuthorizeBasicPublish` — user with write permission on exchange publishes OK; without → 403
- `TestAuthorizeBasicConsume` — user with read permission on queue consumes OK; without → 403
- `TestAuthorizeBasicGet` — user with read permission on queue gets OK; without → 403
- `TestReservedExchangeName` — declaring `amq.custom` with passive=false → 403; with passive=true → OK
- `TestReservedQueueName` — declaring `amq.custom` with passive=false → 403; with passive=true → OK

### Commit B5: Authorization integration test

**Files to add:**
- `authz_integration_test.go` (package main, top-level)

**Tests (full end-to-end with real AMQP client):**
- `TestAuthzFullFlow` — set up users with different permissions:
  - `admin`: `.*` on all three → can do everything
  - `producer`: configure `^$`, write `.*`, read `^$` → can publish but not consume
  - `consumer`: configure `^$`, write `^$`, read `.*` → can consume but not publish
  - `restricted`: configure `^app1-.*`, write `^app1-.*`, read `^app1-.*` → only app1-* resources
- Verify each user can/cannot perform operations per their permissions
- Verify guest loopback restriction (connect from localhost OK, simulate remote → refused)
- Verify reserved `amq.` name enforcement end-to-end

### Commit B6: Documentation and config sample

**Files to modify:**
- `docs/AUTHORIZATION.md` (new) — permission model, auth file format, permission table, examples
- `config.sample.yaml` — add security.authorization section with examples
- `README.md` — link to authorization docs

---

## Execution Order and Dependencies

```
A1 (TLS listener) ──→ A2 (Builder/CLI) ──→ A3 (TLS integration test + docs)
                                              │
B1 (Permission model) ──→ B2 (Authorize impl) ──→ B3 (Vhost check) ──→ B4 (Per-op checks) ──→ B5 (Authz integration test) ──→ B6 (Docs)
```

Parts A and B are independent and can be interleaved or done in parallel.
Recommended order: **B1 → B2 → B3 → B4 → B5 → A1 → A2 → A3 → B6**
(Authorization first since it's higher value and more complex; TLS is straightforward.)

---

## Performance Considerations

- **Authorize() hot path**: `basic.publish` and `basic.consume` call Authorize on every message. The regex match must be fast.
  - Cache compiled regex patterns per-user (compile once on auth, reuse for all checks)
  - `regexp.MatchString` compiles every call — use `regexp.Compile` + stored `*regexp.Regexp`
  - Consider LRU permission result cache per-channel if profiling shows overhead
- **TLS overhead**: TLS handshake adds ~1-2ms per connection. Connections are long-lived in AMQP, so this is amortized. No per-message overhead (TLS session is persistent).
- **Vhost check**: Done once per connection at `connection.open` — negligible overhead.

---

## Risk Areas

1. **Connection struct changes** — adding `User` field to `protocol.Connection` touches a core struct. Must be backward-compatible (nil when auth disabled).
2. **Auth file format migration** — existing users have old `permissions` format. Must auto-migrate on load without breaking.
3. **Channel-level vs connection-level errors** — authorization failures are channel soft-errors (403), not connection hard-errors. Must send `channel.close` not `connection.close`.
4. **Passive declare permission** — RabbitMQ 4.3.1 changed passive declarations to require at least one permission. We should match: passive declare requires any of configure/write/read on the resource.
5. **Default exchange mapping** — blank exchange name must be mapped to `amq.default` for permission checks, but only for authz (not for routing).
