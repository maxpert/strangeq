# Management Control Plane

StrangeQ ships an optional **management control plane**: a RabbitMQ-4.4-management-subset
HTTP API plus an embedded single-page application (SPA). It lives in the **same binary**
as the broker — the UI assets (`index.html`, `app.js`, `styles.css`) are compiled in with
`go:embed`, so there is no separate folder to copy, no build step, no CDN, and no
external origin at runtime.

The control plane is a **separate listener** from telemetry. It is **off by default** and
binds **loopback only** by default. It is a read-mostly *consumer* of the broker: it reads
a coherent entity snapshot and drives a small set of mutations through the same broker
paths an AMQP client uses.

> For the telemetry endpoint (`/metrics`, `/health`) and the full Prometheus metric
> reference, see [METRICS.md](METRICS.md). For per-user authorization semantics shared with
> AMQP clients, see [AUTHORIZATION.md](AUTHORIZATION.md).

## Enabling it

Command-line flags (each falls back to `management.*` in the config file / environment):

```bash
amqp-server --management \
  --management-address 127.0.0.1 \
  --management-port 15672
```

| Flag | Config key | Default | Description |
|------|-----------|---------|-------------|
| `--management` | `management.enabled` | `false` | Enable the control-plane listener |
| `--management-address` | `management.address` | `127.0.0.1` | Bind address (loopback by default) |
| `--management-port` | `management.port` | `15672` | Bind port |

The environment equivalent is `AMQP_MANAGEMENT_ENABLED=true`, `AMQP_MANAGEMENT_ADDRESS`,
`AMQP_MANAGEMENT_PORT`, and so on.

A minimal config block (the same values the code defaults to):

```yaml
management:
  enabled: true
  address: 127.0.0.1
  port: 15672
  authentication: inherit          # inherit | basic | none
```

> The shipped `src/amqp-go/config.sample.yaml` documents the `telemetry:` block but does
> **not** currently ship a `management:` block; the keys and defaults below are taken from
> `src/amqp-go/config/config.go` and `src/amqp-go/interfaces/config.go`.

### Full `management.*` surface

```yaml
management:
  enabled: false                 # SEC-001: off by default
  address: 127.0.0.1             # SEC-002: loopback by default
  port: 15672
  node_name: ""                  # "" -> "amqp-go@" + hostname
  authentication: inherit        # inherit | basic | none

  allow_unauthenticated: false           # AUTH-041: loopback reads when broker auth is off
  allow_unauthenticated_mutations: false # AUTH-041: loopback topology writes
  allow_connection_close: false          # AUTH-015: enable connection.close

  admin_users: []                # usernames granted the full capability set
  trusted_proxies: []            # CIDRs/IPs allowed to set X-Forwarded-For / X-Real-IP (SEC-112)

  basic:
    username: ""
    password_hash: ""            # bcrypt; required when authentication: basic
    admin: false                 # grants administrator-equivalent capabilities
  tls:
    enabled: false
    cert_file: ""
    key_file: ""
    ca_file: ""                  # mTLS is refused (see below)
  cors:
    allowed_origins: []          # empty = CORS off; "*" is rejected (SEC-094)

  session:
    idle_timeout_seconds: 28800
    max_sessions: 1024
  limits:
    max_page_size: 500
    request_timeout_seconds: 30
    max_in_flight: 64
    max_body_bytes: 2097152       # 2 MiB
    max_import_body_bytes: 20971520 # 20 MiB
    max_response_bytes: 4194304   # 4 MiB
    max_name_bytes: 256
  rate_limits:                    # per principal, fixed one-minute window; 0 = unlimited
    publish: 0
    get: 0
    purge: 0
    declare: 0
    definitions_import_seconds: 10
  definitions:
    max_queues: 0
    max_exchanges: 0
    max_bindings: 0
  async:
    message_threshold: 10000      # queue purge over this depth -> 202 + job
    entity_threshold: 1000        # definitions import over this count -> 202 + job
    max_jobs: 128
    job_retention_seconds: 3600
  get:
    max_count: 100                # upper bound for a single POST .../get
  audit:
    logfile: ""                   # "" -> the broker logger
```

### Startup refusal matrix

The surface **refuses to start** (the broker exits) on an unsafe or inconsistent
configuration rather than degrading silently. `config.Validate` enforces these before the
broker binds, and `management.New` repeats them so an embedder that constructs the options
directly cannot bypass them. A disabled surface ignores its `management.*` block entirely.

| Mode | Condition | Result |
|------|-----------|--------|
| `inherit` | broker authentication disabled **and** `allow_unauthenticated: false` | refuse |
| `inherit` | `allow_unauthenticated: true` **but** `address` is not loopback | refuse |
| `basic` | `basic.password_hash` empty | refuse |
| `basic` | `basic.password_hash` is not a valid bcrypt hash | refuse |
| `none` | `address` is not loopback | refuse |
| any | `port` outside `1..65535` | refuse |
| any | a `trusted_proxies` entry is not a valid IP/CIDR | refuse |
| any | any `limits.*`, `session.*`, `get.max_count`, or `async.max_jobs` is `< 1` | refuse |
| any | `tls.enabled` without `tls.cert_file` **and** `tls.key_file` | refuse |
| any | `tls.ca_file` set (mutual TLS) | refuse — no cert-to-identity mapping exists (OQ-015) |
| any | `cors.allowed_origins` contains `*` or an empty entry | refuse |

**Loopback** means the literal `127.0.0.0/8` / `::1` range, the empty string, or
`localhost`. Any other hostname is treated as non-loopback (a hostname can resolve to a
non-loopback address, so the gate fails closed).

At startup the surface also logs `SECURITY WARNING` lines for a non-loopback bind without
TLS, for `authentication: none`, for `allow_unauthenticated`, for
`allow_unauthenticated_mutations`, and for `allow_connection_close`.

## Authentication and authorization

The control plane uses a **two-gate** authorization model on top of authentication:

1. **Capability gate (deny-by-default).** Every `/api/*` request needs at least the
   `read_topology` capability. A principal that holds no capability is refused **403** on
   every request, reads included.
2. **Per-resource gate.** An **entity read** (queues, exchanges, bindings, consumers,
   channels, connections, vhosts, definitions, permissions) additionally requires that the
   principal holds a permission entry for the requested vhost. Per AUTH-011 this is a
   vhost-*existence* check, not a match against the AMQP read regex. A topology mutation is
   checked against the broker's configure/write/read permission triples for the named
   resource (see [AUTHORIZATION.md](AUTHORIZATION.md)).

### Tags → capabilities

Capabilities are derived from the broker user's **tags** (and from `management.admin_users`).
An absent tag contributes nothing — a user with no tag holds nothing.

| Tag | Capabilities |
|-----|--------------|
| `management` | `read_topology` |
| `monitoring` | `read_topology`, `read_cross_vhost` |
| `policymaker` | `read_topology`, `manage_policies` |
| `administrator` | all six below |
| (username in `admin_users`) | all six below |

The six capabilities are `read_topology`, `read_cross_vhost`, `manage_policies`,
`mutate_topology`, `manage_users`, and `close_others_connections`.

### Authentication modes

| Mode | Behaviour |
|------|-----------|
| `inherit` | Authenticate against the broker's auth backend (auth.json). Requires broker authentication unless `allow_unauthenticated: true` on a loopback bind. |
| `basic` | A single principal from `management.basic.{username,password_hash}`; constant-time compare, bcrypt hash required. `admin: true` grants the full capability set. |
| `none` | No authentication; loopback only. Every request is the unauthenticated `guest` identity. |

A missing or invalid credential is **401** with a reason that never distinguishes an
unknown user from a wrong password. Repeated failures are limited per effective client
(`security.authfailurelimit`); over budget, further attempts are refused **429** before any
bcrypt work.

### Sessions and CSRF

`POST /api/strangeq/session` mints a server-side session and sets two cookies:

- `SQ_SESSION` — opaque 128-bit id, `HttpOnly`, `SameSite=Strict`, `Secure` under TLS.
- `SQ_CSRF` — 256-bit CSRF token, readable by the SPA, same attributes.

The session carries the principal, its capabilities, and the CSRF token; the cookie carries
only the id. A **cookie-authenticated state-changing** request (POST/PUT/DELETE) must echo
the token in `X-CSRF-Token` and have a matching `Origin`/`Referer`; otherwise it is refused
**403**. A **Basic-authenticated** mutation is exempt from CSRF (it is not ambient
credentials). Sessions are bounded (`session.max_sessions`, oldest evicted first) and
idle-expire (`session.idle_timeout_seconds`). `SIGHUP` re-reads `auth.json` and invalidates
sessions for changed or removed users.

### Client restrictions

- The unauthenticated `guest` identity is **loopback only**; a cookie minted on loopback
  cannot be replayed from a non-loopback client.
- A broker user may carry `loopback_only` (see [AUTHORIZATION.md](AUTHORIZATION.md)).
- `security.allowedhosts` / `security.blockedhosts` apply to the effective client exactly
  as they do on the AMQP handshake.
- `management.trusted_proxies` is the only way `X-Forwarded-For` / `X-Real-IP` are
  honoured; a trusted proxy that forwards **no** client address fails closed.

### Rate limits and audit

- Fixed one-minute windows, per principal: `rate_limits.{publish,get,purge,declare}` and
  the `definitions_import_seconds` minimum interval (0 = unlimited). Refused requests are
  **429** with `X-RateLimit-*` advisory headers and `Retry-After`.
- An in-flight cap (`limits.max_in_flight`) sheds excess concurrency with **503**.
- Every mutation is audited. A **destructive** mutation (delete/purge/close/import) is
  refused **503** if its audit record cannot be durably written.

### Auth-disabled loopback-only mode

When broker authentication is off, the control plane can still serve reads on the loopback
interface. Either set `authentication: none` (loopback-only by construction), or keep
`authentication: inherit` and set `allow_unauthenticated: true` **with a loopback bind**.
In this mode every request is the unauthenticated `guest` identity; topology writes require
the additional `allow_unauthenticated_mutations: true`, and closing connections requires
`allow_connection_close: true`.

## REST API reference

All endpoints are under `/api`. Responses are compact JSON. Errors use the envelope
`{"error": <token>, "reason": <text>, "request_id": <id>}`; reason text never reveals
whether a user or resource exists. Successful `GET`/`HEAD` responses carry an `ETag` and
honour `If-None-Match` → **304**; conditional writes accept `If-Match` (strong) → **412**.

Status codes below are the *notable* ones; every authenticated route can also answer
**401** (no/invalid credential), **403** (capability/resource gate), **405** (wrong method,
with `Allow`), **429** (rate limited), and **503** (shutting down / broker unavailable).

### Session and identity

| Method | Path | Purpose |
|--------|------|---------|
| POST | `/api/strangeq/session` | Log in; returns principal, capabilities, CSRF token, sets cookies. Public (no prior credential). `200`, `401`, `429`. |
| DELETE | `/api/strangeq/session` | Log out; clears cookies. `204`. |
| GET | `/api/whoami` | The current principal, tags, capabilities, vhost permissions. `200`. |
| GET | `/api/overview` | Totals, top queues by ready depth, alarms, unsafe-recovery state. `200`. |
| GET | `/api/strangeq/version` | Product/version/commit/build/Go version, `api_compatibility`, resolved capabilities, and the endpoint list. `200`. |

### Health

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/api/health` | Readiness. `200` (`status: ok` or `alarm`), `503` (`not_ready`). |
| GET | `/api/health/checks` | The named checks. `200`. |
| GET | `/api/health/checks/{check}` | One check. `404` unknown; `501` for checks StrangeQ cannot answer (see divergences). |

### Metrics

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/api/metrics` | The Prometheus text exposition wrapped as `{"text": "..."}`. `200`; `501` when telemetry is disabled; `503` on a scrape failure. |

### Nodes (synthetic, single node)

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/api/nodes` | The one synthetic node. `200`. |
| GET | `/api/nodes/{name}` | The node. `404` unknown. |
| POST / DELETE | `/api/nodes/{name}` | `501` — no node operations / clustering. |

### Connections

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/api/connections` | List; `?user=` filters. `200`. |
| GET | `/api/connections/username/{user}` | List by user. `200`. |
| GET | `/api/connections/{name}/channels` | Channels on one connection. `404` unknown connection. |
| GET | `/api/connections/{name}` | One connection. `404`. |
| DELETE | `/api/connections/{name}` | Force-close one connection (graceful `connection.close 320`). `204`, `404`, `403`. |
| DELETE | `/api/connections/username/{user}` | Close every connection of a user. `204`, `403`. |

### Channels (read-only)

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/api/channels` | List channels. `200`. |
| GET | `/api/channels/{id}` | One channel. `{id}` is the canonical `{connection}:{number}` composite; a bare number is accepted only when unambiguous. `404`. |

### Consumers

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/api/consumers` | List consumers; `?queue=`/`?connection=` filter. `200`. |
| GET | `/api/consumers/{vhost}` | List consumers for the vhost. `404` unknown vhost. |

### Exchanges

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/api/exchanges` | List; `?type=` filters. `200`. |
| PUT | `/api/exchanges/{vhost}/{name}` | Declare. `201` created, `204` equivalent re-declare, `400` inequivalent, `412` `If-Match`. |
| DELETE | `/api/exchanges/{vhost}/{name}` | Delete; `?if-unused=`. `204`, `400` in use, `403` system exchange, `404`. |
| GET | `/api/exchanges/{vhost}/{name}/bindings/{direction}` | `{direction}` is `source` or `destination`. `404`. |
| POST | `/api/exchanges/{vhost}/{name}/publish` | Publish a message (`properties`, `routing_key`, `payload`, `payload_encoding`). `200 {routed}`; `400`; `413` oversized; `503` queue backpressure. |
| GET | `/api/exchanges/{vhost}/{name}` | One exchange. `404`. |

### Queues

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/api/queues` | Summary list; `?durable=`, `?has_consumers=`, `?non_empty=`, `?disable_stats=`, `?enable_queue_totals=`. `200`. |
| GET | `/api/queues/detailed` | Full queue records. `200`. |
| GET | `/api/queues/{vhost}` | Full records for the vhost. `404` unknown vhost. |
| GET | `/api/queues/{vhost}/{name}` | One queue. `404`. |
| GET | `/api/queues/{vhost}/{name}/bindings` | Bindings whose destination is the queue. `404`. |
| PUT | `/api/queues/{vhost}/{name}` | Declare. `201`/`204`/`400`/`412`. |
| POST | `/api/queues/{vhost}` | Create (server-named `amq.gen.*` when the body name is empty); returns `Location`. `201`, `400`. |
| DELETE | `/api/queues/{vhost}/{name}` | Delete; `?if-empty=`/`?if-unused=` (hyphenated only). `204`, `400` conditional, `404`. |
| DELETE | `/api/queues/{vhost}/{name}/contents` | Purge. `204`; `202` + job when the queue is over `async.message_threshold`; `404`. |
| POST | `/api/queues/{vhost}/{name}/get` | Synchronous `basic.get` (`count`, `ackmode`, `encoding`, `truncate`). `200`, `400`, `404`. |

### Bindings

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/api/bindings` | List; `?source=`/`?destination=` exact filters. `200`; `501` when the broker exposes no binding seam. |
| GET | `/api/bindings/{vhost}` | Same, vhost-scoped. `200`; `404` unknown vhost; `501`. |
| POST | `/api/bindings/{vhost}/e/{source}/{dtype}/{destination}` | Create a binding; `{dtype}` is `q` (queue) or `e` (exchange). Returns `Location` with the properties key. `201`, `400`, `404`. |
| DELETE | `/api/bindings/{vhost}/e/{source}/{dtype}/{destination}/{props}` | Delete by percent-decoded `properties_key`. `204`, `404`, `501`. |

### Vhosts

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/api/vhosts` | The single synthetic vhost `"/"`. `200`. |
| GET | `/api/vhosts/{name}` | `200` for `/`; `404` otherwise. |
| PUT | `/api/vhosts/{name}` | `501` — multi-vhost is not implemented. |
| DELETE | `/api/vhosts/{name}` | `409` for `/` (cannot delete the only vhost); `501` otherwise. |
| GET | `/api/vhosts/{vhost}/permissions` | Permissions in the vhost. `200`. |
| GET | `/api/vhosts/{vhost}/topic-permissions` | `200 []`. |
| GET | `/api/vhosts/{vhost}/connections` | Connections in the vhost. `200`. |

### Users and permissions (read-only)

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/api/users` | List users (name, tags). Requires `manage_users`. `200`, `403`. |
| GET | `/api/users/{name}` | One user. Requires `manage_users`. `200`, `404`. |
| GET | `/api/users/{user}/permissions` | A user's vhost permissions. Requires `manage_users`. `200`. |
| GET | `/api/permissions` | All permission records. Requires `manage_users`. `200`. |
| GET | `/api/permissions/{vhost}/{user}` | One permission record. Requires `manage_users`. `200`, `404`. |
| PUT / POST / DELETE | `/api/users/{name}` | `501` — no runtime user store. |
| PUT / DELETE | `/api/permissions/{vhost}/{user}` | `501` — no runtime permission store. |
| POST | `/api/auth/hash_password` | `501` — password hashing is not offered. |

### Definitions

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/api/definitions` | Export the topology (vhosts, users/permissions when the caller holds `manage_users`, exchanges, queues, bindings). `200`, `503`. |
| GET | `/api/definitions/{vhost}` | Same, vhost-scoped. `404` unknown vhost. |
| POST | `/api/definitions` | Import; `?dry_run=true` plans without mutating. `200` (result + skipped/failed); `202` + job over `async.entity_threshold`; `400` bad JSON; `403` without `mutate_topology`; `429`; `503`. |
| POST | `/api/definitions/{vhost}` | Same, vhost-scoped. `404` unknown vhost. |

### Jobs

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/api/strangeq/jobs/{id}` | Poll an async job (`purge`, `definitions_import`). `200`, `404`. |

### Honest empties and not-implemented

These routes exist so a client never sees a 404 typo. Reads answer **`200 []`** and writes
answer **`501`**:

`/api/policies`, `/api/operator-policies`, `/api/parameters`, `/api/global-parameters`,
`/api/feature-flags`, `/api/deprecated-features`, `/api/deprecated-features/used`,
`/api/vhost-limits`, `/api/user-limits`, `/api/federation-links`,
`/api/topic-permissions`, `/api/shovels` (and the parameterized
`/api/vhost-limits/{vhost}/{name}`, `/api/user-limits/{user}/{name}` for writes).

`POST /api/strangeq/rebalance` answers **501** (no clustering).

## The UI

The SPA is served at `/` on the management listener and deep-links by path. It talks to the
same-origin `/api/*` at runtime and renders every untrusted string as text (no `innerHTML`
with data). It is gated by the same capability and resource gates as the API.

| Screen | Route | Contents |
|--------|-------|----------|
| S1 Overview | `/` | Health dot, alarm banner, unsafe-recovery banner, connection/channel/queue/consumer/exchange totals, queues-with-messages, and a Top-queues-by-ready table. Auto-refreshes every 2 s while visible. |
| S2 Connections list | `/connections` | Filter/sort, per-row Close. |
| S3 Connection detail | `/connections/{name}` | Connection fields and its channels. |
| S4 Channels | `/channels`, `/channels/{id}` | Read-only list and detail; the UI states there is no close-channel operation. |
| S5 Consumers | `/consumers` | Consumer list. |
| S6 Exchanges list | `/exchanges` | List, filter by type, declare. |
| S7 Exchange detail | `/exchanges/{name}` | Fields, bindings (source/destination), publish. |
| S8 Queues list | `/queues` | List, filter, declare. |
| S9 Queue detail | `/queues/{name}` | Counts, bindings, get, purge, delete. |
| S10 Bindings | `/bindings` | List and create/delete bindings. |
| S11 Nodes | `/nodes` | The single synthetic node; states "single-node broker; no cluster". |
| S12 Users & Permissions | `/users`, `/permissions` | Read-only; no mutation controls. |
| S13 Definitions | `/definitions` | Export (download) and import with a dry-run preview; shows the non-atomic disclosure and skipped sections. |
| S14 About | `/about` | Product/version/commit, API compatibility, resolved capabilities, and the divergence matrix. |
| Metrics | `/metrics` | The Metrics tab (see below). |

### Metrics tab

The `/metrics` screen renders the broker's Prometheus matrix from the same-origin,
auth-gated `GET /api/metrics`:

- a **45-tile vital-stats panel** grouped into Throughput, Topology and Health, with three
  tile outcomes: a family the endpoint never emitted renders an em-dash with "not emitted
  by this broker" (never a fabricated `0`); a family present with a zero value renders `0`;
  and a family present whose label matcher selects none of its series renders `0` (a sum
  over an empty set is a defined zero, so a traffic-path-only counter reads `0` when only
  pre-initialized off-path sites are present). The "not emitted by this broker" title
  applies **only** to the absent-family case — a filtered tile never fabricates that claim.
  A tile whose matcher is malformed or selects the whole excluded domain degrades to an
  em-dash with an unknown severity naming the bad pattern, never a fabricated `0`.
  The WAL full-flush alarm is scoped to `barrier` durability: under `durability: flush`
  every confirm is a full drive-cache flush, so the `group_commit` count still renders but
  raises no alarm;
- a **grouped, searchable family browser** over every scraped family;
- a **raw text** view (`?view=raw`) of the exposition;
- rates derived from **two samples**, with a pause/backoff control.

A tile whose family is absent is counted in an honest "X of N unknown" notice. The screen
reuses the telemetry process registry, so the two never drift; when telemetry is disabled
the route answers `501` and the screen shows a truthful notice. See [METRICS.md](METRICS.md)
for the metric families and their meanings.

## Honest divergences from RabbitMQ

StrangeQ implements a **subset** of the RabbitMQ management API and UI. The divergences are
deliberate and surfaced honestly rather than faked:

- **Vhosts** — only the single vhost `"/"` exists. Creating one is `501`; deleting `"/"` is
  `409`. Entity data is global; vhosts are not independent (D-1).
- **Clustering / HA** — single-node only (D-2). Node operations and rebalance are `501`.
- **Federation and shovels** — no implementation; reads answer `200 []`, writes `501`.
- **Plugins** — no plugin surface; the UI omits it entirely.
- **Policies, parameters, global parameters, feature flags, deprecated features, vhost
  limits, user limits** — absent; reads answer `200 []`, writes `501`.
- **Runtime user/permission CRUD** — no runtime user store (BOB-6); user and permission
  mutations answer `501`. Users and permissions are read from `auth.json` (see
  [AUTHORIZATION.md](AUTHORIZATION.md)).
- **Quorum, stream, and priority queues** — classic queues only (D-3); declaring a
  non-`classic` type is `400`.
- **Health checks** — `virtual-hosts`, `node-is-quorum-critical`, `ready-to-serve-clients`,
  `port-listener`, and `protocol-listener` answer `501`, never a fabricated `200`.
- **Channels** — read-only; there is no close-channel operation.
- **Two `/metrics`** — on the telemetry listener `/metrics` is the unauthenticated
  Prometheus text; on the management listener `/metrics` is the SPA deep link. Different
  origins, different content.

The S14 About screen lists these divergences at runtime, and the resolved-capability list
from `GET /api/strangeq/version` reports which optional broker seams are present.

## Reaching the UI

Start the broker with `--management` (or `management.enabled: true`). The startup output
prints the listener URL:

```
Management API listening on http://127.0.0.1:15672
  Version:  http://127.0.0.1:15672/api/strangeq/version
  Overview: http://127.0.0.1:15672/api/overview
```

Open `http://127.0.0.1:15672/` in a browser. The listener is **loopback-bound by default**;
to expose it, set a non-loopback `management.address` and enable `management.tls` (and read
the startup security warnings before doing so).
