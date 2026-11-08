package web

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// stubAPI is a minimal, self-contained stand-in for the spec-01 management API.
// The headless-browser tests drive the real SPA against it over the same
// origin, so the SPA's fetch/route/refresh behaviour is exercised end to end
// without depending on the (separately owned) API package.
type stubAPI struct {
	mu sync.Mutex

	requireLogin bool
	challenge    bool
	sessions     map[string]bool
	token        string
	wantCSRF     bool
	lastCSRF     string
	lastMutation string

	readFail    bool
	bindings501 bool
	users403    bool
	caps        map[string]any
	queues      []map[string]any
	overview    map[string]any

	// Item overrides for the list endpoints so a test can install >1 row and
	// observe server-side reversal (sort_reverse=true).
	exchangeItems   []map[string]any
	connectionItems []map[string]any
	channelItems    []map[string]any
	consumerItems   []map[string]any
	userItems       []map[string]any

	// versionFail makes GET /api/strangeq/version return 500, so a test can
	// exercise the "capability unknown" path. omitTotal makes the paged queues
	// envelope omit total_count, exercising the em-dash/unknown indicator.
	versionFail bool
	omitTotal   bool

	// reqLog records every request's endpoint kind and raw query string, so a
	// test can assert on the parameters a screen actually sent.
	reqLog []recordedReq

	// healthStatus / healthReason drive GET /api/health so a test can flip the
	// broker alarm state deterministically.
	healthStatus string
	healthReason string

	// queueTotal / queuePageCount turn the /api/queues list into a paged
	// envelope whose page/page_size echo the request, so the pagination
	// controls can be exercised end to end. queuePages records every requested
	// page.
	queueTotal     int
	queuePageCount int
	queuePages     []int

	// definitions-import controls: a forced status (413/429) and custom outcome
	// tables, plus the last raw body for byte-fidelity assertions.
	defsImportStatus int
	defsApplied      []map[string]any
	defsSkipped      []map[string]any
	defsFailed       []map[string]any
	lastDefsBody     string
	defsBodies       []string

	declared         []string
	lastQueueDeclare map[string]any
	hits             atomic.Int64
}

// recordedReq is one observed request: the endpoint kind (the segment after
// /api/), its raw query string, and the HTTP method.
type recordedReq struct {
	kind   string
	raw    string
	method string
}

func (s *stubAPI) hitCount() int64 { return s.hits.Load() }

// queriesFor returns the raw query strings of every request for an endpoint.
func (s *stubAPI) queriesFor(kind string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.reqLog))
	for _, e := range s.reqLog {
		if e.kind == kind {
			out = append(out, e.raw)
		}
	}
	return out
}

// postQueriesFor returns the raw query strings of every POST for an endpoint,
// so a test can distinguish a mutation from a read of the same path.
func (s *stubAPI) postQueriesFor(kind string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.reqLog))
	for _, e := range s.reqLog {
		if e.kind == kind && e.method == http.MethodPost {
			out = append(out, e.raw)
		}
	}
	return out
}

// querySeen reports whether any request for the endpoint carried substr.
func (s *stubAPI) querySeen(kind, substr string) bool {
	for _, q := range s.queriesFor(kind) {
		if strings.Contains(q, substr) {
			return true
		}
	}
	return false
}

// queryCount counts requests for an endpoint.
func (s *stubAPI) queryCount(kind string) int { return len(s.queriesFor(kind)) }

// queuePageSeen reports whether the queues list was requested for page n.
func (s *stubAPI) queuePageSeen(n int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, p := range s.queuePages {
		if p == n {
			return true
		}
	}
	return false
}

// queuePageRequests counts how many queues-list requests targeted page n.
func (s *stubAPI) queuePageRequests(n int) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := 0
	for _, p := range s.queuePages {
		if p == n {
			c++
		}
	}
	return c
}

// queueDeclareBody returns the body of the most recent queue declaration.
func (s *stubAPI) queueDeclareBody() map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastQueueDeclare
}

func newStub() *stubAPI {
	return &stubAPI{
		sessions: map[string]bool{},
		token:    "csrf-token-abc",
		caps: map[string]any{
			"queue_message_totals": true, "channels": true, "bindings": false,
			"close_connection": true, "user_crud": false, "tag_capabilities": true,
			"consumer_counts": true, "atomic_import": false, "message_rates": false,
			"vhost_scoping": false, "coherent_snapshot": true, "unauthenticated": false,
		},
		overview: map[string]any{
			"management_version": "0.4.0", "product": "StrangeQ", "product_version": "0.4.0",
			"object_totals":     map[string]any{"connections": 1, "channels": 2, "queues": 3, "consumers": 2, "exchanges": 7, "bindings": nil},
			"queue_totals":      map[string]any{"messages_ready": 12, "messages_unacknowledged": 1, "messages": 13, "queues_with_messages": 2},
			"alarms":            map[string]any{"memory": false, "disk": false, "reason": ""},
			"unsafe_recovery":   map[string]any{"degraded": false, "discarded_artifacts": 0},
			"snapshot_coherent": true,
		},
		queues: []map[string]any{
			{"name": "orders", "vhost": "/", "type": "classic", "durable": true, "auto_delete": false, "exclusive": false, "arguments": map[string]any{}, "state": "running", "consumers": 2, "messages_ready": 5, "messages_unacknowledged": 1, "messages": 6, "stats_partial": false},
			{"name": "events", "vhost": "/", "type": "classic", "durable": true, "auto_delete": false, "exclusive": false, "arguments": map[string]any{}, "state": "idle", "consumers": 0, "messages_ready": 7, "messages_unacknowledged": 0, "messages": 7, "stats_partial": false},
			{"name": "tasks", "vhost": "/", "type": "classic", "durable": false, "auto_delete": true, "exclusive": false, "arguments": map[string]any{}, "state": "running", "consumers": 0, "messages_ready": nil, "messages_unacknowledged": nil, "messages": nil, "stats_partial": true},
		},
	}
}

func (s *stubAPI) set(fn func(*stubAPI)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fn(s)
}

func (s *stubAPI) csrfSeen() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastCSRF
}

func (s *stubAPI) declarations() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.declared...)
}

// lastDefinitionsBody returns the raw POST body the import endpoint received.
func (s *stubAPI) lastDefinitionsBody() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastDefsBody
}

// definitionsBodies returns every raw import POST body in arrival order
// (dry-run first, then apply).
func (s *stubAPI) definitionsBodies() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.defsBodies...)
}

func pathSegments(r *http.Request) []string {
	ep := strings.Trim(r.URL.EscapedPath(), "/")
	if ep == "" {
		return nil
	}
	parts := strings.Split(ep, "/")
	for i, p := range parts {
		if d, err := url.PathUnescape(p); err == nil {
			parts[i] = d
		}
	}
	return parts
}

func writeJSON(w http.ResponseWriter, status int, obj any) {
	b, _ := json.Marshal(obj)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write(b)
}

// writeRead emits an ETag and honours If-None-Match, the same determinism
// contract the SPA relies on for its no-flicker refresh.
func writeRead(w http.ResponseWriter, r *http.Request, obj any) {
	b, _ := json.Marshal(obj)
	sum := sha256.Sum256(b)
	et := `"` + hex.EncodeToString(sum[:]) + `"`
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("ETag", et)
	if r.Header.Get("If-None-Match") == et {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(b)
}

func (s *stubAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.hits.Add(1)
	segs := pathSegments(r)
	if len(segs) < 2 || segs[0] != "api" {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "not_found"})
		return
	}
	kind := segs[1]
	s.mu.Lock()
	s.reqLog = append(s.reqLog, recordedReq{kind: kind, raw: r.URL.RawQuery, method: r.Method})
	s.mu.Unlock()

	switch kind {
	case "whoami":
		s.mu.Lock()
		need := s.requireLogin
		ok := s.sessionOK(r)
		chal := s.challenge
		s.mu.Unlock()
		if need && !ok {
			/* Chrome defers a credentials-carrying fetch whose 401 carries
			 * WWW-Authenticate: Basic. The default stub omits the header so
			 * the SPA's own 401 handling is exercised deterministically; the
			 * challenge=true mode reproduces the production header. */
			if chal {
				w.Header().Set("WWW-Authenticate", `Basic realm="StrangeQ Management"`)
			}
			writeJSON(w, http.StatusUnauthorized, map[string]any{"error": "unauthorized", "reason": "sign in required"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"name": "guest", "tags": []string{"administrator"}, "vhost": "/",
			"capabilities": []string{"read_topology", "read_cross_vhost", "mutate_topology", "manage_users"},
		})
		return
	case "strangeq":
		if len(segs) >= 3 && segs[2] == "session" {
			s.handleSession(w, r)
			return
		}
		if len(segs) >= 3 && segs[2] == "version" {
			s.mu.Lock()
			fail := s.versionFail
			s.mu.Unlock()
			if fail || s.getFail(w, r) {
				if fail {
					writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "internal_error", "reason": "version unavailable"})
				}
				return
			}
			s.mu.Lock()
			caps := s.caps
			s.mu.Unlock()
			writeRead(w, r, map[string]any{
				"product": "StrangeQ", "version": "0.4.0", "commit": "abc123",
				"build_date": "2026-11-08", "go_version": "go1.27", "api_compatibility": "rabbitmq-4.4-management-subset",
				"capabilities": caps,
				"endpoints":    []string{"GET /api/overview"},
			})
			return
		}
	case "health":
		s.mu.Lock()
		st := s.healthStatus
		reason := s.healthReason
		s.mu.Unlock()
		if st == "" {
			st = "ok"
		}
		body := map[string]any{"status": st, "uptime_seconds": 42}
		if reason != "" {
			body["reason"] = reason
		}
		writeJSON(w, http.StatusOK, body)
		return
	}

	if s.getFail(w, r) {
		return
	}

	switch kind {
	case "overview":
		s.mu.Lock()
		ov := s.overview
		s.mu.Unlock()
		writeRead(w, r, ov)
	case "queues":
		s.handleQueues(w, r, segs)
	case "exchanges":
		s.handleExchanges(w, r, segs)
	case "connections":
		s.handleConnections(w, r, segs)
	case "channels":
		s.mu.Lock()
		chItems := s.channelItems
		s.mu.Unlock()
		if chItems == nil {
			chItems = []map[string]any{
				{"number": 1, "connection": "c1", "user": "guest", "vhost": "/", "state": "running", "consumers": 1, "unacked": nil, "prefetch_count": nil, "confirm": true, "transactional": nil, "stats_partial": true},
			}
		}
		writeRead(w, r, envelope(maybeReverse(r, chItems), len(chItems)))
	case "consumers":
		s.mu.Lock()
		coItems := s.consumerItems
		s.mu.Unlock()
		if coItems == nil {
			coItems = []map[string]any{
				{"consumer_tag": "ctag-1", "queue": "orders", "channel": nil, "connection": nil, "ack_required": true, "exclusive": false, "prefetch_count": 10, "arguments": map[string]any{}, "unacked": nil},
			}
		}
		writeRead(w, r, envelope(maybeReverse(r, coItems), len(coItems)))
	case "bindings":
		s.mu.Lock()
		notImpl := s.bindings501
		caps := s.caps
		s.mu.Unlock()
		if notImpl || caps["bindings"] != true {
			writeJSON(w, http.StatusNotImplemented, map[string]any{"error": "not_implemented", "reason": "aggregate binding listing is not implemented (BOB-4)"})
			return
		}
		writeRead(w, r, envelope([]map[string]any{
			{"source": "amq.topic", "destination": "orders", "destination_type": "queue", "routing_key": "orders.#", "arguments": map[string]any{}},
		}, 1))
	case "nodes":
		writeRead(w, r, []map[string]any{
			{"name": "amqp-go@host", "type": "disc", "running": true, "mem_used": 123, "mem_limit": 456, "disk_free": 789, "disk_free_limit": 100, "uptime": 99, "processors": 8, "os": "darwin"},
		})
	case "users":
		s.mu.Lock()
		forbidden := s.users403
		userItems := s.userItems
		s.mu.Unlock()
		if forbidden {
			writeJSON(w, http.StatusForbidden, map[string]any{"error": "access_refused", "reason": "capability manage_users is required"})
			return
		}
		if userItems == nil {
			userItems = []map[string]any{{"name": "guest", "tags": []string{"administrator"}}}
		}
		writeRead(w, r, userItems)
	case "permissions":
		s.mu.Lock()
		forbidden := s.users403
		s.mu.Unlock()
		if forbidden {
			writeJSON(w, http.StatusForbidden, map[string]any{"error": "access_refused", "reason": "capability manage_users is required"})
			return
		}
		writeRead(w, r, []map[string]any{{"user": "guest", "vhost": "/", "configure": ".*", "write": ".*", "read": ".*"}})
	case "definitions":
		if r.Method == http.MethodPost {
			if !s.guardCSRF(w, r) {
				return
			}
			body, _ := io.ReadAll(r.Body)
			dry := r.URL.Query().Get("dry_run") == "true"
			s.mu.Lock()
			status := s.defsImportStatus
			applied, skipped, failed := s.defsApplied, s.defsSkipped, s.defsFailed
			s.lastDefsBody = string(body)
			s.defsBodies = append(s.defsBodies, string(body))
			s.mu.Unlock()
			switch status {
			case http.StatusRequestEntityTooLarge:
				writeJSON(w, status, map[string]any{"error": "payload_too_large", "reason": "definitions body exceeds the import limit"})
				return
			case http.StatusTooManyRequests:
				w.Header().Set("Retry-After", "1")
				writeJSON(w, status, map[string]any{"error": "rate_limited", "reason": "definitions import rate limited"})
				return
			}
			if applied == nil {
				applied = []map[string]any{{"type": "exchange", "name": "events", "vhost": "/", "result": "created"}}
			}
			if skipped == nil {
				skipped = []map[string]any{
					{"type": "vhost", "name": "/", "reason": "already exists"},
					{"type": "user", "name": "guest", "reason": "user/permission management is not implemented (BOB-6)"},
					{"type": "permission", "name": "guest", "vhost": "/", "reason": "user/permission management is not implemented (BOB-6)"},
				}
			}
			if failed == nil {
				failed = []map[string]any{}
			}
			writeJSON(w, http.StatusOK, map[string]any{
				"applied": applied, "skipped": skipped, "failed": failed,
				"strangeq": map[string]any{"subset": true, "atomic": false, "dry_run": dry},
			})
			return
		}
		writeRead(w, r, map[string]any{
			"strangeq": map[string]any{
				"product": "StrangeQ", "version": "0.4.0", "subset": true, "atomic": false,
				"excludes":          []string{"user_password_hashes", "policies", "parameters", "vhosts_other_than_root", "implicit_default_bindings"},
				"excluded_sections": []string{"users", "permissions", "bindings"},
			},
			"vhosts":    []map[string]any{{"name": "/", "synthetic": true}},
			"exchanges": []map[string]any{}, "queues": []map[string]any{}, "bindings": []map[string]any{},
		})
	default:
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "not_found", "reason": "unknown endpoint"})
	}
}

func (s *stubAPI) handleSession(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		body, _ := io.ReadAll(r.Body)
		var creds struct {
			Username string `json:"username"`
			Password string `json:"password"`
		}
		_ = json.Unmarshal(body, &creds)
		if creds.Username == "" || creds.Password == "" {
			writeJSON(w, http.StatusUnauthorized, map[string]any{"error": "unauthorized", "reason": "bad credentials"})
			return
		}
		sid := "sess-" + creds.Username
		s.mu.Lock()
		s.sessions[sid] = true
		s.mu.Unlock()
		http.SetCookie(w, &http.Cookie{Name: "SQ_SESSION", Value: sid, HttpOnly: true, Path: "/", SameSite: http.SameSiteStrictMode})
		http.SetCookie(w, &http.Cookie{Name: "SQ_CSRF", Value: s.token, Path: "/", SameSite: http.SameSiteStrictMode})
		writeJSON(w, http.StatusOK, map[string]any{
			"name": creds.Username, "tags": []string{"administrator"}, "vhost": "/",
			"capabilities": []string{"read_topology", "mutate_topology"},
			"csrf_token":   s.token, "expires_in": 28800,
			"vhost_permissions": []map[string]any{{"vhost": "/", "configure": ".*", "write": ".*", "read": ".*"}},
		})
	case http.MethodDelete:
		s.mu.Lock()
		s.sessions = map[string]bool{}
		s.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	default:
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "bad_request"})
	}
}

func (s *stubAPI) sessionOK(r *http.Request) bool {
	c, err := r.Cookie("SQ_SESSION")
	if err != nil {
		return false
	}
	return s.sessions[c.Value]
}

// getFail returns true when it has written a 500 for a read request.
func (s *stubAPI) getFail(w http.ResponseWriter, r *http.Request) bool {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		return false
	}
	s.mu.Lock()
	fail := s.readFail
	s.mu.Unlock()
	if fail {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "internal_error", "reason": "injected"})
		return true
	}
	return false
}

// guardCSRF enforces the CSRF header for a cookie-authenticated mutation.
func (s *stubAPI) guardCSRF(w http.ResponseWriter, r *http.Request) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastMutation = r.Method + " " + r.URL.EscapedPath()
	s.lastCSRF = r.Header.Get("X-CSRF-Token")
	if !s.wantCSRF {
		return true
	}
	if r.Header.Get("X-CSRF-Token") != s.token {
		writeJSON(w, http.StatusForbidden, map[string]any{"error": "access_refused", "reason": "CSRF token missing or invalid"})
		return false
	}
	return true
}

func (s *stubAPI) handleQueues(w http.ResponseWriter, r *http.Request, segs []string) {
	if len(segs) == 2 {
		s.mu.Lock()
		qs := s.queues
		total := s.queueTotal
		pageCount := s.queuePageCount
		omitTotal := s.omitTotal
		s.mu.Unlock()
		qs = maybeReverse(r, qs)
		if total > 0 {
			page := atoiDefault(r.URL.Query().Get("page"), 1)
			size := atoiDefault(r.URL.Query().Get("page_size"), 50)
			s.mu.Lock()
			s.queuePages = append(s.queuePages, page)
			s.mu.Unlock()
			if pageCount <= 0 {
				pageCount = (total + size - 1) / size
			}
			env := map[string]any{
				"items": qs, "total_count": total, "item_count": len(qs), "filtered_count": total,
				"page": page, "page_size": size, "page_count": pageCount,
			}
			if omitTotal {
				delete(env, "total_count")
				delete(env, "filtered_count")
			}
			writeRead(w, r, env)
			return
		}
		writeRead(w, r, envelope(qs, len(qs)))
		return
	}
	if len(segs) >= 4 {
		name := segs[3]
		if len(segs) == 5 && segs[4] == "contents" && r.Method == http.MethodDelete {
			if !s.guardCSRF(w, r) {
				return
			}
			w.Header().Set("X-Sq-Messages-Purged", "1234")
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if len(segs) == 5 && segs[4] == "get" && r.Method == http.MethodPost {
			if !s.guardCSRF(w, r) {
				return
			}
			writeJSON(w, http.StatusOK, []map[string]any{
				{"payload": "hello world", "payload_encoding": "string", "properties": map[string]any{}, "routing_key": "rk", "redelivered": false, "message_count": 1, "exchange": "", "truncated": false},
			})
			return
		}
		switch r.Method {
		case http.MethodPut:
			if !s.guardCSRF(w, r) {
				return
			}
			body, _ := io.ReadAll(r.Body)
			var parsed map[string]any
			_ = json.Unmarshal(body, &parsed)
			s.mu.Lock()
			s.declared = append(s.declared, "queue:"+name)
			s.lastQueueDeclare = parsed
			s.mu.Unlock()
			w.WriteHeader(http.StatusCreated)
			return
		case http.MethodDelete:
			if !s.guardCSRF(w, r) {
				return
			}
			w.Header().Set("X-Sq-Messages-Deleted", "6")
			w.WriteHeader(http.StatusNoContent)
			return
		case http.MethodGet:
			s.mu.Lock()
			var found map[string]any
			for _, q := range s.queues {
				if q["name"] == name {
					found = q
					break
				}
			}
			s.mu.Unlock()
			if found == nil {
				writeJSON(w, http.StatusNotFound, map[string]any{"error": "not_found", "reason": "no such queue"})
				return
			}
			writeRead(w, r, found)
			return
		}
	}
	writeJSON(w, http.StatusNotFound, map[string]any{"error": "not_found"})
}

func (s *stubAPI) handleExchanges(w http.ResponseWriter, r *http.Request, segs []string) {
	if len(segs) == 2 {
		s.mu.Lock()
		items := s.exchangeItems
		s.mu.Unlock()
		if items == nil {
			items = []map[string]any{
				{"name": "", "vhost": "/", "type": "direct", "durable": true, "auto_delete": false, "internal": false, "arguments": map[string]any{}},
				{"name": "amq.topic", "vhost": "/", "type": "topic", "durable": true, "auto_delete": false, "internal": false, "arguments": map[string]any{}},
				{"name": "custom", "vhost": "/", "type": "fanout", "durable": false, "auto_delete": false, "internal": false, "arguments": map[string]any{}},
			}
		}
		writeRead(w, r, envelope(maybeReverse(r, items), len(items)))
		return
	}
	if len(segs) >= 4 {
		name := segs[3]
		if len(segs) == 5 && segs[4] == "publish" && r.Method == http.MethodPost {
			if !s.guardCSRF(w, r) {
				return
			}
			writeJSON(w, http.StatusOK, map[string]any{"routed": true})
			return
		}
		switch r.Method {
		case http.MethodPut:
			if !s.guardCSRF(w, r) {
				return
			}
			s.mu.Lock()
			s.declared = append(s.declared, "exchange:"+name)
			s.mu.Unlock()
			w.WriteHeader(http.StatusCreated)
			return
		case http.MethodDelete:
			if !s.guardCSRF(w, r) {
				return
			}
			w.WriteHeader(http.StatusNoContent)
			return
		case http.MethodGet:
			writeRead(w, r, map[string]any{"name": name, "vhost": "/", "type": "fanout", "durable": false, "auto_delete": false, "internal": false, "arguments": map[string]any{}})
			return
		}
	}
	writeJSON(w, http.StatusNotFound, map[string]any{"error": "not_found"})
}

func (s *stubAPI) handleConnections(w http.ResponseWriter, r *http.Request, segs []string) {
	if len(segs) == 2 {
		s.mu.Lock()
		items := s.connectionItems
		s.mu.Unlock()
		if items == nil {
			items = []map[string]any{
				{"name": "c1", "user": "guest", "vhost": "/", "state": "running", "channels": 1, "protocol": "AMQP 0-9-1", "host": "127.0.0.1", "peer_host": "127.0.0.1", "peer_port": 5000, "connected_at": "2026-11-08T00:00:00Z", "last_activity": "2026-11-08T00:00:01Z", "timeout": nil, "recv_oct": nil, "send_oct": nil, "channels_detail": nil},
			}
		}
		writeRead(w, r, envelope(maybeReverse(r, items), len(items)))
		return
	}
	if len(segs) == 4 && segs[3] == "channels" {
		writeJSON(w, http.StatusNotImplemented, map[string]any{"error": "not_implemented", "reason": "channel detail is not implemented (BOB-3)"})
		return
	}
	if len(segs) == 3 {
		switch r.Method {
		case http.MethodGet:
			writeRead(w, r, map[string]any{"name": segs[2], "user": "guest", "vhost": "/", "state": "running", "channels": 1, "protocol": "AMQP 0-9-1", "peer_host": "127.0.0.1", "peer_port": 5000, "connected_at": "2026-11-08T00:00:00Z", "last_activity": "2026-11-08T00:00:01Z"})
			return
		case http.MethodDelete:
			if !s.guardCSRF(w, r) {
				return
			}
			w.WriteHeader(http.StatusNoContent)
			return
		}
	}
	writeJSON(w, http.StatusNotFound, map[string]any{"error": "not_found"})
}

// maybeReverse returns items reversed when the request asked for
// sort_reverse=true, modelling a server that honours the sort direction, so a
// test can observe the order the SPA rendered.
func maybeReverse(r *http.Request, items []map[string]any) []map[string]any {
	if r.URL.Query().Get("sort_reverse") != "true" || len(items) < 2 {
		return items
	}
	out := make([]map[string]any, len(items))
	for i := range items {
		out[i] = items[len(items)-1-i]
	}
	return out
}

func envelope(items any, total int) map[string]any {
	return map[string]any{
		"items": items, "total_count": total, "item_count": total, "filtered_count": total,
		"page": 1, "page_size": 100, "page_count": 1,
	}
}

// atoiDefault parses s as an int, returning def for an empty or malformed value.
func atoiDefault(s string, def int) int {
	if s == "" {
		return def
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return def
	}
	return n
}
