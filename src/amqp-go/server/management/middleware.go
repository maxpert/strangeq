package management

import (
	"bytes"
	"context"
	"crypto/subtle"
	"fmt"
	"net/http"
	"net/netip"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
)

// cspPolicy is the Content-Security-Policy for UI and browser-renderable API
// responses (SEC-031/032/035).
const cspPolicy = "default-src 'self'; script-src 'self'; style-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'; connect-src 'self'"

// responseRecorder captures the status for metrics and guards against a second
// WriteHeader.
type responseRecorder struct {
	http.ResponseWriter
	status  int
	written bool
}

func (w *responseRecorder) WriteHeader(code int) {
	if w.written {
		return
	}
	w.status = code
	w.written = true
	w.ResponseWriter.WriteHeader(code)
}

func (w *responseRecorder) Write(b []byte) (int, error) {
	if !w.written {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(b)
}

func (w *responseRecorder) statusCode() int {
	if w.status == 0 {
		return http.StatusOK
	}
	return w.status
}

// ensureHeader lets a handler set a header before the status is written.
func (w *responseRecorder) ensureHeader(k, v string) {
	if !w.written {
		w.Header().Set(k, v)
	}
}

// serveHTTP is the single entry point: routing, authentication, the two gates,
// CSRF, metrics, and panic isolation all live here.
func (s *Server) serveHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	reqID := sanitizeField(r.Header.Get("X-Request-Id"), 128)
	if reqID == "" {
		reqID, _ = randomHex(8)
	}
	r = r.WithContext(withRequestID(r.Context(), reqID))

	// Per-request deadline (management.limits.request_timeout_seconds). A
	// handler that respects the request context is bounded; zero means no
	// deadline beyond the listener's own read/write timeouts.
	if secs := s.mcfg.Limits.RequestTimeoutSeconds; secs > 0 {
		ctx, cancel := context.WithTimeout(r.Context(), time.Duration(secs)*time.Second)
		defer cancel()
		r = r.WithContext(ctx)
	}

	rw := &responseRecorder{ResponseWriter: w}
	path := r.URL.EscapedPath()

	// OPS-062: record EVERY request, including every early return below —
	// shutdown 503, body-cap 413, in-flight shed 503, negotiation/404/405/401/
	// 429/403. The route is not known yet, so the bounded endpoint label starts
	// at the sentinel "pre_route" (OPS-004) and is replaced with the matched
	// route TEMPLATE as soon as routing succeeds; a request that never matches a
	// route keeps "pre_route". Recording in a defer means no early return can be
	// uncounted, and it is the single recorder for both the count and the
	// duration.
	endpoint := "pre_route"
	defer func() {
		s.sink.ManagementRequests(endpoint, r.Method, statusClass(rw.statusCode()))
		s.sink.ManagementDuration(endpoint, time.Since(start).Seconds())
	}()

	if path != "/api" && !strings.HasPrefix(path, "/api/") {
		s.serveUI(rw, r, reqID)
		return
	}

	h := rw.Header()
	h.Set("X-Request-Id", reqID)
	h.Set("Cache-Control", "no-store")
	h.Set("Vary", "Authorization")
	applySecurityHeaders(h)

	// CORS (management.cors.allowed_origins, SEC-094). The origin is reflected
	// only when it is explicitly allow-listed; credentials are permitted
	// because the browser path is cookie-authenticated. A preflight is answered
	// before routing/authentication so it carries no credential.
	if origin := r.Header.Get("Origin"); origin != "" && s.corsAllows(origin) {
		h.Set("Access-Control-Allow-Origin", origin)
		h.Set("Access-Control-Allow-Credentials", "true")
		addVary(h, "Origin")
		if r.Method == http.MethodOptions {
			h.Set("Access-Control-Allow-Methods", "GET, HEAD, POST, PUT, DELETE, OPTIONS")
			h.Set("Access-Control-Allow-Headers", "Authorization, Content-Type, X-CSRF-Token, X-Request-Id, If-None-Match, If-Match")
			h.Set("Access-Control-Max-Age", "600")
			rw.WriteHeader(http.StatusNoContent)
			return
		}
	}

	if s.shuttingDwn.Load() {
		s.writeAPIError(rw, r, apiErr(http.StatusServiceUnavailable, errUnavailable,
			"management surface is shutting down", withHeader("Retry-After", "1")))
		return
	}

	// Body cap BEFORE routing (SEC-065/MIL-046): a declared body larger than
	// any endpoint can accept is refused 413 without touching a route, an
	// authenticator, or a handler. The bound is the LARGEST per-endpoint limit
	// (definitions import); each endpoint then enforces its own tighter one.
	if limit := s.preRoutingBodyLimit(); limit > 0 && r.ContentLength > int64(limit) {
		s.writeAPIError(rw, r, apiErr(http.StatusRequestEntityTooLarge, errPayloadTooLarge,
			"request body exceeds "+strconv.Itoa(limit)+" bytes"))
		return
	}

	// In-flight cap (SEC-061).
	if n := s.inflight.Add(1); int(n) > s.mcfg.Limits.MaxInFlight {
		s.inflight.Add(-1)
		s.rateLimitedTotal.Add(1)
		s.sink.ManagementRateLimited()
		s.writeAPIError(rw, r, apiErr(http.StatusServiceUnavailable, errUnavailable,
			"too many concurrent management requests", withHeader("Retry-After", "1")))
		return
	}
	s.sink.ManagementInFlight(int(s.inflight.Load()))
	defer func() { s.sink.ManagementInFlight(int(s.inflight.Add(-1))) }()

	if ae := negotiationError(r, strings.HasPrefix(path, "/api/definitions")); ae != nil {
		s.writeAPIError(rw, r, ae)
		return
	}

	// Route.
	m := s.router.match(r.Method, path)
	if m == nil {
		if allowed := s.router.allowedFor(path); len(allowed) > 0 {
			s.writeAPIError(rw, r, apiErr(http.StatusMethodNotAllowed, errMethodNotAllowed,
				"method not allowed", withAllow(strings.Join(allowed, ", "))))
			return
		}
		s.writeAPIError(rw, r, errNotFoundMsg("unknown management endpoint"))
		return
	}
	r = r.WithContext(withEndpoint(r.Context(), m.pattern))
	endpoint = m.pattern

	// The session-login endpoint authenticates itself and must be reachable
	// without a prior credential (AUTH-031).
	if !m.public {
		// Authentication (AUTH-005/006).
		p, ci, aerr := s.authenticate(r)
		if aerr != nil {
			if s.failureIsRateLimited(ci) {
				s.rateLimitedTotal.Add(1)
				s.sink.ManagementRateLimited()
				s.writeAPIError(rw, r, apiErr(http.StatusTooManyRequests, errRateLimited,
					"too many failed authentication attempts", withHeader("Retry-After", "1")))
				return
			}
			rw.Header().Set("WWW-Authenticate", `Basic realm="StrangeQ Management"`)
			s.writeAPIError(rw, r, aerr)
			return
		}

		if ae := s.authorizationDisabledGate(ci); ae != nil {
			s.writeAPIError(rw, r, ae)
			return
		}
		if ae := s.capabilityGate(p); ae != nil {
			s.writeAPIError(rw, r, ae)
			return
		}
		// AUTH-011: an ENTITY read requires at least one vhost permission for
		// the vhost the request targets. Non-entity endpoints (health, whoami,
		// overview, …) are governed by the capability gate alone. Built-in
		// principals (p.User == nil) hold a synthesized full permission.
		if isEntityReadRoute(m) {
			// FR-CONV-003/D-1: a route that captures a {vhost} accepts only
			// the decoded "/". The vhost EXISTENCE check runs before the
			// resource gate so an unknown vhost is 404 "unknown vhost" (what
			// the handler itself would answer), not the 403 the resource gate
			// produces for a principal with no permission on that
			// (nonexistent) vhost.
			if ae := entityVHostGuard(m, r); ae != nil {
				s.writeAPIError(rw, r, ae)
				return
			}
			if ae := s.resourceReadGate(p, requestVhost(r)); ae != nil {
				s.writeAPIError(rw, r, ae)
				return
			}
		}
		if ae := s.csrfGate(r, p); ae != nil {
			s.writeAPIError(rw, r, ae)
			return
		}

		r = r.WithContext(withPrincipal(r.Context(), p))
		r = r.WithContext(withClient(r.Context(), ci))
		// A cookie-authenticated response varies on the Cookie header, exactly
		// as a Basic-authenticated one varies on Authorization (SEC-020).
		if p.ViaCookie {
			addVary(h, "Cookie")
		}
	} else {
		r = r.WithContext(withClient(r.Context(), s.resolveClient(r)))
	}

	// Dispatch with panic isolation (NFR-020).
	s.dispatch(rw, r, m)
}

func (s *Server) dispatch(rw *responseRecorder, r *http.Request, m *route) {
	defer func() {
		if rec := recover(); rec != nil {
			s.panicCount.Add(1)
			// OPS-060: the recovered panic is counted by bounded route
			// TEMPLATE, never the raw path. The label domain is the router's
			// pattern set. A no-op when management metrics are disabled.
			s.sink.ManagementPanic(m.pattern)
			s.log.Error("management handler panicked",
				zap.Any("panic", rec), zap.String("endpoint", m.pattern),
				zap.String("request_id", requestID(r)))
			s.writeAPIError(rw, r, apiErr(http.StatusInternalServerError, errInternal,
				"internal error"))
		}
	}()
	m.handler(rw, r)
}

// csrfGate enforces SEC-020…024 for cookie-authenticated state-changing
// requests. A Basic-authenticated mutation is exempt (SEC-024).
func (s *Server) csrfGate(r *http.Request, p *Principal) *apiError {
	if p == nil {
		return nil
	}
	switch r.Method {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		return nil
	}
	if !p.ViaCookie {
		return nil
	}
	sess, ok := s.sessions.get(p.SessionID)
	if !ok {
		return errUnauth("session expired")
	}
	tok := r.Header.Get("X-CSRF-Token")
	if tok == "" || subtle.ConstantTimeCompare([]byte(tok), []byte(sess.csrf)) != 1 {
		return errRefused("CSRF token missing or invalid")
	}
	if !s.originMatches(r) {
		return errRefused("Origin/Referer does not match the management listener")
	}
	return nil
}

func (s *Server) originMatches(r *http.Request) bool {
	host := r.Host
	origin := r.Header.Get("Origin")
	if origin == "" {
		origin = r.Header.Get("Referer")
	}
	if origin == "" {
		// Same-origin XHRs may omit both; the CSRF token is then the only
		// signal, which is sufficient (a cross-site form cannot set the
		// header).
		return true
	}
	h := originHost(origin)
	return h == host
}

func originHost(origin string) string {
	o := origin
	if i := strings.Index(o, "://"); i >= 0 {
		o = o[i+3:]
	}
	if i := strings.IndexAny(o, "/?#"); i >= 0 {
		o = o[:i]
	}
	return o
}

// applySecurityHeaders sets the CSP/anti-sniff/anti-framing headers
// (SEC-031/032/035).
func applySecurityHeaders(h http.Header) {
	h.Set("Content-Security-Policy", cspPolicy)
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("Referrer-Policy", "no-referrer")
	h.Set("X-Frame-Options", "DENY")
}

// addVary appends a value to the Vary header, case-insensitively, without
// duplicating an existing entry.
func addVary(h http.Header, value string) {
	existing := h.Get("Vary")
	for _, part := range strings.Split(existing, ",") {
		if strings.EqualFold(strings.TrimSpace(part), value) {
			return
		}
	}
	if strings.TrimSpace(existing) == "" {
		h.Set("Vary", value)
		return
	}
	h.Set("Vary", existing+", "+value)
}

// corsAllows reports whether origin is explicitly allow-listed
// (management.cors.allowed_origins). An empty list means CORS is off.
func (s *Server) corsAllows(origin string) bool {
	origin = strings.TrimSpace(origin)
	if origin == "" {
		return false
	}
	for _, o := range s.mcfg.CORS.AllowedOrigins {
		if strings.TrimSpace(o) == origin {
			return true
		}
	}
	return false
}

// serveUI serves the injected UI handler, or a minimal placeholder when none
// was injected (the management package never embeds the UI; spec 02 owns it).
func (s *Server) serveUI(w *responseRecorder, r *http.Request, reqID string) {
	h := w.Header()
	h.Set("X-Request-Id", reqID)
	applySecurityHeaders(h)
	if s.opts.UIHandler != nil {
		s.opts.UIHandler.ServeHTTP(w, r)
		s.sink.ManagementUIRequests(statusClass(w.statusCode()))
		return
	}
	if r.URL.Path != "/" && r.URL.Path != "/index.html" {
		http.NotFound(w, r)
		s.sink.ManagementUIRequests(4)
		return
	}
	h.Set("Content-Type", "text/html; charset=utf-8")
	h.Set("Cache-Control", "no-store")
	body := []byte(placeholderPage)
	h.Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(http.StatusOK)
	if r.Method != http.MethodHead {
		_, _ = w.Write(body)
	}
	s.sink.ManagementUIRequests(2)
}

// placeholderPage is served at "/" when no UI handler is injected.
const placeholderPage = `<!doctype html>
<html lang="en"><head><meta charset="utf-8"><title>StrangeQ Management</title></head>
<body><h1>StrangeQ Management API</h1>
<p>The management API is available under <code>/api/</code>. No UI bundle is embedded in this build.</p>
<p>See <code>GET /api/strangeq/version</code> for the endpoint list.</p>
</body></html>`

// writeJSON marshals v, applies the response cap, and emits it with an ETag on
// GET/HEAD plus If-None-Match → 304 handling.
func (s *Server) writeJSON(w http.ResponseWriter, r *http.Request, status int, v any) {
	body, err := marshalCompact(v)
	if err != nil {
		s.writeAPIError(w, r, apiErr(http.StatusInternalServerError, errInternal, "response encoding failed"))
		return
	}
	s.writeBytes(w, r, status, body)
}

// writeBytes emits a JSON body honouring limits, ETag, and 304.
func (s *Server) writeBytes(w http.ResponseWriter, r *http.Request, status int, body []byte) {
	if max := s.mcfg.Limits.MaxResponseBytes; max > 0 && len(body) > max {
		s.sink.ManagementSnapshotPartial(endpointLabel(r))
		s.writeAPIError(w, r, apiErr(http.StatusServiceUnavailable, errUnavailable,
			"response exceeds management.limits.max_response_bytes", withHeader("Retry-After", "1")))
		return
	}
	h := w.Header()
	h.Set("Content-Type", "application/json; charset=utf-8")
	if r.Method == http.MethodGet || r.Method == http.MethodHead {
		tag := etagFor(body)
		h.Set("ETag", tag)
		if inm := r.Header.Get("If-None-Match"); inm != "" && etagMatches(inm, tag) {
			w.WriteHeader(http.StatusNotModified)
			return
		}
	}
	h.Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(status)
	if r.Method != http.MethodHead {
		_, _ = w.Write(body)
	}
}

func etagMatches(ifNoneMatch, tag string) bool {
	for _, part := range strings.Split(ifNoneMatch, ",") {
		p := strings.TrimSpace(part)
		if p == "*" || p == tag || strings.TrimPrefix(p, "W/") == tag {
			return true
		}
	}
	return false
}

// etagStrongMatches implements the STRONG comparison required by If-Match
// (RFC 9110 §13.1.1): "*" matches any current representation, otherwise the
// validator must be the exact opaque tag. A weak validator (W/"…") NEVER
// matches, unlike the weak comparison If-None-Match uses.
func etagStrongMatches(ifMatch, tag string) bool {
	for _, part := range strings.Split(ifMatch, ",") {
		p := strings.TrimSpace(part)
		if p == "*" || p == tag {
			return true
		}
	}
	return false
}

// writeAPIError renders the error envelope.
func (s *Server) writeAPIError(w http.ResponseWriter, r *http.Request, e *apiError) {
	for k, v := range e.headers {
		w.Header().Set(k, v)
	}
	if e.allow != "" {
		w.Header().Set("Allow", e.allow)
	}
	status := e.status
	reason := e.reason
	if status == http.StatusInternalServerError {
		// Only a genuine 500 is scrubbed to a generic reason (SEC-072). A 501
		// keeps the subsystem text and a 503 keeps the backpressure text: both
		// are actionable and reveal nothing the handler did not already state.
		reason = "internal error"
	}
	env := errorEnvelope{Error: e.token, Reason: reason, RequestID: requestID(r)}
	body, err := marshalCompact(env)
	if err != nil {
		body = []byte(`{"error":"internal_error","reason":"internal error"}`)
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Del("ETag")
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(status)
	if r.Method != http.MethodHead {
		_, _ = w.Write(body)
	}
}

// etag for a body already encoded.
func etagBody(body []byte) string { return etagFor(body) }

// readBodyLimited reads the request body up to limit bytes, returning 413 when
// it is exceeded (SEC-065).
func (s *Server) readBodyLimited(r *http.Request, limit int) ([]byte, *apiError) {
	if limit <= 0 {
		limit = 2 << 20
	}
	r.Body = http.MaxBytesReader(nil, r.Body, int64(limit))
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(r.Body); err != nil {
		return nil, apiErr(http.StatusRequestEntityTooLarge, errPayloadTooLarge,
			"request body exceeds "+strconv.Itoa(limit)+" bytes")
	}
	return buf.Bytes(), nil
}

// preRoutingBodyLimit is the largest body any management endpoint may accept.
func (s *Server) preRoutingBodyLimit() int {
	limit := s.mcfg.Limits.MaxBodyBytes
	if v := s.mcfg.Limits.MaxImportBodyBytes; v > limit {
		limit = v
	}
	return limit
}

// counterWindow is a fixed-window per-key counter (SEC-067/068/069).
type counterWindow struct {
	mu     sync.Mutex
	limit  int
	window time.Duration
	now    func() time.Time
	counts map[string]int
	start  time.Time
}

func newCounterWindow(window time.Duration, limit int, now func() time.Time) *counterWindow {
	if now == nil {
		now = time.Now
	}
	return &counterWindow{limit: limit, window: window, now: now, counts: make(map[string]int)}
}

// allow reports whether a request under key is within the limit. limit <= 0
// means unlimited.
func (c *counterWindow) allow(key string) bool {
	if c == nil || c.limit <= 0 {
		return true
	}
	now := c.now()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.start.IsZero() || now.Sub(c.start) >= c.window {
		c.start = now
		c.counts = make(map[string]int)
	}
	if c.counts[key] >= c.limit {
		return false
	}
	c.counts[key]++
	return true
}

// peek reports the remaining allowance and the seconds until the current
// fixed window resets, WITHOUT consuming anything. It is used to emit the
// X-RateLimit-* advisory headers after allow() has run.
func (c *counterWindow) peek(key string) (remaining, resetSeconds int) {
	if c == nil || c.limit <= 0 {
		return 0, 0
	}
	now := c.now()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.start.IsZero() || now.Sub(c.start) >= c.window {
		return c.limit, int(c.window.Seconds())
	}
	remaining = c.limit - c.counts[key]
	if remaining < 0 {
		remaining = 0
	}
	resetSeconds = int((c.window - now.Sub(c.start)).Seconds())
	if resetSeconds < 0 {
		resetSeconds = 0
	}
	return remaining, resetSeconds
}

// setRateLimitHeaders emits the advisory X-RateLimit-* headers for a limited
// endpoint (limit<=0 is unlimited and emits nothing).
func setRateLimitHeaders(w http.ResponseWriter, lim *counterWindow, key string) {
	if lim == nil || lim.limit <= 0 {
		return
	}
	remaining, resetSeconds := lim.peek(key)
	h := w.Header()
	h.Set("X-RateLimit-Limit", strconv.Itoa(lim.limit))
	h.Set("X-RateLimit-Remaining", strconv.Itoa(remaining))
	h.Set("X-RateLimit-Reset", strconv.Itoa(resetSeconds))
}

// parseProxyEntry accepts a CIDR prefix or a bare IP (treated as /32 or /128).
func parseProxyEntry(entry string) (netip.Prefix, error) {
	entry = strings.TrimSpace(entry)
	if entry == "" {
		return netip.Prefix{}, fmt.Errorf("empty entry")
	}
	if strings.Contains(entry, "/") {
		return netip.ParsePrefix(entry)
	}
	addr, err := netip.ParseAddr(entry)
	if err != nil {
		return netip.Prefix{}, err
	}
	addr = addr.Unmap()
	return netip.PrefixFrom(addr, addr.BitLen()), nil
}

// --- audit ---

// auditRecord is the structured record emitted for every mutation
// (SEC-080/082). Attacker-controlled fields are sanitised before they are
// written.
type auditRecord struct {
	Timestamp string
	RequestID string
	Principal string
	Client    string
	Method    string
	Path      string
	Entity    string
	Action    string
	Outcome   string
	Status    int
}

// auditSink writes audit records. The production sink is the broker logger; a
// test may inject a sink that fails to exercise SEC-084.
type auditSink interface {
	Write(auditRecord, bool) error
}

type loggerAuditSink struct{ log *zap.Logger }

func (l loggerAuditSink) Write(a auditRecord, denied bool) error {
	if l.log == nil {
		return nil
	}
	fields := []zap.Field{
		zap.String("timestamp", a.Timestamp),
		zap.String("request_id", a.RequestID),
		zap.String("principal", a.Principal),
		zap.String("client", a.Client),
		zap.String("method", a.Method),
		zap.String("path", a.Path),
		zap.String("entity", a.Entity),
		zap.String("action", a.Action),
		zap.String("outcome", a.Outcome),
		zap.Int("status", a.Status),
	}
	if denied {
		l.log.Warn("management mutation", fields...)
	} else {
		l.log.Info("management mutation", fields...)
	}
	return nil
}

// audit records one mutation. destructive records must be durable before the
// request succeeds: a failing sink returns 503 for them, while a
// non-destructive failure only increments the dropped counter (SEC-084).
//
// kind is the entity kind ("queue"), action the verb ("delete"), detail the
// target (the resource name, already attacker-controlled and sanitised here).
func (s *Server) audit(r *http.Request, kind, action, detail string, status int, destructive bool) *apiError {
	limit := s.mcfg.Limits.MaxNameBytes
	if limit <= 0 {
		limit = 256
	}
	p := principalFrom(r)
	name := ""
	if p != nil {
		name = p.Name
	}
	rec := auditRecord{
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		RequestID: requestID(r),
		Principal: sanitizeField(name, limit),
		Client:    sanitizeField(clientFrom(r).EffectiveIP, limit),
		Method:    r.Method,
		Path:      sanitizeField(r.URL.EscapedPath(), limit),
		Entity:    sanitizeField(kind+":"+detail, limit),
		Action:    sanitizeField(action, limit),
		Outcome:   sanitizeField(fmt.Sprintf("%d", status), 16),
		Status:    status,
	}
	denied := status >= 400
	if err := s.auditSink.Write(rec, denied); err != nil {
		s.auditDropped.Add(1)
		s.sink.ManagementAuditDropped()
		if destructive {
			return apiErr(http.StatusServiceUnavailable, errUnavailable,
				"audit sink unavailable; refusing a destructive mutation that cannot be recorded",
				withHeader("Retry-After", "1"))
		}
	}
	return nil
}

// auditMutation records a completed mutation after the fact.
func (s *Server) auditMutation(r *http.Request, kind, target, action string, status int) {
	_ = s.audit(r, kind, action, target, status, false)
}

// auditDestructive records a destructive mutation BEFORE it happens; a failed
// write returns a 503 so the mutation does not proceed unaudited (SEC-084).
func (s *Server) auditDestructive(r *http.Request, kind, target, action string, status int) *apiError {
	return s.audit(r, kind, action, target, status, true)
}
