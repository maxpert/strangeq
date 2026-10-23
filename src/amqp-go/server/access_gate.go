package server

import (
	"net"
	"net/netip"
	"sync"
	"time"

	"github.com/maxpert/amqp-go/config"
	amqperrors "github.com/maxpert/amqp-go/errors"
	"github.com/maxpert/amqp-go/protocol"
	"go.uber.org/zap"
)

// Client-visible deny texts. Host, user and rate-limit denials intentionally
// share these strings: a distinct message would tell an attacker whether a
// username exists or whether a host is merely rate-limited.
const (
	accessRefusedText      = "ACCESS_REFUSED - connection refused by access policy"
	authenticationFailText = "Authentication failed"

	// maxLimiterKeys bounds the failed-auth token-bucket table. At capacity,
	// idle (fully refilled) buckets are swept; if none can be freed, the new
	// peer is not tracked (fail-open for tracking only — host lists and auth
	// still apply).
	maxLimiterKeys = 65536
)

// accessGate holds the parsed connection policy for one Server. server.go is
// shared with other work streams, so the state lives in a package-level
// registry keyed by *Server instead of a new Server field.
//
// Seam S-4: a later change may move this onto a Server field initialized by
// builder.go; the method signatures on Server stay the same.
type accessGate struct {
	allowHosts config.HostMatcher
	blockHosts config.HostMatcher
	allowUsers map[string]struct{}
	blockUsers map[string]struct{}
	limiter    *failureLimiter // nil when disabled or unnecessary

	// parseErr is set when the configured lists cannot be parsed. Validate
	// rejects such configs, so this only guards a Server built by hand; the
	// gate then denies every peer (fail closed).
	parseErr error
}

// gates maps *Server to *accessGate. Entries are never removed: servers are
// long-lived, and tests create hundreds at most.
var gates sync.Map

// gate returns the gate for s, building it once from s.Config.Security.
func (s *Server) gate() *accessGate {
	if existing, ok := gates.Load(s); ok {
		return existing.(*accessGate)
	}
	built, err := buildAccessGate(s.Config)
	if err != nil {
		if s.Log != nil {
			s.Log.Error("Invalid access-control configuration; refusing all connections",
				zap.Error(err))
		}
	}
	if built.limiter != nil && s.Log != nil {
		log := s.Log
		built.limiter.warn = func(msg string) { log.Warn(msg) }
	}
	actual, _ := gates.LoadOrStore(s, built)
	return actual.(*accessGate)
}

func buildAccessGate(cfg *config.AMQPConfig) (*accessGate, error) {
	gate := &accessGate{
		allowUsers: map[string]struct{}{},
		blockUsers: map[string]struct{}{},
	}
	if cfg == nil {
		return gate, nil
	}

	sec := cfg.Security
	allowHosts, err := config.ParseHostList("security.allowedhosts", sec.AllowedHosts)
	if err != nil {
		return &accessGate{parseErr: err}, err
	}
	blockHosts, err := config.ParseHostList("security.blockedhosts", sec.BlockedHosts)
	if err != nil {
		return &accessGate{parseErr: err}, err
	}
	gate.allowHosts = allowHosts
	gate.blockHosts = blockHosts
	for _, name := range sec.AllowedUsers {
		gate.allowUsers[name] = struct{}{}
	}
	for _, name := range sec.BlockedUsers {
		gate.blockUsers[name] = struct{}{}
	}

	if sec.AuthFailureLimit > 0 {
		window := time.Duration(sec.AuthFailureWindowMS) * time.Millisecond
		if window <= 0 {
			window = time.Minute
		}
		gate.limiter = newFailureLimiter(sec.AuthFailureLimit, window)
	}
	return gate, nil
}

// admitPeer enforces host policy and the failed-auth budget. It runs before
// SASL and before the authentication-disabled early return, so host lists are
// network policy applied even when authentication is off.
func (s *Server) admitPeer(conn *protocol.Connection) error {
	gate := s.gate()
	if gate.parseErr != nil {
		return s.refuseAccess(conn, "host_not_allowed", accessRefusedText, "")
	}

	addr, addrOK := peerAddr(conn)
	if !addrOK {
		// Non-TCP peer (net.Pipe in tests, or an unexpected net.Addr): allowed
		// only when no host policy is configured, mirroring isLoopbackConn's
		// fail-closed stance.
		if !gate.allowHosts.Empty() || !gate.blockHosts.Empty() {
			return s.refuseAccess(conn, "host_not_allowed", accessRefusedText, "")
		}
		return nil
	}

	if gate.blockHosts.Contains(addr) {
		return s.refuseAccess(conn, "host_blocked", accessRefusedText, "")
	}
	if !gate.allowHosts.Empty() && !gate.allowHosts.Contains(addr) {
		return s.refuseAccess(conn, "host_not_allowed", accessRefusedText, "")
	}
	if gate.limiter != nil && !gate.limiter.allow(limiterKey(addr)) {
		return s.refuseAccess(conn, "rate_limited", accessRefusedText, "")
	}
	return nil
}

// recordAuthFailure consumes one token from the peer's failed-auth budget and
// increments the auth-failure counter. reason is one of bad_credentials,
// unsupported_mechanism, user_blocked, user_not_allowed, rate_limited.
func (s *Server) recordAuthFailure(conn *protocol.Connection, reason string) {
	if gate := s.gate(); gate.limiter != nil {
		if addr, ok := peerAddr(conn); ok {
			gate.limiter.fail(limiterKey(addr))
		}
	}
	s.recordAuthFailureMetric(reason)
}

// checkUserAccess applies the user allow/block lists once identity is known.
func (s *Server) checkUserAccess(conn *protocol.Connection, username string) error {
	gate := s.gate()
	if len(gate.blockUsers) == 0 && len(gate.allowUsers) == 0 {
		return nil
	}
	if _, blocked := gate.blockUsers[username]; blocked {
		return s.refuseUser(conn, username, "user_blocked")
	}
	if len(gate.allowUsers) > 0 {
		if _, allowed := gate.allowUsers[username]; !allowed {
			return s.refuseUser(conn, username, "user_not_allowed")
		}
	}
	return nil
}

// refuseUser denies an authenticated identity without revealing whether the
// denial came from an allow list or a block list. It counts as a failed auth
// for the per-peer limiter.
func (s *Server) refuseUser(conn *protocol.Connection, username, reason string) error {
	if gate := s.gate(); gate.limiter != nil {
		if addr, ok := peerAddr(conn); ok {
			gate.limiter.fail(limiterKey(addr))
		}
	}
	return s.refuseAccess(conn, reason, authenticationFailText, username)
}

// refuseAccess logs, counts and closes a connection refused by policy. The
// return value is always non-nil: the handshake loop only aborts when the
// handler returns an error.
func (s *Server) refuseAccess(conn *protocol.Connection, reason, text, username string) error {
	fields := []zap.Field{
		zap.String("connection_id", conn.ID),
		zap.String("remote_addr", remoteAddrString(conn)),
		zap.String("reason", reason),
	}
	if username != "" {
		fields = append(fields, zap.String("username", username))
	}
	if s.Log != nil {
		s.Log.Warn("Connection refused by access policy", fields...)
	}
	s.recordAuthFailureMetric(reason)
	// S2/S4: the refusal goes through WS-B's typed *ConnectionError path —
	// failConnection writes exactly one connection.close (CompareAndSwap) and
	// arms the close-ok timer; the handshake loop then awaits close-ok, closes
	// the socket, and the deferred slot release frees the pending-handshake
	// slot. The return value is always non-nil: the handshake loop only aborts
	// when the handler returns an error.
	return s.connectionError(conn, amqperrors.AccessRefused, text, 10, 11)
}

// recordAuthFailureMetric feeds amqp_auth_failures_total without widening the
// MetricsCollector interface (server/metrics.go is owned by another stream).
func (s *Server) recordAuthFailureMetric(reason string) {
	if recorder, ok := s.MetricsCollector.(interface{ RecordAuthFailure(string) }); ok {
		recorder.RecordAuthFailure(reason)
	}
}

// remoteAddrString returns the peer address for logs, or "" when unavailable.
func remoteAddrString(conn *protocol.Connection) string {
	if conn == nil || conn.Conn == nil || conn.Conn.RemoteAddr() == nil {
		return ""
	}
	return conn.Conn.RemoteAddr().String()
}

// peerAddr extracts a netip.Addr from a TCP peer, unmapping IPv4-mapped IPv6.
func peerAddr(conn *protocol.Connection) (netip.Addr, bool) {
	if conn == nil || conn.Conn == nil {
		return netip.Addr{}, false
	}
	remote := conn.Conn.RemoteAddr()
	if remote == nil {
		return netip.Addr{}, false
	}
	tcp, ok := remote.(*net.TCPAddr)
	if !ok || tcp.IP == nil {
		return netip.Addr{}, false
	}
	addr, ok := netip.AddrFromSlice(tcp.IP)
	if !ok {
		return netip.Addr{}, false
	}
	return addr.Unmap(), true
}

// limiterKey maps a peer to its bucket: the address itself for IPv4, and the
// /64 prefix for IPv6 (a single host usually owns an entire /64, so per-address
// keys would be trivial to evade).
func limiterKey(addr netip.Addr) string {
	addr = addr.Unmap()
	if addr.Is4() {
		return addr.String()
	}
	prefix, err := addr.Prefix(64)
	if err != nil {
		return addr.String()
	}
	return prefix.Masked().String()
}

// failureLimiter is a per-key token bucket: capacity limit, refilled
// continuously at limit tokens per window. Failures consume a token; a peer
// with less than one token is refused before any bcrypt work.
type failureLimiter struct {
	limit  float64
	window time.Duration
	// now is injectable so tests can advance time without sleeping.
	now func() time.Time
	// warn, when set, reports a full table (tracking skipped, fail-open) at
	// most once per minute on the limiter's clock — never the wall clock, so
	// tests stay deterministic.
	warn     func(msg string)
	lastWarn time.Time

	mu      sync.Mutex
	buckets map[string]*tokenBucket
}

type tokenBucket struct {
	tokens float64
	last   time.Time
}

func newFailureLimiter(limit int, window time.Duration) *failureLimiter {
	return &failureLimiter{
		limit:   float64(limit),
		window:  window,
		now:     time.Now,
		buckets: make(map[string]*tokenBucket, 64),
	}
}

// allow reports whether key may attempt authentication now (bucket at least
// one token). It does not consume: only failures do.
func (l *failureLimiter) allow(key string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	b := l.bucket(key)
	b.refill(l)
	return b.tokens >= 1
}

// fail consumes one token, if any remain.
func (l *failureLimiter) fail(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	b := l.bucket(key)
	b.refill(l)
	if b.tokens >= 1 {
		b.tokens--
	}
}

func (l *failureLimiter) bucket(key string) *tokenBucket {
	if b, ok := l.buckets[key]; ok {
		return b
	}
	if len(l.buckets) >= maxLimiterKeys {
		l.sweepLocked()
	}
	b := &tokenBucket{tokens: l.limit, last: l.now()}
	if len(l.buckets) < maxLimiterKeys {
		l.buckets[key] = b
	} else {
		l.warnFullLocked()
	}
	return b
}

// warnFullLocked reports a full table at most once per minute on the
// limiter's (injectable) clock. Tracking fails open; host lists still apply.
func (l *failureLimiter) warnFullLocked() {
	if l.warn == nil {
		return
	}
	now := l.now()
	if !l.lastWarn.IsZero() && now.Sub(l.lastWarn) < time.Minute {
		return
	}
	l.lastWarn = now
	l.warn("failed-auth limiter table full: not tracking new peers (fail-open; host lists still apply)")
}

// sweepLocked drops buckets that are already full (idle peers), so the table
// only grows with peers that have recently failed.
func (l *failureLimiter) sweepLocked() {
	now := l.now()
	for key, b := range l.buckets {
		b.refillAt(l, now)
		if b.tokens >= l.limit {
			delete(l.buckets, key)
		}
	}
}

func (b *tokenBucket) refill(l *failureLimiter) {
	b.refillAt(l, l.now())
}

func (b *tokenBucket) refillAt(l *failureLimiter, now time.Time) {
	if now.After(b.last) {
		elapsed := now.Sub(b.last)
		b.tokens += l.limit * (float64(elapsed) / float64(l.window))
		if b.tokens > l.limit {
			b.tokens = l.limit
		}
		b.last = now
	}
}
