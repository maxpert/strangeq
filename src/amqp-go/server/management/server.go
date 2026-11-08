package management

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/server"
	"go.uber.org/zap"
	"golang.org/x/crypto/bcrypt"
)

// Options is the dependency-injection surface of the management server
// (spec mgmt-01 §14). Every field except Config is optional.
type Options struct {
	Server        *server.Server
	Config        *config.AMQPConfig
	Authenticator interfaces.Authenticator // nil when broker auth is disabled
	AuthFile      string                   // resolved auth.json path; "" => no users
	Logger        *zap.Logger
	Metrics       ManagementMetricsSink // nil when telemetry is disabled

	Version, Commit, BuildDate, GoVersion string

	// UIHandler serves the management UI (spec 02). nil ⇒ a minimal placeholder
	// page is served at "/" instead. The management package never imports the
	// UI package; the UI is injected (no cycle).
	UIHandler http.Handler
}

// Server is the management control-plane HTTP surface.
type Server struct {
	opts   Options
	cfg    *config.AMQPConfig
	mcfg   interfaces.ManagementConfig
	log    *zap.Logger
	sink   ManagementMetricsSink
	router *router

	httpSrv *http.Server
	ln      net.Listener
	mu      sync.Mutex

	sessions *sessionStore
	jobs     *jobPool
	users    *userSource

	adminUsers     map[string]struct{}
	trustedProxies []netip.Prefix

	allowedHosts config.HostMatcher
	blockedHosts config.HostMatcher

	nodeName  string
	startTime time.Time

	endpoints []string

	inflight    atomic.Int64
	shuttingDwn atomic.Bool
	started     atomic.Bool

	// bcryptMatch is a test seam: production compares with bcrypt.
	bcryptMatch func(hash, password string) bool

	// rate limiters (fixed one-minute windows).
	authFailLimit *counterWindow
	publishLimit  *counterWindow
	getLimit      *counterWindow
	purgeLimit    *counterWindow
	declareLimit  *counterWindow
	importLimit   *counterWindow

	auditSink auditSink

	// test-observable counters
	forbiddenLockHits atomic.Int64
	insecureBind      atomic.Bool
	rateLimitedTotal  atomic.Int64
	auditDropped      atomic.Int64
	panicCount        atomic.Int64
}

// New constructs the management server. It validates the startup refusal
// matrix (defence in depth: config.Validate already ran) and prepares the
// session store, job pool, and route table. It does NOT bind: call Start.
func New(o Options) (*Server, error) {
	if o.Config == nil {
		return nil, errors.New("management: Options.Config is required")
	}
	cfg := o.Config
	mcfg := cfg.Management

	log := o.Logger
	if log == nil {
		log = zap.NewNop()
	}

	s := &Server{
		opts:      o,
		cfg:       cfg,
		mcfg:      mcfg,
		log:       log,
		sink:      sinkOrNoop(o.Metrics),
		users:     loadUserSource(o.AuthFile),
		startTime: time.Now(),
	}
	s.bcryptMatch = func(hash, password string) bool {
		return bcrypt.CompareHashAndPassword([]byte(hash), []byte(password)) == nil
	}
	s.auditSink = loggerAuditSink{log: log}

	if !mcfg.Enabled {
		// Disabled surface: no listener, no goroutines, no allocations beyond
		// this struct. Start() is a no-op and Addr() is nil (SEC-001/014).
		s.sessions = newSessionStore(1, time.Second)
		return s, nil
	}

	// Startup refusal matrix (spec mgmt-01 §2). config.Validate enforces the
	// same rules; repeating them here means an embedder that constructs
	// Options directly cannot bypass them.
	if err := s.validateStartup(); err != nil {
		return nil, err
	}

	s.adminUsers = make(map[string]struct{}, len(mcfg.AdminUsers))
	for _, n := range mcfg.AdminUsers {
		s.adminUsers[strings.TrimSpace(n)] = struct{}{}
	}
	if err := s.parseTrustedProxies(); err != nil {
		return nil, err
	}
	var err error
	if s.allowedHosts, err = config.ParseHostList("security.allowedhosts", cfg.Security.AllowedHosts); err != nil {
		return nil, err
	}
	if s.blockedHosts, err = config.ParseHostList("security.blockedhosts", cfg.Security.BlockedHosts); err != nil {
		return nil, err
	}

	s.nodeName = strings.TrimSpace(mcfg.NodeName)
	if s.nodeName == "" {
		host, herr := os.Hostname()
		if herr != nil || host == "" {
			host = "localhost"
		}
		s.nodeName = "amqp-go@" + host
	}

	s.sessions = newSessionStore(mcfg.Session.MaxSessions,
		time.Duration(mcfg.Session.IdleTimeoutSeconds)*time.Second)

	workers := mcfg.Limits.MaxInFlight / 8
	if workers < 2 {
		workers = 2
	}
	if workers > 8 {
		workers = 8
	}
	s.jobs = newJobPool(mcfg.Async.MaxJobs,
		time.Duration(mcfg.Async.JobRetentionSeconds)*time.Second, workers)

	const window = time.Minute
	s.authFailLimit = newCounterWindow(window, authFailureLimit(cfg), nil)
	s.publishLimit = newCounterWindow(window, mcfg.RateLimits.Publish, nil)
	s.getLimit = newCounterWindow(window, mcfg.RateLimits.Get, nil)
	s.purgeLimit = newCounterWindow(window, mcfg.RateLimits.Purge, nil)
	s.declareLimit = newCounterWindow(window, mcfg.RateLimits.Declare, nil)
	// SEC-067: definitions_import_seconds is the minimum interval between
	// imports by one principal, so the window IS that interval and exactly one
	// import is allowed per window (0 = unlimited).
	s.importLimit = newCounterWindow(time.Duration(mcfg.RateLimits.DefinitionsImportSeconds)*time.Second, 1, nil)

	s.router = buildRouter(s)
	s.endpoints = routeEndpointList(s.router)

	// Log the security warnings (SEC-110/AUTH-042/OPS-022) and pin the
	// insecure-bind gauge.
	for _, wmsg := range cfg.ManagementSecurityWarnings() {
		log.Warn(wmsg, zap.String("component", "management"))
	}
	if !config.IsLoopbackManagementAddress(mcfg.Address) && !mcfg.TLS.Enabled {
		s.insecureBind.Store(true)
		s.sink.ManagementInsecureBind(true)
	}
	if mcfg.Authentication == config.ManagementAuthenticationNone || mcfg.AllowUnauthenticated {
		s.sink.ManagementUnauthenticated(true)
	}
	return s, nil
}

func authFailureLimit(cfg *config.AMQPConfig) int {
	if cfg == nil {
		return 5
	}
	if cfg.Security.AuthFailureLimit <= 0 {
		return 5
	}
	return cfg.Security.AuthFailureLimit
}

func (s *Server) validateStartup() error {
	m := s.mcfg
	switch s.authMode() {
	case "inherit":
		if !s.brokerAuthEnabled() {
			if !m.AllowUnauthenticated {
				return errors.New("management.authentication is \"inherit\" but broker authentication is disabled; set management.allow_unauthenticated: true (loopback only)")
			}
			if !config.IsLoopbackManagementAddress(m.Address) {
				return fmt.Errorf("management.allow_unauthenticated requires a loopback management.address; got %q", m.Address)
			}
		}
	case "basic":
		if m.Basic.PasswordHash == "" {
			return errors.New("management.authentication is \"basic\" but management.basic.password_hash is empty")
		}
		if _, err := bcrypt.Cost([]byte(m.Basic.PasswordHash)); err != nil {
			return fmt.Errorf("management.basic.password_hash is not a valid bcrypt hash: %v", err)
		}
	case "none":
		if !config.IsLoopbackManagementAddress(m.Address) {
			return fmt.Errorf("management.authentication is \"none\" but management.address %q is not loopback", m.Address)
		}
	}
	return nil
}

func (s *Server) parseTrustedProxies() error {
	for _, entry := range s.mcfg.TrustedProxies {
		p, err := parseProxyEntry(entry)
		if err != nil {
			return fmt.Errorf("management.trusted_proxies entry %q: %w", entry, err)
		}
		s.trustedProxies = append(s.trustedProxies, p)
	}
	return nil
}

// Enabled reports whether the surface is configured on.
func (s *Server) Enabled() bool { return s.mcfg.Enabled }

// Handler returns the HTTP handler, for tests and embedders that wish to mount
// the surface themselves.
func (s *Server) Handler() http.Handler { return http.HandlerFunc(s.serveHTTP) }

// tlsEnabled reports whether the management listener terminates TLS.
func (s *Server) tlsEnabled() bool { return s.mcfg.TLS.Enabled }

// Start binds the listener synchronously and begins serving. A disabled
// surface starts nothing (SEC-001). A bind error is returned before any
// goroutine is spawned (NFR-025).
func (s *Server) Start() error {
	if !s.mcfg.Enabled {
		return nil
	}
	if s.started.Swap(true) {
		return errors.New("management: already started")
	}
	addr := net.JoinHostPort(s.mcfg.Address, strconv.Itoa(s.mcfg.Port))
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("management listen %s: %w", addr, err)
	}

	if s.tlsEnabled() {
		cert, err := tls.LoadX509KeyPair(s.mcfg.TLS.CertFile, s.mcfg.TLS.KeyFile)
		if err != nil {
			_ = ln.Close()
			return fmt.Errorf("management tls: %w", err)
		}
		ln = tls.NewListener(ln, &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		})
	}
	s.mu.Lock()
	s.ln = ln
	httpSrv := &http.Server{
		Handler:           http.HandlerFunc(s.serveHTTP),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		// The write deadline must outlive the per-request deadline
		// (management.limits.request_timeout_seconds), or a request allowed to
		// run for N seconds is cut off by the listener at 10s. See
		// managementWriteTimeout.
		WriteTimeout: managementWriteTimeout(s.mcfg),
		IdleTimeout:  120 * time.Second,
	}
	s.httpSrv = httpSrv
	s.mu.Unlock()
	go func() {
		if err := httpSrv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.log.Warn("management server stopped", zap.Error(err))
		}
	}()
	s.log.Info("Management surface listening",
		zap.String("addr", ln.Addr().String()),
		zap.String("authentication", s.authMode()),
		zap.Bool("tls", s.tlsEnabled()))
	return nil
}

// Addr returns the bound address, or nil before a successful Start.
func (s *Server) Addr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln == nil {
		return nil
	}
	return s.ln.Addr()
}

// managementWriteTimeout is the listener WriteTimeout (LOW-5). A request may
// run for management.limits.request_timeout_seconds, so the listener write
// deadline must be at least that plus a small slack; it is never SMALLER than
// the 10s default. A request with a deadline longer than the listener timeout
// would be truncated by net/http with a partial response, not the handler's
// own status.
func managementWriteTimeout(mcfg interfaces.ManagementConfig) time.Duration {
	const (
		defaultWriteTimeout = 10 * time.Second
		writeSlack          = 5 * time.Second
	)
	wt := defaultWriteTimeout
	if secs := mcfg.Limits.RequestTimeoutSeconds; secs > 0 {
		if want := time.Duration(secs)*time.Second + writeSlack; want > wt {
			wt = want
		}
	}
	return wt
}

// Shutdown stops the surface gracefully. It is idempotent, safe to call before
// Start, and RESTARTABLE: a subsequent Start binds again (NFR-040/OPS-044).
// The bounded job pool is intentionally left running — it holds no listener
// resources and is created once in New; use Close for terminal teardown.
func (s *Server) Shutdown(ctx context.Context) error {
	s.shuttingDwn.Store(true)
	s.mu.Lock()
	srv := s.httpSrv
	ln := s.ln
	s.httpSrv = nil
	s.ln = nil
	s.mu.Unlock()

	var err error
	if srv != nil {
		err = srv.Shutdown(ctx)
	}
	// http.Server.Shutdown closes the listeners it has TRACKED; a Start that
	// has not yet reached Serve's trackListener would leave the socket open
	// and break a subsequent rebind. Close it explicitly (an already-closed
	// listener returns an error we ignore).
	if ln != nil {
		_ = ln.Close()
	}
	s.started.Store(false)
	s.shuttingDwn.Store(false)
	return err
}

// Close stops the surface AND its background job workers. It is the terminal
// teardown for embedders; Shutdown (used by Server.Shutdown) is restartable and
// deliberately leaves the pool running.
func (s *Server) Close() error {
	if s.jobs != nil {
		s.jobs.close()
	}
	return nil
}

// ReloadUsers re-reads auth.json and invalidates sessions for users that
// changed or were removed (AUTH-051/OPS-042). Wired to SIGHUP by cmd/amqp-server.
func (s *Server) ReloadUsers() int {
	changed := s.users.reload()
	if len(changed) == 0 {
		return 0
	}
	set := make(map[string]struct{}, len(changed))
	for _, n := range changed {
		set[n] = struct{}{}
	}
	n := s.sessions.invalidateUsers(func(name string) bool {
		_, gone := set[name]
		return gone
	})
	s.sink.ManagementSessions(s.sessions.count())
	return n
}

// uptimeSeconds reports the broker uptime, or the surface uptime when no
// broker is attached (test construction).
func (s *Server) uptimeSeconds() int64 {
	start := s.startTime
	if s.opts.Server != nil && !s.opts.Server.StartTime.IsZero() {
		start = s.opts.Server.StartTime
	}
	d := time.Since(start)
	if d < 0 {
		return 0
	}
	return int64(d.Seconds())
}

var _ = sync.Mutex{}
var _ = atomic.Int64{}
