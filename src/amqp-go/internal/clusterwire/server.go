package clusterwire

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type admission struct {
	slots chan struct{}
	bytes atomic.Int64
	limit int64
}

func newAdmission(parallel int, maxBody int64) *admission {
	return &admission{slots: make(chan struct{}, parallel), limit: 2*maxBody + (4 << 20)}
}

func (a *admission) reserve(size int64) bool {
	select {
	case a.slots <- struct{}{}:
	default:
		return false
	}
	if a.bytes.Add(size) > a.limit {
		a.release(size)
		return false
	}
	return true
}

func (a *admission) release(size int64) {
	a.bytes.Add(-size)
	<-a.slots
}

type Server struct {
	config  Config
	handler Handler
	auth    authenticator
	replay  replayCache
	server  *http.Server
	cancel  context.CancelFunc

	raftAdmission, commandAdmission, membershipAdmission *admission

	mu       sync.Mutex
	listener net.Listener
	closed   bool
	done     chan struct{}
	serveErr error
}

func NewServer(cfg Config, handler Handler) (*Server, error) {
	cfg, auth, err := normalize(cfg)
	if err != nil {
		return nil, err
	}
	if handler == nil {
		return nil, errors.New("missing peer handler")
	}
	cleartext := cfg.TLS == nil && cfg.AllowInsecure
	if !cleartext && (cfg.TLS == nil || (len(cfg.TLS.Certificates) == 0 && cfg.TLS.GetCertificate == nil && cfg.TLS.GetConfigForClient == nil)) {
		return nil, errors.New("peer listener requires a TLS server certificate")
	}
	if err := validateAddress(cfg.Listen, true, cleartext); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &Server{
		config: cfg, handler: handler, auth: auth, cancel: cancel, done: make(chan struct{}),
		raftAdmission: newAdmission(32, cfg.MaxBodyBytes), commandAdmission: newAdmission(32, cfg.MaxBodyBytes),
		membershipAdmission: newAdmission(8, cfg.MaxBodyBytes),
	}
	s.server = &http.Server{
		Handler:           http.HandlerFunc(s.handle),
		TLSConfig:         cfg.TLS,
		ReadHeaderTimeout: min(2*time.Second, cfg.Timeout),
		ReadTimeout:       cfg.Timeout,
		WriteTimeout:      2 * cfg.Timeout,
		IdleTimeout:       30 * time.Second,
		MaxHeaderBytes:    16 << 10,
		BaseContext:       func(net.Listener) context.Context { return ctx },
	}
	return s, nil
}

func (s *Server) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.listener != nil {
		return errors.New("peer listener already started or closed")
	}
	listener, err := net.Listen("tcp", s.config.Listen)
	if err != nil {
		return err
	}
	s.listener = listener
	go func() {
		defer close(s.done)
		if s.config.TLS == nil {
			s.serveErr = s.server.Serve(listener)
		} else {
			s.serveErr = s.server.ServeTLS(listener, "", "")
		}
	}()
	return nil
}

func (s *Server) Addr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener == nil {
		return nil
	}
	return s.listener.Addr()
}

func (s *Server) Close(ctx context.Context) error {
	s.mu.Lock()
	s.closed = true
	s.cancel()
	listener := s.listener
	s.mu.Unlock()
	if listener == nil {
		return nil
	}
	err := s.server.Shutdown(ctx)
	// Serve can race Shutdown before it registers the listening socket.
	_ = listener.Close()
	if err != nil {
		_ = s.server.Close()
		return err
	}
	select {
	case <-s.done:
		if !errors.Is(s.serveErr, http.ErrServerClosed) &&
			!errors.Is(s.serveErr, net.ErrClosed) {
			return s.serveErr
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Server) handle(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if req.URL.RawQuery != "" || req.URL.RawPath != "" {
		http.Error(w, "invalid peer path", http.StatusBadRequest)
		return
	}

	var quota *admission
	switch req.URL.Path {
	case raftPath:
		quota = s.raftAdmission
	case commandPath:
		quota = s.commandAdmission
	case membershipPath:
		quota = s.membershipAdmission
	default:
		http.NotFound(w, req)
		return
	}
	size := req.ContentLength
	if size < 0 {
		http.Error(w, "content length required", http.StatusLengthRequired)
		return
	}
	if size > s.config.MaxBodyBytes {
		http.Error(w, "peer payload too large", http.StatusRequestEntityTooLarge)
		return
	}
	auth, ok := s.auth.verifyRequest(req, &s.replay)
	if !ok {
		http.Error(w, "peer authorization failed", http.StatusUnauthorized)
		return
	}
	if !quota.reserve(size) {
		http.Error(w, "peer busy", http.StatusTooManyRequests)
		return
	}
	defer quota.release(size)
	body := make([]byte, size)
	if _, err := io.ReadFull(req.Body, body); err != nil || bodyDigest(body) != auth.digest {
		http.Error(w, "peer payload rejected", http.StatusBadRequest)
		return
	}
	ctx, cancel := context.WithTimeout(req.Context(), s.config.Timeout)
	defer cancel()
	var output []byte
	var err error
	switch req.URL.Path {
	case commandPath:
		output, err = s.handler.Command(ctx, body)
	case membershipPath:
		output, err = s.handler.Membership(ctx, body)
	case raftPath:
		group, messages, decodeErr := decodeBatch(body)
		if decodeErr != nil {
			http.Error(w, "invalid Raft batch", http.StatusBadRequest)
			return
		}
		err = s.handler.Raft(ctx, group, messages)
	}
	if err != nil {
		http.Error(w, "peer operation failed", http.StatusServiceUnavailable)
		return
	}
	if int64(len(output)) > s.config.MaxBodyBytes {
		http.Error(w, "peer response too large", http.StatusServiceUnavailable)
		return
	}
	s.auth.signResponse(w.Header(), req.URL.Path, auth, output)
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.Itoa(len(output)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(output)
}
