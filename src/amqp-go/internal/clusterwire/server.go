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
	config     Config
	handler    Handler
	auth       authenticator
	replay     replayCache
	waitReplay replayCache
	server     *http.Server
	cancel     context.CancelFunc

	raftAdmission, commandAdmission, waitAdmission, membershipAdmission *admission
	raftBodies                                                          raftBodyPool

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
		waitAdmission:       newAdmission(256, maxWaitBodyBytes),
		membershipAdmission: newAdmission(8, cfg.MaxBodyBytes),
		raftBodies:          raftBodyPool{free: raftBodySlots},
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
	maxBody := s.config.MaxBodyBytes
	replay := &s.replay
	switch req.URL.Path {
	case raftPath:
		quota = s.raftAdmission
	case commandPath, readPath:
		quota = s.commandAdmission
	case waitPath:
		quota = s.waitAdmission
		maxBody = min(maxBody, maxWaitBodyBytes)
		replay = &s.waitReplay
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
	if size > maxBody {
		http.Error(w, "peer payload too large", http.StatusRequestEntityTooLarge)
		return
	}
	auth, ok := s.auth.verifyRequest(req, replay)
	if !ok {
		http.Error(w, "peer authorization failed", http.StatusUnauthorized)
		return
	}
	if !quota.reserve(size) {
		http.Error(w, "peer busy", http.StatusTooManyRequests)
		return
	}
	defer quota.release(size)
	var body []byte
	var borrowed bool
	if req.URL.Path == raftPath {
		body, borrowed = s.raftBodies.borrow(int(size))
	} else {
		body = make([]byte, size)
	}
	if borrowed {
		defer func() {
			if body != nil {
				s.raftBodies.release(body)
			}
		}()
	}
	if _, err := io.ReadFull(req.Body, body); err != nil || bodyDigest(body) != auth.digest {
		http.Error(w, "peer payload rejected", http.StatusBadRequest)
		return
	}
	ctx, cancel := context.WithTimeout(req.Context(), s.config.Timeout)
	defer cancel()
	var prefix, output []byte
	var err error
	switch req.URL.Path {
	case commandPath:
		if parts, ok := s.handler.(commandPartsHandler); ok {
			prefix, output, err = parts.CommandParts(ctx, body)
		} else {
			output, err = s.handler.Command(ctx, body)
		}
	case readPath:
		switch reader := s.handler.(type) {
		case readPartsHandler:
			prefix, output, err = reader.ReadCommandParts(ctx, body)
		case readHandler:
			output, err = reader.ReadCommand(ctx, body)
		default:
			http.NotFound(w, req)
			return
		}
	case waitPath:
		waiter, ok := s.handler.(waitHandler)
		if !ok {
			http.NotFound(w, req)
			return
		}
		output, err = waiter.WaitCommand(ctx, body)
	case membershipPath:
		output, err = s.handler.Membership(ctx, body)
	case raftPath:
		group, messages, decodeErr := decodeBatch(body)
		if borrowed {
			s.raftBodies.release(body)
			body = nil
		}
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
	prefixBytes, outputBytes := int64(len(prefix)), int64(len(output))
	if prefixBytes > maxBody || outputBytes > maxBody-prefixBytes {
		http.Error(w, "peer response too large", http.StatusServiceUnavailable)
		return
	}
	s.auth.signResponse(w.Header(), req.URL.Path, auth, prefix, output)
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(prefixBytes+outputBytes, 10))
	w.WriteHeader(http.StatusOK)
	if len(prefix) != 0 {
		n, err := w.Write(prefix)
		if err != nil || n != len(prefix) || len(output) == 0 {
			return
		}
	}
	_, _ = w.Write(output)
}
