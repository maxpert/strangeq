package management

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"net/http"
	"sort"
	"sync"
	"time"
)

const (
	sessionCookieName = "SQ_SESSION"
	csrfCookieName    = "SQ_CSRF"
)

// session is one server-side management session. The cookie carries only an
// opaque id; the principal, its capabilities, and the CSRF token live here
// (AUTH-030/031).
type session struct {
	id        string
	principal *Principal
	csrf      string
	created   time.Time
	lastSeen  time.Time
}

// sessionStore is a bounded, idle-expiring, server-side session table
// (AUTH-031/032, SEC-040/042). The clock is injectable so expiry is tested
// deterministically without sleeping.
type sessionStore struct {
	mu       sync.Mutex
	byID     map[string]*session
	order    []string // ids in creation order, for oldest-first eviction
	max      int
	idle     time.Duration
	now      func() time.Time
	evicted  int64
	rejected int64
}

func newSessionStore(max int, idle time.Duration) *sessionStore {
	if max < 1 {
		max = 1024
	}
	if idle <= 0 {
		idle = 8 * time.Hour
	}
	return &sessionStore{
		byID: make(map[string]*session),
		max:  max,
		idle: idle,
		now:  time.Now,
	}
}

// create mints a new session for p. Over the bound, the oldest session is
// evicted; creation never fails (AUTH-032: cap, never refuse a login).
func (s *sessionStore) create(p *Principal) (*session, error) {
	id, err := randomToken(16) // 128-bit opaque id
	if err != nil {
		return nil, err
	}
	csrf, err := randomToken(32) // 256-bit → 43-char base64url token
	if err != nil {
		return nil, err
	}
	now := s.now()
	sess := &session{id: id, principal: p, csrf: csrf, created: now, lastSeen: now}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.expireLocked(now)
	for len(s.order) >= s.max {
		oldest := s.order[0]
		s.order = s.order[1:]
		delete(s.byID, oldest)
		s.evicted++
	}
	s.byID[id] = sess
	s.order = append(s.order, id)
	p.SessionID = id
	return sess, nil
}

// get returns the session for id, refreshing its idle timer. Expired sessions
// are dropped and reported absent.
func (s *sessionStore) get(id string) (*session, bool) {
	if id == "" {
		return nil, false
	}
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.byID[id]
	if !ok {
		return nil, false
	}
	if now.Sub(sess.lastSeen) > s.idle {
		delete(s.byID, id)
		s.removeOrderLocked(id)
		s.expireCountLocked()
		return nil, false
	}
	sess.lastSeen = now
	return sess, true
}

// delete removes a session (logout).
func (s *sessionStore) delete(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.byID[id]; !ok {
		return
	}
	delete(s.byID, id)
	s.removeOrderLocked(id)
}

// invalidateUsers drops every session whose principal name satisfies pred.
// Used by the SIGHUP auth.json reload (AUTH-051/OPS-042) so a changed or
// removed user cannot keep an open session.
func (s *sessionStore) invalidateUsers(pred func(name string) bool) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for id, sess := range s.byID {
		if pred(sess.principal.Name) {
			delete(s.byID, id)
			s.removeOrderLocked(id)
			n++
		}
	}
	return n
}

func (s *sessionStore) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.expireLocked(s.now())
	return len(s.byID)
}

func (s *sessionStore) evictedTotal() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.evicted
}

func (s *sessionStore) expireLocked(now time.Time) {
	for id, sess := range s.byID {
		if now.Sub(sess.lastSeen) > s.idle {
			delete(s.byID, id)
			s.removeOrderLocked(id)
			s.expireCountLocked()
		}
	}
}

func (s *sessionStore) expireCountLocked() { s.rejected++ }

func (s *sessionStore) removeOrderLocked(id string) {
	for i, v := range s.order {
		if v == id {
			s.order = append(s.order[:i], s.order[i+1:]...)
			return
		}
	}
}

// setSessionCookies writes the SQ_SESSION (HttpOnly) and SQ_CSRF
// (non-HttpOnly) cookies (SEC-020/044). Secure is set under TLS.
func (s *Server) setSessionCookies(w http.ResponseWriter, sess *session) {
	secure := s.tlsEnabled()
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    sess.id,
		Path:     "/",
		HttpOnly: true,
		Secure:   secure,
		SameSite: http.SameSiteStrictMode,
	})
	http.SetCookie(w, &http.Cookie{
		Name:     csrfCookieName,
		Value:    sess.csrf,
		Path:     "/",
		HttpOnly: false, // the UI must read it to echo X-CSRF-Token
		Secure:   secure,
		SameSite: http.SameSiteStrictMode,
	})
}

// clearSessionCookies expires both cookies (logout).
func (s *Server) clearSessionCookies(w http.ResponseWriter) {
	secure := s.tlsEnabled()
	for _, name := range []string{sessionCookieName, csrfCookieName} {
		http.SetCookie(w, &http.Cookie{
			Name:     name,
			Value:    "",
			Path:     "/",
			MaxAge:   -1,
			HttpOnly: name == sessionCookieName,
			Secure:   secure,
			SameSite: http.SameSiteStrictMode,
		})
	}
}

// randomToken returns n cryptographically random bytes base64url-encoded
// without padding (43 chars for 32 bytes).
func randomToken(n int) (string, error) {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

func randomHex(n int) (string, error) {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

var _ = sort.Strings
