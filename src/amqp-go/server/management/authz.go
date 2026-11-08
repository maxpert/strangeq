package management

import (
	"crypto/subtle"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"strings"

	"github.com/maxpert/amqp-go/interfaces"
)

// clientInfo is the effective client of a request (SEC-112).
type clientInfo struct {
	SocketIP    string
	EffectiveIP string
	BehindProxy bool
	Loopback    bool
}

// resolveClient derives the effective client. Only when the socket peer is in
// management.trusted_proxies is the effective client taken from
// X-Forwarded-For (rightmost untrusted hop) or X-Real-IP (SEC-112).
func (s *Server) resolveClient(r *http.Request) clientInfo {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	host = strings.Trim(host, "[]")
	ci := clientInfo{SocketIP: host, EffectiveIP: host}

	if addr, err := netip.ParseAddr(host); err == nil && s.isTrustedProxy(addr) {
		ci.BehindProxy = true
		if eff := s.forwardedClient(r); eff != "" {
			ci.EffectiveIP = eff
		}
	}
	if addr, err := netip.ParseAddr(ci.EffectiveIP); err == nil {
		ci.Loopback = addr.Unmap().IsLoopback()
	}
	return ci
}

func (s *Server) isTrustedProxy(addr netip.Addr) bool {
	addr = addr.Unmap()
	for _, p := range s.trustedProxies {
		if p.Contains(addr) {
			return true
		}
	}
	return false
}

// forwardedClient walks X-Forwarded-For from the right and returns the first
// hop that is NOT a trusted proxy — the real client.
func (s *Server) forwardedClient(r *http.Request) string {
	xff := r.Header.Get("X-Forwarded-For")
	if xff == "" {
		if real := strings.TrimSpace(r.Header.Get("X-Real-IP")); real != "" {
			if ip, err := netip.ParseAddr(strings.Trim(real, "[]")); err == nil {
				return ip.Unmap().String()
			}
		}
		return ""
	}
	hops := strings.Split(xff, ",")
	for i := len(hops) - 1; i >= 0; i-- {
		h := strings.TrimSpace(hops[i])
		if h == "" {
			continue
		}
		ip, err := netip.ParseAddr(strings.Trim(h, "[]"))
		if err != nil {
			continue
		}
		if !s.isTrustedProxy(ip) {
			return ip.Unmap().String()
		}
	}
	return ""
}

// authMode returns the effective authentication mode ("" → inherit).
func (s *Server) authMode() string {
	m := s.mcfg.Authentication
	if m == "" {
		return "inherit"
	}
	return m
}

// authenticate resolves the request's principal from Basic credentials or the
// session cookie. A missing or invalid credential is 401 with a reason that
// does not distinguish unknown-user from wrong-password (AUTH-005/006).
func (s *Server) authenticate(r *http.Request) (*Principal, clientInfo, *apiError) {
	ci := s.resolveClient(r)

	// Session cookie first: it is the browser path and needs no password.
	if p, ok := s.principalFromCookie(r, ci); ok {
		return p, ci, nil
	}

	if user, pass, ok := r.BasicAuth(); ok {
		p, err := s.login(r, user, pass, ci)
		if err != nil {
			return nil, ci, err
		}
		return p, ci, nil
	}

	// No credential presented.
	if p := s.unauthenticatedPrincipal(r, ci); p != nil {
		return p, ci, nil
	}
	return nil, ci, errUnauth("authentication required")
}

// principalFromCookie resolves the SQ_SESSION cookie to a Principal.
func (s *Server) principalFromCookie(r *http.Request, ci clientInfo) (*Principal, bool) {
	c, err := r.Cookie(sessionCookieName)
	if err != nil || c.Value == "" {
		return nil, false
	}
	sess, ok := s.sessions.get(c.Value)
	if !ok {
		return nil, false
	}
	p := sess.principal
	p.ViaCookie = true
	p.SessionID = sess.id
	p.Authenticated = true
	return p, true
}

// unauthenticatedPrincipal returns the loopback-only guest identity when the
// configuration allows unauthenticated access, else nil (AUTH-041).
func (s *Server) unauthenticatedPrincipal(r *http.Request, ci clientInfo) *Principal {
	mode := s.authMode()
	switch mode {
	case "none":
		// Validated loopback-only at startup; still require the effective
		// client to be loopback in case of a trusted-proxy rewrite.
		if !ci.Loopback {
			return nil
		}
		return guestPrincipal()
	case "inherit":
		if s.brokerAuthEnabled() {
			return nil
		}
		if !s.mcfg.AllowUnauthenticated || !ci.Loopback {
			return nil
		}
		return guestPrincipal()
	default: // basic
		return nil
	}
}

func guestPrincipal() *Principal {
	return &Principal{
		Name:            "guest",
		Caps:            readTopologyOnly(),
		Authenticated:   false,
		Unauthenticated: true,
	}
}

// login validates a username/password pair against the configured backend for
// the request's effective client.
func (s *Server) login(r *http.Request, username, password string, ci clientInfo) (*Principal, *apiError) {
	mode := s.authMode()
	switch mode {
	case "basic":
		return s.loginBasic(username, password)
	case "none":
		// Basic credentials are not a thing in `none` mode; treat as
		// unauthenticated (still subject to the loopback rule).
		if p := s.unauthenticatedPrincipal(r, ci); p != nil {
			return p, nil
		}
		return nil, errUnauth("authentication required")
	default: // inherit
		return s.loginInherit(username, password, ci)
	}
}

func (s *Server) loginBasic(username, password string) (*Principal, *apiError) {
	m := s.mcfg.Basic
	if subtle.ConstantTimeCompare([]byte(username), []byte(m.Username)) != 1 ||
		!s.bcryptMatch(m.PasswordHash, password) {
		return nil, errUnauth("invalid credentials")
	}
	caps := readTopologyOnly()
	if m.Admin {
		caps = capsFromList(administratorCapabilities)
	}
	return &Principal{
		Name:          m.Username,
		Caps:          caps,
		Authenticated: true,
		// No broker *interfaces.User: the resource gate falls back to the
		// capability gate (an admin basic principal holds mutate_topology).
	}, nil
}

func (s *Server) loginInherit(username, password string, ci clientInfo) (*Principal, *apiError) {
	if s.opts.Authenticator == nil {
		return nil, errUnauth("authentication backend unavailable")
	}
	user, err := s.opts.Authenticator.Authenticate(username, password)
	if err != nil || user == nil {
		return nil, errUnauth("invalid credentials")
	}
	if user.LoopbackOnly && !ci.Loopback {
		return nil, errRefused("user is restricted to loopback connections")
	}
	if err := s.checkHostACL(ci); err != nil {
		return nil, err
	}
	return &Principal{
		Name:          user.Username,
		Tags:          append([]string(nil), user.Tags...),
		Caps:          capabilitiesFor(user.Tags, user.Username, s.adminUsers),
		User:          user,
		Authenticated: true,
	}, nil
}

// checkHostACL applies security.allowedhosts / blockedhosts to the effective
// client, exactly as the AMQP handshake does (AUTH-009).
func (s *Server) checkHostACL(ci clientInfo) *apiError {
	addr, err := netip.ParseAddr(ci.EffectiveIP)
	if err != nil {
		return errRefused("client address not permitted")
	}
	if !s.blockedHosts.Empty() && s.blockedHosts.Contains(addr) {
		return errRefused("client address blocked by policy")
	}
	if !s.allowedHosts.Empty() && !s.allowedHosts.Contains(addr) {
		return errRefused("client address not in the allowed list")
	}
	return nil
}

// failureIsRateLimited records a failed authentication for the effective
// client and reports whether the caller is now over budget (AUTH-007/SEC-050).
func (s *Server) failureIsRateLimited(ci clientInfo) bool {
	s.sink.ManagementAuthFailure("bad_credentials")
	if s.mcfg.Limits.MaxInFlight <= 0 {
		return false
	}
	return !s.authFailLimit.allow(ci.EffectiveIP)
}

func (s *Server) brokerAuthEnabled() bool {
	return s.opts.Config != nil && s.opts.Config.Security.AuthenticationEnabled
}

// capabilityGate enforces AUTH-010/023: every /api/* request needs at least
// read_topology. A principal with no tag holds nothing and is refused 403 on
// every request, reads included.
func (s *Server) capabilityGate(p *Principal) *apiError {
	if !p.Has(capReadTopology) {
		s.sink.ManagementCapabilityDenied(capReadTopology)
		return errRefused("principal holds no management capability (a management tag is required)")
	}
	return nil
}

// authorizationDisabledGate enforces AUTH-018: when the broker's authorization
// layer is disabled, a non-loopback effective client is refused regardless of
// the bind address.
func (s *Server) authorizationDisabledGate(ci clientInfo) *apiError {
	if !s.authzEnabled() && !ci.Loopback {
		return errRefused("broker authorization is disabled; non-loopback management clients are refused")
	}
	return nil
}

func (s *Server) authzEnabled() bool {
	return s.opts.Config != nil && s.opts.Config.Security.AuthorizationEnabled
}

// resourceReadGate enforces AUTH-011: a read needs at least one
// VHostPermissions entry for the requested vhost. Built-in principals
// (basic/none) carry a synthesized full permission for "/".
func (s *Server) resourceReadGate(p *Principal, vhost string) *apiError {
	if p.User == nil {
		return nil
	}
	p.User.RLock()
	defer p.User.RUnlock()
	for i := range p.User.VHostPermissions {
		if p.User.VHostPermissions[i].VHost == vhost {
			return nil
		}
	}
	return errRefused("access to vhost " + vhost + " refused for user " + p.Name)
}

// mutationGates enforces the two gates for a state-changing request: the
// capability gate (must hold read_topology at minimum, per AUTH-010) plus the
// resource gate below. It returns the first failing apiError.
func (s *Server) mutationGates(p *Principal, action interfaces.OperationAction, rtype interfaces.ResourceType, resource, vhost string) *apiError {
	if p.Unauthenticated {
		if !s.mcfg.AllowUnauthenticatedMutations {
			return errRefused("unauthenticated management mutations are disabled (management.allow_unauthenticated_mutations)")
		}
		return nil
	}
	if p.User != nil && s.opts.Authenticator != nil {
		if !p.Has(capMutate) && !p.Has(capManageUsers) && !p.Has(capCloseOthers) {
			s.sink.ManagementCapabilityDenied(capMutate)
			return errRefused("principal lacks the capability to mutate broker topology")
		}
		op := interfaces.Operation{Action: action, ResourceType: rtype, Resource: resource, VHost: vhost}
		if err := s.opts.Authenticator.Authorize(p.User, op); err != nil {
			return errRefused(mutationRefusal(action, rtype, resource))
		}
		return nil
	}
	// basic / non-broker-auth principals: capability based.
	if !p.Has(capMutate) {
		s.sink.ManagementCapabilityDenied(capMutate)
		return errRefused("principal lacks the capability to mutate broker topology")
	}
	return nil
}

// mutationRefusal names the action and the resource but never says whether the
// resource exists (AUTH-014/SEC-071).
func mutationRefusal(action interfaces.OperationAction, rtype interfaces.ResourceType, resource string) string {
	return fmt.Sprintf("access refused for %s on %s %q", action, rtype, resource)
}

// requireCapability returns 403 unless p holds cap.
func (s *Server) requireCapability(p *Principal, cap string) *apiError {
	if !p.Has(cap) {
		s.sink.ManagementCapabilityDenied(cap)
		return errRefused("principal lacks the " + cap + " capability")
	}
	return nil
}

func capsFromList(list []string) map[string]bool {
	out := make(map[string]bool, len(list))
	for _, c := range list {
		out[c] = true
	}
	return out
}

var _ = strings.TrimSpace
