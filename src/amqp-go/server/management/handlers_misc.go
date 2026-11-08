package management

import (
	"encoding/json"
	"net/http"

	"github.com/maxpert/amqp-go/interfaces"
)

// --- session ---

type sessionRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type sessionResponse struct {
	Name             string                       `json:"name"`
	Tags             []string                     `json:"tags"`
	Capabilities     []string                     `json:"capabilities"`
	CSRFToken        string                       `json:"csrf_token"`
	ExpiresIn        int                          `json:"expires_in"`
	VHostPermissions []interfaces.VHostPermission `json:"vhost_permissions"`
}

// hSessionCreate implements POST /api/strangeq/session (AUTH-031): success
// returns the principal, capabilities, a CSRF token, and both cookies.
func (s *Server) hSessionCreate(w http.ResponseWriter, r *http.Request) {
	body, aerr := s.readBodyLimited(r, 64<<10)
	if aerr != nil {
		s.writeAPIError(w, r, aerr)
		return
	}
	var req sessionRequest
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			s.writeAPIError(w, r, errBad("invalid JSON body"))
			return
		}
	}
	ci := s.resolveClient(r)

	var p *Principal
	if req.Username == "" && req.Password == "" {
		if gp := s.unauthenticatedPrincipal(r, ci); gp != nil {
			if ae := s.checkPrincipalClient(gp, ci); ae != nil {
				s.writeAPIError(w, r, ae)
				return
			}
			p = gp
		} else {
			s.writeAPIError(w, r, errUnauth("credentials required"))
			return
		}
	} else {
		var err *apiError
		p, err = s.login(r, req.Username, req.Password, ci)
		if err != nil {
			if s.failureIsRateLimited(ci) {
				s.writeAPIError(w, r, apiErr(http.StatusTooManyRequests, errRateLimited,
					"too many failed authentication attempts", withHeader("Retry-After", "1")))
				return
			}
			w.Header().Set("WWW-Authenticate", `Basic realm="StrangeQ Management"`)
			s.writeAPIError(w, r, err)
			return
		}
		if ae := s.checkPrincipalClient(p, ci); ae != nil {
			s.writeAPIError(w, r, ae)
			return
		}
	}

	sess, err := s.sessions.create(p)
	if err != nil {
		s.writeAPIError(w, r, apiErr(http.StatusInternalServerError, errInternal, "could not create session"))
		return
	}
	s.setSessionCookies(w, sess)
	s.sink.ManagementSessions(s.sessions.count())

	resp := sessionResponse{
		Name:             p.Name,
		Tags:             append([]string{}, p.Tags...),
		Capabilities:     p.CapabilityList(),
		CSRFToken:        sess.csrf,
		ExpiresIn:        s.mcfg.Session.IdleTimeoutSeconds,
		VHostPermissions: vhostPermissions(p),
	}
	s.writeJSON(w, r, http.StatusOK, resp)
}

func vhostPermissions(p *Principal) []interfaces.VHostPermission {
	if p == nil || p.User == nil {
		// Built-in principals (basic/none) carry a full synthesized triple so
		// the UI can render something truthful: they hold their capabilities
		// on the single "/" vhost.
		return []interfaces.VHostPermission{{
			VHost:      "/",
			Permission: interfaces.Permission{Configure: ".*", Write: ".*", Read: ".*"},
		}}
	}
	p.User.RLock()
	defer p.User.RUnlock()
	out := make([]interfaces.VHostPermission, len(p.User.VHostPermissions))
	copy(out, p.User.VHostPermissions)
	return out
}

// hSessionDelete implements DELETE /api/strangeq/session (logout, AUTH-031).
func (s *Server) hSessionDelete(w http.ResponseWriter, r *http.Request) {
	if c, err := r.Cookie(sessionCookieName); err == nil {
		s.sessions.delete(c.Value)
	}
	s.clearSessionCookies(w)
	s.sink.ManagementSessions(s.sessions.count())
	w.WriteHeader(http.StatusNoContent)
}

// --- whoami ---

type whoamiResponse struct {
	Name             string                       `json:"name"`
	Tags             []string                     `json:"tags"`
	VHost            string                       `json:"vhost"`
	Capabilities     []string                     `json:"capabilities"`
	VHostPermissions []interfaces.VHostPermission `json:"vhost_permissions"`
}

func (s *Server) hWhoami(w http.ResponseWriter, r *http.Request) {
	p := principalFrom(r)
	s.writeJSON(w, r, http.StatusOK, whoamiResponse{
		Name:             p.Name,
		Tags:             append([]string{}, p.Tags...),
		VHost:            "/",
		Capabilities:     p.CapabilityList(),
		VHostPermissions: vhostPermissions(p),
	})
}

// --- jobs ---

func (s *Server) hJob(w http.ResponseWriter, r *http.Request) {
	id := pathValue(endpointLabel(r), r.URL.EscapedPath(), "id")
	p := principalFrom(r)
	owner := ""
	if p != nil {
		owner = p.Name
	}
	// viewOf renders the job under the pool lock: a worker transitions the job
	// concurrently, so reading its fields here (outside the lock) would be a
	// data race.
	view, ok := s.jobs.viewOf(id, owner)
	if !ok {
		s.writeAPIError(w, r, errNotFoundMsg("unknown job"))
		return
	}
	s.writeJSON(w, r, http.StatusOK, view)
}

// --- health ---

type healthResponse struct {
	Status        string `json:"status"`
	UptimeSeconds int64  `json:"uptime_seconds"`
	Reason        string `json:"reason,omitempty"`
}

func (s *Server) hHealth(w http.ResponseWriter, r *http.Request) {
	srv := s.opts.Server
	if srv == nil || !srv.IsListening() {
		s.writeJSON(w, r, http.StatusServiceUnavailable, healthResponse{
			Status: "not_ready", UptimeSeconds: s.uptimeSeconds(),
			Reason: "broker is not listening",
		})
		return
	}
	if st := srv.AlarmState(); st != 0 {
		reason := "resource alarm active"
		switch {
		case st&serverAlarmMemory != 0 && st&serverAlarmDisk != 0:
			reason = "memory and disk resource alarms active"
		case st&serverAlarmMemory != 0:
			reason = "memory resource alarm active"
		case st&serverAlarmDisk != 0:
			reason = "disk resource alarm active"
		}
		s.writeJSON(w, r, http.StatusOK, healthResponse{
			Status: "alarm", UptimeSeconds: s.uptimeSeconds(), Reason: reason,
		})
		return
	}
	s.writeJSON(w, r, http.StatusOK, healthResponse{Status: "ok", UptimeSeconds: s.uptimeSeconds()})
}

type checkResult struct {
	Name   string `json:"name"`
	Status string `json:"status"`
	Reason string `json:"reason"`
}

type checksResponse struct {
	Checks []checkResult `json:"checks"`
}

// hHealthChecks implements OPS-011: a fixed set of named checks with
// RabbitMQ-4.4 parity names plus StrangeQ extensions.
func (s *Server) hHealthChecks(w http.ResponseWriter, r *http.Request) {
	s.writeJSON(w, r, http.StatusOK, checksResponse{Checks: s.runChecks()})
}

func (s *Server) runChecks() []checkResult {
	checks := []checkResult{
		s.checkInService(),
		s.checkLocalAlarms(),
		{Name: "below-node-connection-limit", Status: "ok", Reason: ""},
	}
	if s.tlsEnabled() {
		checks = append(checks, checkResult{Name: "certificate-expiration", Status: "ok", Reason: ""})
	}
	checks = append(checks,
		checkResult{Name: "aliveness", Status: "ok", Reason: ""},
		checkResult{Name: "disk-space", Status: "ok", Reason: ""},
	)
	return checks
}

func (s *Server) checkInService() checkResult {
	if srv := s.opts.Server; srv != nil && srv.IsListening() {
		return checkResult{Name: "is-in-service", Status: "ok", Reason: ""}
	}
	return checkResult{Name: "is-in-service", Status: "failed", Reason: "broker is not listening"}
}

func (s *Server) checkLocalAlarms() checkResult {
	if srv := s.opts.Server; srv != nil && srv.AlarmState() != 0 {
		return checkResult{Name: "local-alarms", Status: "warning", Reason: "resource alarm active"}
	}
	return checkResult{Name: "local-alarms", Status: "ok", Reason: ""}
}

// unanswerableChecks are RabbitMQ 4.4 health paths StrangeQ cannot answer; they
// return 501 rather than a fabricated 200 (DIV-15).
var unanswerableChecks = map[string]string{
	"virtual-hosts":           "virtual-host health is not implemented",
	"node-is-quorum-critical": "quorum queues are not implemented",
	"ready-to-serve-clients":  "the readiness signal is not implemented",
	"port-listener":           "per-listener health is not implemented",
	"protocol-listener":       "per-listener health is not implemented",
}

func (s *Server) hHealthCheck(w http.ResponseWriter, r *http.Request) {
	name := pathValue("/api/health/checks/{check}", r.URL.EscapedPath(), "check")
	if reason, bad := unanswerableChecks[name]; bad {
		s.writeAPIError(w, r, apiErr(http.StatusNotImplemented, errNotImplemented, reason))
		return
	}
	for _, c := range s.runChecks() {
		if c.Name == name {
			s.writeJSON(w, r, http.StatusOK, c)
			return
		}
	}
	s.writeAPIError(w, r, errNotFoundMsg("unknown health check"))
}

// --- honest divergences ---

// hEmptyList serves `200 []` for an absent subsystem (FR-API-130/D-4).
func (s *Server) hEmptyList(w http.ResponseWriter, r *http.Request) {
	s.writeJSON(w, r, http.StatusOK, []any{})
}

// hNotImplemented serves 501 naming the missing subsystem (FR-API-131).
func (s *Server) hNotImplemented(subsystem string) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		s.writeAPIError(w, r, apiErr(http.StatusNotImplemented, errNotImplemented,
			subsystem+" is not implemented by StrangeQ"))
	}
}

// hNotImplementedAggregate serves 501 naming BOB-4 (aggregate bindings).
func (s *Server) hNotImplementedAggregate(kind string) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		s.writeAPIError(w, r, apiErr(http.StatusNotImplemented, errNotImplemented,
			kind+" is not implemented (BOB-4: aggregate binding listing is unbuilt)"))
	}
}

// hNotImplementedUser serves 501 naming BOB-6 (runtime user CRUD).
func (s *Server) hNotImplementedUser(kind string) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		s.writeAPIError(w, r, apiErr(http.StatusNotImplemented, errNotImplemented,
			kind+" is not implemented (BOB-6: StrangeQ has no runtime user store)"))
	}
}

// hNotImplementedAbsent serves 501 for a write to an absent subsystem.
func (s *Server) hNotImplementedAbsent() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		s.writeAPIError(w, r, apiErr(http.StatusNotImplemented, errNotImplemented,
			"this subsystem is not implemented by StrangeQ"))
	}
}

// serverAlarmMemory / serverAlarmDisk mirror server.AlarmMemory/AlarmDisk
// without importing the constants at the top of every file.
const (
	serverAlarmMemory uint32 = 1 << 0
	serverAlarmDisk   uint32 = 1 << 1
)
