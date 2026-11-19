package management

import (
	"context"
	"errors"
	"net/http"
	"strconv"

	"github.com/maxpert/amqp-go/internal/clusteradmin"
)

func (s *Server) canControlCluster(p *Principal) bool {
	return s.opts.ClusterControl != nil && p != nil && p.Authenticated && !p.Unauthenticated && p.Has(capManageUsers)
}

func (s *Server) hCluster(w http.ResponseWriter, r *http.Request) {
	observer := s.opts.Cluster
	if s.opts.ClusterView != nil && r.URL.Query().Get("view") != "local" {
		observer = s.opts.ClusterView
	}
	status, err := observer.Observe(r.Context())
	if err != nil {
		s.writeAPIError(w, r, clusterAPIError(err))
		return
	}
	status.CanControl = s.canControlCluster(principalFrom(r))
	s.writeJSON(w, r, http.StatusOK, status)
}

func (s *Server) hClusterNodeRunning(running bool) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.canControlCluster(principalFrom(r)) {
			s.sink.ManagementCapabilityDenied(capManageUsers)
			s.writeAPIError(w, r, errRefused("cluster node control requires an authenticated administrator"))
			return
		}
		ci := clientFrom(r)
		if !ci.Loopback || ci.BehindProxy {
			s.writeAPIError(w, r, errRefused("cluster node control requires a direct loopback client"))
			return
		}
		id := pathValue(endpointLabel(r), r.URL.EscapedPath(), "id")
		// The controller receives only bounded canonical positive decimal IDs,
		// never shell fragments, aliases, signed numbers, or overflowed IDs.
		if !canonicalClusterNodeID(id) {
			s.writeAPIError(w, r, errBad("node ID must be a canonical positive decimal integer"))
			return
		}
		action := "stop"
		if running {
			action = "start"
		}
		// Record the request through the audit gate before changing the process.
		// The middleware records the final HTTP outcome.
		if ae := s.auditDestructive(r, "cluster_node", id, action+"_requested", http.StatusAccepted); ae != nil {
			s.writeAPIError(w, r, ae)
			return
		}
		if err := s.opts.ClusterControl.SetRunning(r.Context(), id, running); err != nil {
			s.writeAPIError(w, r, clusterAPIError(err))
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

func canonicalClusterNodeID(id string) bool {
	if len(id) == 0 || len(id) > 20 || id[0] < '1' || id[0] > '9' {
		return false
	}
	for i := 1; i < len(id); i++ {
		if id[i] < '0' || id[i] > '9' {
			return false
		}
	}
	n, err := strconv.ParseUint(id, 10, 64)
	return err == nil && strconv.FormatUint(n, 10) == id
}

func clusterAPIError(err error) *apiError {
	switch {
	case errors.Is(err, clusteradmin.ErrNodeNotFound):
		return errNotFoundMsg("unknown cluster node")
	case errors.Is(err, clusteradmin.ErrUseAnotherBroker):
		return apiErr(http.StatusConflict, errConflict, "Open another broker’s management URL to stop the broker serving this panel.")
	case errors.Is(err, clusteradmin.ErrStopBlocked):
		return apiErr(http.StatusConflict, errConflict, "Restart any stopped broker and wait for the Raft groups to become ready before stopping a node.")
	case errors.Is(err, clusteradmin.ErrConflict):
		return apiErr(http.StatusConflict, errConflict, "cluster node state change conflicts with its current state")
	case errors.Is(err, clusteradmin.ErrUnavailable), errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return apiErr(http.StatusServiceUnavailable, errUnavailable, "cluster inspection or control is unavailable", withHeader("Retry-After", "1"))
	default:
		return apiErr(http.StatusInternalServerError, errInternal, "cluster operation failed")
	}
}

func clusterControlAction(m *route) string {
	if m.method != http.MethodPost {
		return ""
	}
	switch m.pattern {
	case "/api/strangeq/cluster/nodes/{id}/start":
		return "start"
	case "/api/strangeq/cluster/nodes/{id}/stop":
		return "stop"
	default:
		return ""
	}
}
