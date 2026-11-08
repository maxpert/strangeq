package management

import (
	"testing"
	"time"

	"github.com/maxpert/amqp-go/config"
)

// TestManagementWriteTimeout_OutlivesRequestDeadline (spec §9 LOW-5): the
// listener WriteTimeout must never cap management.limits.request_timeout_seconds.
// It is max(10s, request_timeout + slack).
func TestManagementWriteTimeout_OutlivesRequestDeadline(t *testing.T) {
	cfg := config.DefaultConfig()
	// The default request timeout is 30s, longer than the historical 10s
	// listener write timeout.
	cfg.Management.Limits.RequestTimeoutSeconds = 30
	if got := managementWriteTimeout(cfg.Management); got <= 30*time.Second {
		t.Fatalf("WriteTimeout = %v must exceed the 30s request deadline", got)
	}

	cfg.Management.Limits.RequestTimeoutSeconds = 2
	if got := managementWriteTimeout(cfg.Management); got != 10*time.Second {
		t.Fatalf("a short request deadline keeps the 10s floor, got %v", got)
	}

	cfg.Management.Limits.RequestTimeoutSeconds = 0
	if got := managementWriteTimeout(cfg.Management); got != 10*time.Second {
		t.Fatalf("a zero request deadline keeps the 10s floor, got %v", got)
	}
}

// TestManagementStart_UsesRequestAwareWriteTimeout pins that Start wires the
// computed value onto the live listener.
func TestManagementStart_UsesRequestAwareWriteTimeout(t *testing.T) {
	e := newEnv(t, false, func(cfg *config.AMQPConfig) {
		cfg.Management.Port = freePort(t)
		cfg.Management.Limits.RequestTimeoutSeconds = 30
	})
	if err := e.mgmt.Start(); err != nil {
		t.Fatalf("management.Start: %v", err)
	}
	e.mgmt.mu.Lock()
	srv := e.mgmt.httpSrv
	e.mgmt.mu.Unlock()
	if srv == nil {
		t.Fatal("Start did not install an http.Server")
	}
	if srv.WriteTimeout <= 30*time.Second {
		t.Fatalf("listener WriteTimeout = %v, must exceed the 30s request deadline", srv.WriteTimeout)
	}
}
