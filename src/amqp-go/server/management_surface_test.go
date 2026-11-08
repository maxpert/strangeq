package server

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/config"
)

// fakeManagementSurface records the builder/shutdown lifecycle of the
// control-plane seam (spec mgmt-01 §4).
type fakeManagementSurface struct {
	started         atomic.Int64
	addr            atomic.Value // net.Addr
	shutdowns       atomic.Int64
	shutdownCtxDone atomic.Bool
}

func (f *fakeManagementSurface) Start() error {
	f.started.Add(1)
	f.addr.Store(&net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 15672})
	return nil
}

func (f *fakeManagementSurface) Addr() net.Addr {
	v, _ := f.addr.Load().(net.Addr)
	return v
}

func (f *fakeManagementSurface) Shutdown(ctx context.Context) error {
	f.shutdowns.Add(1)
	if ctx.Err() != nil {
		f.shutdownCtxDone.Store(true)
	}
	return nil
}

// TestManagementSurface_BuilderSeam: the builder STORES the surface but does
// not Start it; Server.Shutdown stops it exactly once.
func TestManagementSurface_BuilderSeam(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Storage.Path = t.TempDir()
	cfg.Server.LogLevel = "silent"

	surf := &fakeManagementSurface{}
	srv, err := NewServerBuilder().WithConfig(cfg).WithZapLogger("silent").WithManagement(surf).Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if srv.Management() != surf {
		t.Fatalf("Build must store the management surface")
	}
	if got := surf.started.Load(); got != 0 {
		t.Fatalf("Build must NOT start the surface (caller starts it), started=%d", got)
	}

	if err := surf.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if surf.Addr() == nil {
		t.Fatalf("Addr must be non-nil after a successful Start")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if got := surf.shutdowns.Load(); got != 1 {
		t.Fatalf("Server.Shutdown must stop the surface exactly once, got %d", got)
	}
}

// TestManagementSurface_DefaultIsAbsent: without WithManagement the Server has
// no surface, so the disabled control plane costs nothing (SEC-001).
func TestManagementSurface_DefaultIsAbsent(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Storage.Path = t.TempDir()
	cfg.Server.LogLevel = "silent"

	srv, err := NewServerBuilder().WithConfig(cfg).WithZapLogger("silent").Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if srv.Management() != nil {
		t.Fatalf("no surface must be attached by default")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown without a surface: %v", err)
	}
}
