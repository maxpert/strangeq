package clusteradmin

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

type testSupervisor struct {
	calls chan string
}

func (s *testSupervisor) Observe(context.Context) (Status, error) {
	return Status{Nodes: []Node{{ID: "1", Managed: true}, {ID: "2", Managed: true}}}, nil
}

func (s *testSupervisor) SetRunning(_ context.Context, id string, _ bool) error {
	s.calls <- id
	return ErrStopBlocked
}

func TestPrivateSupervisor(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "sq-ipc-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	path := filepath.Join(dir, "control.sock")
	s := &testSupervisor{calls: make(chan string, 2)}
	closeSocket, err := ListenSupervisor(t.Context(), path, s, s)
	if err != nil {
		t.Fatal(err)
	}
	defer closeSocket()

	info, err := os.Lstat(path)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("supervisor socket must be owner-private: %v, %v", info, err)
	}
	client, err := NewSupervisorClient(path, "1")
	if err != nil {
		t.Fatal(err)
	}
	status, err := client.Observe(t.Context())
	if err != nil || len(status.Nodes) != 2 || status.Nodes[0].Managed || !status.Nodes[1].Managed {
		t.Fatalf("unexpected node control scope: %+v, %v", status, err)
	}
	if err := client.SetRunning(t.Context(), "1", false); !errors.Is(err, ErrUseAnotherBroker) {
		t.Fatalf("self-stop: %v", err)
	}
	if err := client.SetRunning(t.Context(), "2", false); !errors.Is(err, ErrStopBlocked) {
		t.Fatalf("supervisor refusal was not preserved: %v", err)
	}
	if n := len(s.calls); n != 1 || <-s.calls != "2" {
		t.Fatalf("expected one forwarded action, got %d", n)
	}
	if err := os.Chmod(dir, 0755); err != nil {
		t.Fatal(err)
	}
	if _, err := NewSupervisorClient(path, "1"); err == nil {
		t.Fatal("accepted a socket in a shared directory")
	}
}
