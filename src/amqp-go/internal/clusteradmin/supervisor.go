package clusteradmin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const supervisorTimeout = 5 * time.Second

// SupervisorClient calls the demo's process owner over a private Unix socket.
// The broker remains responsible for the web UI and its access controls.
type SupervisorClient struct {
	path   string
	nodeID string
}

type supervisorRequest struct {
	Action string `json:"action"`
	NodeID string `json:"node_id,omitempty"`
}

type supervisorReply struct {
	Status Status `json:"status"`
	Error  string `json:"error,omitempty"`
}

var supervisorErrors = map[string]error{
	"conflict": ErrConflict, "missing": ErrNodeNotFound,
	"stop_blocked": ErrStopBlocked, "unavailable": ErrUnavailable,
}

func NewSupervisorClient(path, nodeID string) (*SupervisorClient, error) {
	if err := privateSocketDirectory(path); err != nil {
		return nil, err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if info.Mode()&os.ModeSocket == 0 || info.Mode().Perm() != 0600 {
		return nil, errors.New("demo supervisor requires a private Unix socket")
	}
	return &SupervisorClient{path: path, nodeID: nodeID}, nil
}

func (c *SupervisorClient) Observe(ctx context.Context) (Status, error) {
	reply, err := c.call(ctx, supervisorRequest{Action: "observe"})
	if err != nil {
		return Status{}, err
	}
	for i := range reply.Status.Nodes {
		node := &reply.Status.Nodes[i]
		if node.ID == c.nodeID {
			node.Managed = false
			node.Detail = "This broker serves this panel. Open another broker's management URL to stop it."
		}
	}
	return reply.Status, nil
}

func (c *SupervisorClient) SetRunning(ctx context.Context, id string, running bool) error {
	action := "start"
	if !running {
		if id == c.nodeID {
			return ErrUseAnotherBroker
		}
		action = "stop"
	}
	_, err := c.call(ctx, supervisorRequest{Action: action, NodeID: id})
	return err
}

func (c *SupervisorClient) call(ctx context.Context, req supervisorRequest) (supervisorReply, error) {
	ctx, cancel := context.WithTimeout(ctx, supervisorTimeout)
	defer cancel()
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "unix", c.path)
	if err != nil {
		return supervisorReply{}, ErrUnavailable
	}
	defer conn.Close()
	deadline, _ := ctx.Deadline()
	_ = conn.SetDeadline(deadline)
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	if err := json.NewEncoder(conn).Encode(req); err != nil {
		return supervisorReply{}, ErrUnavailable
	}
	var reply supervisorReply
	if err := json.NewDecoder(io.LimitReader(conn, 2<<20)).Decode(&reply); err != nil {
		return supervisorReply{}, ErrUnavailable
	}
	if reply.Error != "" {
		if err := supervisorErrors[reply.Error]; err != nil {
			return supervisorReply{}, err
		}
		return supervisorReply{}, ErrUnavailable
	}
	return reply, nil
}

// ListenSupervisor serves only process supervision over an owner-private
// local socket. Closing waits for the accept loop and active calls.
func ListenSupervisor(ctx context.Context, path string, observer Observer, control Controller) (func(), error) {
	if observer == nil || control == nil {
		return nil, errors.New("demo supervisor requires observation and process control")
	}
	if err := privateSocketDirectory(path); err != nil {
		return nil, err
	}
	listener, err := net.Listen("unix", path)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(path, 0600); err != nil {
		_ = listener.Close()
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { _ = listener.Close() })
	go func() {
		defer close(done)
		slots := make(chan struct{}, 16)
		var workers sync.WaitGroup
		for {
			conn, err := listener.Accept()
			if err != nil {
				break
			}
			select {
			case slots <- struct{}{}:
				workers.Go(func() {
					defer func() { <-slots }()
					serveSupervisorCall(ctx, conn, observer, control)
				})
			default:
				_ = conn.Close()
			}
		}
		workers.Wait()
	}()
	return func() {
		cancel()
		_ = listener.Close()
		<-done
		stop()
	}, nil
}

func serveSupervisorCall(ctx context.Context, conn net.Conn, observer Observer, control Controller) {
	defer conn.Close()
	ctx, cancel := context.WithTimeout(ctx, supervisorTimeout)
	defer cancel()
	deadline, _ := ctx.Deadline()
	_ = conn.SetDeadline(deadline)
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	var req supervisorRequest
	if err := json.NewDecoder(io.LimitReader(conn, 1024)).Decode(&req); err != nil {
		return
	}
	var reply supervisorReply
	var err error
	switch req.Action {
	case "observe":
		reply.Status, err = observer.Observe(ctx)
	case "start", "stop":
		err = control.SetRunning(ctx, req.NodeID, req.Action == "start")
	default:
		err = ErrUnavailable
	}
	if err != nil {
		reply.Error = "unavailable"
		for code, target := range supervisorErrors {
			if errors.Is(err, target) {
				reply.Error = code
				break
			}
		}
	}
	_ = json.NewEncoder(conn).Encode(reply)
}

func privateSocketDirectory(path string) error {
	if !filepath.IsAbs(path) {
		return errors.New("demo supervisor socket path must be absolute")
	}
	info, err := os.Lstat(filepath.Dir(path))
	if err != nil {
		return fmt.Errorf("inspect demo supervisor socket directory: %w", err)
	}
	if !info.IsDir() || info.Mode().Perm() != 0700 {
		return errors.New("demo supervisor socket requires a private 0700 directory")
	}
	return nil
}
