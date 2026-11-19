package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/maxpert/amqp-go/internal/clusteradmin"
)

type supervisedProcess struct {
	cmd      *exec.Cmd
	done     chan struct{}
	stopping bool
	seen     bool
}

type brokerNode struct {
	id, amqp, peer, observeURL, configPath, logPath string
	process                                         *supervisedProcess
	detail                                          string
}

type supervisor struct {
	controlMu                   sync.Mutex
	mu                          sync.Mutex
	closed                      bool
	binary, directory, password string
	http                        *http.Client
	nodes                       []*brokerNode
}

func (s *supervisor) SetRunning(ctx context.Context, id string, running bool) error {
	s.controlMu.Lock()
	defer s.controlMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	// Refresh observations before stopping a broker. Serialize requests so
	// another action cannot replace a process between observation and stop.
	var status clusteradmin.Status
	if !running {
		var err error
		status, err = s.Observe(ctx)
		if err != nil {
			return err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return clusteradmin.ErrUnavailable
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	for _, node := range s.nodes {
		if node.id != id {
			continue
		}
		if running {
			return s.start(node)
		}
		p := node.process
		if p == nil || p.stopping {
			return clusteradmin.ErrConflict
		}
		for _, other := range s.nodes {
			if other.id != id && (other.process == nil || other.process.stopping) {
				return clusteradmin.ErrStopBlocked
			}
		}
		if !canStop(status, id) {
			return clusteradmin.ErrStopBlocked
		}
		if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
			return fmt.Errorf("%w: stop node %s: %v", clusteradmin.ErrUnavailable, id, err)
		}
		p.stopping = true
		return nil
	}
	return clusteradmin.ErrNodeNotFound
}

func canStop(status clusteradmin.Status, target string) bool {
	if len(status.Groups) != shardCount+1 {
		return false
	}
	ready := make(map[string]bool, len(status.Nodes))
	for _, node := range status.Nodes {
		ready[node.ID] = node.State == "running" && node.Ready != nil && *node.Ready
		if !ready[node.ID] {
			return false
		}
	}
	for _, group := range status.Groups {
		if len(group.Voters) != nodeCount || group.ActiveVoters == nil || *group.ActiveVoters != nodeCount {
			return false
		}
		remaining := 0
		for _, voter := range group.Voters {
			if voter != target && ready[voter] {
				remaining++
			}
		}
		if group.Quorum <= 0 || remaining < group.Quorum {
			return false
		}
	}
	return true
}

// start runs under mu. Only the process and config registered by the launcher
// can be started or stopped.
func (s *supervisor) start(node *brokerNode) error {
	if node.process != nil {
		return clusteradmin.ErrConflict
	}
	logFile, err := os.OpenFile(node.logPath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return fmt.Errorf("%w: open broker log: %v", clusteradmin.ErrUnavailable, err)
	}
	defer logFile.Close()
	cmd := exec.Command(s.binary, "--config", node.configPath)
	cmd.Dir, cmd.Env = s.directory, brokerEnvironment()
	cmd.Stdout, cmd.Stderr = logFile, logFile
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("%w: %v", clusteradmin.ErrUnavailable, err)
	}
	p := &supervisedProcess{cmd: cmd, done: make(chan struct{})}
	node.process, node.detail = p, ""
	go func() {
		err := cmd.Wait()
		s.mu.Lock()
		node.process = nil
		if err != nil && !p.stopping {
			node.detail = err.Error()
		}
		close(p.done)
		s.mu.Unlock()
	}()
	return nil
}

type observation struct {
	node    *brokerNode
	process *supervisedProcess
	report  clusteradmin.Status
}

func (s *supervisor) Observe(ctx context.Context) (clusteradmin.Status, error) {
	s.mu.Lock()
	samples := make([]observation, len(s.nodes))
	for i, node := range s.nodes {
		samples[i] = observation{node: node, process: node.process}
	}
	s.mu.Unlock()

	var wg sync.WaitGroup
	for i := range samples {
		sample := &samples[i]
		if sample.process != nil {
			wg.Go(func() { sample.report = s.fetch(ctx, sample.node) })
		}
	}
	wg.Wait()
	if err := ctx.Err(); err != nil {
		return clusteradmin.Status{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	status := clusteradmin.Status{
		ClusterID: clusterID, Observer: "Demo supervisor", ObservedAt: time.Now().UTC(),
		Nodes: make([]clusteradmin.Node, 0, len(samples)), Groups: []clusteradmin.Group{},
	}
	groups := make(map[string]clusteradmin.Group)
	for _, sample := range samples {
		node := sample.node
		p := node.process
		row := clusteradmin.Node{ID: node.id, Address: node.peer, Managed: true, Detail: node.detail}
		switch {
		case p == nil:
			row.State = "stopped"
		case p.stopping:
			row.State = "stopping"
		case sample.process == p && sample.report.Observer == node.id:
			for _, seen := range sample.report.Nodes {
				if seen.ID == node.id {
					row.Ready = seen.Ready
					break
				}
			}
			p.seen = true
			row.State = "running"
			for _, group := range sample.report.Groups {
				current, exists := groups[group.ID]
				if !exists || prefer(group, current) {
					groups[group.ID] = group
				}
			}
		case p.seen:
			row.State, row.Detail = "unknown", "Broker process is running. Status request failed."
		default:
			row.State = "starting"
		}
		status.Nodes = append(status.Nodes, row)
	}
	for _, group := range groups {
		status.Groups = append(status.Groups, group)
	}
	slices.SortFunc(status.Groups, func(a, b clusteradmin.Group) int {
		if a.Catalog && !b.Catalog {
			return -1
		}
		if b.Catalog && !a.Catalog {
			return 1
		}
		return compareDecimal(a.ID, b.ID)
	})
	return status, nil
}

func prefer(candidate, current clusteradmin.Group) bool {
	if order := compareDecimal(candidate.Term, current.Term); order != 0 {
		return order > 0
	}
	if order := compareDecimal(candidate.ConfigIndex, current.ConfigIndex); order != 0 {
		return order > 0
	}
	if (candidate.ActiveVoters != nil) != (current.ActiveVoters != nil) {
		return candidate.ActiveVoters != nil
	}
	return compareDecimal(candidate.Commit, current.Commit) > 0
}

func compareDecimal(a, b string) int {
	aa, _ := strconv.ParseUint(a, 10, 64)
	bb, _ := strconv.ParseUint(b, 10, 64)
	if aa < bb {
		return -1
	}
	if aa > bb {
		return 1
	}
	return 0
}

func (s *supervisor) fetch(ctx context.Context, node *brokerNode) clusteradmin.Status {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, node.observeURL, nil)
	if err != nil {
		return clusteradmin.Status{}
	}
	req.SetBasicAuth(observerUser, s.password)
	resp, err := s.http.Do(req)
	if err != nil {
		return clusteradmin.Status{}
	}
	defer resp.Body.Close()
	var observed clusteradmin.Status
	if resp.StatusCode != http.StatusOK ||
		json.NewDecoder(io.LimitReader(resp.Body, 2<<20)).Decode(&observed) != nil ||
		observed.ClusterID != clusterID || observed.Observer != node.id {
		return clusteradmin.Status{}
	}
	return observed
}

func (s *supervisor) close() {
	s.mu.Lock()
	s.closed = true
	var children []*supervisedProcess
	for _, node := range s.nodes {
		if p := node.process; p != nil {
			_ = p.cmd.Process.Signal(syscall.SIGTERM)
			p.stopping = true
			children = append(children, p)
		}
	}
	s.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	for _, p := range children {
		select {
		case <-p.done:
		case <-ctx.Done():
			_ = p.cmd.Process.Kill()
			<-p.done
		}
	}
}
