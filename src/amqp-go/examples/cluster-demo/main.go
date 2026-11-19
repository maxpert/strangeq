// cluster-demo supervises three local brokers with embedded management.
package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/internal/clusteradmin"
	"golang.org/x/crypto/bcrypt"
)

const (
	clusterID  = "strangeq-demo"
	nodeCount  = 3
	shardCount = 4
	adminUser  = "demo"
)

func main() {
	binary := flag.String("broker", "", "path to an amqp-server binary from this checkout")
	flag.Parse()
	if *binary == "" {
		fmt.Fprintln(os.Stderr, "Use run.sh to build and start the demo, or pass -broker.")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, *binary); err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintln(os.Stderr, "Cluster demo:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, binary string) error {
	binary, err := filepath.Abs(binary)
	if err != nil {
		return err
	}
	dir, err := os.MkdirTemp("", "strangeq-cluster-demo-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)

	token, err := secret()
	if err != nil {
		return err
	}
	adminPassword, err := secret()
	if err != nil {
		return err
	}
	adminAuth, err := credentials(adminPassword)
	if err != nil {
		return err
	}
	tokenPath := filepath.Join(dir, "peer.token")
	if err := os.WriteFile(tokenPath, []byte(token), 0600); err != nil {
		return err
	}
	// The separate short path fits the Unix socket address limit even when
	// the temporary data directory has a long path.
	socketDir, err := os.MkdirTemp("/tmp", "sq-demo-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(socketDir)
	ports, err := availablePorts(3 * nodeCount)
	if err != nil {
		return err
	}
	seeds := make([]string, nodeCount)
	for i := 0; i < nodeCount; i++ {
		seeds[i] = fmt.Sprintf("%d@%s", i+1, address(ports[3*i+1]))
	}
	s := &supervisor{
		binary: binary, directory: dir, password: adminPassword,
		socket: filepath.Join(socketDir, "control.sock"),
		http: &http.Client{
			Timeout:       1200 * time.Millisecond,
			Transport:     &http.Transport{IdleConnTimeout: 20 * time.Second},
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
	}
	defer s.http.CloseIdleConnections()
	defer s.close()
	for i := 0; i < nodeCount; i++ {
		id := strconv.Itoa(i + 1)
		configPath := filepath.Join(dir, "node-"+id+".yaml")
		cfg := config.DefaultConfig()
		cfg.Network.Address, cfg.Network.Port = address(ports[3*i]), ports[3*i]
		cfg.Storage.Path = filepath.Join(dir, "node-"+id)
		cfg.Cluster = config.ClusterConfig{
			ID: clusterID, NodeID: uint64(i + 1), Seeds: seeds, Shards: shardCount,
			PeerAddress: address(ports[3*i+1]), TokenFile: tokenPath, AllowLoopbackPlaintext: true,
		}
		cfg.Management.Enabled = true
		cfg.Management.Port = ports[3*i+2]
		cfg.Management.Authentication, cfg.Management.Basic = config.ManagementAuthenticationBasic, adminAuth
		if err := cfg.Save(configPath); err != nil {
			return err
		}
		setupCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		cmd := exec.CommandContext(setupCtx, binary, "--config", configPath, "--initialize-cluster")
		cmd.Dir, cmd.Env = dir, brokerEnvironment()
		output, initErr := cmd.CombinedOutput()
		cancel()
		if initErr != nil {
			return fmt.Errorf("initialize node %s: %w\n%s", id, initErr, output)
		}
		s.nodes = append(s.nodes, &brokerNode{
			id: id, amqp: cfg.Network.Address, peer: cfg.Cluster.PeerAddress,
			managementURL: "http://" + address(cfg.Management.Port),
			configPath:    configPath, logPath: filepath.Join(dir, "node-"+id+".log"),
		})
	}

	stopSupervisor, err := clusteradmin.ListenSupervisor(ctx, s.socket, s, s)
	if err != nil {
		return err
	}
	defer stopSupervisor() // Drain supervisor calls before closing the brokers.
	for _, node := range s.nodes {
		if err := s.SetRunning(ctx, node.id, true); err != nil {
			return fmt.Errorf("start node %s: %w", node.id, err)
		}
	}

	fmt.Printf("Username: %s\nPassword: %s\n", adminUser, adminPassword)
	for _, node := range s.nodes {
		fmt.Printf("Node %s management: %s/\n", node.id, node.managementURL)
		fmt.Printf("Node %s AMQP: amqp://%s/\n", node.id, node.amqp)
	}
	fmt.Printf("Logs and storage for this run: %s\n", dir)
	fmt.Println("Open any broker's management URL. Use another broker's URL to stop the broker serving your tab.")
	fmt.Println("Press Ctrl-C to stop the demo and remove its temporary data.")
	<-ctx.Done()
	return nil
}

func secret() (string, error) {
	var raw [32]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw[:]), nil
}

func credentials(password string) (interfaces.ManagementBasicConfig, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	return interfaces.ManagementBasicConfig{Username: adminUser, PasswordHash: string(hash), Admin: true}, err
}

func address(port int) string { return net.JoinHostPort("127.0.0.1", strconv.Itoa(port)) }

func availablePorts(n int) ([]int, error) {
	var listeners []net.Listener
	defer func() {
		for _, l := range listeners {
			_ = l.Close()
		}
	}()
	ports := make([]int, 0, n)
	for range n {
		l, err := net.Listen("tcp4", address(0))
		if err != nil {
			return nil, err
		}
		listeners = append(listeners, l)
		ports = append(ports, l.Addr().(*net.TCPAddr).Port)
	}
	return ports, nil
}

// Keep this demo on loopback even when the invoking shell configures a broker
// through environment overrides.
func brokerEnvironment() []string {
	var env []string
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, "AMQP_") {
			env = append(env, entry)
		}
	}
	return env
}
