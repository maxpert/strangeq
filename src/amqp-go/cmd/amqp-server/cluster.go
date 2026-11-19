package main

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/internal/cluster"
	"github.com/maxpert/amqp-go/internal/clusterqueue"
	"github.com/maxpert/amqp-go/internal/clusterraft"
	"github.com/maxpert/amqp-go/server"
)

func openCluster(cfg *config.AMQPConfig, bootstrap bool) (*cluster.Node, error) {
	c := cfg.Cluster
	if !c.Enabled() {
		if _, err := os.Stat(filepath.Join(cfg.Storage.Path, "cluster")); err == nil {
			return nil, errors.New("persistent cluster storage requires its matching cluster configuration")
		} else if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
		if bootstrap {
			return nil, errors.New("--initialize-cluster requires cluster configuration")
		}
		return nil, nil
	}
	if c.Local && bootstrap {
		return nil, errors.New("local storage initializes when the broker opens")
	}
	if c.Local {
		entries, err := os.ReadDir(cfg.Storage.Path)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
		for _, entry := range entries {
			if entry.Name() != "cluster" || !entry.IsDir() {
				return nil, errors.New("local storage root contains unknown or legacy data")
			}
		}
	}
	options, err := clusterOptions(cfg)
	if err != nil {
		return nil, err
	}
	if c.Local {
		return cluster.OpenLocal(options)
	}
	if bootstrap {
		return nil, cluster.Initialize(options)
	}
	return cluster.Open(options)
}

func clusterOptions(cfg *config.AMQPConfig) (cluster.Config, error) {
	c := cfg.Cluster
	var token []byte
	var err error
	if c.PeerAddress != "" {
		token, err = os.ReadFile(c.TokenFile)
		if err != nil {
			return cluster.Config{}, fmt.Errorf("read cluster token: %w", err)
		}
	}
	listen := c.PeerListen
	if listen == "" {
		listen = c.PeerAddress
	}
	tlsConfig, err := loadPeerTLS(c)
	if err != nil {
		return cluster.Config{}, err
	}
	seeds := make([]cluster.Peer, 0, len(c.Seeds))
	for _, p := range c.Seeds {
		id, addr, err := config.ParseClusterSeed(p)
		if err != nil {
			return cluster.Config{}, err
		}
		seeds = append(seeds, cluster.Peer{ID: id, Address: addr})
	}
	options := cluster.Config{
		ClusterID: c.ID, Peer: cluster.Peer{ID: c.NodeID, Address: c.PeerAddress}, Seeds: seeds,
		Discovery: c.Discovery,
		Listen:    listen, Directory: filepath.Join(cfg.Storage.Path, "cluster"), Token: strings.TrimSpace(string(token)),
		Shards: c.Shards, AllowInsecure: c.AllowLoopbackPlaintext, TLS: tlsConfig,
		ExperimentalRaftRangeSync:               c.ExperimentalRaftRangeSync,
		ExperimentalAckAndClaim:                 c.ExperimentalAckAndClaim,
		ExperimentalAssignments:                 c.ExperimentalAssignments,
		ExperimentalReplicaBodies:               c.ExperimentalReplicaBodies,
		ExperimentalRaftJournalDualStream:       c.ExperimentalRaftJournalDualStream,
		ExperimentalJournalBodyCompression:      c.ExperimentalJournalBodyCompression,
		ExperimentalJournalSmallBodyCompression: c.ExperimentalJournalSmallBodyCompression,
		NewMachine: func(uint64) clusterraft.Machine {
			if c.ExperimentalAssignments {
				return server.NewClusterAssignmentMachine()
			}
			return server.NewClusterMachine()
		},
		ExpiryCommand: func(now time.Time) []byte {
			request, _ := clusterqueue.EncodeCommand(clusterqueue.QueueCommand{Kind: clusterqueue.ExpireSessions, Deadline: now.UnixNano()})
			return request
		},
	}
	return options, nil
}

func loadPeerTLS(c config.ClusterConfig) (*tls.Config, error) {
	if c.PeerCertFile == "" {
		return nil, nil
	}
	key, err := tls.LoadX509KeyPair(c.PeerCertFile, c.PeerKeyFile)
	if err != nil {
		return nil, fmt.Errorf("load peer TLS key pair: %w", err)
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{key}}
	if c.PeerCAFile != "" {
		pem, err := os.ReadFile(c.PeerCAFile)
		if err != nil {
			return nil, fmt.Errorf("read peer TLS CA: %w", err)
		}
		cfg.RootCAs = x509.NewCertPool()
		if !cfg.RootCAs.AppendCertsFromPEM(pem) {
			return nil, errors.New("invalid peer TLS CA certificate")
		}
	}
	return cfg, nil
}

// reloadClusterPeers reads only peer settings; the running AMQP gateway and
// immutable local identity stay attached to the original node.
func reloadClusterPeers(node *cluster.Node, startup *config.AMQPConfig, configPath string, originalDirectory ...string) error {
	if node == nil || !startup.Cluster.Local {
		return nil
	}
	cfg, _, err := config.LoadConfig(configPath)
	if err != nil {
		return err
	}
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	if len(originalDirectory) > 0 {
		cwd = originalDirectory[0]
	}
	cfg.ResolvePaths(cwd)
	// Only peer settings are reloaded. Storage and protocol settings belong to
	// the running engine, so validate with the immutable startup settings.
	current := *startup
	current.Cluster.PeerAddress = cfg.Cluster.PeerAddress
	current.Cluster.PeerListen = cfg.Cluster.PeerListen
	current.Cluster.TokenFile = cfg.Cluster.TokenFile
	current.Cluster.PeerCertFile = cfg.Cluster.PeerCertFile
	current.Cluster.PeerKeyFile = cfg.Cluster.PeerKeyFile
	current.Cluster.PeerCAFile = cfg.Cluster.PeerCAFile
	current.Cluster.AllowLoopbackPlaintext = cfg.Cluster.AllowLoopbackPlaintext
	if err := current.Validate(); err != nil {
		return err
	}
	if current.Cluster.PeerAddress == "" {
		return nil
	}
	options, err := clusterOptions(&current)
	if err != nil {
		return err
	}
	return node.ActivatePeers(options)
}
