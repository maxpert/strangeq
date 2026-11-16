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
		if bootstrap {
			return nil, errors.New("--initialize-cluster requires cluster configuration")
		}
		return nil, nil
	}
	token, err := os.ReadFile(c.TokenFile)
	if err != nil {
		return nil, fmt.Errorf("read cluster token: %w", err)
	}
	listen := c.PeerListen
	if listen == "" {
		listen = c.PeerAddress
	}
	tlsConfig, err := loadPeerTLS(c)
	if err != nil {
		return nil, err
	}
	seeds := make([]cluster.Peer, 0, len(c.Seeds))
	for _, p := range c.Seeds {
		id, addr, err := config.ParseClusterSeed(p)
		if err != nil {
			return nil, err
		}
		seeds = append(seeds, cluster.Peer{ID: id, Address: addr})
	}
	options := cluster.Config{
		ClusterID: c.ID, Peer: cluster.Peer{ID: c.NodeID, Address: c.PeerAddress}, Seeds: seeds,
		Discovery: c.Discovery,
		Listen:    listen, Directory: filepath.Join(cfg.Storage.Path, "cluster"), Token: strings.TrimSpace(string(token)),
		Shards: c.Shards, AllowInsecure: c.AllowLoopbackPlaintext, TLS: tlsConfig,
		NewMachine: func(uint64) clusterraft.Machine { return server.NewClusterMachine() },
		ExpiryCommand: func(now time.Time) []byte {
			request, _ := clusterqueue.EncodeCommand(clusterqueue.QueueCommand{Kind: clusterqueue.ExpireSessions, Deadline: now.UnixNano()})
			return request
		},
	}
	if bootstrap {
		return nil, cluster.Initialize(options)
	}
	return cluster.Open(options)
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
