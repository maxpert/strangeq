package config

import (
	"errors"
	"net"
	"strconv"
	"strings"
)

type ClusterConfig struct {
	Local                                   bool     `json:"local"`
	ID                                      string   `json:"id"`
	NodeID                                  uint64   `json:"node_id"`
	PeerAddress                             string   `json:"peer_address"`
	PeerListen                              string   `json:"peer_listen"`
	Seeds                                   []string `json:"seeds"`
	Discovery                               []string `json:"discovery"`
	Shards                                  uint64   `json:"shards"`
	TokenFile                               string   `json:"token_file"`
	PeerCertFile                            string   `json:"peer_cert_file"`
	PeerKeyFile                             string   `json:"peer_key_file"`
	PeerCAFile                              string   `json:"peer_ca_file"`
	AllowLoopbackPlaintext                  bool     `json:"allow_loopback_plaintext"`
	ExperimentalRaftRangeSync               bool     `json:"experimental_raft_range_sync"`
	ExperimentalAckAndClaim                 bool     `json:"experimental_ack_and_claim"`
	ExperimentalAssignments                 bool     `json:"experimental_assignments"`
	ExperimentalReplicaBodies               bool     `json:"experimental_replica_bodies"`
	ExperimentalRaftJournalDualStream       bool     `json:"experimental_raft_journal_dual_stream"`
	ExperimentalJournalBodyCompression      bool     `json:"experimental_journal_body_compression"`
	ExperimentalJournalSmallBodyCompression bool     `json:"experimental_journal_small_body_compression"`
}

func (c ClusterConfig) Enabled() bool { return c.Local || c.ID != "" }

func (c *AMQPConfig) validateCluster() error {
	cfg := c.Cluster
	if !cfg.Enabled() {
		if cfg.NodeID != 0 || cfg.PeerAddress != "" || cfg.PeerListen != "" || len(cfg.Seeds) != 0 ||
			len(cfg.Discovery) != 0 ||
			cfg.Shards != 0 || cfg.TokenFile != "" || cfg.PeerCertFile != "" || cfg.PeerKeyFile != "" ||
			cfg.PeerCAFile != "" || cfg.AllowLoopbackPlaintext ||
			cfg.ExperimentalRaftRangeSync || cfg.ExperimentalRaftJournalDualStream || cfg.ExperimentalAckAndClaim || cfg.ExperimentalAssignments ||
			cfg.ExperimentalReplicaBodies || cfg.ExperimentalJournalBodyCompression || cfg.ExperimentalJournalSmallBodyCompression {
			return errors.New("cluster.id is required with cluster options")
		}
		return nil
	}
	if cfg.ExperimentalRaftJournalDualStream && !cfg.ExperimentalRaftRangeSync {
		return errors.New("experimental_raft_journal_dual_stream requires experimental_raft_range_sync")
	}
	if cfg.ExperimentalAssignments && cfg.ExperimentalAckAndClaim {
		return errors.New("experimental_assignments requires its own storage mode")
	}
	if cfg.ExperimentalReplicaBodies && !cfg.ExperimentalAssignments {
		return errors.New("experimental_replica_bodies requires experimental_assignments")
	}
	if cfg.ExperimentalJournalBodyCompression && !cfg.ExperimentalAssignments {
		return errors.New("experimental_journal_body_compression requires experimental_assignments")
	}
	if cfg.ExperimentalJournalSmallBodyCompression && !cfg.ExperimentalJournalBodyCompression {
		return errors.New("experimental_journal_small_body_compression requires experimental_journal_body_compression")
	}
	if cfg.Local {
		if len(cfg.Seeds) != 0 || len(cfg.Discovery) != 0 {
			return errors.New("local cluster cannot configure seeds or discovery")
		}
		if cfg.Shards > 1024 {
			return errors.New("local cluster permits at most 1024 shards")
		}
		if c.Storage.UnsafeRecovery {
			return errors.New("unsafe storage recovery is unavailable in cluster mode")
		}
		if cfg.PeerAddress == "" {
			if cfg.PeerListen != "" || cfg.TokenFile != "" || cfg.PeerCertFile != "" || cfg.PeerKeyFile != "" || cfg.PeerCAFile != "" || cfg.AllowLoopbackPlaintext {
				return errors.New("local peer activation requires peer_address with transport settings")
			}
			return nil
		}
	}
	if (!cfg.Local && cfg.NodeID == 0) || cfg.PeerAddress == "" || cfg.TokenFile == "" || cfg.Shards > 1024 {
		return errors.New("cluster requires node_id, peer_address, token_file, and at most 1024 shards")
	}
	if !cfg.Local && (len(cfg.Seeds) < 1 || len(cfg.Seeds) > 3) {
		return errors.New("cluster requires one to three original voters in seeds")
	}
	if c.Storage.UnsafeRecovery {
		return errors.New("unsafe storage recovery is unavailable in cluster mode")
	}
	if !validAdvertisedAddress(cfg.PeerAddress) {
		return errors.New("cluster peer_address must be host:port")
	}
	if cfg.PeerCertFile == "" || cfg.PeerKeyFile == "" {
		if !cfg.AllowLoopbackPlaintext || cfg.PeerCertFile != "" || cfg.PeerKeyFile != "" || cfg.PeerCAFile != "" {
			return errors.New("peer TLS requires both peer_cert_file and peer_key_file")
		}
		if !isLoopback(cfg.PeerAddress) || (cfg.PeerListen != "" && !isLoopback(cfg.PeerListen)) {
			return errors.New("plaintext cluster addresses require numeric loopback")
		}
	}
	for _, entry := range cfg.Seeds {
		_, addr, err := ParseClusterSeed(entry)
		if err != nil {
			return err
		}
		if cfg.PeerCertFile == "" && !isLoopback(addr) {
			return errors.New("plaintext cluster seeds require numeric loopback")
		}
	}
	for _, addr := range cfg.Discovery {
		if !validAdvertisedAddress(addr) {
			return errors.New("cluster discovery addresses must be host:port")
		}
		if cfg.PeerCertFile == "" && !isLoopback(addr) {
			return errors.New("plaintext cluster discovery requires numeric loopback")
		}
	}
	return nil
}

func isLoopback(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	return err == nil && net.ParseIP(host).IsLoopback()
}

func ParseClusterSeed(seed string) (uint64, string, error) {
	number, addr, found := strings.Cut(seed, "@")
	id, err := strconv.ParseUint(number, 10, 64)
	if !found || err != nil || id == 0 || !validAdvertisedAddress(addr) {
		return 0, "", errors.New("cluster seed must be a positive node ID followed by @host:port")
	}
	return id, addr, nil
}

func validAdvertisedAddress(addr string) bool {
	host, port, err := net.SplitHostPort(addr)
	if err != nil || host == "" || strings.ContainsAny(host, " /?#@\t\r\n") {
		return false
	}
	n, err := strconv.ParseUint(port, 10, 16)
	return err == nil && n != 0
}
