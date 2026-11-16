// Package clusterwire carries authenticated Raft replication and forwarded
// broker operations over a single peer listener.
package clusterwire

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"go.etcd.io/raft/v3/raftpb"
)

const (
	raftPath       = "/strangeq/peer/v1/raft"
	commandPath    = "/strangeq/peer/v1/command"
	membershipPath = "/strangeq/peer/v1/membership"

	defaultTimeout      = 5 * time.Second
	defaultMaxBodyBytes = 32 << 20
)

var (
	// ErrUnsent indicates the peer could not have executed the request. Other
	// transport errors leave the command outcome uncertain.
	ErrUnsent   = errors.New("peer request was not sent")
	ErrTooLarge = errors.New("cluster peer message exceeds its size limit")
)

type Handler interface {
	Raft(context.Context, uint64, []*raftpb.Message) error
	Command(context.Context, []byte) ([]byte, error)
	Membership(context.Context, []byte) ([]byte, error)
}

type Config struct {
	Listen        string
	ClusterID     string
	Token         string // 32 random bytes encoded as unpadded base64url.
	TLS           *tls.Config
	AllowInsecure bool // Enable cleartext on numeric loopback when TLS is nil.
	Timeout       time.Duration
	MaxBodyBytes  int64
}

func normalize(cfg Config) (Config, authenticator, error) {
	if cfg.ClusterID == "" || len(cfg.ClusterID) > 128 || strings.ContainsAny(cfg.ClusterID, "\t\r\n") {
		return cfg, authenticator{}, errors.New("invalid peer cluster ID")
	}
	key, err := base64.RawURLEncoding.Strict().DecodeString(cfg.Token)
	if err != nil || len(key) != 32 {
		return cfg, authenticator{}, errors.New("peer token must encode 32 bytes as unpadded base64url")
	}
	if cfg.Timeout < 0 || cfg.MaxBodyBytes < 0 || cfg.MaxBodyBytes > 1<<30 {
		return cfg, authenticator{}, errors.New("invalid peer timeout or body limit")
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = defaultTimeout
	}
	if cfg.MaxBodyBytes == 0 {
		cfg.MaxBodyBytes = defaultMaxBodyBytes
	}
	if cfg.TLS != nil {
		cfg.TLS = cfg.TLS.Clone()
		if cfg.TLS.InsecureSkipVerify {
			return cfg, authenticator{}, errors.New("peer TLS requires certificate verification")
		}
		cfg.TLS.MinVersion = max(tls.VersionTLS12, cfg.TLS.MinVersion)
		if cfg.TLS.MaxVersion != 0 && cfg.TLS.MaxVersion < cfg.TLS.MinVersion {
			return cfg, authenticator{}, errors.New("invalid peer TLS version interval")
		}
	}
	return cfg, authenticator{cluster: cfg.ClusterID, key: key}, nil
}

func validateAddress(address string, listener, cleartext bool) error {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("peer address must be host:port: %w", err)
	}
	numericPort, err := strconv.ParseUint(port, 10, 16)
	if err != nil || (!listener && (numericPort == 0 || host == "")) ||
		strings.ContainsAny(host, " /?#@\r\n\t") {
		return errors.New("invalid peer address")
	}
	if cleartext && !net.ParseIP(host).IsLoopback() {
		return errors.New("cleartext peer address must be numeric loopback")
	}
	return nil
}
