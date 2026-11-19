// Package clusterwire carries authenticated Raft replication and forwarded
// broker operations over a single peer listener.
package clusterwire

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
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
	readPath       = "/strangeq/peer/v1/read"
	waitPath       = "/strangeq/peer/v1/wait"
	membershipPath = "/strangeq/peer/v1/membership"

	defaultTimeout      = 5 * time.Second
	defaultMaxBodyBytes = 32 << 20
	MaxWaitQueueBytes   = 1024
	maxWaitBodyBytes    = 16 + (8 << 10)
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

// A command parts handler returns the consecutive, owned slices of one reply.
// The slices must remain immutable while the server signs and writes them.
type commandPartsHandler interface {
	CommandParts(context.Context, []byte) ([]byte, []byte, error)
}

type readHandler interface {
	ReadCommand(context.Context, []byte) ([]byte, error)
}

// A read parts handler owns its consecutive reply slices until writing ends.
type readPartsHandler interface {
	ReadCommandParts(context.Context, []byte) ([]byte, []byte, error)
}

type waitHandler interface {
	WaitCommand(context.Context, []byte) ([]byte, error)
}

type Config struct {
	Listen          string
	ClusterID       string
	Token           string // 32 random bytes encoded as unpadded base64url.
	TLS             *tls.Config
	AllowInsecure   bool // Enable cleartext on numeric loopback when TLS is nil.
	Timeout         time.Duration
	MaxBodyBytes    int64
	ProtocolVersion uint8 // Zero retains the original peer authentication domain.
}

func normalize(cfg Config) (Config, authenticator, error) {
	if cfg.ClusterID == "" || len(cfg.ClusterID) > 128 || strings.ContainsAny(cfg.ClusterID, "\t\r\n") {
		return cfg, authenticator{}, errors.New("invalid peer cluster ID")
	}
	key, err := base64.RawURLEncoding.Strict().DecodeString(cfg.Token)
	if err != nil || len(key) != 32 {
		return cfg, authenticator{}, errors.New("peer token must encode 32 bytes as unpadded base64url")
	}
	if cfg.ProtocolVersion != 0 {
		scope := hmac.New(sha256.New, key)
		_, _ = scope.Write([]byte("StrangeQ peer protocol"))
		_, _ = scope.Write([]byte{cfg.ProtocolVersion})
		key = scope.Sum(nil)
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
