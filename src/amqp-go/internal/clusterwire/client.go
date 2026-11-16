package clusterwire

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"sync/atomic"
	"time"

	"go.etcd.io/raft/v3/raftpb"
)

type Client struct {
	config    Config
	auth      authenticator
	rpc, raft *http.Client
}

func NewClient(cfg Config) (*Client, error) {
	cfg, auth, err := normalize(cfg)
	if err != nil {
		return nil, err
	}
	return &Client{config: cfg, auth: auth, rpc: newHTTPClient(cfg), raft: newHTTPClient(cfg)}, nil
}

func newHTTPClient(cfg Config) *http.Client {
	connectTimeout := min(500*time.Millisecond, cfg.Timeout)
	dialer := &net.Dialer{Timeout: connectTimeout, KeepAlive: 30 * time.Second}
	return &http.Client{
		Transport: &http.Transport{
			DialContext:            dialer.DialContext,
			TLSClientConfig:        cfg.TLS.Clone(),
			TLSHandshakeTimeout:    connectTimeout,
			ForceAttemptHTTP2:      true,
			IdleConnTimeout:        30 * time.Second,
			MaxIdleConns:           128,
			MaxIdleConnsPerHost:    8,
			MaxConnsPerHost:        48,
			MaxResponseHeaderBytes: 16 << 10,
			DisableCompression:     true,
		},
		Timeout: cfg.Timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

func (c *Client) Raft(ctx context.Context, address string, group uint64, messages []*raftpb.Message) error {
	body, err := encodeBatch(group, messages, c.config.MaxBodyBytes)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrUnsent, err)
	}
	_, err = c.request(ctx, c.raft, address, raftPath, body)
	return err
}

func (c *Client) Command(ctx context.Context, address string, body []byte) ([]byte, error) {
	return c.request(ctx, c.rpc, address, commandPath, body)
}

func (c *Client) Membership(ctx context.Context, address string, body []byte) ([]byte, error) {
	return c.request(ctx, c.rpc, address, membershipPath, body)
}

func (c *Client) Close() {
	c.rpc.CloseIdleConnections()
	c.raft.CloseIdleConnections()
}

func (c *Client) request(ctx context.Context, client *http.Client, address, path string, data []byte) ([]byte, error) {
	cleartext := c.config.AllowInsecure && c.config.TLS == nil
	if err := validateAddress(address, false, cleartext); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrUnsent, err)
	}
	if int64(len(data)) > c.config.MaxBodyBytes {
		return nil, fmt.Errorf("%w: %w", ErrUnsent, ErrTooLarge)
	}
	scheme := "https"
	if cleartext {
		scheme = "http"
	}
	target := url.URL{Scheme: scheme, Host: address, Path: path}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target.String(), bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrUnsent, err)
	}
	// Leave replay decisions to the shard caller, which knows command identity.
	req.GetBody = nil
	req.Header.Set("Content-Type", "application/octet-stream")
	auth, err := c.auth.signRequest(req, data)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrUnsent, err)
	}

	var connected atomic.Bool
	trace := &httptrace.ClientTrace{GotConn: func(httptrace.GotConnInfo) { connected.Store(true) }}
	response, err := client.Do(req.WithContext(httptrace.WithClientTrace(req.Context(), trace)))
	if err != nil {
		if !connected.Load() {
			return nil, fmt.Errorf("%w: %w", ErrUnsent, err)
		}
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
		return nil, fmt.Errorf("peer operation HTTP status %d", response.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, c.config.MaxBodyBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > c.config.MaxBodyBytes {
		return nil, ErrTooLarge
	}
	if err := c.auth.verifyResponse(response.Header, path, auth, body); err != nil {
		return nil, err
	}
	return body, nil
}
