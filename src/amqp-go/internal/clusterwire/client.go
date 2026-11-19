package clusterwire

import (
	"bytes"
	"context"
	"encoding/binary"
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
	config          Config
	auth            authenticator
	rpc, raft, wait *http.Client
}

func NewClient(cfg Config) (*Client, error) {
	cfg, auth, err := normalize(cfg)
	if err != nil {
		return nil, err
	}
	return &Client{config: cfg, auth: auth, rpc: newHTTPClient(cfg), raft: newHTTPClient(cfg), wait: newHTTPClient(cfg)}, nil
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

// CommandForGroup sends the group prefix and the caller's command as one
// request. The caller keeps body immutable until the call returns.
func (c *Client) CommandForGroup(ctx context.Context, address string, group uint64, body []byte) ([]byte, error) {
	var prefix [8]byte
	binary.BigEndian.PutUint64(prefix[:], group)
	return c.requestWithPrefix(ctx, c.rpc, address, commandPath, prefix[:], body)
}

func (c *Client) Read(ctx context.Context, address string, body []byte) ([]byte, error) {
	return c.request(ctx, c.rpc, address, readPath, body)
}

func (c *Client) Wait(ctx context.Context, address string, body []byte) ([]byte, error) {
	return c.request(ctx, c.wait, address, waitPath, body)
}

func (c *Client) Membership(ctx context.Context, address string, body []byte) ([]byte, error) {
	return c.request(ctx, c.rpc, address, membershipPath, body)
}

func (c *Client) Close() {
	c.rpc.CloseIdleConnections()
	c.raft.CloseIdleConnections()
	c.wait.CloseIdleConnections()
}

func (c *Client) request(ctx context.Context, client *http.Client, address, path string, data []byte) ([]byte, error) {
	return c.requestWithPrefix(ctx, client, address, path, nil, data)
}

func (c *Client) requestWithPrefix(ctx context.Context, client *http.Client, address, path string, prefix, data []byte) ([]byte, error) {
	cleartext := c.config.AllowInsecure && c.config.TLS == nil
	if err := validateAddress(address, false, cleartext); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrUnsent, err)
	}
	maxBody := c.config.MaxBodyBytes
	if path == waitPath {
		maxBody = min(maxBody, maxWaitBodyBytes)
	}
	prefixBytes, dataBytes := int64(len(prefix)), int64(len(data))
	if prefixBytes > maxBody || dataBytes > maxBody-prefixBytes {
		return nil, fmt.Errorf("%w: %w", ErrUnsent, ErrTooLarge)
	}
	scheme := "https"
	if cleartext {
		scheme = "http"
	}
	target := url.URL{Scheme: scheme, Host: address, Path: path}
	var reader io.Reader
	if len(prefix) == 0 {
		reader = bytes.NewReader(data)
	} else {
		command := newCommandBody(prefix, data)
		defer command.Close()
		reader = command
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target.String(), reader)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrUnsent, err)
	}
	if len(prefix) != 0 {
		req.ContentLength = prefixBytes + dataBytes
	}
	// Leave replay decisions to the shard caller, which knows command identity.
	req.GetBody = nil
	req.Header.Set("Content-Type", "application/octet-stream")
	auth, err := c.auth.signRequestParts(req, prefix, data)
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
	body, err := readPeerResponseBody(response, maxBody)
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > maxBody {
		return nil, ErrTooLarge
	}
	if err := c.auth.verifyResponse(response.Header, path, auth, body); err != nil {
		return nil, err
	}
	return body, nil
}

func readPeerResponseBody(response *http.Response, limit int64) ([]byte, error) {
	length := response.ContentLength
	if length > limit {
		return nil, ErrTooLarge
	}
	if length < 0 {
		return io.ReadAll(io.LimitReader(response.Body, limit+1))
	}
	body := make([]byte, int(length))
	if length == 0 && response.Body == http.NoBody {
		return body, nil
	}
	for offset := 0; offset < len(body); {
		n, err := response.Body.Read(body[offset:])
		offset += n
		if err == io.EOF {
			if offset != len(body) {
				return nil, io.ErrUnexpectedEOF
			}
			return body, nil
		}
		if err != nil {
			return nil, err
		}
	}
	// HTTP/2 can deliver the last payload bytes before the stream's end.
	// Let the transport finish checking framing and surface read errors.
	var extra [1]byte
	n, err := io.ReadFull(response.Body, extra[:])
	if n != 0 {
		if length == limit {
			return nil, ErrTooLarge
		}
		return nil, fmt.Errorf("peer response exceeds declared Content-Length %d", length)
	}
	if err != io.EOF {
		return nil, err
	}
	return body, nil
}
