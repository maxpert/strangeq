package clusterwire

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

type commandPartsTransport func(*http.Request) (*http.Response, error)

func (send commandPartsTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	return send(request)
}

func fixtureGroupCommand(client *Client, ctx context.Context, group uint64, body []byte) ([]byte, error) {
	const address = "127.0.0.1:8817"
	if command, ok := any(client).(interface {
		CommandForGroup(context.Context, string, uint64, []byte) ([]byte, error)
	}); ok {
		return command.CommandForGroup(ctx, address, group, body)
	}
	wire := binary.BigEndian.AppendUint64(make([]byte, 0, 8+len(body)), group)
	return client.Command(ctx, address, append(wire, body...))
}

func TestPeerGroupCommandDoesNotAllocateAJoinedRequestBody(t *testing.T) {
	const group = uint64(41)
	client, err := NewClient(Config{ClusterID: testCluster, Token: token(1), AllowInsecure: true})
	require.NoError(t, err)
	t.Cleanup(client.Close)
	body := bytes.Repeat([]byte{7, 8, 9, 0}, 16<<10)
	wire := append(binary.BigEndian.AppendUint64(nil, group), body...)
	wantDigest := bodyDigest(wire)
	checkWire := true
	client.rpc.Transport = commandPartsTransport(func(req *http.Request) (*http.Response, error) {
		defer req.Body.Close()
		if checkWire {
			got, err := io.ReadAll(req.Body)
			require.NoError(t, err)
			require.EqualValues(t, len(wire), req.ContentLength)
			require.True(t, bytes.Equal(wire, got), "the outgoing request must preserve the existing group and command bytes")
			require.Equal(t, wantDigest, req.Header.Get(headerDigest))
		} else if _, err := io.Copy(io.Discard, req.Body); err != nil {
			return nil, err
		}
		headers := http.Header{}
		auth := credentials{timestamp: req.Header.Get(headerTime), nonce: req.Header.Get(headerNonce)}
		client.auth.signResponse(headers, commandPath, auth, nil, nil)
		return &http.Response{StatusCode: http.StatusOK, Header: headers, Body: http.NoBody, Request: req}, nil
	})
	_, err = fixtureGroupCommand(client, t.Context(), group, body)
	require.NoError(t, err)
	checkWire = false
	measure := testing.Benchmark(func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := fixtureGroupCommand(client, b.Context(), group, body); err != nil {
				b.Fatal(err)
			}
		}
	})
	require.Less(t, measure.AllocedBytesPerOp(), int64(len(body)/4),
		"peer routing must send the caller's payload without allocating a concatenated request")
}

func TestPeerGroupCommandReleasesCallerBodyWhenCanceled(t *testing.T) {
	client, err := NewClient(Config{ClusterID: testCluster, Token: token(1), AllowInsecure: true})
	require.NoError(t, err)
	t.Cleanup(client.Close)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var requestBody io.ReadCloser
	client.rpc.Transport = commandPartsTransport(func(req *http.Request) (*http.Response, error) {
		requestBody = req.Body
		cancel()
		return nil, context.Canceled
	})
	body := bytes.Repeat([]byte{0x44}, 64<<10)
	_, err = fixtureGroupCommand(client, ctx, 41, body)
	require.ErrorIs(t, err, context.Canceled)
	require.NotNil(t, requestBody)
	defer requestBody.Close()
	// RoundTripper may close a canceled request asynchronously. The caller
	// may reuse its buffer as soon as CommandForGroup returns.
	clear(body)
	n, err := requestBody.Read(make([]byte, 16))
	require.ErrorIs(t, err, io.ErrClosedPipe)
	require.Zero(t, n, "a retained transport body may not read bytes after the method returns")
}
