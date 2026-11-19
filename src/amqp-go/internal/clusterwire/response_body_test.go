package clusterwire

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

type responseBodyRead struct {
	window []byte
	count  int
}

type responseBodyTransport struct {
	*http.Transport
	length int64
	reads  []responseBodyRead
}

func (t *responseBodyTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	response, err := t.Transport.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	t.length = response.ContentLength
	response.Body = &observedResponseBody{ReadCloser: response.Body, transport: t}
	return response, nil
}

type observedResponseBody struct {
	io.ReadCloser
	transport *responseBodyTransport
}

func (b *observedResponseBody) Read(window []byte) (int, error) {
	// Force several data-bearing reads while leaving their actual destination
	// slices visible to the test. A final EOF check need not carry any data.
	count, err := b.ReadCloser.Read(window[:min(len(window), 8<<10)])
	if count != 0 {
		b.transport.reads = append(b.transport.reads, responseBodyRead{window, count})
	}
	return count, err
}

type chunkedPeerResponse struct{ http.ResponseWriter }

func (w chunkedPeerResponse) WriteHeader(status int) {
	w.Header().Del("Content-Length")
	w.Header().Set("Transfer-Encoding", "chunked")
	w.ResponseWriter.WriteHeader(status)
}

func TestPeerCommandReadsResponseInExactBuffer(t *testing.T) {
	payload := bytes.Repeat([]byte{0, 255, 7, 128}, 16<<10)
	for _, tc := range []struct {
		name    string
		chunked bool
		length  int64
	}{
		{name: "known-length", length: int64(len(payload))},
		{name: "chunked", chunked: true, length: -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{Listen: "127.0.0.1:0", ClusterID: testCluster, Token: token(1), AllowInsecure: true}
			peer, err := NewServer(cfg, handlerFunc{
				command: func(context.Context, []byte) ([]byte, error) { return payload, nil },
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = peer.Close(context.Background()) })
			listener := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if tc.chunked {
					w = chunkedPeerResponse{w}
				}
				peer.handle(w, req)
			}))
			t.Cleanup(listener.Close)
			client, err := NewClient(cfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(client.Close)
			transport := &responseBodyTransport{Transport: client.rpc.Transport.(*http.Transport)}
			client.rpc.Transport = transport

			output, err := client.Command(t.Context(), listener.Listener.Addr().String(), []byte("claim"))
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(payload, output) {
				t.Fatalf("signed command reply contents changed, got %d bytes, want %d", len(output), len(payload))
			}
			if transport.length != tc.length {
				t.Fatalf("HTTP body length %d, want %d", transport.length, tc.length)
			}
			if len(transport.reads) < 2 {
				t.Fatalf("expected several data-bearing reads, got %d", len(transport.reads))
			}
			if tc.chunked {
				return
			}
			offset := 0
			for _, read := range transport.reads {
				remaining := len(output) - offset
				if len(read.window) != remaining || cap(read.window) != remaining || &read.window[0] != &output[offset] {
					t.Fatalf("known-length reply grew or copied its body buffer at offset %d: read window length %d, capacity %d, want %d in the returned buffer",
						offset, len(read.window), cap(read.window), remaining)
				}
				offset += read.count
			}
			if offset != len(output) {
				t.Fatalf("observed %d response bytes, want %d", offset, len(output))
			}
		})
	}
}
