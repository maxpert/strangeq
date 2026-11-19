package clusterwire

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

type commandReplyPartsFixture struct {
	handlerFunc
	prefix []byte
	body   []byte
}

func (h commandReplyPartsFixture) Command(context.Context, []byte) ([]byte, error) {
	return append(bytes.Clone(h.prefix), h.body...), nil
}

func (h commandReplyPartsFixture) CommandParts(context.Context, []byte) ([]byte, []byte, error) {
	return h.prefix, h.body, nil
}

func (h commandReplyPartsFixture) ReadCommand(ctx context.Context, data []byte) ([]byte, error) {
	return h.Command(ctx, data)
}

func (h commandReplyPartsFixture) ReadCommandParts(ctx context.Context, data []byte) ([]byte, []byte, error) {
	return h.CommandParts(ctx, data)
}

type replyWriteProbe struct {
	http.ResponseWriter
	writes [][]byte
}

func (w *replyWriteProbe) Write(data []byte) (int, error) {
	w.writes = append(w.writes, data)
	return w.ResponseWriter.Write(data)
}

func TestPeerCommandWritesOwnedReplyPartsWithoutJoining(t *testing.T) {
	checkPeerWritesOwnedReplyParts(t, commandPath)
}

func TestPeerReadWritesOwnedReplyPartsWithoutJoining(t *testing.T) {
	checkPeerWritesOwnedReplyParts(t, readPath)
}

func checkPeerWritesOwnedReplyParts(t *testing.T, path string) {
	t.Helper()
	handler := commandReplyPartsFixture{
		prefix: []byte{0, 1, 0, 3, 5, 8, 13, 21, 34},
		body:   bytes.Repeat([]byte{255, 0, 7, 21}, 16<<10),
	}
	cfg := Config{Listen: "127.0.0.1:0", ClusterID: testCluster, Token: token(1), AllowInsecure: true}
	peer, err := NewServer(cfg, handler)
	require.NoError(t, err)
	t.Cleanup(func() { _ = peer.Close(context.Background()) })
	input := []byte("return durable claim")
	request := httptest.NewRequest(http.MethodPost, "http://127.0.0.1"+path, bytes.NewReader(input))
	auth, err := peer.auth.signRequest(request, input)
	require.NoError(t, err)
	response := httptest.NewRecorder()
	probe := &replyWriteProbe{ResponseWriter: response}
	peer.handle(probe, request)
	require.Equal(t, http.StatusOK, response.Code)
	require.Equal(t, strconv.Itoa(len(handler.prefix)+len(handler.body)), response.Header().Get("Content-Length"))
	require.NoError(t, peer.auth.verifyResponse(response.Header(), path, auth, response.Body.Bytes()))
	require.True(t, bytes.Equal(append(bytes.Clone(handler.prefix), handler.body...), response.Body.Bytes()))
	require.Len(t, probe.writes, 2, "the signed response must write the original prefix and payload separately")
	require.Same(t, &handler.prefix[0], &probe.writes[0][0])
	require.Same(t, &handler.body[0], &probe.writes[1][0], "reply framing must avoid copying the owned body")
}
