package clusterwire

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
)

type raftBodyReadProbe struct {
	*bytes.Reader
	dst []byte
}

func (p *raftBodyReadProbe) Read(dst []byte) (int, error) {
	p.dst = dst
	return p.Reader.Read(dst)
}

func TestPeerRaftReusesWireScratchWithoutAliasingDecodedEntry(t *testing.T) {
	var received []*raftpb.Entry
	cfg := Config{Listen: "127.0.0.1:0", ClusterID: testCluster, Token: token(1), AllowInsecure: true}
	peer, err := NewServer(cfg, handlerFunc{raft: func(_ context.Context, group uint64, batch []*raftpb.Message) error {
		if group != 41 || len(batch) != 1 || len(batch[0].GetEntries()) != 1 {
			t.Fatal("the request did not decode as one entry in the expected group")
		}
		received = append(received, batch[0].GetEntries()[0])
		return nil
	}})
	require.NoError(t, err)
	t.Cleanup(func() { _ = peer.Close(context.Background()) })

	send := func(body []byte) []byte {
		t.Helper()
		entry := &raftpb.Entry{Term: new(uint64(1)), Index: new(uint64(1)), Data: body}
		message := &raftpb.Message{Type: raftpb.MsgApp.Enum(), From: new(uint64(1)), To: new(uint64(2)), Entries: []*raftpb.Entry{entry}}
		wire, err := encodeBatch(41, []*raftpb.Message{message}, 1<<20)
		require.NoError(t, err)
		probe := &raftBodyReadProbe{Reader: bytes.NewReader(wire)}
		req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1"+raftPath, probe)
		req.ContentLength = int64(len(wire))
		_, err = peer.auth.signRequest(req, wire)
		require.NoError(t, err)
		response := httptest.NewRecorder()
		peer.handle(response, req)
		require.Equal(t, http.StatusOK, response.Code)
		require.Len(t, probe.dst, len(wire))
		return probe.dst
	}
	first := bytes.Repeat([]byte{0x25}, 64<<10)
	second := bytes.Repeat([]byte{0xba}, 64<<10)
	firstScratch, secondScratch := send(first), send(second)
	require.Len(t, received, 2)
	require.True(t, bytes.Equal(first, received[0].GetData()), "a later input read must preserve the earlier decoded entry")
	require.True(t, bytes.Equal(second, received[1].GetData()))
	require.Same(t, &firstScratch[0], &secondScratch[0], "a serialized Raft handler should reuse its wire input buffer")
}

func TestRaftWireScratchBoundsConcurrentRetention(t *testing.T) {
	pool := raftBodyPool{free: 4}
	const limit = (1 << 20) + (4 << 10)
	held := make([][]byte, 0, 4)
	for range cap(held) {
		buffer, borrowed := pool.borrow(limit)
		require.True(t, borrowed)
		for _, other := range held {
			require.NotSame(t, &other[0], &buffer[0])
		}
		held = append(held, buffer)
	}
	_, borrowed := pool.borrow(limit)
	require.False(t, borrowed, "an admitted fifth request borrows no retained slot")
	pool.release(held[0])
	_, borrowed = pool.borrow(limit + 1)
	require.False(t, borrowed, "an oversized frame cannot grow a retained slot")
	warmed, borrowed := pool.borrow(limit)
	require.True(t, borrowed)
	require.Same(t, &held[0][0], &warmed[0])
	pool.release(warmed)
	for _, buffer := range held[1:] {
		pool.release(buffer)
	}
}
