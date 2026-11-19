package clusterraft

import (
	"bytes"
	"hash/crc32"
	"io"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"

	"github.com/maxpert/amqp-go/internal/clusterproposal"
	"github.com/maxpert/amqp-go/internal/clusterqueue"
	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestJournalBodyChecksumCompositionAndRangeFallback(t *testing.T) {
	data := make([]byte, 256<<10)
	rng := rand.NewChaCha8([32]byte{1, 7, 9})
	_, _ = rng.Read(data)
	initial := uint32(0x1498b6a3)
	want := crc32.Update(initial, journalChecksum, data)
	ranges := []clusterproposal.BodyRange{{Offset: 9, Length: 32768}, {Offset: 40000, Length: 65536}, {Offset: 150000, Length: 65537}}
	actual, proof := prepareJournalBodyChecksums(initial, data, func([]byte) []clusterproposal.BodyRange { return ranges })
	require.Equal(t, want, actual)
	require.Len(t, proof, 3)
	for _, p := range proof {
		at := clusterproposal.PrefixBytes + int(p.Offset)
		require.Equal(t, crc32.Checksum(data[at:at+int(p.Length)], journalChecksum), p.CRC32C)
	}
	invalid := [][]clusterproposal.BodyRange{nil, {{Offset: 0, Length: 0}}, {{Offset: 0, Length: 1024}}, {{Offset: math.MaxUint32, Length: 32768}}, {{Offset: 0, Length: math.MaxUint32}}, {{Offset: 0, Length: 65536}, {Offset: 1, Length: 32768}}, make([]clusterproposal.BodyRange, clusterproposal.MaxBodyChecksumRanges+1)}
	for _, r := range invalid {
		got, proof := prepareJournalBodyChecksums(initial, data, func([]byte) []clusterproposal.BodyRange { return r })
		require.Equal(t, want, got)
		require.Nil(t, proof)
	}
	got, proof := prepareJournalBodyChecksums(initial, data, nil)
	require.Equal(t, want, got)
	require.Nil(t, proof)
}

func TestJournalBodyChecksumSourceExactIdentityRangeAndClear(t *testing.T) {
	command := bytes.Repeat([]byte{0x84}, 100000)
	span := clusterproposal.BodyRange{Offset: 19, Length: 65536}
	body := command[19 : 19+65536]
	sum := crc32.Checksum(body, journalChecksum)
	backing := bytes.NewReader(bytes.Clone(command))
	s := &journalBodyChecksumSource{source: backing, command: command, checksums: []clusterproposal.BodyChecksum{{BodyRange: span, CRC32C: sum}}}
	got, ok := s.JournalBodyCRC32C(19, body)
	require.True(t, ok)
	require.Equal(t, sum, got)
	for _, bad := range []struct {
		offset int64
		body   []byte
	}{{19, bytes.Clone(body)}, {18, body}, {-1, body}, {19, body[:len(body)-1]}, {math.MaxInt64, body}, {19, nil}} {
		_, ok := s.JournalBodyCRC32C(bad.offset, bad.body)
		require.False(t, ok)
	}
	require.Same(t, backing, s.ColdJournalSource())
	s.clear()
	_, ok = s.JournalBodyCRC32C(19, body)
	require.False(t, ok)
	require.Nil(t, s.command)
	require.Nil(t, s.checksums)
}

func TestJournalBodyChecksumRecentCacheChargesConsumesAndRejectsClones(t *testing.T) {
	op := preparedRecordOp(65536)
	op.bodyCRCRanges = func(command []byte) []clusterproposal.BodyRange {
		return []clusterproposal.BodyRange{{Offset: 0, Length: uint32(len(command))}}
	}
	size, err := op.size()
	require.NoError(t, err)
	_, delta, err := prepareJournalRecord(op, size)
	require.NoError(t, err)
	ref := delta.entries[0]
	entry := op.entries[0]
	raw := make([]byte, size)
	rawDelta, err := op.encode(raw)
	require.NoError(t, err)
	require.Equal(t, rawDelta.entries, delta.entries)
	prepared, _, err := prepareJournalRecord(op, size)
	require.NoError(t, err)
	actual, _, err := encodeJournalFrame(nil, 1, []journalRecord{{group: 11, prepared: prepared}})
	require.NoError(t, err)
	oracle, _, err := encodeJournalFrameRawCRCReference(nil, 1, []journalRecord{{group: 11, body: raw}})
	require.NoError(t, err)
	require.Equal(t, oracle, actual)
	var cache journalRecentEntries
	cache.rememberChecksummed(delta.entries, op.entries, delta.bodyChecksums)
	metadata := cap(delta.bodyChecksums[0]) * clusterproposal.BodyChecksumBytes
	require.Equal(t, max(int(ref.size), cap(entry.Data))+metadata, cache.bytes)
	require.Nil(t, cache.takeBodyChecksums(ref, proto.Clone(entry).(*pb.Entry)))
	wrong := ref
	wrong.term++
	require.Nil(t, cache.takeBodyChecksums(wrong, entry))
	proof := cache.takeBodyChecksums(ref, entry)
	require.NotEmpty(t, proof)
	require.Equal(t, max(int(ref.size), cap(entry.Data)), cache.bytes)
	require.Nil(t, cache.takeBodyChecksums(ref, entry))
	cache.clear()
	cache.remember(delta.entries, op.entries)
	require.Nil(t, cache.takeBodyChecksums(ref, entry))
	cache.clear()
	require.Zero(t, cache.bytes)
}

type bodyChecksumQueueProbe struct {
	inner clusterqueue.Machine
	hits  int
	last  *journalBodyChecksumSource
}

func (m *bodyChecksumQueueProbe) Apply(data []byte) []byte              { return m.inner.Apply(data) }
func (m *bodyChecksumQueueProbe) Snapshot() ([]byte, error)             { return m.inner.Snapshot() }
func (m *bodyChecksumQueueProbe) Restore(data []byte) error             { return m.inner.Restore(data) }
func (m *bodyChecksumQueueProbe) ReleaseJournalSources()                { m.inner.ReleaseJournalSources() }
func (m *bodyChecksumQueueProbe) AcceptsImmutableJournalCommands() bool { return true }
func (m *bodyChecksumQueueProbe) JournalBodyCRCRanges(data []byte) []clusterproposal.BodyRange {
	return m.inner.JournalBodyCRCRanges(data)
}
func (m *bodyChecksumQueueProbe) ApplyJournaled(data []byte, want bool, source io.ReaderAt) ([]byte, error) {
	m.observe(data, source)
	return m.inner.ApplyJournaled(data, want, source)
}
func (m *bodyChecksumQueueProbe) observe(data []byte, source io.ReaderAt) {
	if s, ok := source.(*journalBodyChecksumSource); ok {
		m.last = s
	}
	if s, ok := source.(interface {
		JournalBodyCRC32C(int64, []byte) (uint32, bool)
	}); ok {
		for _, span := range m.JournalBodyCRCRanges(data) {
			body := data[int(span.Offset):int(span.Offset+span.Length)]
			if _, hit := s.JournalBodyCRC32C(int64(span.Offset), body); hit {
				m.hits++
			}
		}
	}
}

func TestJournalBodyChecksumRealGroupApplyHitsThenStrictFallbackAndColdCorruption(t *testing.T) {
	dir := t.TempDir()
	node, err := openSegmentedGroupNode(dir, true, false, 2<<20, 16<<20, nil, journalSegmentHooks{processRoots: true, processBootID: "body-crc-kernel"})
	require.NoError(t, err)
	node.writer.durability = &journalDurability{}
	t.Cleanup(func() { _ = node.close() })
	machine := &bodyChecksumQueueProbe{}
	group := openTestJournalGroup(t, node, Config{ID: 1, GroupID: 11, Dir: filepath.Join(dir, "group"), Peers: []uint64{1}, Machine: machine, TickInterval: -1, ProcessDurability: true, DeferJournalCommit: true})
	require.NoError(t, group.Campaign(t.Context()))
	declare, err := clusterqueue.EncodeCommand(clusterqueue.QueueCommand{Kind: clusterqueue.DeclareQueue, Queue: "crc", QueueSpec: clusterqueue.QueueSpec{Durable: true}})
	require.NoError(t, err)
	reply, err := group.Propose(t.Context(), declare)
	require.NoError(t, err)
	declared, err := clusterqueue.DecodeResponse(reply)
	require.NoError(t, err)
	require.NoError(t, declared.Err())
	body := bytes.Repeat([]byte{0x6c}, 65536)
	owned, err := clusterqueue.EncodeOwnedPublishCommand("crc", declared.Queue.Incarnation, clusterqueue.PublishRecord{Body: body})
	require.NoError(t, err)
	submission, err := owned.Begin()
	require.NoError(t, err)
	_, err = group.ProposeOwned(t.Context(), submission)
	require.NoError(t, err)
	require.NoError(t, waitTestLocalWrites(t.Context(), group))
	require.Equal(t, 1, machine.hits, "actual Raft append/apply identity must reuse its prepared proof")
	require.NotNil(t, machine.last)
	machine.last.mu.Lock()
	cleared := machine.last.command == nil && machine.last.checksums == nil
	machine.last.mu.Unlock()
	require.True(t, cleared, "apply must release all ephemeral aliases")
	group.mu.Lock()
	base := group.store.state.snapshot.metadata.GetIndex()
	ref := group.store.state.entries[group.applied-base-1]
	group.mu.Unlock()
	require.NoError(t, node.writer.requireFullDurability())
	raw, err := clusterqueue.EncodePublishCommand("crc", declared.Queue.Incarnation, clusterqueue.PublishRecord{Body: body})
	require.NoError(t, err)
	_, err = group.Propose(t.Context(), raw)
	require.NoError(t, err)
	require.NoError(t, waitTestLocalWrites(t.Context(), group))
	require.Equal(t, 1, machine.hits, "strict activation must not attach a proof")
	_, err = machine.Snapshot()
	require.NoError(t, err)
	// No hot payload cache is used by this probe: Snapshot must verify the
	// physically stored body against the CRC that was reused at live apply.
	spans := machine.JournalBodyCRCRanges(raw)
	require.Len(t, spans, 1)
	at := ref.offset + int64(ref.dataOffset) + clusterproposal.PrefixBytes + int64(spans[0].Offset)
	reader := node.reader.(*journalSegmentReader)
	reader.store.mu.RLock()
	extent := reader.store.manifest.extents[0]
	for _, e := range reader.store.manifest.extents {
		if e.base <= at {
			extent = e
		}
	}
	reader.store.mu.RUnlock()
	file, err := os.OpenFile(reader.store.path(extent.id), os.O_RDWR, 0)
	require.NoError(t, err)
	_, err = file.WriteAt([]byte{body[0] ^ 1}, at-extent.base)
	require.NoError(t, err)
	require.NoError(t, file.Close())
	_, err = machine.Snapshot()
	require.ErrorContains(t, err, "checksum")
}
