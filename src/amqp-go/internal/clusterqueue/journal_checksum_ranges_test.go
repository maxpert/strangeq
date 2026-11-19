package clusterqueue

import (
	"bytes"
	"hash/crc32"
	"io"
	"testing"

	"github.com/maxpert/amqp-go/internal/clusterproposal"
	"github.com/stretchr/testify/require"
)

func TestJournalBodyCRCRangesUseCanonicalUnpackedLayout(t *testing.T) {
	var m Machine
	bodies := [][]byte{bytes.Repeat([]byte{1}, 65536), []byte("tiny"), bytes.Repeat([]byte{3}, 65537)}
	wire, err := EncodePublishBatch("q", 7, bodies)
	require.NoError(t, err)
	ranges := m.JournalBodyCRCRanges(wire)
	require.Len(t, ranges, 2)
	for i, j := range []int{0, 2} {
		span := ranges[i]
		require.Equal(t, bodies[j], wire[int(span.Offset):int(span.Offset+span.Length)])
	}
	single, err := EncodePublishCommand("q", 7, PublishRecord{Prefix: []byte("envelope"), Body: bodies[0]})
	require.NoError(t, err)
	ranges = m.JournalBodyCRCRanges(single)
	require.Len(t, ranges, 1)
	require.Equal(t, append([]byte("envelope"), bodies[0]...), single[int(ranges[0].Offset):])
	tiny, err := EncodePublishCommand("q", 7, PublishRecord{Body: bytes.Repeat([]byte{8}, 1024)})
	require.NoError(t, err)
	require.Nil(t, m.JournalBodyCRCRanges(tiny))
	for _, version := range []byte{packedPublishVersion, smallPackedPublishVersion} {
		packed := bytes.Clone(single)
		packed[0] = version
		require.Nil(t, m.JournalBodyCRCRanges(packed))
	}
	invalid := bytes.Clone(wire)
	invalid = invalid[:len(invalid)-1]
	require.Nil(t, m.JournalBodyCRCRanges(invalid))
}

type bodyCRCQueueSource struct {
	*bytes.Reader
	body   []byte
	offset int64
	sum    uint32
	calls  int
	cold   io.ReaderAt
}

func (s *bodyCRCQueueSource) JournalBodyCRC32C(offset int64, body []byte) (uint32, bool) {
	s.calls++
	if offset != s.offset || len(body) != len(s.body) || &body[0] != &s.body[0] {
		return 0, false
	}
	return s.sum, true
}
func (s *bodyCRCQueueSource) ColdJournalSource() io.ReaderAt { return s.cold }

func TestJournalBodyCRCUsesProofButColdStateStripsItAndChecksActualBytes(t *testing.T) {
	body := bytes.Repeat([]byte{0x76}, 65536)
	physical := bytes.Clone(body)
	reader := bytes.NewReader(physical)
	source := &bodyCRCQueueSource{Reader: reader, body: body, offset: 0, sum: crc32.Checksum(body, journalBodyChecksum), cold: reader}
	message, err := (journalBodyOrigin{source: source}).message(1, body)
	require.NoError(t, err)
	require.Equal(t, 1, source.calls)
	require.Equal(t, source.sum, message.cold.checksum)
	require.Same(t, reader, message.cold.source)
	_, retained := message.cold.source.(interface {
		JournalBodyCRC32C(int64, []byte) (uint32, bool)
	})
	require.False(t, retained, "cold state must not keep proof or command backing")
	out := make([]byte, len(body))
	require.NoError(t, message.cold.read(out))
	require.Equal(t, body, out)
	physical[0] ^= 1
	require.ErrorContains(t, message.cold.read(out), "checksum")
	fallbackBody := bytes.Clone(body)
	fallback, err := (journalBodyOrigin{source: source}).message(2, fallbackBody)
	require.NoError(t, err)
	require.Equal(t, crc32.Checksum(fallbackBody, journalBodyChecksum), fallback.cold.checksum)
	small := bytes.Repeat([]byte{1}, 1024)
	_, err = (journalBodyOrigin{source: source}).message(3, small)
	require.NoError(t, err)
	require.Equal(t, 2, source.calls, "tiny path must not consult metadata")
	require.Equal(t, clusterproposal.MinBodyChecksumBytes, minJournalBodyBytes)
}
