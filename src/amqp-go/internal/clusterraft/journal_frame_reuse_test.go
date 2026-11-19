package clusterraft

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

type journalFrameRecordingFile struct {
	contents  []byte
	addresses []string
}

func (f *journalFrameRecordingFile) WriteAt(p []byte, offset int64) (int, error) {
	if offset != int64(len(f.contents)) {
		return 0, fmt.Errorf("journal frame offset %d differs from end %d", offset, len(f.contents))
	}
	f.addresses = append(f.addresses, fmt.Sprintf("%p", &p[0]))
	f.contents = append(f.contents, p...)
	return len(p), nil
}

func (*journalFrameRecordingFile) Sync() error  { return nil }
func (*journalFrameRecordingFile) Close() error { return nil }

func TestJournalWriterReusesBoundedFrameBuffer(t *testing.T) {
	file := &journalFrameRecordingFile{}
	limits := journalLimits{
		pendingBytes: 64 << 10, pendingRequests: 1,
		batchBytes: 8 << 10, batchRequests: 1,
	}
	writer, err := newJournalWriter(file, 0, 1, limits)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, writer.close()) })

	bodies := [][]byte{
		bytes.Repeat([]byte{1}, 4096),
		bytes.Repeat([]byte{2}, 4096),
		bytes.Repeat([]byte{3}, 32768),
		bytes.Repeat([]byte{4}, 4096),
	}
	locations := make([]journalLocation, 0, len(bodies))
	for _, body := range bodies {
		result := awaitJournalCompletion(t, enqueueJournal(t, writer, 1, body, true))
		require.NoError(t, result.err)
		locations = append(locations, result.location)
	}
	require.NoError(t, writer.close())

	var replayed int
	_, err = scanJournal(bytes.NewReader(file.contents), int64(len(file.contents)), -1, 1,
		func(records []journalRecord) error {
			require.Len(t, records, 1)
			require.Equal(t, bodies[replayed], records[0].body)
			require.Equal(t, locations[replayed], records[0].location)
			replayed++
			return nil
		})
	require.NoError(t, err)
	require.Equal(t, len(bodies), replayed)
	require.Equal(t, file.addresses[0], file.addresses[1], "ordinary appends reuse their frame backing buffer")
	require.NotEqual(t, file.addresses[0], file.addresses[2], "oversized appends allocate separately")
	require.Equal(t, file.addresses[0], file.addresses[3], "an oversized append must not replace the reusable buffer")
}
