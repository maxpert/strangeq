package clusterraft

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestJournalWriterReusesEncodedBodiesAfterCompletion(t *testing.T) {
	file := &journalFrameRecordingFile{}
	limits := journalLimits{
		pendingBytes: 1 << 20, pendingRequests: 16,
		batchBytes: 256 << 10, batchRequests: 1,
	}
	writer, err := newJournalWriter(file, 0, 1, limits)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, writer.close()) })

	staging := make(map[string]bool)
	bodies := make([][]byte, 0, limits.pendingRequests+1)
	for i := range cap(bodies) {
		body := bytes.Repeat([]byte{byte(i)}, 512<<10)
		bodies = append(bodies, body)
		result, err := writer.enqueueEncoded(1, len(body), true, func(dst []byte) error {
			staging[fmt.Sprintf("%p", &dst[0])] = true
			copy(dst, body)
			return nil
		})
		require.NoError(t, err)
		require.NoError(t, awaitJournalCompletion(t, result).err)
	}
	rejected := errors.New("intentional encoder rejection")
	_, err = writer.enqueueEncoded(1, 64<<10, true, func([]byte) error { return rejected })
	require.ErrorIs(t, err, rejected)
	require.NoError(t, writer.close())

	index := 0
	_, err = scanJournal(bytes.NewReader(file.contents), int64(len(file.contents)), -1, 1, func(batch []journalRecord) error {
		require.Len(t, batch, 1)
		require.Equal(t, bodies[index], batch[0].body)
		index++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, len(bodies), index)
	require.LessOrEqual(t, len(staging), 8, "ordinary record encoders must reuse a bounded number of completed staging buffers")
}
