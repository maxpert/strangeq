//go:build linux

package clusterraft

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

type journalRangeSyncFixture struct {
	gatedJournalFile
	calls  []string
	ranged func([]byte, int64) (int, error)
}

func TestJournalRangeSyncSupportedOnBenchmarkVolume(t *testing.T) {
	directory := os.Getenv("STRANGEQ_RANGE_TEST_DIRECTORY")
	if directory == "" {
		t.Skip("requires the benchmark volume")
	}
	file, err := os.CreateTemp(directory, "range-sync-test-")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, file.Close())
		require.NoError(t, os.Remove(file.Name()))
	})
	prefix := []byte("durable journal prefix")
	written, err := file.WriteAt(prefix, 0)
	require.NoError(t, err)
	require.Equal(t, len(prefix), written)
	require.NoError(t, file.Sync())

	body := bytes.Repeat([]byte{0xf5, 0x67, 0x23, 0x9b}, 16<<10)
	written, err = (journalRangeSyncFile{journalDataFile{file}}).writeAtSync(body, int64(len(prefix)))
	require.NoError(t, err, "benchmark volume must actually support the full synchronous range write")
	require.Equal(t, len(body), written)
	actual, err := os.ReadFile(file.Name())
	require.NoError(t, err)
	require.Equal(t, append(bytes.Clone(prefix), body...), actual)
}

func TestJournalRangeSyncTogglesOnExistingJournal(t *testing.T) {
	dir := t.TempDir()
	normal, err := OpenNodeJournal(dir, true)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, normal.Close()) })
	require.IsType(t, journalDataFile{}, normal.store.writer.file)
	require.NoError(t, normal.Close())

	ranged, err := OpenNodeJournalWithRangeSync(dir, false)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, ranged.Close()) })
	require.IsType(t, journalRangeSyncFile{}, ranged.store.writer.file)
	config := Config{
		ID: 1, GroupID: 1, Dir: filepath.Join(dir, "group"), Peers: []uint64{1},
		Machine: &testMachine{}, TickInterval: -1,
	}
	group := openTestJournalGroup(t, ranged.store, config)
	require.NoError(t, group.Campaign(t.Context()))
	body, err := group.Propose(t.Context(), []byte("range sync body"))
	require.NoError(t, err)
	require.Equal(t, []byte("applied range sync body"), body)
	require.NoError(t, group.Close())
	require.NoError(t, ranged.Close())

	restored, err := OpenNodeJournal(dir, false)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, restored.Close()) })
	require.IsType(t, journalDataFile{}, restored.store.writer.file)
	config.Peers = nil
	config.Machine = &testMachine{}
	group = openTestJournalGroup(t, restored.store, config)
	snapshot, err := group.MachineSnapshot(t.Context())
	require.NoError(t, err)
	require.JSONEq(t, "[\"range sync body\"]", string(snapshot))
}

func (f *journalRangeSyncFixture) WriteAt(data []byte, offset int64) (int, error) {
	f.calls = append(f.calls, "write")
	return f.gatedJournalFile.WriteAt(data, offset)
}

func (f *journalRangeSyncFixture) Sync() error {
	f.calls = append(f.calls, "sync")
	return nil
}

func (f *journalRangeSyncFixture) writeAtSync(data []byte, offset int64) (int, error) {
	f.calls = append(f.calls, "range")
	if f.ranged != nil {
		return f.ranged(data, offset)
	}
	return f.gatedJournalFile.WriteAt(data, offset)
}

func TestJournalRangeSyncPreservesDirtyPrefixAndFallback(t *testing.T) {
	t.Run("dirty_prefix_and_empty_barrier", func(t *testing.T) {
		file := &journalRangeSyncFixture{}
		writer, err := newJournalWriter(file, 0, 1, testJournalLimits())
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, writer.close()) })
		var locations []journalLocation
		appendBody := func(body string, durable bool) {
			t.Helper()
			got := awaitJournalCompletion(t, enqueueJournal(t, writer, 11, []byte(body), durable))
			require.NoError(t, got.err)
			locations = append(locations, got.location)
		}
		barrier := func() {
			t.Helper()
			before := file.bytes()
			got := awaitJournalCompletion(t, enqueueJournal(t, writer, 11, nil, true))
			require.NoError(t, got.err)
			require.Zero(t, got.location)
			require.Equal(t, before, file.bytes())
		}

		appendBody("dirty A", false)
		require.Equal(t, []string{"write"}, file.calls)
		appendBody("durable B", true)
		require.Equal(t, []string{"write", "write", "sync"}, file.calls,
			"a range write cannot fence A's earlier dirty frame")
		appendBody("durable C", true)
		require.Equal(t, []string{"write", "write", "sync", "range"}, file.calls)
		barrier()
		require.Equal(t, []string{"write", "write", "sync", "range"}, file.calls,
			"a clean empty barrier must not issue a write or sync")
		appendBody("dirty D", false)
		barrier()
		require.Equal(t, []string{"write", "write", "sync", "range", "write", "sync"}, file.calls,
			"an empty barrier must still sync an earlier dirty frame")
		appendBody("durable E", true)
		require.Equal(t, []string{"write", "write", "sync", "range", "write", "sync", "range"}, file.calls)

		data := file.bytes()
		var bodies []string
		var actual []journalLocation
		_, err = scanJournal(bytes.NewReader(data), int64(len(data)), -1, 1, func(records []journalRecord) error {
			for _, record := range records {
				require.Equal(t, uint64(11), record.group)
				bodies = append(bodies, string(record.body))
				actual = append(actual, record.location)
			}
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, []string{"dirty A", "durable B", "durable C", "dirty D", "durable E"}, bodies)
		require.Equal(t, locations, actual, "range writes must preserve frame sequence and exact body offsets")
	})

	for _, test := range []struct {
		name     string
		n        int
		err      error
		wantErr  error
		fallback bool
	}{
		{name: "unsupported_syscall", n: -1, err: unix.ENOSYS, fallback: true},
		{name: "unsupported_filesystem", n: 0, err: unix.EOPNOTSUPP, fallback: true},
		{name: "partial_unsupported", n: 1, err: unix.EOPNOTSUPP, wantErr: unix.EOPNOTSUPP},
		{name: "short_write", n: 1, wantErr: io.ErrShortWrite},
		{name: "io_error", n: -1, err: unix.EIO, wantErr: unix.EIO},
		{name: "invalid_argument", n: -1, err: unix.EINVAL, wantErr: unix.EINVAL},
	} {
		t.Run(test.name, func(t *testing.T) {
			file := &journalRangeSyncFixture{}
			file.ranged = func(data []byte, offset int64) (int, error) {
				if test.n > 0 {
					_, err := file.gatedJournalFile.WriteAt(data[:test.n], offset)
					if err != nil {
						return 0, err
					}
				}
				return test.n, test.err
			}
			writer, err := newJournalWriter(file, 0, 1, testJournalLimits())
			require.NoError(t, err)
			t.Cleanup(func() { _ = writer.close() })
			got := awaitJournalCompletion(t, enqueueJournal(t, writer, 22, []byte("body"), true))
			if test.fallback {
				require.NoError(t, got.err)
				require.Equal(t, []string{"range", "write", "sync"}, file.calls)
				data := file.bytes()
				_, err := scanJournal(bytes.NewReader(data), int64(len(data)), -1, 1, func(records []journalRecord) error {
					require.Equal(t, []journalRecord{{group: 22, body: []byte("body"), location: got.location}}, records)
					return nil
				})
				require.NoError(t, err)
				require.NoError(t, writer.close())
			} else {
				require.ErrorIs(t, got.err, test.wantErr)
				require.Zero(t, got.location)
				require.Equal(t, []string{"range"}, file.calls, "partial progress and ordinary errors must never fall back")
				late, err := writer.enqueue(33, nil, true)
				require.Nil(t, late)
				require.ErrorIs(t, err, test.wantErr)
				require.ErrorIs(t, writer.close(), test.wantErr)
			}
		})
	}
}
