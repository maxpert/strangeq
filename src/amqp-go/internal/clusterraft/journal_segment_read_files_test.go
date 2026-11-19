package clusterraft

import (
	"bytes"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestJournalSegmentReusesReadDescriptorAcrossBodyReads(t *testing.T) {
	body := bytes.Repeat([]byte{0xa9}, 64<<10)
	frame, locations, err := encodeJournalFrame(nil, 1, []journalRecord{{group: 11, body: body}})
	require.NoError(t, err)
	output, reader, _ := openTestRetainedJournalSegments(t, t.TempDir(), true, 256<<10, 1<<20, journalSegmentHooks{}, nil)
	writeRetainedJournalFixtureFrame(t, output, frame, 0)
	var first *os.File
	for range 8 {
		actual := make([]byte, len(body))
		n, err := reader.readAt(actual, locations[0].offset, func(file *os.File, dst []byte, offset int64) (int, error) {
			if first == nil {
				first = file
			} else {
				require.Same(t, first, file, "hot extent must reuse its owned read descriptor")
			}
			return file.ReadAt(dst, offset)
		})
		require.NoError(t, err)
		require.Equal(t, len(body), n)
		require.Equal(t, body, actual)
		_, err = first.Stat()
		require.NoError(t, err, "successful body read must keep its reusable extent descriptor open")
	}
	require.NoError(t, output.Close())
	require.NoError(t, reader.Close())
	_, err = first.Stat()
	require.ErrorIs(t, err, os.ErrClosed, "closing the owner must close its reusable read descriptors")
}

func TestJournalSegmentReadDescriptorEvictionPreservesBlockedRead(t *testing.T) {
	const bodyBytes = 512
	body := bytes.Repeat([]byte{0x73}, bodyBytes)
	sample, _, err := encodeJournalFrame(nil, 1, []journalRecord{{group: 11, body: body}})
	require.NoError(t, err)
	extentBytes := int64(len(sample))
	output, reader, _ := openTestRetainedJournalSegments(t, t.TempDir(), true, extentBytes, 64<<10, journalSegmentHooks{}, nil)
	locations := make([]journalLocation, maxJournalReadFiles+3)
	for i := range locations {
		frame, refs, err := encodeJournalFrame(nil, uint64(i+1), []journalRecord{{group: 11, body: body}})
		require.NoError(t, err)
		base := int64(i) * extentBytes
		writeRetainedJournalFixtureFrame(t, output, frame, base)
		locations[i] = refs[0]
		locations[i].offset += base
	}
	entered, release := make(chan *os.File, 1), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	done := make(chan error, 1)
	go func() {
		data := make([]byte, len(body))
		_, err := reader.readAt(data, locations[0].offset, func(file *os.File, dst []byte, at int64) (int, error) {
			entered <- file
			<-release
			return file.ReadAt(dst, at)
		})
		if err == nil && !bytes.Equal(data, body) {
			err = errors.New("blocked read returned different bytes")
		}
		done <- err
	}()
	var blocked *os.File
	select {
	case blocked = <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("read did not reach descriptor gate")
	}
	var idle []*os.File
	for _, location := range locations[1:] {
		data := make([]byte, len(body))
		_, err := reader.readAt(data, location.offset, func(file *os.File, dst []byte, at int64) (int, error) {
			idle = append(idle, file)
			return file.ReadAt(dst, at)
		})
		require.NoError(t, err)
		require.Equal(t, body, data)
		reader.store.readFiles.mu.Lock()
		count := len(reader.store.readFiles.files)
		reader.store.readFiles.mu.Unlock()
		require.LessOrEqual(t, count, maxJournalReadFiles, "cold extents cannot grow the descriptor cache")
	}
	_, err = idle[0].Stat()
	require.ErrorIs(t, err, os.ErrClosed, "oldest idle extent descriptor must be evicted")
	_, err = blocked.Stat()
	require.NoError(t, err, "cache pressure cannot close an in-flight read descriptor")
	unblock()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("blocked read did not finish")
	}
}

func TestJournalSegmentReadDescriptorCloseWaitsForIssuedRead(t *testing.T) {
	body := []byte("read held through close")
	frame, locations, err := encodeJournalFrame(nil, 1, []journalRecord{{group: 11, body: body}})
	require.NoError(t, err)
	output, reader, _ := openTestRetainedJournalSegments(t, t.TempDir(), true, 4096, 64<<10, journalSegmentHooks{}, nil)
	writeRetainedJournalFixtureFrame(t, output, frame, 0)
	entered, release := make(chan *os.File, 1), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	readDone := make(chan error, 1)
	go func() {
		data := make([]byte, len(body))
		_, err := reader.readAt(data, locations[0].offset, func(file *os.File, dst []byte, at int64) (int, error) {
			entered <- file
			<-release
			return file.ReadAt(dst, at)
		})
		if err == nil && !bytes.Equal(data, body) {
			err = errors.New("closing read returned different bytes")
		}
		readDone <- err
	}()
	var file *os.File
	select {
	case file = <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("read did not reach descriptor gate")
	}
	require.NoError(t, output.Close())
	closed := make(chan error, 1)
	go func() { closed <- reader.Close() }()
	require.Eventually(t, func() bool { reader.store.mu.RLock(); defer reader.store.mu.RUnlock(); return reader.store.closed }, time.Second, time.Millisecond)
	_, err = file.Stat()
	require.NoError(t, err, "owner cannot close an issued read's cached descriptor")
	select {
	case <-closed:
		t.Fatal("reader closed while issued disk read was blocked")
	default:
	}
	_, err = reader.ReadAt(make([]byte, 1), 0)
	require.ErrorIs(t, err, errJournalClosed)
	unblock()
	select {
	case err := <-readDone:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("held read did not finish")
	}
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("reader did not close after read finished")
	}
	_, err = file.Stat()
	require.ErrorIs(t, err, os.ErrClosed)
}

func TestJournalSegmentCachedDescriptorObeysRetainedSourceAndRetirement(t *testing.T) {
	dir, first, _, _ := seedRetainedCheckpointFixture(t)
	extentBytes := int64(len(first))
	output, reader, _ := openTestRetainedJournalSegments(t, dir, false, extentBytes, 4096, journalSegmentHooks{}, nil)
	var cached *os.File
	data := make([]byte, len(first))
	_, err := reader.readAt(data, 0, func(file *os.File, dst []byte, at int64) (int, error) { cached = file; return file.ReadAt(dst, at) })
	require.NoError(t, err)
	lease, err := reader.store.retainSource(journalRetirementRange{group: 11, start: 0, end: extentBytes}, nil, false)
	require.NoError(t, err)
	defer lease.release()
	_, err = installJournalSegmentCheckpointFixture(output, reader, 1, extentBytes)
	require.NoError(t, err)
	before := reader.store.quota.used
	require.NoError(t, output.reclaimSealedFiles())
	require.FileExists(t, cached.Name(), "retained source protects its selected extent")
	_, err = lease.ReadAt(data, 0)
	require.NoError(t, err)
	require.Equal(t, first, data)
	_, err = cached.Stat()
	require.NoError(t, err, "retained source reads must share the reusable extent handle")
	lease.release()
	require.NoError(t, output.reclaimSealedFiles())
	require.NoFileExists(t, cached.Name(), "idle cached descriptor cannot retain a retired extent")
	_, err = cached.Stat()
	require.ErrorIs(t, err, os.ErrClosed, "retirement must close cached handle before releasing disk bytes")
	require.Equal(t, before-extentBytes, reader.store.quota.used)
}

func TestJournalSegmentCachedDescriptorAllowsExistingSourceDuringClose(t *testing.T) {
	body := []byte("retained through close")
	frame, locations, err := encodeJournalFrame(nil, 1, []journalRecord{{group: 11, body: body}})
	require.NoError(t, err)
	output, reader, _ := openTestRetainedJournalSegments(t, t.TempDir(), true, 4096, 64<<10, journalSegmentHooks{}, nil)
	writeRetainedJournalFixtureFrame(t, output, frame, 0)
	var cached *os.File
	data := make([]byte, len(body))
	_, err = reader.readAt(data, locations[0].offset, func(file *os.File, dst []byte, at int64) (int, error) { cached = file; return file.ReadAt(dst, at) })
	require.NoError(t, err)
	lease, err := reader.store.retainSource(journalRetirementRange{group: 11, start: locations[0].offset, end: locations[0].offset + int64(len(body))}, nil, false)
	require.NoError(t, err)
	defer lease.release()
	require.NoError(t, output.Close())
	closed := make(chan error, 1)
	go func() { closed <- reader.Close() }()
	require.Eventually(t, func() bool { reader.store.mu.RLock(); defer reader.store.mu.RUnlock(); return reader.store.closed }, time.Second, time.Millisecond)
	_, err = lease.ReadAt(data, 0)
	require.NoError(t, err)
	require.Equal(t, body, data)
	_, err = cached.Stat()
	require.NoError(t, err)
	select {
	case <-closed:
		t.Fatal("existing retained source must hold owner closure")
	default:
	}
	lease.release()
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("reader did not close after source release")
	}
	_, err = cached.Stat()
	require.ErrorIs(t, err, os.ErrClosed)
}
