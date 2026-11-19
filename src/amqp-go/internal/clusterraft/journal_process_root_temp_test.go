package clusterraft

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func seedProcessRootTemp(t *testing.T, dir string) string {
	t.Helper()
	file, err := os.CreateTemp(dir, processRootTempPrefix)
	require.NoError(t, err)
	_, err = file.Write(bytes.Repeat([]byte{0xa7}, 4096))
	require.NoError(t, err)
	require.NoError(t, file.Close())
	return file.Name()
}

func TestProcessRootTempsCollectedAfterColdRootedRecovery(t *testing.T) {
	dir := t.TempDir()
	node := openPrototypeProcessNode(t, dir, "temp-kernel", true, true, nil)
	group := openTestJournalGroup(t, node, Config{ID: 1, GroupID: 11, Dir: filepath.Join(dir, "group"), Peers: []uint64{1}, Machine: &testMachine{}, TickInterval: -1, DeferJournalCommit: true, ProcessDurability: true})
	require.NoError(t, group.Campaign(t.Context()))
	body := bytes.Repeat([]byte{0xc3}, 32<<10)
	_, err := group.Propose(t.Context(), body)
	require.NoError(t, err)
	require.NoError(t, waitTestLocalWrites(t.Context(), group))
	// Capture the kernel image before closing either owner can certify it.
	image := copyPrototypeImage(t, dir)
	roots := make(map[string][]byte)
	for _, name := range []string{processPhysicalRoot, processLiveRoot} {
		roots[name], err = os.ReadFile(filepath.Join(image, name))
		require.NoError(t, err)
	}
	var stale []string
	for range 8 {
		stale = append(stale, seedProcessRootTemp(t, image))
	}
	unrelated := filepath.Join(image, ".process-rootish-keep")
	require.NoError(t, os.WriteFile(unrelated, []byte("unrelated"), 0600))
	quota := &journalSegmentQuota{limit: 2 << 20}
	artifacts, err := inventoryJournalSegmentArtifacts(image, quota)
	require.NoError(t, err)
	for _, name := range stale {
		charge, found := artifacts[filepath.Base(name)]
		require.True(t, found)
		require.Zero(t, charge, "unpublished root scratch must remain uncharged")
	}
	recovered := openPrototypeProcessNode(t, image, "temp-kernel", false, true, nil)
	for _, name := range stale {
		require.NoFileExists(t, name)
	}
	for name, before := range roots {
		after, err := os.ReadFile(filepath.Join(image, name))
		require.NoError(t, err)
		require.Equal(t, before, after, "cleanup cannot alter either valid recovery root")
	}
	kept, err := os.ReadFile(unrelated)
	require.NoError(t, err)
	require.Equal(t, []byte("unrelated"), kept)
	require.Equal(t, quota.used, recovered.reader.(*journalSegmentReader).store.quota.used)
	restored := openTestJournalGroup(t, recovered, Config{ID: 1, GroupID: 11, Dir: filepath.Join(image, "group"), Machine: &testMachine{}, TickInterval: -1, ProcessDurability: true})
	require.Equal(t, group.Status().Commit, restored.Status().Commit)
	// Remove any command cache before retaining a cold extent-backed reader.
	restored.mu.Lock()
	recovered.recentEntries.clear()
	last := restored.store.state.hard.GetCommit()
	entries, err := restored.store.Entries(last, last+1, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, entries, 1)
	source, err := restored.store.commandSource(entries[0])
	require.NoError(t, err)
	retained, release, err := retainJournalSourceFixture(source)
	require.NoError(t, err)
	restored.mu.Unlock()
	defer release()
	actual := make([]byte, len(body))
	require.NoError(t, readJournalAt(retained, actual, 0))
	require.Equal(t, body, actual, "stale scratch collection must preserve cold payload bytes")
}

func TestProcessRootTempsDoNotBlockFreshBootstrap(t *testing.T) {
	for _, process := range []bool{false, true} {
		t.Run(map[bool]string{false: "strict", true: "process"}[process], func(t *testing.T) {
			dir := t.TempDir()
			stale := seedProcessRootTemp(t, dir)
			hooks := journalSegmentHooks{}
			if process {
				hooks.processRoots, hooks.processBootID = true, "fresh-kernel"
			}
			node, err := openSegmentedGroupNode(dir, true, false, 64<<10, 2<<20, nil, hooks)
			require.NoError(t, err)
			t.Cleanup(func() { _ = node.close() })
			require.NoFileExists(t, stale)
			require.FileExists(t, filepath.Join(dir, journalSegmentManifestName))
			if process {
				_, err = readProcessRecoveryRoot(filepath.Join(dir, processPhysicalRoot))
				require.NoError(t, err)
				_, err = readProcessRecoveryRoot(filepath.Join(dir, processLiveRoot))
				require.NoError(t, err)
			}
		})
	}
}

func TestProcessRootTempsWaitForValidAuthority(t *testing.T) {
	for _, tc := range []struct {
		name, artifact string
		create         bool
	}{
		{"missing-manifest-reopen", "", false},
		{"extent-without-manifest", "extent-0000000000000001.raft-journal", true},
		{"manifest-temp-without-manifest", journalSegmentManifestTempPrefix + "old", true},
		{"corrupt-manifest", journalSegmentManifestName, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			stale := seedProcessRootTemp(t, dir)
			if tc.artifact != "" {
				require.NoError(t, os.WriteFile(filepath.Join(dir, tc.artifact), []byte("invalid authority"), 0600))
			}
			_, err := openSegmentedGroupNode(dir, tc.create, false, 64<<10, 2<<20, nil, journalSegmentHooks{})
			require.Error(t, err)
			require.FileExists(t, stale, "failed authority validation must not collect scratch")
			if tc.artifact != "" {
				require.FileExists(t, filepath.Join(dir, tc.artifact))
			}
		})
	}
	t.Run("corrupt-physical-root", func(t *testing.T) {
		dir := t.TempDir()
		node := openPrototypeProcessNode(t, dir, "corrupt-kernel", true, true, nil)
		require.NoError(t, node.close())
		stale := seedProcessRootTemp(t, dir)
		root := filepath.Join(dir, processPhysicalRoot)
		before, err := os.ReadFile(root)
		require.NoError(t, err)
		before[len(before)-1] ^= 0xff
		require.NoError(t, os.WriteFile(root, before, 0600))
		_, err = openSegmentedGroupNode(dir, false, false, 64<<10, 2<<20, nil, journalSegmentHooks{processRoots: true, processBootID: "corrupt-kernel"})
		require.ErrorIs(t, err, errJournalCorrupt)
		require.FileExists(t, stale)
		after, err := os.ReadFile(root)
		require.NoError(t, err)
		require.Equal(t, before, after)
	})
	t.Run("nonregular-root-temp", func(t *testing.T) {
		dir := t.TempDir()
		unrelated := filepath.Join(dir, "keep")
		require.NoError(t, os.WriteFile(unrelated, []byte("unrelated"), 0600))
		link := filepath.Join(dir, processRootTempPrefix+"link")
		require.NoError(t, os.Symlink(unrelated, link))
		_, err := openSegmentedGroupNode(dir, true, false, 64<<10, 2<<20, nil, journalSegmentHooks{})
		require.ErrorIs(t, err, errJournalCorrupt)
		kept, err := os.ReadFile(unrelated)
		require.NoError(t, err)
		require.Equal(t, []byte("unrelated"), kept)
		_, err = os.Lstat(link)
		require.NoError(t, err)
		require.NoFileExists(t, filepath.Join(dir, journalSegmentManifestName))
	})
}
