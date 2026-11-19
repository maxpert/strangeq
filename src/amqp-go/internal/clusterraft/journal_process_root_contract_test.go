package clusterraft

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func contractBoot(boot string) func(string) ([]byte, error) {
	return func(path string) ([]byte, error) {
		if path != "/proc/sys/kernel/random/boot_id" {
			return nil, errors.New("unexpected boot identity path")
		}
		return []byte(boot), nil
	}
}

func openContractProcessNode(t *testing.T, dir, boot string, create, process bool) *journalGroupNode {
	t.Helper()
	hooks, extent, err := processRootServingOptionsForPlatform(dir, create, JournalOptions{ProcessDurability: process}, "linux", contractBoot(boot))
	require.NoError(t, err)
	node, err := openSegmentedGroupNode(dir, create, false, extent, journalGroupMaxFileBytes, nil, hooks)
	require.NoError(t, err)
	if process {
		node.writer.durability = &journalDurability{}
	} else {
		require.NoError(t, node.writer.requireFullDurability())
	}
	t.Cleanup(func() { _ = node.close() })
	return node
}

func TestProcessRootServingOptionsContract(t *testing.T) {
	for _, tc := range []struct {
		name, platform  string
		process, rooted bool
	}{
		{"linux-process", "linux", true, true},
		{"linux-strict", "linux", false, false},
		{"darwin-process", "darwin", true, false},
		{"darwin-strict", "darwin", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			hooks, extent, err := processRootServingOptionsForPlatform(t.TempDir(), true, JournalOptions{ProcessDurability: tc.process}, tc.platform, func(string) ([]byte, error) {
				calls++
				return []byte("kernel-A\n"), nil
			})
			require.NoError(t, err)
			require.Equal(t, tc.rooted, hooks.processRoots)
			if tc.rooted {
				require.Equal(t, "kernel-A", hooks.processBootID)
				require.EqualValues(t, 32<<20, extent)
				require.Equal(t, 1, calls)
			} else {
				require.Empty(t, hooks.processBootID)
				require.EqualValues(t, journalServingExtentBytes, extent)
				require.Zero(t, calls)
			}
		})
	}
	t.Run("boot-fails-closed", func(t *testing.T) {
		for _, read := range []func(string) ([]byte, error){
			contractBoot(" \n"),
			func(string) ([]byte, error) { return nil, os.ErrPermission },
		} {
			_, _, err := processRootServingOptionsForPlatform(t.TempDir(), true, JournalOptions{ProcessDurability: true}, "linux", read)
			require.ErrorContains(t, err, "kernel boot identity")
		}
	})
	t.Run("unrooted-reopen-and-conversion-rejected", func(t *testing.T) {
		dir := t.TempDir()
		_, _, err := processRootServingOptionsForPlatform(dir, false, JournalOptions{ProcessDurability: true}, "linux", contractBoot("kernel-A"))
		require.ErrorContains(t, err, "root missing on reopen")
		strict, err := OpenNodeJournal(dir, true)
		require.NoError(t, err)
		require.NoError(t, strict.Close())
		hooks, extent, err := processRootServingOptionsForPlatform(dir, true, JournalOptions{ProcessDurability: true}, "linux", contractBoot("kernel-A"))
		require.NoError(t, err)
		_, err = openSegmentedGroupNode(dir, true, false, extent, journalGroupMaxFileBytes, nil, hooks)
		require.ErrorContains(t, err, "fresh journal directory")
	})
	t.Run("orphan-live-root-rejected", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dir, processLiveRoot), []byte("orphan"), 0600))
		_, _, err := processRootServingOptionsForPlatform(dir, true, JournalOptions{}, "linux", contractBoot("kernel-A"))
		require.ErrorContains(t, err, "orphan process live root")
	})
}

func TestProcessRootVersionContract(t *testing.T) {
	dir := t.TempDir()
	node := openContractProcessNode(t, dir, "kernel-A", true, true)
	require.NoError(t, node.close())
	for _, name := range []string{processPhysicalRoot, processLiveRoot} {
		root, err := readProcessRecoveryRoot(filepath.Join(dir, name))
		require.NoError(t, err)
		require.EqualValues(t, processRecoveryRootVersion, root.Version)
		original, err := os.ReadFile(filepath.Join(dir, name))
		require.NoError(t, err)
		for _, version := range []string{"missing", "unknown"} {
			t.Run(name+"/"+version, func(t *testing.T) {
				image := copyPrototypeImage(t, dir)
				var fields map[string]json.RawMessage
				require.NoError(t, json.Unmarshal(original[:len(original)-4], &fields))
				if version == "missing" {
					delete(fields, "version")
				} else {
					fields["version"] = json.RawMessage("99")
				}
				data, err := json.Marshal(fields)
				require.NoError(t, err)
				data = binary.BigEndian.AppendUint32(data, crc32.Checksum(data, journalChecksum))
				require.NoError(t, os.WriteFile(filepath.Join(image, name), data, 0600))
				_, err = readProcessRecoveryRoot(filepath.Join(image, name))
				require.ErrorIs(t, err, errJournalCorrupt)
				require.ErrorContains(t, err, "unsupported process recovery root version")
				// Physical-root rejection occurs before open; live-root rejection
				// occurs after the normal directory lock during same-boot recovery.
				hooks, extent, err := processRootServingOptionsForPlatform(image, false, JournalOptions{}, "linux", contractBoot("kernel-A"))
				if err == nil {
					_, err = openSegmentedGroupNode(image, false, false, extent, journalGroupMaxFileBytes, nil, hooks)
				}
				require.ErrorIs(t, err, errJournalCorrupt)
			})
		}
	}
	_, _, err := processRootServingOptionsForPlatform(dir, false, JournalOptions{}, "darwin", contractBoot("kernel-A"))
	require.ErrorContains(t, err, "require Linux kernel boot identity")
}

func TestProcessRootServingRestartTransitions(t *testing.T) {
	dir := t.TempDir()
	node := openContractProcessNode(t, dir, "kernel-A", true, true)
	group := openTestJournalGroup(t, node, Config{ID: 1, GroupID: 11, Dir: filepath.Join(dir, "group"), Peers: []uint64{1}, Machine: &testMachine{}, TickInterval: -1, DeferJournalCommit: true, ProcessDurability: true})
	require.NoError(t, group.Campaign(t.Context()))
	_, err := group.Propose(t.Context(), []byte("certified"))
	require.NoError(t, err)
	require.NoError(t, waitTestLocalWrites(t.Context(), group))
	fence, err := node.writer.enqueueEncoded(1, 0, true, nil, func([]byte) error { return nil })
	require.NoError(t, err)
	require.NoError(t, (<-fence).err)
	physicalCommit := group.Status().Commit
	physical, err := readProcessRecoveryRoot(filepath.Join(dir, processPhysicalRoot))
	require.NoError(t, err)
	_, err = group.Propose(t.Context(), []byte("kernel-only"))
	require.NoError(t, err)
	require.NoError(t, waitTestLocalWrites(t.Context(), group))
	liveCommit := group.Status().Commit
	require.Greater(t, liveCommit, physicalCommit)
	for _, tc := range []struct {
		name, boot    string
		process, live bool
	}{
		{"same-boot-process", "kernel-A", true, true},
		{"same-boot-strict-activation", "kernel-A", false, true},
		{"changed-boot-process", "kernel-B", true, false},
		{"changed-boot-strict", "kernel-B", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			image := copyPrototypeImage(t, dir)
			recovered := openContractProcessNode(t, image, tc.boot, false, tc.process)
			restored := openTestJournalGroup(t, recovered, Config{ID: 1, GroupID: 11, Dir: filepath.Join(image, "group"), Machine: &testMachine{}, TickInterval: -1, ProcessDurability: tc.process})
			wantCommit := physicalCommit
			if tc.live {
				wantCommit = liveCommit
			}
			require.Equal(t, wantCommit, restored.Status().Commit)
			roots := recovered.reader.(*journalSegmentReader).store.processRoots
			require.Equal(t, tc.process, roots.process.Load())
			certified, err := readProcessRecoveryRoot(filepath.Join(image, processPhysicalRoot))
			require.NoError(t, err)
			require.Equal(t, tc.boot, certified.Boot)
			if !tc.process && tc.live {
				require.Greater(t, certified.End, physical.End)
			}
		})
	}
}

func TestProcessRootNormalOptionsBootstrap(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("rooted serving bootstrap requires Linux")
	}
	for _, pair := range []bool{false, true} {
		dir := t.TempDir()
		if pair {
			node, err := OpenNodeJournalPairWithOptions(dir, true, 1, JournalOptions{ProcessDurability: true})
			require.NoError(t, err)
			for _, stream := range node.streams {
				require.NotNil(t, stream.store.reader.(*journalSegmentReader).store.processRoots)
			}
			require.NoError(t, node.Close())
			reopened, err := OpenNodeJournalPairWithOptions(dir, false, 1, JournalOptions{})
			require.NoError(t, err)
			require.NoError(t, reopened.Close())
		} else {
			node, err := OpenNodeJournalWithOptions(dir, true, JournalOptions{ProcessDurability: true})
			require.NoError(t, err)
			require.NotNil(t, node.store.reader.(*journalSegmentReader).store.processRoots)
			require.NoError(t, node.Close())
			reopened, err := OpenNodeJournal(dir, false)
			require.NoError(t, err)
			require.False(t, reopened.store.reader.(*journalSegmentReader).store.processRoots.process.Load())
			require.NoError(t, reopened.Close())
		}
	}
}
