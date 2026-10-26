package storage

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// review-4 N-1 — the metadata tier has the SAME client-controlled-name hole as
// the segment tier, and Step 4 logged it as "different tier".
//
// It is the same defect with the same silent-loss shape. The reviewer executed
// StoreQueue("../../../pwned-queue") -> err=nil, with the CBOR file written
// OUTSIDE the data directory. Beyond the arbitrary-write primitive, ListQueues
// enumerates metadata/queues/*.cbor, so such a queue's record — including its
// write-once delivery-tag Ordinal — is invisible at the next boot, after which
// existingOrdinalLocked assigns a FRESH ordinal and every confirmed durable
// record already on disk under the old ordinal takes recovery's Case B "dead
// incarnation" branch and is discarded.
//
// The mechanism is the same one chosen for B-1: the artifact carries its own
// identity and the reader CHECKS it. For segments that identity is a marker file
// inside the directory; for metadata the CBOR record's own Name field already is
// one, so the filename becomes a slot rather than a decodable encoding.
// ---------------------------------------------------------------------------

// hostileNames are exactly the shapes an authenticated client can send in
// queue.declare / exchange.declare / basic.consume. Nothing between the wire and
// filepath.Join inspects them.
func hostileNames() []string {
	return []string{
		"../../../pwned-queue",
		"../escaped",
		"..",
		".",
		"",
		"a/b/c",
		"/etc/shadow-ish",
		"nul\x00byte",
		"%2f",
		"%",
		// Aimed straight at the canary file three levels up.
		"../../../outside/victim",
		// A legal 255-byte AMQP shortstr. name+".cbor"+".tmp" is 264 bytes, so
		// atomicWrite's temp file is past NAME_MAX and this queue can never
		// persist its write-once Ordinal at all.
		strings.Repeat("z", 255),
		"/" + strings.Repeat("y", 254),
	}
}

// canaryTree builds <root>/data plus a sibling <root>/outside that a traversal
// would land in, and returns both.
func canaryTree(t *testing.T) (dataDir, canaryDir string) {
	t.Helper()
	root := t.TempDir()
	dataDir = filepath.Join(root, "data")
	canaryDir = filepath.Join(root, "outside")
	require.NoError(t, os.MkdirAll(dataDir, 0o755))
	require.NoError(t, os.MkdirAll(canaryDir, 0o755))
	return dataDir, canaryDir
}

// filesUnder lists every regular file at or below dir, relative to it.
func filesUnder(t *testing.T, dir string) []string {
	t.Helper()
	var out []string
	err := filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		rel, rerr := filepath.Rel(dir, p)
		if rerr != nil {
			return rerr
		}
		out = append(out, rel)
		return nil
	})
	require.NoError(t, err)
	sort.Strings(out)
	return out
}

func TestMetadataPath_ClientControlledNamesCannotEscapeTheDataDirectory(t *testing.T) {
	dataDir, canaryDir := canaryTree(t)
	root := filepath.Dir(dataDir)
	pm, err := NewPersistentMetadataStore(dataDir)
	require.NoError(t, err)

	// A file the traversal could clobber or unlink. metadata/queues/<name>.cbor
	// with name="../../outside/victim" resolves exactly here.
	victim := filepath.Join(canaryDir, "victim.cbor")
	require.NoError(t, os.WriteFile(victim, []byte("do not touch"), 0o644))

	for _, name := range hostileNames() {
		t.Run(name, func(t *testing.T) {
			// Each subtest is independent: restore the canary so a failure is
			// attributed to the name that caused it, not to an earlier one.
			require.NoError(t, os.WriteFile(victim, []byte("do not touch"), 0o644))
			// The store may refuse a name it cannot represent; what it may NOT
			// do is write, read or unlink outside the metadata directory.
			_ = pm.StoreQueue(&protocol.Queue{Name: name, Durable: true, Ordinal: 7})
			_ = pm.StoreExchange(&protocol.Exchange{Name: name, Kind: "direct", Durable: true})
			_ = pm.StoreBinding(name, name, name, nil)
			_ = pm.StoreExchangeBinding(name, name, name, nil)
			// DeleteConsumer is reachable from basic.cancel with a
			// client-controlled queue name AND consumer tag, and it is an
			// os.Remove.
			_ = pm.DeleteConsumer(name, name)
			_, _ = pm.GetConsumer(name, name)

			body, rerr := os.ReadFile(victim)
			require.NoError(t, rerr, "ESCAPE: name %q unlinked a file outside the data directory", name)
			require.Equal(t, "do not touch", string(body),
				"ESCAPE: name %q overwrote a file outside the data directory", name)

			// The whole ROOT is walked, not just the data directory: the
			// reviewer's own reproduction landed pwned-queue.cbor as a SIBLING
			// of the data directory, which a walk rooted at dataDir cannot see.
			wantPrefix := filepath.Join("data", MetadataDir) + string(filepath.Separator)
			for _, f := range filesUnder(t, root) {
				if f == filepath.Join("outside", "victim.cbor") {
					continue
				}
				require.True(t, strings.HasPrefix(f, wantPrefix),
					"ESCAPE: name %q created %s, outside the metadata directory", name, f)
			}
		})
	}
}

// TestMetadataPath_OrdinaryNamesKeepTheirExistingFile is the migration half:
// every name that is already a legal single path element must keep the EXACT
// file it has today, including a "%"-prefixed one. review-4's B-1 was caused by
// a scheme that made "%" special; the metadata tier must not repeat it.
func TestMetadataPath_OrdinaryNamesKeepTheirExistingFile(t *testing.T) {
	dataDir := t.TempDir()
	pm, err := NewPersistentMetadataStore(dataDir)
	require.NoError(t, err)

	ordinary := []string{
		"orders", "my.queue", "amq.gen-JzTY6a2Cs0M1F0J2vTvKLg",
		"orders queue", "queue:with:colons", "héllo-ünicode",
		"50%off", "%2f", "%", "%deadbeef", `a\b`,
		// The longest name the OLD build could actually write: atomicWrite's
		// temp file is name + ".cbor" + ".tmp", so 246 bytes is the boundary and
		// 247 already fails. Anything at or below it must keep its exact file.
		strings.Repeat("q", 246),
	}
	for _, name := range ordinary {
		require.NoError(t, pm.StoreQueue(&protocol.Queue{Name: name, Durable: true, Ordinal: 3}))
		want := filepath.Join(dataDir, MetadataDir, QueuesDir, name+FileExtension)
		_, serr := os.Stat(want)
		require.NoError(t, serr,
			"queue %q must still use the metadata file it uses today: %s", name, want)
	}
}

// TestMetadataPath_EveryStoredNameSurvivesListAndGet is the property that makes
// the traversal fix safe: whatever slot a record lands in, ListQueues must
// attribute it to the queue that wrote it, and GetQueue must find it by name.
// A queue whose record is invisible at boot loses its write-once Ordinal.
func TestMetadataPath_EveryStoredNameSurvivesListAndGet(t *testing.T) {
	dataDir := t.TempDir()
	pm, err := NewPersistentMetadataStore(dataDir)
	require.NoError(t, err)

	names := append(hostileNames(),
		"orders", "%2f", "/", "%", "50%off", "héllo-ünicode", `a\b`, "café",
	)
	stored := make(map[string]uint64)
	for i, name := range names {
		if _, dup := stored[name]; dup {
			continue
		}
		ordinal := uint64(100 + i)
		if err := pm.StoreQueue(&protocol.Queue{Name: name, Durable: true, Ordinal: ordinal}); err != nil {
			// A refusal is acceptable; silent loss is not. Record that it was
			// refused and assert it stays absent rather than half-written.
			_, gerr := pm.GetQueue(name)
			require.Error(t, gerr,
				"queue %q was refused by StoreQueue but is readable back — a half-written record", name)
			continue
		}
		stored[name] = ordinal
	}
	require.NotEmpty(t, stored, "PREMISE: the fixture must store at least one record")

	// A fresh store, so nothing is answered from the write-side cache.
	pm2, err := NewPersistentMetadataStore(dataDir)
	require.NoError(t, err)

	listed := make(map[string]uint64)
	queues, err := pm2.ListQueues()
	require.NoError(t, err)
	for _, q := range queues {
		if prev, dup := listed[q.Name]; dup {
			require.Failf(t, "duplicate", "queue %q listed twice (ordinals %d and %d)", q.Name, prev, q.Ordinal)
		}
		listed[q.Name] = q.Ordinal
	}

	for name, ordinal := range stored {
		got, ok := listed[name]
		require.True(t, ok,
			"INVISIBLE AT BOOT: queue %q was stored but ListQueues did not return it; its write-once "+
				"Ordinal %d is lost and a fresh one will be minted over it", name, ordinal)
		require.Equal(t, ordinal, got, "queue %q came back with another queue's ordinal", name)

		q, gerr := pm2.GetQueue(name)
		require.NoError(t, gerr, "queue %q was stored but GetQueue cannot find it", name)
		require.Equal(t, name, q.Name, "GetQueue(%q) returned another queue's record", name)
		require.Equal(t, ordinal, q.Ordinal)
	}
}

// ---------------------------------------------------------------------------
// review-4 N-4 — "every fault names its artifact by absolute path" held only
// because the tests used t.TempDir()
// ---------------------------------------------------------------------------

// TestStoragePath_ArtifactPathsAreAbsoluteAtShippedDefaults pins N-4.
//
// step4.md §5 asserts that every Fatal/Degraded fault names its artifact by
// absolute path, and the tests asserted filepath.IsAbs — and passed, because
// t.TempDir() is absolute. There was no filepath.Abs anywhere in production code
// and config.DefaultConfig() ships Storage.Path: "./data", so at SHIPPED DEFAULTS
// the refusal message told an operator to "move the listed files OUT OF THE
// DIRECTORY" while naming them relative to whatever directory the process
// started in.
//
// The fixture's premise is the relative path itself, so it is asserted.
func TestStoragePath_ArtifactPathsAreAbsoluteAtShippedDefaults(t *testing.T) {
	t.Chdir(t.TempDir())

	const relative = "./data" // config.DefaultConfig()'s Storage.Path, verbatim
	require.False(t, filepath.IsAbs(relative),
		"PREMISE: the fixture must feed a RELATIVE data directory, or it asserts nothing")

	ds, err := NewDisruptorStorageWithEngineConfig(relative, interfaces.EngineConfig{})
	require.NoError(t, err)
	defer ds.Close()

	require.True(t, filepath.IsAbs(ds.dataDir),
		"an operator acting on a refusal message gets %q, which is relative to the process CWD",
		ds.dataDir)
	require.NotNil(t, ds.segments)
	require.True(t, filepath.IsAbs(ds.segments.dataDir),
		"segment faults would name %q, which is relative to the process CWD", ds.segments.dataDir)

	// And a real fault carries it. A queue name that cannot be a path element
	// forces a generated directory, whose Degraded fault names a full path.
	require.NoError(t, writeLegacyBatchForTest(ds.segments, "../nope", segTestMessages("../nope", []uint64{1})))
	val, ok := ds.segments.queueSegments.Load("../nope")
	require.True(t, ok)
	require.True(t, filepath.IsAbs(val.(*QueueSegments).dataDir),
		"a segment directory must be named absolutely, got %q", val.(*QueueSegments).dataDir)
}

// TestMetadataPath_AGeneratedSlotCanNeverDisplaceALiteralOne pins the reason the
// generated slots live in a subdirectory.
//
// Queue "/" needs a generated slot, and the obvious generated spelling of "/" is
// "%2f.cbor" — which is queue "%2f"'s OWN literal file. In one flat directory,
// whichever declared first would take it and the other would have to be refused,
// so a previously-legal name ("%2f" has always worked) could start failing
// because of a name that never worked. That is review-4 B-2's shape, and this
// test exists so nobody flattens the namespaces later.
func TestMetadataPath_AGeneratedSlotCanNeverDisplaceALiteralOne(t *testing.T) {
	for _, order := range [][2]string{{"/", "%2f"}, {"%2f", "/"}} {
		t.Run(order[0]+"_then_"+order[1], func(t *testing.T) {
			dataDir := t.TempDir()
			pm, err := NewPersistentMetadataStore(dataDir)
			require.NoError(t, err)

			require.True(t, metadataSlotIsEscaped("/"),
				"PREMISE: %q must need a generated slot, or this fixture asserts nothing", "/")
			require.False(t, metadataSlotIsEscaped("%2f"),
				"PREMISE: %q must keep its literal slot — it has always worked", "%2f")

			ordinals := map[string]uint64{order[0]: 11, order[1]: 22}
			for _, name := range order {
				require.NoError(t, pm.StoreQueue(&protocol.Queue{
					Name: name, Durable: true, Ordinal: ordinals[name],
				}), "queue %q must be storable regardless of declare order", name)
			}

			fresh, err := NewPersistentMetadataStore(dataDir)
			require.NoError(t, err)
			listed := map[string]uint64{}
			queues, err := fresh.ListQueues()
			require.NoError(t, err)
			for _, q := range queues {
				listed[q.Name] = q.Ordinal
			}
			for _, name := range order {
				require.Equal(t, ordinals[name], listed[name],
					"queue %q lost or swapped its write-once Ordinal; listed=%v", name, listed)
				q, gerr := fresh.GetQueue(name)
				require.NoError(t, gerr)
				require.Equal(t, ordinals[name], q.Ordinal)
			}
		})
	}
}
