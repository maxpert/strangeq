package storage

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Queue-name -> segment-directory IDENTITY.
//
// review-4 B-1: the previous scheme reserved "%" as an escape prefix and
// hex-DECODED any directory starting with it. A queue named "%2f" genuinely
// produced <data>/segments/%2f on the old build (it is a perfectly legal single
// path element), so the decode attributed that directory — and every
// checkpointed durable message in it — to a queue named "/". err == nil, no
// fault, no log.
//
// The root problem is that an escape prefix cannot be recognised from the
// STRING ALONE: legacy directories were filepath.Join(dataDir, queueName) for
// any name that was a valid single path element, which includes every possible
// encoded form. These tests pin the property the fix must establish by a
// mechanism OTHER than the string:
//
//	no directory is ever attributed to a queue that did not write it, and
//	no queue's directory is ever missed.
//
// Every fixture below states its premise and asserts it at runtime (canon
// rule 11).
// ---------------------------------------------------------------------------

// legacySegmentDir builds a segment directory EXACTLY the way the pre-fix build
// did — filepath.Join(<data>/segments, queueName) + os.MkdirAll — and writes
// real, CRC-valid segment records into it with the production codec.
//
// It asserts the premise that makes the fixture meaningful: the name really is
// a single directory entry immediately under <data>/segments, i.e. this is a
// name the OLD build wrote literally rather than one that escaped. A fixture
// built from a name that escaped would prove nothing about migration.
func legacySegmentDir(t *testing.T, dataDir, queueName string, offsets []uint64) string {
	t.Helper()

	segRoot := filepath.Join(dataDir, "segments")
	require.NoError(t, os.MkdirAll(segRoot, 0o755))

	dir := filepath.Join(segRoot, queueName)
	require.Equal(t, segRoot, filepath.Dir(dir),
		"PREMISE: %q must be a single directory entry directly under the segments directory; "+
			"a name that escaped is not a name the old build wrote literally", queueName)
	require.NoError(t, os.MkdirAll(dir, 0o755),
		"PREMISE: the old build could create this directory, so it exists in real deployments")

	var buf []byte
	for _, rm := range segTestMessages(queueName, offsets) {
		b, err := serializeSegmentMessage(rm.Message, rm.Offset, false)
		require.NoError(t, err)
		buf = append(buf, b...)
	}
	segPath := filepath.Join(dir, fmt.Sprintf("%020d%s", time.Now().UnixNano(), SegmentFileExtension))
	require.NoError(t, os.WriteFile(segPath, buf, 0o644))

	// A legacy directory carries NO marker of any kind — that is the whole
	// point. Assert it, so a future marker-writing helper cannot silently make
	// this fixture stop being a legacy fixture.
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 1,
		"PREMISE: a legacy directory holds nothing but its segment files (found %v)", entries)

	return dir
}

func recoveredOffsets(msgs []*RecoveryMessage) []uint64 {
	out := make([]uint64, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, m.Offset)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// TestSegmentPath_LegacyDirectoryRecoversUnderItsOwnQueueName is the direct
// reproduction of review-4 B-1. Every name here is one the old build wrote as a
// literal directory, and every one of them must come back under ITS OWN name.
//
// The pre-fix suite enumerated "50%off" — "%" in the middle, the one position
// the scheme did not reserve — and therefore proved nothing about the byte the
// fix made special.
func TestSegmentPath_LegacyDirectoryRecoversUnderItsOwnQueueName(t *testing.T) {
	// Names the old build produced verbatim. The first five are hex-DECODABLE
	// after the "%", which is what made them mis-attributed.
	legacy := []string{
		"%2f",       // percent-encoding of "/" — RabbitMQ's own default-vhost spelling
		"%2F",       // uppercase variant
		"%ab",       // decodes to a single 0xAB byte
		"%",         // decodes to the empty name
		"%deadbeef", // decodes to four garbage bytes
		"%00",       // decodes to NUL
		`a\b`,       // a legal POSIX filename the pre-fix scheme also encoded
		"50%off",    // the control the pre-fix suite already had
		"orders",    // ordinary control
	}

	for _, name := range legacy {
		t.Run(fmt.Sprintf("%q", name), func(t *testing.T) {
			dataDir := t.TempDir()
			legacySegmentDir(t, dataDir, name, []uint64{1, 2, 3})

			recovered, err := reopenAndRecover(t, dataDir)
			require.NoError(t, err)

			msgs, ok := recovered[name]
			require.True(t, ok,
				"LOST: legacy directory %q recovered under keys %q, not under its own queue name",
				name, keysOf(recovered))
			require.Equal(t, []uint64{1, 2, 3}, recoveredOffsets(msgs))
			require.Len(t, recovered, 1,
				"legacy directory %q must produce exactly ONE queue, got %q", name, keysOf(recovered))
		})
	}
}

// TestSegmentPath_LegacyDirectoryIsNotStolenByAnEncodedName is B-1's inverse and
// worse half: queue "/" encodes to "%2f", which is ALSO the legacy directory of
// queue "%2f". Pre-fix both queues resolve to the same physical directory — two
// QueueSegments, two compactors, two .compact reapers over one copy of the data.
func TestSegmentPath_LegacyDirectoryIsNotStolenByAnEncodedName(t *testing.T) {
	dataDir := t.TempDir()

	// Queue "%2f" already owns <data>/segments/%2f, written by the old build.
	legacyDir := legacySegmentDir(t, dataDir, "%2f", []uint64{1, 2, 3})

	sm, err := NewSegmentManagerWithConfig(dataDir, segTestConfig())
	require.NoError(t, err)
	defer sm.Close()

	// Recovery first, exactly as a real boot does.
	recovered, err := sm.RecoverFromSegments()
	require.NoError(t, err)
	require.Contains(t, recovered, "%2f",
		"the legacy directory must be attributed to queue %q, got %q", "%2f", keysOf(recovered))

	// Now a live queue literally named "/" checkpoints. It must NOT land in the
	// directory that belongs to "%2f".
	require.NoError(t, writeLegacyBatchForTest(sm, "/", segTestMessages("/", []uint64{10, 11})))

	slashVal, ok := sm.queueSegments.Load("/")
	require.True(t, ok, "no QueueSegments for queue %q", "/")
	slashDir := slashVal.(*QueueSegments).dataDir
	require.NotEqual(t, legacyDir, slashDir,
		"COLLISION: queue %q adopted the legacy directory of queue %q (%s); one directory, two owners",
		"/", "%2f", legacyDir)

	pctVal, ok := sm.queueSegments.Load("%2f")
	require.True(t, ok, "no QueueSegments for queue %q", "%2f")
	require.Equal(t, legacyDir, pctVal.(*QueueSegments).dataDir,
		"queue %q must keep the directory it already owns", "%2f")

	// And both must survive a restart under their own names.
	require.NoError(t, sm.Close())
	again, err := reopenAndRecover(t, dataDir)
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 2, 3}, recoveredOffsets(again["%2f"]),
		"queue %q lost its records; keys=%q", "%2f", keysOf(again))
	require.Equal(t, []uint64{10, 11}, recoveredOffsets(again["/"]),
		"queue %q lost its records; keys=%q", "/", keysOf(again))
}

// TestSegmentPath_LongNamesAlwaysProduceACreatableDirectory is review-4 B-2.
// The pre-fix encoding is 2x+1, so a 255-byte "%"-prefixed name — legal before
// the change, and a working directory on disk — became 511 bytes, mkdir returned
// ENAMETOOLONG, a Degraded fault was raised, and performCheckpoint discarded it:
// that queue's WAL was then never reclaimed, forever, with no log line.
//
// NAME_MAX is 255 on both APFS and ext4 and is reachable directly; no injected
// hook is involved.
func TestSegmentPath_LongNamesAlwaysProduceACreatableDirectory(t *testing.T) {
	long := func(first byte, n int) string {
		b := make([]byte, n)
		for i := range b {
			b[i] = 'a'
		}
		b[0] = first
		return string(b)
	}

	cases := []struct{ label, name string }{
		// The regression: legal before, ENAMETOOLONG after.
		{"pct_prefixed_255", long('%', 255)},
		// The control review-4 measured as still working.
		{"plain_255", long('b', 255)},
		// Never worked (it escaped), but its encoded form must still be a
		// creatable single path element rather than a permanent failure.
		{"slash_255", "/" + long('c', 254)},
	}

	for _, tc := range cases {
		t.Run(tc.label, func(t *testing.T) {
			name := tc.name
			require.LessOrEqual(t, len(name), 255,
				"PREMISE: an AMQP shortstr cannot exceed 255 bytes, so this is a reachable name")

			dataDir := t.TempDir()
			sm, err := NewSegmentManagerWithConfig(dataDir, segTestConfig())
			require.NoError(t, err)

			err = writeLegacyBatchForTest(sm, name, segTestMessages(name, []uint64{1, 2}))
			require.NoError(t, err,
				"a %d-byte queue name must still checkpoint; a permanent silent checkpoint failure "+
					"means this queue's WAL is never reclaimed again", len(name))

			val, ok := sm.queueSegments.Load(name)
			require.True(t, ok)
			base := filepath.Base(val.(*QueueSegments).dataDir)
			require.LessOrEqual(t, len(base), 255,
				"the generated directory name is %d bytes, past NAME_MAX", len(base))

			require.NoError(t, sm.Close())
			recovered, err := reopenAndRecover(t, dataDir)
			require.NoError(t, err)
			require.Equal(t, []uint64{1, 2}, recoveredOffsets(recovered[name]),
				"a %d-byte queue name lost its records; %d queues recovered", len(name), len(recovered))
		})
	}
}

// segIdentityCorpus builds the adversarial name corpus. Its shape mirrors the
// corpus review-4 used (301,080 names) so the property claim is comparable.
func segIdentityCorpus() []string {
	var corpus []string

	// Exhaustive single- and two-byte names, and "%"+each byte — the shape B-1
	// lives in.
	for b := 0; b < 256; b++ {
		corpus = append(corpus, string([]byte{byte(b)}))
		corpus = append(corpus, "%"+string([]byte{byte(b)}))
		for c := 0; c < 256; c++ {
			corpus = append(corpus, string([]byte{byte(b), byte(c)}))
		}
	}

	// Hand-picked cases, including every one review-4 named.
	corpus = append(corpus,
		"", ".", "..", "/", `\`, "\x00", "%", "%%", "%2f", "%2F", "%2e%2e",
		"%abcd", "%deadbeef", "%foo", "%00", "50%off", "orders", "my.queue",
		"amq.gen-JzTY6a2Cs0M1F0J2vTvKLg", "orders queue", "queue:with:colons",
		"héllo-ünicode", "café", "../../outside/pwned", "../escaped", "a/b/c",
		`a\b`, "a\x00b", ".DS_Store", "/etc/foo", `..\..\x`, ".queue-name",
		strings.Repeat("%", 255), strings.Repeat("a", 255), "%"+strings.Repeat("a", 254),
	)

	// Random names over three adversarial alphabets plus uniform bytes.
	rng := rand.New(rand.NewSource(20260730))
	alphabets := []string{"0123456789abcdefABCDEF%", `%./\`, "\x00\x01\x02\x03%.\\/ab"}
	for _, alpha := range alphabets {
		for i := 0; i < 66667; i++ {
			n := 1 + rng.Intn(12)
			var sb strings.Builder
			for j := 0; j < n; j++ {
				sb.WriteByte(alpha[rng.Intn(len(alpha))])
			}
			corpus = append(corpus, sb.String())
		}
	}
	for i := 0; i < 100000; i++ {
		n := 1 + rng.Intn(20)
		b := make([]byte, n)
		for j := range b {
			b[j] = byte(rng.Intn(256))
		}
		corpus = append(corpus, string(b))
	}

	return corpus
}

// oldBuildWroteLiterally reports whether the PRE-FIX build's
// filepath.Join(<segments>, name) + MkdirAll produced a single directory entry
// named exactly `name` immediately under the segments directory. This is the
// definition of "a legacy directory", derived from the OLD code rather than from
// the new code's own predicate — so the property below is not the fix grading
// its own homework.
func oldBuildWroteLiterally(name string) bool {
	if name == "" || name == "." || name == ".." {
		return false
	}
	if strings.ContainsRune(name, '/') || strings.ContainsRune(name, 0) {
		return false
	}
	return len(name) <= 255
}

// TestSegmentPath_OwnershipPropertiesOverTheAdversarialCorpus is the
// pure-function half of the property test: legacy attribution, path-element
// safety, and NAME_MAX, over the whole corpus.
func TestSegmentPath_OwnershipPropertiesOverTheAdversarialCorpus(t *testing.T) {
	corpus := segIdentityCorpus()
	require.Greater(t, len(corpus), 300000,
		"PREMISE: the corpus must be at least as large as the one review-4 used (301,080)")
	t.Logf("CORPUS SIZE: %d", len(corpus))

	var legacyMisattributed, notSinglePathElement, tooLong int
	var firstLegacy, firstEscape, firstLong string

	for _, name := range corpus {
		// P1 — LEGACY ATTRIBUTION. A directory carrying no ownership marker is,
		// by construction, one the old build wrote literally, so it must be
		// attributed to exactly its own name.
		if oldBuildWroteLiterally(name) {
			if got := segmentDirOwner(name, "", false); got != name {
				legacyMisattributed++
				if firstLegacy == "" {
					firstLegacy = fmt.Sprintf("legacy dir %q -> owner %q (want %q)", name, got, name)
				}
			}
		}

		// P2 — every directory name this build can generate is a single path
		// element strictly under the segments directory.
		for _, cand := range segmentDirCandidates(name) {
			if cand == "" || cand == "." || cand == ".." ||
				strings.ContainsRune(cand, '/') || strings.ContainsRune(cand, 0) ||
				cand != filepath.Base(filepath.Join("root", cand)) {
				notSinglePathElement++
				if firstEscape == "" {
					firstEscape = fmt.Sprintf("name %q -> candidate %q", name, cand)
				}
			}
			// P3 — and it fits in a directory entry.
			if len(cand) > 255 {
				tooLong++
				if firstLong == "" {
					firstLong = fmt.Sprintf("name %q (len %d) -> candidate %q (len %d)",
						name, len(name), cand, len(cand))
				}
			}
		}
	}

	assert.Zero(t, legacyMisattributed,
		"LEGACY-DIR mis-attribution failures: %d; first: %s", legacyMisattributed, firstLegacy)
	assert.Zero(t, notSinglePathElement,
		"ESCAPE failures: %d; first: %s", notSinglePathElement, firstEscape)
	assert.Zero(t, tooLong,
		"NAME_MAX failures: %d; first: %s", tooLong, firstLong)

	// P4 — THE ONE POINT WHERE THE GENERATED AND LITERAL NAMESPACES TOUCH.
	//
	// review-5's attack 3 certified that the two namespaces are "genuinely
	// disjoint, not merely disjoint-for-tested-inputs". That is true of every
	// name a queue can actually have, and it is exactly one counterexample away
	// from being false: segmentDirEncoded("") is the bare prefix "%", which is
	// also the literal directory of a queue named "%" — a name that CAN exist.
	//
	// This property used to be carried, accidentally, by "" sitting in the
	// on-disk corpus of the sibling test. That entry was removed because it
	// asserted the defect (it passed only via a zero-byte marker returning
	// ("", true, nil) — review-5 CRITICAL-1's exact call and return). Removing
	// it was right, but it took this property with it, so the property is pinned
	// here instead: in the PURE-FUNCTION test, where it needs no reachability
	// premise and no queue named "" has to exist for it to mean something
	// (review-7's recommendation).
	//
	// What it protects: if segmentDirEncodedPrefix ever changes, or the encoding
	// grows a length prefix, or "" stops mapping to the bare prefix, the
	// collision moves — and the probe-suffix mechanism that resolves it must
	// still be reached. A queue literally named "%" must keep "%", because it
	// was there first and its directory is legacy.
	require.Equal(t, segmentDirEncodedPrefix, segmentDirEncoded(""),
		"segmentDirEncoded(\"\") is no longer the bare prefix; the literal/generated collision this "+
			"test pins has moved, and the probe-suffix path that resolves it may no longer be exercised")
	require.True(t, segmentDirNameIsLiteral(segmentDirEncodedPrefix),
		"%q is no longer a legal literal queue name, so the collision below is unreachable and this "+
			"assertion no longer pins anything", segmentDirEncodedPrefix)
	require.Equal(t, segmentDirEncodedPrefix, segmentDirCandidates(segmentDirEncodedPrefix)[0],
		"a queue literally named %q must prefer its own literal directory; it owns that name and any "+
			"directory of that spelling is legacy data it wrote", segmentDirEncodedPrefix)
	require.Equal(t, segmentDirEncoded(""), segmentDirCandidates("")[0],
		"the empty name has no literal candidate, so its FIRST candidate must be the generated one — "+
			"which is precisely why it collides with queue %q's literal directory", segmentDirEncodedPrefix)
	require.NotEqual(t, segmentDirCandidates("")[1], segmentDirCandidates(segmentDirEncodedPrefix)[1],
		"the two colliding names must diverge after their first candidate, or the probe mechanism "+
			"cannot separate them")
}

// TestSegmentPath_OwnershipIsInjectiveOnARealFilesystem is the on-disk half:
// distinct queue names must never share a directory, and each must recover
// exactly its own records after a restart. The corpus is smaller than the
// pure-function one because each member costs a real directory, a real segment
// file and a real fdatasync.
func TestSegmentPath_OwnershipIsInjectiveOnARealFilesystem(t *testing.T) {
	// Every collision pair review-4 named, plus the legacy directories that
	// make them collide.
	//
	// "" IS DELIBERATELY NOT IN THIS CORPUS, and its coverage is not dropped —
	// it moved to the explicit refusal assertion below. It was here on the false
	// premise that "" is a queue name storage must serve. It is not: AMQP 0-9-1
	// queue.declare with an empty name means "server, generate one", and
	// server/queue_handlers.go substitutes protocol.GenerateQueueName() before
	// any name reaches this tier. So "" arriving here means a name was INVENTED
	// — by a degenerate ownership marker, which is exactly how review-5
	// CRITICAL-1 handed one queue's checkpointed durable messages to queue "".
	// Asserting that "" checkpoints successfully asserted the defect.
	names := []string{
		"%2f", "/", "%", "%2F", "%ab", "\xab", "%00", "\x00",
		"%deadbeef", "..", ".", "../../outside/pwned", "a/b/c", `a\b`,
		"orders", "50%off", "héllo-ünicode", ".queue-name",
		strings.Repeat("%", 255), "%" + strings.Repeat("a", 254),
	}
	rng := rand.New(rand.NewSource(7))
	alpha := `%./\ab0123456789`
	for i := 0; i < 300; i++ {
		n := 1 + rng.Intn(10)
		var sb strings.Builder
		for j := 0; j < n; j++ {
			sb.WriteByte(alpha[rng.Intn(len(alpha))])
		}
		names = append(names, sb.String())
	}
	// De-duplicate: two identical names are the SAME queue and must share.
	seen := make(map[string]bool, len(names))
	uniq := names[:0]
	for _, n := range names {
		if !seen[n] {
			seen[n] = true
			uniq = append(uniq, n)
		}
	}
	names = uniq
	t.Logf("FILESYSTEM CORPUS SIZE: %d", len(names))

	dataDir := t.TempDir()

	// Seed legacy directories for the "%"-prefixed members, so the collision
	// pairs are live rather than hypothetical.
	legacyPartners := []string{"%2f", "%", "%ab", "%00", "%deadbeef"}
	for _, n := range legacyPartners {
		legacySegmentDir(t, dataDir, n, []uint64{1})
	}

	sm, err := NewSegmentManagerWithConfig(dataDir, segTestConfig())
	require.NoError(t, err)
	_, err = sm.RecoverFromSegments()
	require.NoError(t, err)

	// The empty name's replacement coverage: storage REFUSES it rather than
	// building a queue around it. See the corpus comment above for why this is
	// the correct predicate and the old one asserted the defect.
	require.Error(t, writeLegacyBatchForTest(sm, "", segTestMessages("", []uint64{999})),
		"the empty queue name was accepted; no queue can be named \"\", so a checkpoint under that "+
			"name means a degenerate ownership marker invented an owner (review-5 CRITICAL-1)")
	_, emptyLoaded := sm.queueSegments.Load("")
	require.False(t, emptyLoaded, "a QueueSegments was built for the empty queue name")

	owner := make(map[string]string, len(names))
	for i, n := range names {
		offset := uint64(1000 + i)
		require.NoError(t, writeLegacyBatchForTest(sm, n, segTestMessages(n, []uint64{offset})),
			"queue %q could not checkpoint", n)

		val, ok := sm.queueSegments.Load(n)
		require.True(t, ok, "no QueueSegments for %q", n)
		dir := val.(*QueueSegments).dataDir

		rel, rerr := filepath.Rel(sm.dataDir, dir)
		require.NoError(t, rerr)
		// ".." exactly, or a ".." COMPONENT — not merely a ".." prefix. A queue
		// legitimately named "..4" produces the single entry "..4", and a
		// HasPrefix check reports that as an escape.
		require.False(t, rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)),
			"ESCAPE: queue %q resolved to %q, outside %q", n, dir, sm.dataDir)
		require.Equal(t, rel, filepath.Base(dir),
			"queue %q resolved to a NESTED path %q", n, rel)

		if prev, dup := owner[dir]; dup {
			require.Failf(t, "shared directory",
				"COLLISION: queues %q and %q both resolved to %q", prev, n, dir)
		}
		owner[dir] = n
	}
	require.NoError(t, sm.Close())

	recovered, err := reopenAndRecover(t, dataDir)
	require.NoError(t, err)
	for i, n := range names {
		want := uint64(1000 + i)
		got := recoveredOffsets(recovered[n])
		require.Contains(t, got, want,
			"queue %q did not recover its own record %d (got %v)", n, want, got)
		for _, o := range got {
			require.True(t, o == want || o == 1,
				"queue %q recovered offset %d, which belongs to another queue", n, o)
		}
	}
}

// ---------------------------------------------------------------------------
// review-4 B-3 — the sealSegment sibling race
// ---------------------------------------------------------------------------

// review-4 B-3 is TWO changes, and they are NOT gated by one fixture. The pair
// is:
//
//	(a) readMessage's active-segment branch holds currentSeg.mutex ACROSS the
//	    ReadAt, not just across the index lookup; and
//	(b) sealSegment performs the index copy, the Close and the handle assign
//	    under ONE hold of currentSegment.mutex, instead of copying the index
//	    under the lock and doing `Close(); file = readFile` outside it.
//
// MUTATION MATRIX (this machine, -race -count=1, each arm a full run):
//
//	arm  mutation                                   result
//	A    clean                                      PASS   reads=295,593 closed=0
//	B    (b) reverted alone                         PASS   reads=296,303 closed=0
//	E    (a) reverted alone, (b) clean              FAIL   reads=289,456 closed=25 + DATA RACE
//
// Arm B is why this fixture is named for (a) and not for the seal. Detection
// here is carried ENTIRELY by the reader-side lock. The reason (b) is invisible
// through the public Read path is structural, not luck: writeMessage holds
// qs.mutex across sealSegment()+openNextSegmentLocked() and readMessage takes
// qs.mutex to snapshot currentSeg, so a reader either snapshots BEFORE the
// writer acquires qs.mutex — and then has only a map lookup and a ReadAt left
// to run while the sealer still has a full F_FULLFSYNC ahead of it — or it
// snapshots AFTER and gets the fresh segment. A reader arriving through
// sm.Read is therefore always microseconds ahead of a Close that is
// milliseconds away. Widening the window INSIDE the sealer cannot change that;
// the reviewer confirmed this by inserting a 2 ms sleep between the Close and
// the assign, which also passed.
//
// (b) IS observable, but only to a holder of the *SegmentFile itself — which is
// exactly what readMessage becomes the instant it releases qs.mutex.
// TestSegmentSeal_HandleSwapIsAtomicUnderTheSegmentLock below gates that half
// directly and goes RED on arm B.
//
// The defect both halves prevent: the reader gets `file already closed`, and
// per architecture.md §4 deliverMessage turns a post-claim GetMessage error
// into a GAP SKIP, so the message is dropped rather than retried.
//
// PREMISE — CONCURRENCY: review-4 needed 32 readers; FOUR separate 8-reader
// configurations found nothing. The reader count is therefore a load-bearing
// premise of this fixture and is asserted, not commented.
// segSealRaceSegmentSize holds roughly four segTestMessages records, so a seal
// lands every fourth write and the active segment is normally non-empty.
const segSealRaceSegmentSize = 240

// segSealRaceDuration is the MINIMUM time the fixture runs. It is a REGIME
// parameter, not a timeout: sealSegment fsyncs, and macOS F_FULLFSYNC caps this
// machine at roughly 190 seals/second, so the seal count is a function of wall
// time.
var segSealRaceDuration = 20 * time.Second

// segSealRaceMaxDuration bounds the extension the fixture grants a slow machine
// to reach segSealRaceMinSeals. It exists so the premise below is a one-sided
// LIVENESS bound ("this run eventually reached the regime") rather than a
// throughput assertion ("this machine seals fast enough"), which would flake on
// a loaded CI runner.
//
// RECALIBRATED (WS-C review finding 2, 4x -> 8x): every segment roll now
// fsyncs the queue DIRECTORY at creation (WS-C C4: the directory entry must
// be durable before a checkpoint's WAL unlink relies on the segment). On this
// machine that is one more F_FULLFSYNC per seal — the very same ~20ms-class
// barrier this comment always attributed sealSegment's own fsync to — which
// cuts the fixture's seal rate from ~100/s to ~30-40/s under suite load. At
// that rate 3,000 seals needs ~80-100s of wall time, so the old 4x (80s)
// extension intermittently ran out (observed 2,318-2,776 seals in the full
// window). The durability barrier is correct and stays; only the liveness
// window grows. segSealRaceMinSeals (the mutation-tested detection premise)
// is NOT lowered, and the syncDir is NOT skipped or weakened.
var segSealRaceMaxDuration = 8 * segSealRaceDuration

// segSealRaceMinSeals is the seal count below which this fixture has measured
// nothing and its green is meaningless, so it is asserted rather than assumed.
//
// It is NOT review-4's ~15.5K. That number was in this file's comment and is
// wrong for this fixture: measured over three clean 20-second -race runs here,
// the fixture reaches 3,381 / 3,723 / 4,304 seals (13,525 / 14,891 / 17,215
// writes). Gating at 15.5K would fail every run. 3,000 is the regime the
// fixture actually establishes, and it is a regime that DETECTS: arm E above
// produced 25 dropped reads at 15,339 writes (~3.8K seals), and review-4's own
// three runs produced 8, 12 and 3 at comparable counts.
const segSealRaceMinSeals = 3000

// TestSegmentSeal_ReaderHoldsSegmentLockAcrossActiveRead gates half (a) of
// review-4 B-3: readMessage's active-segment branch must hold currentSeg.mutex
// across the ReadAt. See the mutation matrix above — this fixture is RED for
// arm E and GREEN for arm B, so it gates the reader-side lock and nothing else.
func TestSegmentSeal_ReaderHoldsSegmentLockAcrossActiveRead(t *testing.T) {
	const readers = 32
	require.GreaterOrEqual(t, readers, 32,
		"PREMISE: this race did not manifest below 32 readers in review-4's four 8-reader runs")

	dataDir := t.TempDir()
	cfg := segTestConfig()
	// PREMISE — REGIME: the racing branch is readMessage's ACTIVE-segment
	// branch, so the newest offset must usually still be in the active
	// segment's index. At SegmentSize=1 every write seals immediately, the
	// active segment is always empty, every read takes the (already fixed)
	// sealed branch and the fixture measures nothing: 989 seals / 19,542 reads
	// / 0 errors. review-4 sized a segment at 3 records for exactly this
	// reason; segSealRaceSegmentSize is that, in bytes, for this codec.
	cfg.SegmentSize = segSealRaceSegmentSize
	sm, err := NewSegmentManagerWithConfig(dataDir, cfg)
	require.NoError(t, err)
	defer sm.Close()

	const queueName = "seal-race"
	require.NoError(t, writeLegacyBatchForTest(sm, queueName, segTestMessages(queueName, []uint64{1})))

	var stop atomic.Bool
	var reads, closedErrs, writes atomic.Int64
	var lastOffset atomic.Uint64
	lastOffset.Store(1)
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := uint64(2); !stop.Load(); i++ {
			msg := segTestMessages(queueName, []uint64{i})[0].Message
			if err := writeLegacyForTest(sm, queueName, msg, i); err != nil {
				return
			}
			writes.Add(1)
			lastOffset.Store(i)
		}
	}()

	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				// The NEWEST offset is the one that lives in the ACTIVE segment's
				// index, which is the branch sealSegment races.
				_, rerr := sm.Read(queueName, lastOffset.Load())
				reads.Add(1)
				if rerr != nil && strings.Contains(rerr.Error(), "file already closed") {
					closedErrs.Add(1)
				}
				runtime.Gosched()
			}
		}()
	}

	// Run for at least segSealRaceDuration, then keep going until the seal
	// regime is reached or the extension runs out. See segSealRaceMaxDuration:
	// this is a liveness wait, not a throughput budget.
	started := time.Now()
	for {
		time.Sleep(250 * time.Millisecond)
		elapsed := time.Since(started)
		if elapsed >= segSealRaceMaxDuration {
			break
		}
		if elapsed >= segSealRaceDuration && segSealCount(t, sm, queueName) >= segSealRaceMinSeals {
			break
		}
	}
	stop.Store(true)
	wg.Wait()

	seals := segSealCount(t, sm, queueName)
	t.Logf("reads=%d closedFileErrs=%d writes=%d seals=%d elapsed=%s",
		reads.Load(), closedErrs.Load(), writes.Load(), seals, time.Since(started).Round(time.Second))
	require.GreaterOrEqual(t, seals, segSealRaceMinSeals,
		"PREMISE: this fixture measures nothing without seals to race; it reached only %d in %s, "+
			"below the %d-seal regime that produced review-4's detections (8, 12 and 3 dropped "+
			"reads) and arm E's 25. A green here would be vacuous",
		seals, time.Since(started).Round(time.Second), segSealRaceMinSeals)
	require.Greater(t, writes.Load(), int64(2000),
		"PREMISE: the fixture must produce a large number of seals; %d is outside the regime",
		writes.Load())
	require.Greater(t, reads.Load(), int64(10000),
		"PREMISE: the readers must actually be reading concurrently; only %d reads completed",
		reads.Load())
	require.Zero(t, closedErrs.Load(),
		"DROPPED: %d of %d reads hit `file already closed` because readMessage's ACTIVE-segment "+
			"branch did not hold currentSeg.mutex across the ReadAt, so sealSegment's handle swap "+
			"landed mid-read; deliverMessage turns each one into a gap skip. This fixture gates the "+
			"READER-SIDE lock only — sealSegment's own Close-under-the-lock ordering is gated by "+
			"TestSegmentSeal_HandleSwapIsAtomicUnderTheSegmentLock, which this one does NOT detect "+
			"(mutation arm B above)",
		closedErrs.Load(), reads.Load())
}

// segSealCount reports how many segments this queue has sealed so far. The
// fixtures above assert on it rather than inferring it from the write count,
// because the writes-per-seal ratio is a property of the codec's record size.
func segSealCount(t *testing.T, sm *SegmentManager, queueName string) int {
	t.Helper()
	val, ok := sm.queueSegments.Load(queueName)
	require.True(t, ok, "no QueueSegments for %q", queueName)
	qs := val.(*QueueSegments)
	qs.sealedMutex.RLock()
	defer qs.sealedMutex.RUnlock()
	return len(qs.sealedSegments)
}

// segSealCountNow reports the sealed-segment count WITHOUT failing the test.
// It is safe to call from a helper goroutine (require.* must not run off the
// test goroutine, because it calls t.FailNow).
func segSealCountNow(sm *SegmentManager, queueName string) int {
	val, ok := sm.queueSegments.Load(queueName)
	if !ok {
		return 0
	}
	qs := val.(*QueueSegments)
	qs.sealedMutex.RLock()
	defer qs.sealedMutex.RUnlock()
	return len(qs.sealedSegments)
}

// TestSegmentSeal_HandleSwapIsAtomicUnderTheSegmentLock gates half (b) of
// review-4 B-3 — the half the fixture above does NOT detect (arm B).
//
// The contract sealSegment owes is stated at the *SegmentFile level, because
// that is the level at which it is owed: ANY holder of a *SegmentFile that
// reads seg.file under seg.mutex.RLock() must observe an OPEN handle, at every
// instant, including across the seal. readMessage becomes exactly such a holder
// the moment it releases qs.mutex; that it currently wins the race by a
// fsync-sized margin is an accident of where the fsync sits, not a guarantee,
// and it is not a margin any future caller inherits.
//
// So this fixture holds the pointer directly instead of racing for it. A writer
// publishes the live *SegmentFile plus a valid record position after each
// write; readers spin on that published pair with exactly readMessage's lock
// discipline. Because the published segment is not refreshed until the next
// write lands, readers are still hammering it when sealSegment reaches it —
// which is the condition sm.Read can never reliably reproduce.
//
// MUTATION-PROVEN. With sealSegment reverted to its pre-fix 3d20384 shape
// (index copy under the lock, `Close(); file = readFile` outside it) and
// NOTHING else changed, this fixture reports 142,767 closed-handle reads out of
// 749,478 in 3 seconds, plus five DATA RACE reports on the seg.file field.
// Clean, it is 0 of ~534K-589K across three runs with no race report.
type segSealReadTarget struct {
	seg *SegmentFile
	pos int64
}

func TestSegmentSeal_HandleSwapIsAtomicUnderTheSegmentLock(t *testing.T) {
	const readers = 8
	// PREMISE — REGIME: unlike the fixture above this one does not need 32
	// readers or a 20-second run, because it does not have to WIN a race: its
	// readers already hold the pointer the sealer is about to mutate. The
	// numbers in the doc comment are what that buys — a ~19% hit rate against
	// the reverted sealer instead of a handful of hits in 300K reads.
	const duration = 3 * time.Second
	const minSeals = 100
	// How long the run may be EXTENDED, past `duration`, to reach the
	// minSeals premise on a loaded machine. The test still races for the
	// whole of this window; only the stop condition moves.
	const sealPremiseHardCap = 30 * time.Second

	dataDir := t.TempDir()
	cfg := segTestConfig()
	cfg.SegmentSize = segSealRaceSegmentSize
	sm, err := NewSegmentManagerWithConfig(dataDir, cfg)
	require.NoError(t, err)
	defer sm.Close()

	const queueName = "seal-handle-swap"
	require.NoError(t, writeLegacyBatchForTest(sm, queueName, segTestMessages(queueName, []uint64{1})))
	val, ok := sm.queueSegments.Load(queueName)
	require.True(t, ok, "no QueueSegments for %q", queueName)
	qs := val.(*QueueSegments)

	var target atomic.Pointer[segSealReadTarget]
	var stop atomic.Bool
	var reads, closedErrs, otherErrs, writes, published atomic.Int64
	var firstOther atomic.Pointer[string]
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := uint64(2); !stop.Load(); i++ {
			msg := segTestMessages(queueName, []uint64{i})[0].Message
			if werr := writeLegacyForTest(sm, queueName, msg, i); werr != nil {
				return
			}
			writes.Add(1)
			// Publish the segment this record landed in, exactly as readMessage
			// snapshots it. If the write just sealed, the fresh currentIndex has
			// no entry for i and nothing is published — so every published
			// target is an ACTIVE segment with a seal still ahead of it.
			qs.mutex.Lock()
			seg := qs.currentSegment
			idx := qs.currentIndex
			qs.mutex.Unlock()
			if seg == nil || idx == nil {
				continue
			}
			idx.mutex.RLock()
			pos, found := idx.entries[i]
			idx.mutex.RUnlock()
			if found {
				target.Store(&segSealReadTarget{seg: seg, pos: pos})
				published.Add(1)
			}
		}
	}()

	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				tgt := target.Load()
				if tgt == nil {
					runtime.Gosched()
					continue
				}
				// readMessage's lock discipline, verbatim.
				tgt.seg.mutex.RLock()
				_, rerr := readSegmentMessageAt(tgt.seg.file, tgt.pos)
				tgt.seg.mutex.RUnlock()
				reads.Add(1)
				if rerr != nil {
					if strings.Contains(rerr.Error(), "file already closed") {
						closedErrs.Add(1)
					} else {
						otherErrs.Add(1)
						msg := rerr.Error()
						firstOther.CompareAndSwap(nil, &msg)
					}
				}
			}
		}()
	}

	time.Sleep(duration)
	// The PREMISE of this test is "enough seals happened", NOT "the machine was
	// idle". A loaded CI box seals fewer than minSeals times in `duration`, and
	// failing the premise there would fail the suite without exercising the
	// race at all. So the run CONTINUES past `duration` until the premise
	// actually holds, bounded by a hard cap so a genuinely broken sealer can
	// never hang the suite.
	runOver := make(chan struct{})
	go func() {
		defer close(runOver)
		cap := time.Now().Add(sealPremiseHardCap)
		for time.Now().Before(cap) {
			if segSealCountNow(sm, queueName) >= minSeals {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		stop.Store(true)
	}()
	<-runOver
	stop.Store(true)
	wg.Wait()

	seals := segSealCount(t, sm, queueName)
	other := ""
	if p := firstOther.Load(); p != nil {
		other = *p
	}
	t.Logf("reads=%d closedFileErrs=%d otherErrs=%d writes=%d seals=%d published=%d",
		reads.Load(), closedErrs.Load(), otherErrs.Load(), writes.Load(), seals, published.Load())

	require.GreaterOrEqual(t, seals, minSeals,
		"PREMISE: no seals, nothing measured; only %d in %s", seals, duration)
	require.Greater(t, published.Load(), int64(minSeals),
		"PREMISE: the readers must have had a live ACTIVE segment to hold; only %d were published",
		published.Load())
	require.Greater(t, reads.Load(), int64(10000),
		"PREMISE: the readers must actually be reading concurrently; only %d reads completed",
		reads.Load())
	require.Zero(t, otherErrs.Load(),
		"the fixture itself is broken: %d reads failed for a reason other than a closed handle, "+
			"first was %q", otherErrs.Load(), other)
	require.Zero(t, closedErrs.Load(),
		"CLOSED HANDLE PUBLISHED: %d of %d reads taken under seg.mutex.RLock() saw `file already "+
			"closed`. sealSegment closed the write handle BEFORE publishing the read-only "+
			"replacement, or did so outside seg.mutex, so a reader holding the *SegmentFile "+
			"observed the gap; deliverMessage turns each one into a gap skip",
		closedErrs.Load(), reads.Load())
}

// ---------------------------------------------------------------------------
// review-4 B-4 — compaction error handling
// ---------------------------------------------------------------------------

// segSpyMetrics counts what an operator would see. RecordSegmentCompactionFailure
// is added to SegmentMetrics by this fix; a spy carrying a method the interface
// does not YET declare still satisfies the interface, so this file compiles —
// and therefore goes RED for its stated reason — on the pre-fix tree.
type segSpyMetrics struct {
	compactions atomic.Int64
	failures    atomic.Int64
	readErrors  atomic.Int64
}

func (s *segSpyMetrics) UpdateSegmentMetrics(string, float64, float64) {}
func (s *segSpyMetrics) RecordSegmentCompaction()                      { s.compactions.Add(1) }
func (s *segSpyMetrics) RecordSegmentReadError()                       { s.readErrors.Add(1) }
func (s *segSpyMetrics) RecordSegmentCompactionFailure()               { s.failures.Add(1) }

// corruptOneUnackedRecordCRC overwrites the 4 CRC bytes of the first UNACKED
// record with real garbage — bit rot / a torn write, not an injected hook — and
// asserts the victim is genuinely unreadable while every other record is not.
func corruptOneUnackedRecordCRC(t *testing.T, seg *SegmentFile, unacked []uint64) uint64 {
	t.Helper()
	victim := unacked[0]

	seg.mutex.RLock()
	pos, ok := seg.index[victim]
	seg.mutex.RUnlock()
	require.True(t, ok, "PREMISE: offset %d must be indexed", victim)

	f, err := os.OpenFile(seg.path, os.O_RDWR, 0o644)
	require.NoError(t, err)
	_, err = f.WriteAt([]byte{0xDE, 0xAD, 0xBE, 0xEF}, pos)
	require.NoError(t, err)
	require.NoError(t, f.Sync())
	require.NoError(t, f.Close())

	_, rerr := readSegmentMessageAt(seg.file, pos)
	require.Error(t, rerr, "PREMISE: the corrupted record must be unreadable")
	for _, o := range unacked[1:] {
		seg.mutex.RLock()
		p := seg.index[o]
		seg.mutex.RUnlock()
		_, oerr := readSegmentMessageAt(seg.file, p)
		require.NoError(t, oerr, "PREMISE: record %d must be undamaged", o)
	}
	return victim
}

// TestSegmentCompaction_UncompactableSegmentIsAttemptedOnceAndReported is
// review-4 B-4. Pre-fix, `_ = qs.compactSegment(segment)` discarded the error
// and left deletedCount/messageCount untouched, so the segment was re-selected
// on every tick: review-4 measured 50 / 100 / 150 attempts at t=1/2/3s with the
// file unchanged, while RecordSegmentCompaction() — called at the TOP of
// compactSegment — reported 150 successful compactions.
func TestSegmentCompaction_UncompactableSegmentIsAttemptedOnceAndReported(t *testing.T) {
	dataDir := t.TempDir()
	sm, err := NewSegmentManagerWithConfig(dataDir, segTestConfig())
	require.NoError(t, err)
	defer sm.Close()

	spy := &segSpyMetrics{}
	sm.SetMetrics(spy)

	const count = 12
	qs, seg := buildSealedSegment(t, sm, "uncompactable", count)
	wired, ok := qs.metrics.(*segSpyMetrics)
	require.True(t, ok && wired == spy,
		"PREMISE: the QueueSegments under test must be wired to the spy collector")
	_, unacked := ackMost(t, qs, seg, count)
	corruptOneUnackedRecordCRC(t, seg, unacked)

	before := readFileBytes(t, seg.path)

	// Drive the real selection path repeatedly, exactly as the ticker would.
	const ticks = 50
	for i := 0; i < ticks; i++ {
		qs.tryCompaction()
	}

	after := readFileBytes(t, seg.path)
	require.Equal(t, before, after,
		"the segment file must be byte-identical after a refused compaction")

	// assert, not require: both numbers are the finding, and require would hide
	// the second behind the first.
	assert.Equal(t, int64(1), spy.failures.Load(),
		"BOUNDED: a permanently uncompactable segment must be attempted ONCE and then quarantined "+
			"from compaction; %d reported attempts across %d ticks",
		spy.failures.Load(), ticks)
	assert.Zero(t, spy.compactions.Load(),
		"LYING METRIC: RecordSegmentCompaction() fired %d times while ZERO compactions completed",
		spy.compactions.Load())
}

// TestSegmentCompaction_HealthySegmentStillCompactsExactlyOnce is the control
// arm: neither the quarantine nor the moved metric may be reachable without a
// fault.
func TestSegmentCompaction_HealthySegmentStillCompactsExactlyOnce(t *testing.T) {
	dataDir := t.TempDir()
	sm, err := NewSegmentManagerWithConfig(dataDir, segTestConfig())
	require.NoError(t, err)
	defer sm.Close()

	spy := &segSpyMetrics{}
	sm.SetMetrics(spy)

	const count = 12
	qs, seg := buildSealedSegment(t, sm, "healthy", count)
	_, unacked := ackMost(t, qs, seg, count)

	sizeBefore := int64(len(readFileBytes(t, seg.path)))
	for i := 0; i < 5; i++ {
		qs.tryCompaction()
	}
	sizeAfter := int64(len(readFileBytes(t, seg.path)))

	require.Less(t, sizeAfter, sizeBefore,
		"PREMISE: the control arm must really compact (%d -> %d bytes)", sizeBefore, sizeAfter)
	require.Zero(t, spy.failures.Load(), "a healthy segment must record no compaction failure")
	require.Equal(t, int64(1), spy.compactions.Load(),
		"a healthy segment must record exactly one COMPLETED compaction, got %d",
		spy.compactions.Load())

	for _, o := range unacked {
		msg, rerr := sm.Read("healthy", o)
		require.NoError(t, rerr, "unacked record %d must survive compaction", o)
		require.Equal(t, segBodyMarker(o), msg.Body)
	}
}

// TestSegmentCompaction_AckBitmapLockIsNotHeldAcrossFileIO is review-4 B-4's
// third limb, made deterministic rather than measured.
//
// Pre-fix, compactSegment created its temp file and THEN took
// bitmapMutex.RLock() for the whole read/write copy loop — so with the bitmap
// write-locked from outside, the temp file appears immediately and the
// compaction sits parked mid-I/O holding a lock that batchAckLoop.flush() and
// SegmentManager.Acknowledge's fallback both need. Post-fix the ack decision is
// taken BEFORE any file exists, so nothing is created while the lock is held.
func TestSegmentCompaction_AckBitmapLockIsNotHeldAcrossFileIO(t *testing.T) {
	dataDir := t.TempDir()
	sm, err := NewSegmentManagerWithConfig(dataDir, segTestConfig())
	require.NoError(t, err)
	defer sm.Close()

	const count = 12
	qs, seg := buildSealedSegment(t, sm, "lockscope", count)
	ackMost(t, qs, seg, count)

	// Compaction now selects survivors from physical segmentRecord.acked state.
	// It must neither read nor write the legacy volatile bitmap, including while
	// creating and installing the candidate file.
	qs.bitmapMutex.Lock()
	done := make(chan error, 1)
	go func() { done <- qs.compactSegment(seg) }()
	select {
	case err := <-done:
		qs.bitmapMutex.Unlock()
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		qs.bitmapMutex.Unlock()
		t.Fatal("compaction blocked on the obsolete acknowledgement bitmap")
	}
}

// ---------------------------------------------------------------------------
// review-4 N-3 — the .idx orphan class the deletion sweep created
// ---------------------------------------------------------------------------

// TestSegmentLoad_LegacyIndexFilesAreReclaimed pins N-3. The Step 4 sweep
// deleted writeIndexToDisk and SegmentIndexFileExtension, but every existing
// deployment's segment directories hold one .idx per sealed segment and nothing
// removes them: loadExistingSegments reaps ".compact" and then `continue`s on
// anything that is not ".seg", three lines away.
func TestSegmentLoad_LegacyIndexFilesAreReclaimed(t *testing.T) {
	dataDir := t.TempDir()
	segPath := seedQueueSegment(t, dataDir, "idx-orphans", 4)
	dir := filepath.Dir(segPath)

	stale := []string{
		strings.TrimSuffix(segPath, SegmentFileExtension) + ".idx",
		filepath.Join(dir, "00000000000000000001.idx"),
	}
	for _, p := range stale {
		require.NoError(t, os.WriteFile(p, []byte("legacy index"), 0o644))
	}
	// A file class this broker does not own must NOT be swept.
	keep := filepath.Join(dir, "operator-note.txt")
	require.NoError(t, os.WriteFile(keep, []byte("keep me"), 0o644))

	recovered, err := reopenAndRecover(t, dataDir)
	require.NoError(t, err)
	require.Len(t, recovered["idx-orphans"], 4, "the queue's records must still recover")

	for _, p := range stale {
		_, serr := os.Stat(p)
		assert.True(t, os.IsNotExist(serr),
			"ORPHAN: %s was written by every prior build and nothing reclaims it now", p)
	}
	_, kerr := os.Stat(keep)
	require.NoError(t, kerr, "the reaper must only remove file classes this broker owns")
}

// ---------------------------------------------------------------------------
// review-4 N-6 — crccheck:false removes the classification's discriminator
// ---------------------------------------------------------------------------

// TestSegmentScan_CRCDisabledIsAnAssertedPremiseNotAComment pins N-6.
//
// The Fatal/Benign split between interior corruption and a torn tail is
// discriminated by a CRC mismatch. With CRCDisabled the write path stores a zero
// CRC and loadSegmentFile skips verification, so no record can EVER fail its CRC
// and the three-way classification is decoration. Canon rule 11 wants that
// premise checked at runtime, so both arms are pinned: CRC on catches interior
// payload damage; CRC off does not, and the broker must SAY that it cannot.
func TestSegmentScan_CRCDisabledIsAnAssertedPremiseNotAComment(t *testing.T) {
	// Damage the PAYLOAD, not the header: with CRC on this is exactly what a CRC
	// exists to catch; with CRC off it is undetectable by construction.
	corrupt := func(t *testing.T, segPath string) {
		t.Helper()
		bounds := segRecordBounds(t, segPath)
		require.GreaterOrEqual(t, len(bounds), 4, "PREMISE: need an interior record to damage")
		victim := bounds[1]

		f, err := os.OpenFile(segPath, os.O_RDWR, 0o644)
		require.NoError(t, err)
		_, err = f.WriteAt([]byte{0x00, 0x01, 0x02, 0x03}, victim[0]+segmentRecordHeaderSize+2)
		require.NoError(t, err)
		require.NoError(t, f.Sync())
		require.NoError(t, f.Close())
	}

	t.Run("crc_on_is_the_premise_the_classification_rests_on", func(t *testing.T) {
		cfg := segTestConfig()
		require.False(t, cfg.CRCDisabled,
			"PREMISE: the three-way Fatal/Degraded/Benign classification is only a classification "+
				"while CRC verification is ON")
		dataDir := t.TempDir()
		segPath := seedQueueSegmentWithConfig(t, dataDir, "crc-on", 6, cfg)
		corrupt(t, segPath)

		sm, err := NewSegmentManagerWithConfig(dataDir, cfg)
		require.NoError(t, err)
		defer sm.Close()
		require.Empty(t, sm.crcDisabledWarning(), "PREMISE: this arm must run with CRC on")
		_, rerr := sm.RecoverFromSegments()
		require.Error(t, rerr, "interior payload damage must be caught while CRC is on")
	})

	t.Run("crc_off_loses_the_discriminator_and_says_so", func(t *testing.T) {
		cfg := segTestConfig()
		cfg.CRCDisabled = true
		dataDir := t.TempDir()
		segPath := seedQueueSegmentWithConfig(t, dataDir, "crc-off", 6, cfg)
		corrupt(t, segPath)

		sm, err := NewSegmentManagerWithConfig(dataDir, cfg)
		require.NoError(t, err)
		defer sm.Close()

		// The premise erodes SILENTLY today. The broker must at least state that
		// its damage classification has lost its discriminator, and it must do so
		// where an operator sees it.
		warning := sm.crcDisabledWarning()
		require.NotEmpty(t, warning,
			"PREMISE ERODED SILENTLY: with crccheck:false no record can fail a CRC, so the "+
				"interior-vs-torn-tail split is length arithmetic only, and nothing says so")
		spy := &recordingLogger{}
		sm.SetLogger(spy)
		require.Contains(t, strings.Join(spy.messages(), "\n"), "crccheck is DISABLED",
			"the lost discriminator must reach an operator, not just a return value")

		// And the pinned consequence: the damage CRC would have caught is not
		// caught. This is recorded so nobody reads the classification table as
		// unconditional.
		_, rerr := sm.RecoverFromSegments()
		require.NoError(t, rerr,
			"PINNED CONSEQUENCE: with CRC off, interior payload damage is NOT classified at all")
	})
}

// seedQueueSegmentWithConfig is seedQueueSegment with an explicit config, so a
// CRC-disabled fixture is written by the real write path under the real flag.
func seedQueueSegmentWithConfig(t *testing.T, dataDir, queueName string, count int, cfg SegmentConfig) string {
	t.Helper()
	sm, err := NewSegmentManagerWithConfig(dataDir, cfg)
	require.NoError(t, err)
	offsets := make([]uint64, 0, count)
	for i := 1; i <= count; i++ {
		offsets = append(offsets, uint64(i))
	}
	require.NoError(t, writeLegacyBatchForTest(sm, queueName, segTestMessages(queueName, offsets)))
	require.NoError(t, sm.Close())

	segDir := filepath.Join(dataDir, "segments", queueName)
	entries, err := os.ReadDir(segDir)
	require.NoError(t, err)
	var segPath string
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), SegmentFileExtension) {
			continue
		}
		info, ierr := e.Info()
		require.NoError(t, ierr)
		if info.Size() > 0 {
			require.Empty(t, segPath, "PREMISE: exactly one non-empty segment file")
			segPath = filepath.Join(segDir, e.Name())
		}
	}
	require.NotEmpty(t, segPath, "PREMISE: the fixture wrote no segment file")
	return segPath
}

// ---------------------------------------------------------------------------
// review-4 N-7 / B-2 — cold-path durability failures had no consumer at all
// ---------------------------------------------------------------------------

// recordingLogger is the smallest interfaces.Logger that lets a test assert a
// cold-path failure became visible.
type recordingLogger struct {
	mu   sync.Mutex
	msgs []string
}

func (l *recordingLogger) record(msg string) {
	l.mu.Lock()
	l.msgs = append(l.msgs, msg)
	l.mu.Unlock()
}

func (l *recordingLogger) messages() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]string, len(l.msgs))
	copy(out, l.msgs)
	return out
}

func (l *recordingLogger) Debug(msg string, _ ...interfaces.LogField) { l.record(msg) }
func (l *recordingLogger) Info(msg string, _ ...interfaces.LogField)  { l.record(msg) }
func (l *recordingLogger) Warn(msg string, _ ...interfaces.LogField)  { l.record(msg) }
func (l *recordingLogger) Error(msg string, _ ...interfaces.LogField) { l.record(msg) }
func (l *recordingLogger) Fatal(msg string, _ ...interfaces.LogField) { l.record(msg) }
func (l *recordingLogger) With(_ ...interfaces.LogField) interfaces.Logger {
	return l
}
func (l *recordingLogger) Sync() error { return nil }

// TestSegmentLoad_FaultsRaisedByTheWritePathAreSurfaced pins N-7:
// getOrCreateQueueSegmentsAt stores loadFaults on EVERY first touch, including
// first touch from Write/CheckpointBatch, but only RecoverFromSegments drains
// them. A queue whose QueueSegments is first constructed by the 5-minute
// checkpoint had its classified segment faults collected into a slice nobody
// read — a fault producer with no consumer.
func TestSegmentLoad_FaultsRaisedByTheWritePathAreSurfaced(t *testing.T) {
	dataDir := t.TempDir()
	segPath := seedQueueSegment(t, dataDir, "write-path-faults", 4)

	// Real damage: overwrite an interior record's CRC bytes.
	bounds := segRecordBounds(t, segPath)
	require.GreaterOrEqual(t, len(bounds), 3)
	f, err := os.OpenFile(segPath, os.O_RDWR, 0o644)
	require.NoError(t, err)
	_, err = f.WriteAt([]byte{0xDE, 0xAD, 0xBE, 0xEF}, bounds[1][0])
	require.NoError(t, err)
	require.NoError(t, f.Sync())
	require.NoError(t, f.Close())

	sm, err := NewSegmentManagerWithConfig(dataDir, segTestConfig())
	require.NoError(t, err)
	defer sm.Close()

	spy := &recordingLogger{}
	sm.SetLogger(spy)

	// First touch from the WRITE path, never from RecoverFromSegments.
	require.NoError(t, writeLegacyBatchForTest(sm, "write-path-faults",
		segTestMessages("write-path-faults", []uint64{99})))

	require.NotEmpty(t, spy.messages(),
		"a segment fault discovered on the write path must reach an operator; "+
			"RecoverFromSegments is the only takeLoadFaults caller and it never runs here")
}

// TestSegmentCheckpoint_FailureIsNotDiscardedSilently pins review-4 B-2's second
// half. performCheckpoint's `checkpointOK = false` is correct for durability but
// discarded the error entirely, so an unusable segment directory meant the WAL
// grew every 5 minutes forever with nothing for an operator to see. The failure
// is provoked with real EACCES from the real permission system.
func TestSegmentCheckpoint_FailureIsNotDiscardedSilently(t *testing.T) {
	t.Skip("checkpoint copying removed in segstore v1")
	dataDir := t.TempDir()
	sm, err := NewSegmentManagerWithConfig(dataDir, segTestConfig())
	require.NoError(t, err)
	defer sm.Close()

	spy := &recordingLogger{}
	sm.SetLogger(spy)

	require.NoError(t, os.Chmod(sm.dataDir, 0o500))
	t.Cleanup(func() { _ = os.Chmod(sm.dataDir, 0o755) })

	probe := filepath.Join(sm.dataDir, "probe")
	if perr := os.Mkdir(probe, 0o755); perr == nil {
		_ = os.Remove(probe)
		t.Skip("SKIPPED LOUDLY: chmod 0500 did not take on this filesystem (running as root?), " +
			"so the checkpoint failure this test needs cannot be provoked")
	}

	err = writeLegacyBatchForTest(sm, "unwritable", segTestMessages("unwritable", []uint64{1}))
	require.Error(t, err, "PREMISE: the checkpoint must genuinely fail")
	require.NotEmpty(t, spy.messages(),
		"SILENT: CheckpointBatch failed and nothing an operator can see says so; this queue's WAL "+
			"is now never reclaimed")
	require.Contains(t, strings.Join(spy.messages(), "\n"), "WAL files cannot be reclaimed",
		"the report must name the consequence, not just the error")
}
