package storage

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
)

const (
	// Segment settings
	DefaultSegmentSize         = 1024 * 1024 * 1024 // 1 GB per segment
	SegmentCheckpointInterval  = 5 * time.Minute    // Checkpoint WAL to segments every 5 minutes
	DefaultCompactionThreshold = 0.5                // Compact when >50% messages deleted
	SegmentFileExtension       = ".seg"

	// segmentCompactSuffix is appended to a segment's path to name the
	// temporary file compaction rewrites into. One constant, because
	// compactSegment creates it and loadExistingSegments reclaims orphans of
	// it (canon rule 12).
	segmentCompactSuffix = ".compact"

	// segmentLegacyIndexSuffix names the per-segment index files every build
	// before Step 4 wrote and nothing ever read. loadExistingSegments reclaims
	// them; nothing produces them.
	segmentLegacyIndexSuffix = ".idx"

	// Segment record framing: [4 bytes CRC32][4 bytes length][<message payload v4>]
	// where the payload is the shared presence-bitmap record produced by
	// appendMessagePayload (identical to the WAL message payload, but with an
	// empty queue name and no per-record type tag). See serializeSegmentMessage.
	segmentRecordHeaderSize = 8

	// SQ-4 NOTE: segment files intentionally do NOT carry a file-level format
	// version header (unlike WAL files). Adding one here is not trivially
	// symmetric with the WAL change: segment index positions are absolute file
	// offsets, and compaction (compactSegment) rewrites a segment in place via
	// serializeSegmentMessage + rename, which would need to re-emit and re-skip
	// a header on every rewrite. Because ITER4 requires a fresh data dir (clean
	// break), no pre-v4 segment records exist, so segments unconditionally carry
	// the v4 payload and no structural version detection is needed on read.
)

// SegmentConfig holds configurable parameters for the segment manager
type SegmentConfig struct {
	SegmentSize         int64
	CompactionThreshold float64
	CompactionInterval  time.Duration
	CheckpointInterval  time.Duration

	// CRCDisabled, when true, skips CRC32 computation on segment writes (writes
	// zero in the CRC field) and skips verification on reads (when stored CRC
	// is zero). Zero value (false) = CRC ON, the safe default. Set via
	// SegmentConfigFromEngine from the user's Storage.CRCCheck flag.
	CRCDisabled bool
}

// DefaultSegmentConfig returns a SegmentConfig with production defaults
func DefaultSegmentConfig() SegmentConfig {
	return SegmentConfig{
		SegmentSize:         DefaultSegmentSize,
		CompactionThreshold: DefaultCompactionThreshold,
		CompactionInterval:  1 * time.Minute,
		CheckpointInterval:  SegmentCheckpointInterval,
	}
}

// SegmentMetrics interface for metrics collection
type SegmentMetrics interface {
	UpdateSegmentMetrics(queueName string, count, sizeBytes float64)
	// RecordSegmentCompaction counts COMPLETED compactions. It used to be
	// called at the top of compactSegment, before any work, so a segment that
	// could not be compacted reported one "compaction" per attempt forever — a
	// counter climbing steadily is how an operator concludes the compactor is
	// healthy (review-4 B-4).
	RecordSegmentCompaction()
	// RecordSegmentCompactionFailure counts segments withdrawn from compaction
	// because their bytes cannot be rewritten. It is the alertable signal for a
	// permanently uncompactable segment.
	RecordSegmentCompactionFailure()
	RecordSegmentReadError()
}

// SegmentManager manages long-term cold storage with compaction
// Messages are checkpointed from WAL to segments periodically
type SegmentManager struct {
	dataDir       string
	queueSegments sync.Map // queueName -> *QueueSegments
	metrics       SegmentMetrics
	cfg           SegmentConfig

	// logger reports COLD-PATH durability failures the call stack cannot
	// return: a checkpoint that could not run (so a WAL file is never
	// reclaimed), a segment withdrawn from compaction, and segment faults
	// discovered on the write path rather than at boot. nil means "no logging",
	// so a caller that never wires one pays nothing. Wired by
	// DisruptorStorage.SetLogger.
	//
	// Before this existed, storage had no logger at all and every one of those
	// conditions was invisible: review-4 B-2's symptom was "the disk fills up".
	logger interfaces.Logger
}

// QueueSegments manages segments for a single queue
type QueueSegments struct {
	queueName string
	dataDir   string
	cfg       SegmentConfig

	// Active segment being written to
	currentSegment *SegmentFile
	currentIndex   *SegmentIndex
	mutex          sync.Mutex

	// Sealed segments (read-only)
	sealedSegments map[uint64]*SegmentFile
	sealedMutex    sync.RWMutex

	// ACK tracking for compaction
	ackBitmap   *roaring64.Bitmap
	bitmapMutex sync.RWMutex

	// Batch ACK channel (M2: reduces bitmapMutex contention from per-ACK to per-batch)
	ackChan      chan uint64
	ackBatchSize int

	// Compaction state
	compactionMux sync.Mutex

	// loadFaults holds the classified faults loadExistingSegments produced for
	// this queue's directory, until RecoverFromSegments drains them exactly
	// once. Reporting them twice would make the refusal message's
	// discarded-artifact count overstate.
	loadFaults      []error
	loadFaultsMutex sync.Mutex

	// closed makes close() idempotent: DisruptorStorage.Close reaches
	// SegmentManager.Close, and so does any deferred Close in a caller. A
	// second close(qs.stopChan) panics.
	closed atomic.Bool

	// Metrics collector
	metrics SegmentMetrics

	// Background goroutines
	stopChan chan struct{}
	wg       sync.WaitGroup

	// NEW FIELDS GO BELOW THIS LINE. QueueSegments is the struct
	// SegmentManager.Acknowledge touches per ack, and every field down to
	// ackBatchSize keeps the byte offset it had at HEAD 3d20384, verified with
	// unsafe.Offsetof rather than argued (segment_layout_test.go pins them).
	//
	// The fields BELOW ackBatchSize did move when lastCompaction was removed
	// and loadFaults/loadFaultsMutex/closed were added — compactionMux 200->176,
	// metrics 208->224, stopChan 224->240, wg 232->248. None of them is on the
	// ack path, so nothing is wrong, but the earlier wording here said "every
	// field above", which was false and is the same over-claim review-5 MAJOR-3
	// found in the layout test's comment.
	logger interfaces.Logger

	// --- WS-C (C2): write-error repair state, appended after logger. ---

	// segPoisoned is non-nil when the active segment could not be repaired
	// after a failed write/fsync (truncate failed). While poisoned, no byte is
	// written to the damaged segment; writeMessageBatchDurable retires it and
	// opens a fresh one first.
	segPoisoned error
	// lastSegmentNum is the highest segment number loaded or created, making
	// new numbers strictly monotone within an incarnation and across restarts
	// even if the wall clock steps backwards. Guarded by qs.mutex.
	lastSegmentNum uint64
}

// SegmentFile represents a single segment file
type SegmentFile struct {
	segmentNum uint64
	path       string
	file       *os.File

	// Metadata
	minOffset    uint64
	maxOffset    uint64
	messageCount atomic.Uint64
	deletedCount atomic.Uint64
	fileSize     atomic.Int64

	// Index for fast lookups
	index map[uint64]int64 // offset -> file position
	mutex sync.RWMutex

	// compactionBlocked withdraws this segment from compaction for the lifetime
	// of the process. See tryCompaction: compaction is space reclamation only,
	// never a durability requirement, so refusing to re-attempt a rewrite that
	// the segment's own bytes make impossible is strictly safer than retrying it
	// once a minute forever (review-4 B-4). Declared LAST so every field the
	// cold-read tier touches keeps its offset.
	compactionBlocked atomic.Bool

	// damagedTail (WS-C C2) is true when this segment was retired after an
	// unrepairable failed write: bytes beyond the indexed region are a failed
	// batch's partial bytes. Tail damage is benign at load (nothing is ever
	// appended after it), so the segment is still sealed and read.
	damagedTail bool
}

// SegmentIndex provides fast offset -> file position lookups
type SegmentIndex struct {
	entries map[uint64]int64 // offset -> file position
	mutex   sync.RWMutex
}

// NewSegmentManager creates a new segment manager with default config
func NewSegmentManager(dataDir string) (*SegmentManager, error) {
	return NewSegmentManagerWithConfig(dataDir, DefaultSegmentConfig())
}

// NewSegmentManagerWithConfig creates a new segment manager with custom config
func NewSegmentManagerWithConfig(dataDir string, cfg SegmentConfig) (*SegmentManager, error) {
	segDir := filepath.Join(dataDir, "segments")
	if err := mkdirAllDurable(segDir); err != nil {
		return nil, fmt.Errorf("failed to create segments directory: %w", err)
	}

	return &SegmentManager{
		dataDir: segDir,
		cfg:     cfg,
	}, nil
}

// SetMetrics sets the metrics collector for the segment manager
func (sm *SegmentManager) SetMetrics(metrics SegmentMetrics) {
	sm.metrics = metrics
	// Update metrics for existing queues
	sm.queueSegments.Range(func(key, value interface{}) bool {
		if qs, ok := value.(*QueueSegments); ok {
			qs.metrics = metrics
		}
		return true
	})
}

// SetLogger wires a structured logger for the segment tier's cold-path
// durability warnings. nil disables logging. Not called on any hot path; the
// logger is read only when something has already failed.
//
// It also emits crcDisabledWarning() once, at wiring time, because the moment a
// logger exists is the first moment that premise can be surfaced at all.
func (sm *SegmentManager) SetLogger(l interfaces.Logger) {
	sm.logger = l
	sm.queueSegments.Range(func(key, value interface{}) bool {
		if qs, ok := value.(*QueueSegments); ok {
			qs.logger = l
		}
		return true
	})
	if w := sm.crcDisabledWarning(); w != "" {
		sm.logWarn(w)
	}
}

// crcDisabledWarning returns a non-empty operator-facing warning when the
// segment tier's damage classification has lost its discriminator, and "" when
// it has not.
//
// review-4 N-6, canon rule 11: the Fatal/Benign split between INTERIOR
// corruption and a TORN TAIL is decided by a CRC32 mismatch. With
// Storage.CRCCheck off, serializeSegmentMessage writes a zero CRC and
// loadSegmentFile skips verification, so no record can ever fail its CRC and a
// garbage tail is classified by length arithmetic alone: it either produces a
// bogus index entry (silently wrong data on read) or trips the
// CRC-valid-but-malformed arms and gates the boot on damage that would have been
// a benign torn tail. The premise is now REPORTED rather than written in a
// comment.
func (sm *SegmentManager) crcDisabledWarning() string {
	if !sm.cfg.CRCDisabled {
		return ""
	}
	return "crccheck is DISABLED: segment damage classification has lost its discriminator. " +
		"No record can fail a CRC32 check, so the interior-corruption / torn-tail split is decided " +
		"by length arithmetic alone: a torn tail may gate the boot as unreadable, and a corrupted " +
		"record may be indexed and served as valid data. Re-enable crccheck to restore it."
}

// logWarn / logError report a cold-path condition when a logger is wired.
func (sm *SegmentManager) logWarn(msg string, fields ...interfaces.LogField) {
	if sm.logger != nil {
		sm.logger.Warn(msg, fields...)
	}
}

func (sm *SegmentManager) logError(msg string, fields ...interfaces.LogField) {
	if sm.logger != nil {
		sm.logger.Error(msg, fields...)
	}
}

func (qs *QueueSegments) logError(msg string, fields ...interfaces.LogField) {
	if qs.logger != nil {
		qs.logger.Error(msg, fields...)
	}
}

// ---------------------------------------------------------------------------
// Queue name <-> segment directory.
//
// Queue names are CLIENT-CONTROLLED (queue.declare carries an arbitrary
// shortstr) and filepath.Join CLEANS its result, so "../../x" resolved to a
// directory outside the data directory entirely: the broker created folders and
// wrote message bodies wherever the process could write, and — because
// RecoverFromSegments only walks the segments directory — the checkpointed copy
// was then unreachable, after performCheckpoint had already unlinked the source
// WAL file. Names that cannot be a single path element must therefore be
// escaped.
//
// WHY THE DIRECTORY NAME IS NOT THE ANSWER ON ITS OWN (review-4 B-1). The first
// attempt reserved "%" as an escape prefix and hex-DECODED any directory
// starting with it. That is unrecoverable ambiguity: the pre-fix build wrote
// filepath.Join(dataDir, queueName) for EVERY name that was a valid single path
// element, and that set includes every possible encoded form. A queue named
// "%2f" really did own <data>/segments/%2f, and the decode handed its
// checkpointed durable messages to a queue named "/" with err == nil. No escape
// prefix can be safe by inspection.
//
// THE MECHANISM. A directory this build creates carries a MARKER FILE holding
// the raw queue name; recovery reads it when present and otherwise treats the
// directory name as literal. Legacy directories have no marker, so they are
// literal BY CONSTRUCTION — genuinely zero migration — and the ambiguity
// disappears because the encoding announces itself instead of being inferred.
// Two consequences worth stating:
//
//   - "%" is no longer reserved. A "%"-prefixed name is an ordinary literal
//     name again, which also removes the 2x+1 expansion that pushed a 255-byte
//     "%"-prefixed name past NAME_MAX (review-4 B-2).
//   - Because the marker, not the string, carries identity, a generated
//     directory name needs only to be a creatable single path element. It is
//     length-bounded, and collisions with a LITERAL directory another queue
//     already owns are detected and probed past rather than silently shared.
//
// NOT INJECTIVE ON A CASE-INSENSITIVE OR NORMALISING FILESYSTEM. On macOS,
// queues "Queue" and "queue" are distinct AMQP entities that map to one
// directory, and NFC/NFD "café" collapses the same way (review-4 N-2). That is
// pre-existing, platform-specific, and reaches beyond storage into what a queue
// name IS; it is registered as an owned follow-up and is NOT closed here.
// ---------------------------------------------------------------------------

const (
	// segmentDirEncodedPrefix begins the generated name of a directory whose
	// queue name is not usable as a path element. It is a readability
	// convention, NOT a decodable encoding: segmentDirMarkerName is what
	// carries identity.
	segmentDirEncodedPrefix = "%"

	// segmentDirMarkerName holds the owning queue name of the directory it sits
	// in. Its absence means "written by a build that predates markers", which
	// is exactly the set of directories whose own name IS the queue name.
	segmentDirMarkerName = ".queue-name"

	// The marker is FRAMED AND CHECKSUMMED rather than being the raw queue
	// name, because the raw form cannot express the state that matters most.
	//
	// This file's contract is that a marker is "missing", "valid", or "present
	// but UNUSABLE", and that the third state is never silently collapsed into
	// either of the other two — every caller already routes it to a classified
	// fault. Raw bytes cannot implement that contract: a marker truncated from
	// "payments-eu-west" to "payme" is a perfectly good queue name, and a
	// zero-length one is a perfectly good empty string. review-5 CRITICAL-1
	// measured both — a truncated marker recovered another queue's messages
	// under key "payme" with err == nil, and a zero-length one attributed a
	// queue's checkpointed messages to queue "" on a clean boot.
	//
	// WHAT "VALID" MEANS, decided deliberately and stated once:
	//
	//   - the header line is present and has exactly four tab-separated fields;
	//   - the magic and version match this build's;
	//   - the body's length is EXACTLY the length the header declares, so a
	//     truncated or trailing-byte-decorated file is rejected rather than
	//     guessed at (this is what makes "orders\n" != "orders" detectable);
	//   - the body's CRC32 matches the header's, so silent corruption of the
	//     name itself is rejected;
	//   - the name is 1..255 bytes. Empty is invalid because no queue can be
	//     named "" — server/queue_handlers.go substitutes a generated amq.gen.*
	//     name for an empty queue.declare — and 255 is the AMQP 0-9-1 shortstr
	//     maximum, so anything longer is damage, not a long name.
	//
	// The name itself is otherwise UNRESTRICTED: interior NULs, separators,
	// spaces and invalid UTF-8 are all legal queue names, and the marker's job
	// is to record the name faithfully, not to have opinions about it. Framing
	// validates the RECORD, never the name.
	//
	// This format has no migration cost: markers were introduced on this branch
	// (no tag contains d5c3f85) so no deployment has one on disk, and a
	// directory with no marker is still read exactly as before.
	segmentDirMarkerMagic   = "strangeq-segment-owner"
	segmentDirMarkerVersion = "v1"

	// segmentDirMarkerMaxNameLen is the AMQP 0-9-1 shortstr maximum, which is
	// the widest a queue name can be on the wire.
	segmentDirMarkerMaxNameLen = 255

	// segmentDirNameMaxLen bounds a GENERATED directory name. NAME_MAX is 255
	// on APFS and ext4; the margin leaves room for a probe suffix.
	segmentDirNameMaxLen = 200

	// segmentDirProbeLimit bounds how many alternatives are tried when the
	// preferred name is already owned by another queue. Only a legacy
	// literal directory can collide with a generated name, so one alternative
	// suffices in practice; the limit exists so the search is bounded rather
	// than "obviously" terminating.
	segmentDirProbeLimit = 64
)

// segmentDirNameIsLiteral reports whether a queue name can be used verbatim as a
// single directory entry under the segments directory.
//
// The excluded set is deliberately the SMALLEST one that closes the escape: the
// empty name, "." and "..", a path separator, and a NUL. Every other byte
// sequence — spaces, colons, UTF-8, "%" ANYWHERE INCLUDING THE FIRST BYTE, any
// length up to the 255-byte shortstr limit — stays literal, so an existing data
// directory's segment folders are the ones this build already uses and NOTHING is
// migrated.
//
// The bound is NAME_MAX rather than segmentDirNameMaxLen: this predicate decides
// whether a name keeps a directory the previous build ALREADY created, and the
// previous build could create any name up to 255 bytes. Capping it lower would
// strand a 210-byte queue's existing directory. segmentDirNameMaxLen bounds only
// GENERATED names, which need room for a probe suffix.
//
// It answers "would the previous build have written <segments>/name as one
// entry", so it is exactly the predicate segmentDirOwner's markerless case
// inverts. A name that PASSES here but that the FILESYSTEM rejects (invalid
// UTF-8 on APFS, for instance) is handled by segmentDirFor's create-fallthrough,
// not here: this is a question about the name, not about the volume.
func segmentDirNameIsLiteral(name string) bool {
	if name == "" || name == "." || name == ".." {
		return false
	}
	if len(name) > 255 {
		return false
	}
	return !strings.ContainsRune(name, filepath.Separator) && !strings.ContainsRune(name, 0)
}

// segmentDirEncoded generates a directory name for a queue whose name cannot be
// used literally. It is hex so an operator can read it back by hand; when hex
// would exceed segmentDirNameMaxLen the tail is replaced by a full SHA-256 of
// the name, which keeps distinct names distinct without growing the entry.
// Identity still comes from the marker file, never from this string.
func segmentDirEncoded(queueName string) string {
	full := segmentDirEncodedPrefix + hex.EncodeToString([]byte(queueName))
	if len(full) <= segmentDirNameMaxLen {
		return full
	}
	sum := sha256.Sum256([]byte(queueName))
	digest := hex.EncodeToString(sum[:])
	// prefix + "-" + digest == segmentDirNameMaxLen
	keep := segmentDirNameMaxLen - 1 - len(digest)
	return full[:keep] + "-" + digest
}

// segmentDirCandidates lists, in preference order, every directory name this
// build may use for a queue. The first entry is the literal name when the name
// can be one; the rest are generated. segmentDirFor picks the first candidate it
// can prove belongs to this queue (or that is free).
func segmentDirCandidates(queueName string) []string {
	out := make([]string, 0, segmentDirProbeLimit+2)
	if segmentDirNameIsLiteral(queueName) {
		out = append(out, queueName)
	}
	encoded := segmentDirEncoded(queueName)
	out = append(out, encoded)
	for i := 1; i <= segmentDirProbeLimit; i++ {
		out = append(out, encoded+"-"+strconv.Itoa(i))
	}
	return out
}

// QueueNameForSegmentDir attributes a segment directory to its queue for callers
// OUTSIDE this package. server.updateSegmentMetrics reads <data>/segments
// directly and used the directory entry's name as the metric's queue label,
// which is a second copy of the "the directory name is the queue name"
// assumption review-4 B-1 falsified — one package outside a sweep that had
// reported the class closed.
//
// segmentsDir is the segments directory, entryName the directory entry inside it.
//
// The bool reports whether the directory could be attributed at all. It exists
// because the alternative — falling back to entryName — is the very assumption
// this function was written to remove: an unusable marker would have produced a
// metric labelled with a hex directory name, or worse with the name of a
// DIFFERENT queue (review-5 MINOR-5). A caller that cannot attribute a
// directory must decline to label it, not guess.
//
// TWO ways to be unattributable, not one. The first version of this function
// closed only the unusable-marker route, while a MARKERLESS directory carrying a
// GENERATED name still answered (hexName, true) — verbatim the hex label the
// paragraph above says the bool exists to prevent (review-7 MINOR-4).
//
// The discriminator is segments, not spelling. A markerless directory's name is
// its owner ONLY IF a pre-marker build wrote it, and such a directory holds
// segment files. A markerless directory with NO segments is instead the residue
// of segmentDirFor's mkdir-before-marker window (review-5's registered "Attack 2
// benign" state — benign for data, not for a label): this build creates the
// directory, then writes the marker, then opens the first segment, so
// markerless-and-empty can only be an artifact and markerless-with-segments can
// only be legacy. Spelling cannot tell them apart — "%612f62" is a perfectly
// legal literal queue name, which is the whole point of the marker — so it is
// not consulted.
//
// The extra listing is confined to markerless directories on a metrics-scrape
// path, and boot recovery stamps a marker into every markerless directory it
// finds, so after one clean boot this branch does not run.
func QueueNameForSegmentDir(segmentsDir, entryName string) (string, bool) {
	dir := filepath.Join(segmentsDir, entryName)
	marker, present, err := readSegmentDirMarker(dir)
	if err != nil {
		return "", false
	}
	if !present && !segmentDirHoldsSegments(dir) {
		return "", false
	}
	return segmentDirOwner(entryName, marker, present), true
}

// segmentDirHoldsSegments reports whether a directory holds at least one segment
// file. It answers "was this written by a build, or merely created by one".
func segmentDirHoldsSegments(dir string) bool {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false
	}
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), SegmentFileExtension) {
			return true
		}
	}
	return false
}

// segmentDirOwner is the ONE place a directory on disk is attributed to a queue.
// A marker names its owner outright; a directory with no marker was written by a
// build that spelled the queue name verbatim, so the directory name IS the queue
// name. There is no decoding step and therefore no ambiguity.
func segmentDirOwner(dirName, marker string, markerPresent bool) string {
	if markerPresent {
		return marker
	}
	return dirName
}

// readSegmentDirMarker returns the queue name a directory claims.
//
// THREE OUTCOMES, and the caller must keep them apart:
//
//	("",   false, nil) — no marker. A legacy directory; its NAME is its owner.
//	(name, true,  nil) — a valid marker naming its owner.
//	("",   true,  err) — present but UNUSABLE: unreadable, or readable and
//	                     degenerate (truncated, zero-length, corrupt, decorated).
//
// The third outcome must NEVER be treated as the first. Doing so attributes the
// directory to its own name, i.e. to the wrong queue, which is the silent loss
// review-4 B-1 measured. It must not be treated as the second either: that is
// review-5 CRITICAL-1, where a zero-length marker made queue "" the owner of
// another queue's checkpointed messages on an otherwise clean boot.
func readSegmentDirMarker(dir string) (string, bool, error) {
	path := filepath.Join(dir, segmentDirMarkerName)
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return "", false, nil
		}
		return "", true, err
	}
	name, derr := decodeSegmentDirMarker(raw)
	if derr != nil {
		return "", true, fmt.Errorf("the ownership marker %s is present but unusable: %w", path, derr)
	}
	return name, true, nil
}

// encodeSegmentDirMarker renders a marker's on-disk bytes. The name is written
// verbatim after the header so an operator can still read it out of the file;
// the header is what makes damage to it detectable.
func encodeSegmentDirMarker(queueName string) []byte {
	body := []byte(queueName)
	header := fmt.Sprintf("%s\t%s\t%d\t%08x\n",
		segmentDirMarkerMagic, segmentDirMarkerVersion, len(body), crc32.ChecksumIEEE(body))
	return append([]byte(header), body...)
}

// decodeSegmentDirMarker parses a marker and returns the queue name it names,
// or an error describing precisely which check failed. Every error here means
// "present but unusable"; see segmentDirMarkerMagic for what valid means and
// why each check is there.
func decodeSegmentDirMarker(raw []byte) (string, error) {
	nl := bytes.IndexByte(raw, '\n')
	if nl < 0 {
		return "", fmt.Errorf("no header line in %d byte(s)", len(raw))
	}
	fields := strings.Split(string(raw[:nl]), "\t")
	if len(fields) != 4 {
		return "", fmt.Errorf("header has %d field(s), want 4", len(fields))
	}
	if fields[0] != segmentDirMarkerMagic {
		return "", fmt.Errorf("header magic is %q, want %q", fields[0], segmentDirMarkerMagic)
	}
	if fields[1] != segmentDirMarkerVersion {
		return "", fmt.Errorf("header version is %q, want %q", fields[1], segmentDirMarkerVersion)
	}
	declaredLen, lerr := strconv.Atoi(fields[2])
	if lerr != nil || declaredLen < 0 {
		return "", fmt.Errorf("header length field %q is not a length", fields[2])
	}
	if len(fields[3]) != 8 {
		return "", fmt.Errorf("header checksum field %q is not 8 hex digits", fields[3])
	}
	declaredCRC, cerr := strconv.ParseUint(fields[3], 16, 32)
	if cerr != nil {
		return "", fmt.Errorf("header checksum field %q is not 8 hex digits", fields[3])
	}

	body := raw[nl+1:]
	if len(body) != declaredLen {
		return "", fmt.Errorf("header declares a %d byte name but the file holds %d byte(s) after the header", declaredLen, len(body))
	}
	if declaredLen == 0 {
		return "", errors.New("names the empty queue, which no queue can be called")
	}
	if declaredLen > segmentDirMarkerMaxNameLen {
		return "", fmt.Errorf("names a %d byte queue, above the %d byte shortstr maximum", declaredLen, segmentDirMarkerMaxNameLen)
	}
	if actual := crc32.ChecksumIEEE(body); actual != uint32(declaredCRC) {
		return "", fmt.Errorf("the name's checksum is %08x but the header declares %08x", actual, declaredCRC)
	}
	return string(body), nil
}

// writeSegmentDirMarker records the queue name inside its segment directory. It
// is written before any segment file can exist in a directory this build
// created, so there is no window in which records are present without an owner.
//
// The write is ATOMIC AND DURABLE (temp file -> fsync -> rename -> fsync dir),
// not os.WriteFile + syncDir. The old shape fsynced the directory entry and
// never the contents, so ENOSPC/EIO or a crash left a zero-length marker
// wearing the real name — review-5 CRITICAL-1's reachability route. With
// atomicWriteFile a failed write leaves the directory MARKERLESS instead, which
// is a state this file already handles correctly, rather than degenerate.
func writeSegmentDirMarker(dir, queueName string) error {
	if queueName == "" {
		return errors.New("refusing to write an ownership marker for the empty queue name")
	}
	if len(queueName) > segmentDirMarkerMaxNameLen {
		return fmt.Errorf("refusing to write an ownership marker for a %d byte queue name, above the %d byte shortstr maximum",
			len(queueName), segmentDirMarkerMaxNameLen)
	}
	return atomicWriteFile(filepath.Join(dir, segmentDirMarkerName), encodeSegmentDirMarker(queueName), 0644)
}

// loadDirEntryNames returns the set of entry names in dir, spelled exactly as
// the filesystem holds them. Only the names are read (no per-entry lstat, no
// sort), because the only question asked of the result is membership.
func loadDirEntryNames(dir string) (map[string]struct{}, error) {
	f, err := os.Open(dir)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()

	names, err := f.Readdirnames(-1)
	if err != nil {
		return nil, err
	}
	set := make(map[string]struct{}, len(names))
	for _, n := range names {
		set[n] = struct{}{}
	}
	return set, nil
}

// segmentDirFor picks — and, when necessary, creates — the directory this
// queue's segments live in.
//
// The property it establishes: the returned directory either carries a marker
// naming THIS queue, or carries no marker and is spelled — as a real directory
// entry, byte for byte — exactly this queue's name (a legacy directory this
// queue wrote), or was just created by this call. No other directory can be
// returned, so NO DIRECTORY IS EVER ATTRIBUTED TO A QUEUE THAT DID NOT WRITE
// IT. Combined with RecoverFromSegments enumerating every directory and
// attributing it by marker, NO QUEUE'S DIRECTORY IS EVER MISSED.
//
// "As a real directory entry" is load-bearing and is the fix for review-5
// CRITICAL-2: os.Stat succeeds against a differently-spelled entry on a folding
// volume, so the name this function ASKED for is not evidence about the name
// that EXISTS. Because of that guarantee, getOrCreateQueueSegmentsAt's
// `dirName != queueName` test is a statement about the filesystem rather than
// about a string this function chose.
//
// KNOWINGLY OUT OF SCOPE: the create branch below folds too. MkdirAll(".../q")
// returns nil against an on-disk "Q" and creates nothing, so a TOCTOU between
// this call's os.Stat and a sibling's first touch can return a folded spelling.
// getOrCreateQueueSegmentsAt re-reads the marker and refuses if the sibling has
// already stamped one, leaving a window only while the sibling has created the
// directory but not yet marked it. Both queues are in one process, the
// checkpoint path is single-goroutine and recovery is sequential, and two
// processes sharing a data directory are already unsupported (there is no
// lockfile anywhere in this tree). Registered, not closed here.
//
// It creates rather than only resolving because a candidate can be rejected by
// the VOLUME rather than by its bytes: APFS enforces valid UTF-8 in filenames, so
// a queue named "\xab" has a perfectly legal literal candidate that mkdir
// refuses. Falling through to the generated candidate keeps such a name working
// — it worked before this change, when every non-literal name was encoded — and
// on a filesystem that accepts those bytes (ext4) the literal directory is used
// and any legacy one is adopted, so neither platform strands data.
func (sm *SegmentManager) segmentDirFor(queueName string) (string, error) {
	var firstErr error
	var entryNames map[string]struct{}
	entriesLoaded := false

	for _, cand := range segmentDirCandidates(queueName) {
		full := filepath.Join(sm.dataDir, cand)
		st, err := os.Stat(full)
		if os.IsNotExist(err) {
			if mkErr := os.MkdirAll(full, 0755); mkErr != nil {
				if firstErr == nil {
					firstErr = mkErr
				}
				continue
			}
			return cand, nil
		}
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if !st.IsDir() {
			continue
		}
		marker, present, merr := readSegmentDirMarker(full)
		if merr != nil {
			// Present but unusable. Treating it as absent would attribute the
			// directory to its own name — the exact mis-attribution this design
			// exists to prevent — so refuse this candidate. Adopting it is not
			// an option either: an unusable marker may name a DIFFERENT queue,
			// so this directory cannot be proven to be ours.
			//
			// This is reported rather than only recorded in firstErr, which
			// surfaces only if EVERY candidate fails. The directory itself is
			// left untouched and RecoverFromSegments raises its own fault for
			// it, so the records in it are stranded and loud, never reattributed.
			if firstErr == nil {
				firstErr = merr
			}
			sm.logWarn("a segment directory this queue would have used has an unusable ownership marker; it cannot be proven to belong to this queue, so the next candidate name is used",
				interfaces.LogField{Key: "queue", Value: queueName},
				interfaces.LogField{Key: "dir", Value: full},
				interfaces.LogField{Key: "error", Value: merr.Error()})
			continue
		}

		if present {
			if marker == queueName {
				return cand, nil
			}
			// The candidate exists and belongs to somebody else. Reachable when
			// a queue whose LITERAL name is a name this queue would generate
			// already owns it (queue "/" generates "%2f"; queue "%2f" owns
			// "%2f"), and on a folding volume when a case-variant sibling got
			// here first. Probing on is correct in both cases — but it used to
			// be SILENT, so an operator never learned that two queues had
			// contended for one directory (review-5 CRITICAL-2, constraint b).
			sm.logWarn("a segment directory this queue would have used is owned by another queue; using the next candidate name",
				interfaces.LogField{Key: "queue", Value: queueName},
				interfaces.LogField{Key: "dir", Value: full},
				interfaces.LogField{Key: "owner", Value: marker})
			continue
		}

		// MARKERLESS: a legacy directory, whose NAME is its owner. It can only
		// ever be adopted by the queue the name spells, so a generated
		// candidate — which never equals queueName — is somebody else's.
		if cand != queueName {
			continue
		}

		// The name we ASKED for spells this queue. That is not yet proof the
		// entry that ANSWERED spells it, because os.Stat(".../queue") succeeds
		// against an on-disk "Queue" on a case-insensitive or normalising
		// volume. Adopting on the strength of the asked-for name is review-5
		// CRITICAL-2: the queue stamped its own name into the other queue's
		// legacy directory and evicted the queue that actually wrote the data.
		//
		// So require an entry spelled EXACTLY this, by byte equality against the
		// real directory listing. Byte equality — not os.SameFile — because
		// inode identity answers a different question and answers it badly: a
		// symlinked segment directory yields two matching entries on any volume
		// including ext4, DirEntry.Info() is lstat-based and matches neither,
		// and inode numbers are unreliable on the network mounts where folding
		// also happens.
		//
		// WHAT EXACT SPELLING DOES TO NORMALISATION: it REFUSES it, it does not
		// cover it, and the difference costs a queue its data. An earlier
		// version of this comment claimed the opposite (review-7 MAJOR-1
		// measured it false). A single ordinary queue — no case-variant sibling
		// anywhere — whose name is NFD "café" and whose own legacy directory is
		// spelled NFC on disk does not match here, so it probes on to a fresh
		// empty directory and SILENTLY DOES NOT RECOVER ITS OWN CHECKPOINTED
		// DURABLE MESSAGES. They stay on disk under the other spelling and
		// RecoverFromSegments still returns them under that key, so nothing is
		// destroyed or handed to another queue — but this queue does not see
		// them. Reachability: a pre-marker legacy directory AND a normalising
		// volume (HFS+, some SMB) or a name whose spelling changed between
		// boots. Inherited from d5c3f85, not introduced here: once recovery has
		// stamped a marker into that legacy directory — which it does on the
		// first boot, before any of this runs — d5c3f85 strands the same queue
		// through the markered-mismatch branch above. Registered in
		// .notes/loop-2/fix-5.md; NOT closed.
		//
		// The listing is read at most ONCE per call and only on this branch, so
		// no directory that carries a marker — i.e. anything this build created
		// — pays for it.
		if !entriesLoaded {
			var lerr error
			entryNames, lerr = loadDirEntryNames(sm.dataDir)
			if lerr != nil && firstErr == nil {
				firstErr = lerr
			}
			entriesLoaded = true
		}
		if entryNames == nil {
			// Without a listing this candidate cannot be proven, so it is
			// skipped — which strands the queue on a generated directory
			// instead of its legacy one, exactly the shape above. The two
			// sibling `continue`s in this loop both log; this one used not to
			// (review-7 MINOR-6), so the one case an operator could actually
			// act on was the one that said nothing.
			sm.logWarn("this queue's segments directory could not be listed, so a legacy directory spelled exactly like the queue cannot be proven to be its own; using the next candidate name",
				interfaces.LogField{Key: "queue", Value: queueName},
				interfaces.LogField{Key: "dir", Value: sm.dataDir},
				interfaces.LogField{Key: "consequence", Value: "if a legacy directory for this queue exists, its checkpointed durable messages are not read; nothing on disk is modified"})
			continue
		}
		if _, exact := entryNames[cand]; !exact {
			// Do not adopt: stamping a marker here would dispossess whoever
			// wrote it. Probe on rather than fault: the directory is left
			// untouched, this queue gets a fresh correctly-marked one, and the
			// legacy directory is still read by RecoverFromSegments — which
			// never calls this function — under the name it is spelled with.
			// Faulting instead would pin the SHARED WAL for every queue in it
			// and could not recover a single record that probing on loses.
			sm.logWarn("a legacy segment directory matched this queue's name only because the filesystem folds spellings; refusing to adopt it and using the next candidate name",
				interfaces.LogField{Key: "queue", Value: queueName},
				interfaces.LogField{Key: "dir", Value: full},
				interfaces.LogField{Key: "consequence", Value: "this queue starts a new segment directory; the existing one is left untouched and is still attributed to the queue its own name spells"})
			continue
		}
		return cand, nil
	}
	return "", interfaces.DegradedFault("segment-open", filepath.Join(sm.dataDir, segmentDirEncoded(queueName)),
		fmt.Sprintf("no usable segment directory could be resolved for queue %q", queueName),
		"this queue's messages are not checkpointed to cold storage; the source WAL file is NOT deleted, so nothing is lost, but the WAL will keep growing until this is fixed",
		firstErr)
}

// Write writes a message to segments (called during checkpoint from WAL)
func (sm *SegmentManager) Write(queueName string, message *protocol.Message, offset uint64) error {
	segments, err := sm.getOrCreateQueueSegments(queueName)
	if err != nil {
		return err
	}
	return segments.writeMessage(message, offset)
}

// Read reads a message from segments by offset
func (sm *SegmentManager) Read(queueName string, offset uint64) (*protocol.Message, error) {
	val, ok := sm.queueSegments.Load(queueName)
	if !ok {
		return nil, fmt.Errorf("segments not found for queue %s", queueName)
	}
	segments := val.(*QueueSegments)

	return segments.readMessage(offset)
}

// Acknowledge marks a message as ACKed for future compaction.
// M2: Sends the offset to a batch ACK channel instead of locking bitmapMutex
// per call. The batchAckLoop goroutine collects ACKs and applies them in
// batches, reducing lock contention from O(N) to O(N/batchSize).
func (sm *SegmentManager) Acknowledge(queueName string, offset uint64) {
	val, ok := sm.queueSegments.Load(queueName)
	if !ok {
		return
	}
	segments := val.(*QueueSegments)

	// Non-blocking send to batch ACK channel (drop if full to prevent backpressure)
	select {
	case segments.ackChan <- offset:
	default:
		// Channel full — apply directly as fallback
		segments.applyAck(offset)
	}
}

// applyAck applies a single ACK to the bitmap and segment counters.
func (qs *QueueSegments) applyAck(offset uint64) {
	qs.bitmapMutex.Lock()
	qs.ackBitmap.Add(offset)
	qs.bitmapMutex.Unlock()
	qs.acknowledgeInSegment(offset)
}

// batchAckLoop collects ACKs from the channel and applies them in batches,
// reducing bitmapMutex lock contention from per-ACK to per-batch.
func (qs *QueueSegments) batchAckLoop() {
	defer qs.wg.Done()

	batch := make([]uint64, 0, qs.ackBatchSize)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		// Single lock acquisition for the entire batch
		qs.bitmapMutex.Lock()
		for _, offset := range batch {
			qs.ackBitmap.Add(offset)
		}
		qs.bitmapMutex.Unlock()

		// Update segment-level deleted counts (no bitmap lock needed)
		for _, offset := range batch {
			qs.acknowledgeInSegment(offset)
		}
		batch = batch[:0]
	}

	for {
		select {
		case offset := <-qs.ackChan:
			batch = append(batch, offset)
			if len(batch) >= qs.ackBatchSize {
				flush()
			}

		case <-ticker.C:
			flush()

		case <-qs.stopChan:
			// Drain remaining ACKs before exit
			for {
				select {
				case offset := <-qs.ackChan:
					batch = append(batch, offset)
				default:
					flush()
					return
				}
			}
		}
	}
}

// acknowledgeInSegment finds the segment containing the offset and increments its deletedCount
func (qs *QueueSegments) acknowledgeInSegment(offset uint64) {
	// Check current segment first
	qs.mutex.Lock()
	if qs.currentSegment != nil {
		if qs.currentIndex != nil {
			qs.currentIndex.mutex.RLock()
			_, exists := qs.currentIndex.entries[offset]
			qs.currentIndex.mutex.RUnlock()
			if exists {
				qs.currentSegment.deletedCount.Add(1)
				qs.mutex.Unlock()
				return
			}
		}
	}
	qs.mutex.Unlock()

	// Check sealed segments
	qs.sealedMutex.RLock()
	defer qs.sealedMutex.RUnlock()

	for _, seg := range qs.sealedSegments {
		if offset >= seg.minOffset && offset <= seg.maxOffset {
			seg.mutex.RLock()
			_, exists := seg.index[offset]
			seg.mutex.RUnlock()
			if exists {
				seg.deletedCount.Add(1)
				return
			}
		}
	}
}

// CheckpointBatch writes a batch of recovery messages to segments for a given queue.
// M5: All messages are written in a single file.Write() call with a single
// mutex acquisition, reducing lock contention and write syscalls from O(N) to O(1).
// Used during WAL checkpoint to migrate messages from WAL to cold storage.
//
// WS-C C2: a nil return now means the batch is DURABLE — written in full (any
// partial write is truncated away or the segment retired before an error is
// returned), fdatasynced, and in a file whose directory entry was made durable
// at creation. Callers (performCheckpoint, forceCheckpointFile) may only unlink
// the source WAL file on nil.
func (sm *SegmentManager) CheckpointBatch(queueName string, messages []*RecoveryMessage) error {
	if len(messages) == 0 {
		return nil
	}
	segments, err := sm.getOrCreateQueueSegments(queueName)
	if err != nil {
		return sm.reportCheckpointFailure(queueName, fmt.Errorf("checkpoint failed for queue %s: %w", queueName, err))
	}
	if err := segments.writeMessageBatchDurable(messages); err != nil {
		return sm.reportCheckpointFailure(queueName, fmt.Errorf("checkpoint batch write failed for queue %s: %w", queueName, err))
	}
	return nil
}

// reportCheckpointFailure logs a failed checkpoint and returns the error
// unchanged.
//
// review-4 B-2: performCheckpoint handles this error correctly for DURABILITY
// (checkpointOK = false, so the source WAL file is not unlinked and nothing is
// lost) but discards it entirely — no log, no metric, nothing plumbed anywhere.
// The visible symptom was therefore that the queue's WAL files are never
// reclaimed, the WAL directory grows every 5 minutes forever, and the only
// diagnosis available is the disk filling up. The fault's own cost string says
// exactly that and nothing ever printed it. It is reported HERE, at the tier that
// knows the queue and the path, rather than at the discard site.
func (sm *SegmentManager) reportCheckpointFailure(queueName string, err error) error {
	sm.logError("checkpoint to cold storage FAILED; this queue's WAL files cannot be reclaimed until it is fixed. Nothing is lost: the source WAL file is left in place.",
		interfaces.LogField{Key: "queue", Value: queueName},
		interfaces.LogField{Key: "error", Value: err.Error()})
	return err
}

// writeMessageBatchDurable writes multiple messages to the current segment in
// a single write, makes them durable with one fdatasync, and only then
// publishes the index and size — all under ONE hold of qs.mutex, so two
// concurrent CheckpointBatch calls (checkpointLoop and retention) cannot
// interleave between the write and the sync.
//
// WS-C C2 failure semantics:
//   - a partial/failed write is truncated back to the pre-batch size (the
//     active segment is append-only with one writer under qs.mutex, and
//     nothing reads past the indexed positions), or the segment is RETIRED
//     (damaged tail, benign at load) if the truncate itself fails;
//   - a failed fsync gets the same treatment (fsyncgate: nobody knows whether
//     the bytes reached the media, and the batch is being failed);
//   - the index is updated only AFTER the fsync, so a failed batch leaves no
//     readable entries and the WAL is kept for a retry;
//   - a failed ROLL after a durable batch is logged, not returned: the batch
//     is durable, and failing the checkpoint would rewrite it (duplicates,
//     removed at recovery by the C8 dedup) while the oversized active segment
//     keeps serving writes and reads.
func (qs *QueueSegments) writeMessageBatchDurable(messages []*RecoveryMessage) error {
	qs.mutex.Lock()
	defer qs.mutex.Unlock()

	if qs.currentSegment == nil || qs.segPoisoned != nil {
		if err := qs.replaceActiveSegmentLocked(); err != nil {
			return fmt.Errorf("segment unavailable: %w", err)
		}
	}
	seg := qs.currentSegment
	pre := seg.fileSize.Load()

	// Serialize all messages and build a single write buffer + index updates
	type indexUpdate struct {
		offset   uint64
		position int64
	}
	updates := make([]indexUpdate, 0, len(messages))
	var buf []byte

	for _, rm := range messages {
		position := pre + int64(len(buf))
		msgBytes, err := serializeSegmentMessage(rm.Message, rm.Offset, qs.cfg.CRCDisabled)
		if err != nil {
			// A record that cannot be encoded (shortstr property >255 bytes) fails
			// the whole checkpoint batch rather than writing a corrupt segment;
			// the source WAL file is not deleted, so the messages stay durable.
			return fmt.Errorf("failed to serialize segment message at offset %d: %w", rm.Offset, err)
		}
		buf = append(buf, msgBytes...)
		updates = append(updates, indexUpdate{offset: rm.Offset, position: position})
	}

	// Single write syscall for all messages (hookable; default is f.Write).
	// A short write without an error is normalised to io.ErrShortWrite.
	n, err := segmentWrite(seg.file, buf)
	if err == nil && n != len(buf) {
		err = io.ErrShortWrite
	}
	if err != nil {
		qs.repairSegmentLocked(seg, pre, "write", err)
		qs.assertSegmentSizeInvariantLocked()
		return fmt.Errorf("failed to write batch to segment: %w", err)
	}

	// One fdatasync makes the whole batch durable BEFORE anything is indexed
	// or counted (hookable; default is fdatasyncFile).
	if err := segmentFsync(seg.file); err != nil {
		qs.repairSegmentLocked(seg, pre, "fsync", err)
		qs.assertSegmentSizeInvariantLocked()
		return fmt.Errorf("checkpoint fsync failed: %w", err)
	}

	// Durable. Publish the index entries in a single lock acquisition, then
	// the metadata.
	qs.currentIndex.mutex.Lock()
	for _, u := range updates {
		qs.currentIndex.entries[u.offset] = u.position
	}
	qs.currentIndex.mutex.Unlock()

	seg.fileSize.Add(int64(n))
	seg.messageCount.Add(uint64(len(messages)))
	for _, rm := range messages {
		if rm.Offset < seg.minOffset || seg.minOffset == 0 {
			seg.minOffset = rm.Offset
		}
		if rm.Offset > seg.maxOffset {
			seg.maxOffset = rm.Offset
		}
	}
	qs.assertSegmentSizeInvariantLocked()

	// Check if we need to roll to a new segment. Open-before-close: on
	// failure the active segment stays writable and the roll is retried on
	// the next batch.
	if seg.fileSize.Load() >= qs.cfg.SegmentSize {
		if err := qs.rollSegmentLocked(); err != nil {
			qs.logError("segment roll failed; the active segment is kept (its data is durable and it stays writable)",
				interfaces.LogField{Key: "queue", Value: qs.queueName},
				interfaces.LogField{Key: "error", Value: err.Error()})
		}
	}

	return nil
}

// Close closes all segments
func (sm *SegmentManager) Close() error {
	sm.queueSegments.Range(func(key, value interface{}) bool {
		segments := value.(*QueueSegments)
		segments.close()
		return true
	})
	return nil
}

// getOrCreateQueueSegments returns or creates segments for a queue.
func (sm *SegmentManager) getOrCreateQueueSegments(queueName string) (*QueueSegments, error) {
	// The already-known case must not pay for directory resolution: it is the
	// only path Write and CheckpointBatch take after the first touch.
	if val, ok := sm.queueSegments.Load(queueName); ok {
		return val.(*QueueSegments), nil
	}
	// REFUSED BEFORE ANY DIRECTORY IS RESOLVED. This guard used to live one
	// call downstream, in getOrCreateQueueSegmentsAt, which runs AFTER
	// segmentDirFor has already mkdir'd a candidate. Two defects followed
	// (review-7 MINOR-2 and MINOR-3): the fault claimed "nothing on disk is
	// modified" while a directory had just been created, and a retried
	// checkpoint leaked one directory per attempt — `%`, `%-1`, ... up to
	// segmentDirProbeLimit — because each attempt probed one candidate further.
	// Refusing here makes the claim true and the leak impossible, instead of
	// documenting either.
	if err := refuseEmptyQueueName(sm.dataDir, queueName); err != nil {
		return nil, err
	}
	dirName, err := sm.segmentDirFor(queueName)
	if err != nil {
		return nil, err
	}
	return sm.getOrCreateQueueSegmentsAt(queueName, dirName)
}

// refuseEmptyQueueName rejects the one name no queue can have.
//
// AMQP 0-9-1 queue.declare's default-name rule means an empty name is replaced
// by a generated one before it leaves the server tier
// (server/queue_handlers.go:82-87), so "" arriving at this tier was INVENTED —
// by a damaged ownership marker, which is how review-5 CRITICAL-1 handed one
// queue's checkpointed durable messages to queue "".
//
// The cost sentence names the WAL pin and nothing else. It used to describe the
// recovery path ("the messages in this directory are not recovered"), which
// cannot reach this guard at all: RecoverFromSegments attributes by
// segmentDirOwner, and a ReadDir entry name is never "" while a valid marker's
// name is 1..255 bytes. The only reachable caller is the checkpoint path
// (review-7 MINOR-2), whose real cost is the one its three sibling faults state.
func refuseEmptyQueueName(dataDir, queueName string) error {
	if queueName != "" {
		return nil
	}
	return interfaces.DegradedFault("segment-open", dataDir,
		"a segment write was attributed to the empty queue name, which no queue can have, so no segment directory is resolved or created for it",
		"this queue's messages are not checkpointed to cold storage; the source WAL file is NOT deleted, so nothing is lost, but the WAL will keep growing until this is fixed",
		nil)
}

// getOrCreateQueueSegmentsAt is getOrCreateQueueSegments bound to an explicit
// directory entry. Recovery uses it so a directory found on disk is opened AS
// FOUND rather than re-derived from the decoded queue name — re-deriving would
// strand any directory whose encoding this build would spell differently.
//
// It returns an error instead of nil. The previous nil return was dereferenced
// unconditionally by Write and CheckpointBatch, so a directory this process
// could not create (a name the filesystem rejects, a collision with a regular
// file) crashed the checkpoint goroutine with a nil-pointer panic. Failing the
// checkpoint instead leaves the source WAL file in place, so nothing is lost.
func (sm *SegmentManager) getOrCreateQueueSegmentsAt(queueName, dirName string) (*QueueSegments, error) {
	// Kept here as well as in getOrCreateQueueSegments because RecoverFromSegments
	// calls THIS function directly, bypassing the upstream guard. One
	// implementation, one message (canon rule 12).
	if err := refuseEmptyQueueName(sm.dataDir, queueName); err != nil {
		return nil, err
	}

	val, ok := sm.queueSegments.Load(queueName)
	if ok {
		return val.(*QueueSegments), nil
	}

	// Create new QueueSegments
	queueDir := filepath.Join(sm.dataDir, dirName)
	if err := os.MkdirAll(queueDir, 0755); err != nil {
		return nil, interfaces.DegradedFault("segment-open", queueDir,
			fmt.Sprintf("the segment directory for queue %q could not be created", queueName),
			"this queue's messages are not checkpointed to cold storage; the source WAL file is NOT deleted, so nothing is lost, but the WAL will keep growing until this is fixed",
			err)
	}
	// C4: make the queue directory's entry durable in its PARENT (segments/)
	// before any segment file is created inside it — the marker's own
	// atomicWriteFile already syncs queueDir itself.
	if serr := syncDir(sm.dataDir); serr != nil {
		return nil, interfaces.DegradedFault("segment-open", sm.dataDir,
			fmt.Sprintf("the segment directory for queue %q was created but its directory entry could not be made durable", queueName),
			"this queue's messages are not checkpointed to cold storage; the source WAL file is NOT deleted, so nothing is lost, but the WAL will keep growing until this is fixed",
			serr)
	}

	// OWNERSHIP. The marker is written before any segment file can exist here,
	// so a directory never holds records without naming its owner.
	//
	// A FAILED MARKER WRITE IS FATAL TO THIS CALL. It used to be fatal only when
	// the directory name did not spell the queue name, and a warning otherwise
	// — but a warning is not a handler when the failure costs a queue its data
	// on the next boot (review-5 CRITICAL-1). Two things make refusing the right
	// answer rather than a new availability risk:
	//
	//   - it costs no availability that was not already lost. This directory is
	//     about to receive a segment file from openNextSegment below; a
	//     filesystem that cannot take a 60-byte marker cannot take that either,
	//     so refusing here reports the real cause earlier and more precisely
	//     instead of inventing a failure.
	//   - proceeding leaves a permanently markerless directory, which is the
	//     residual state that keeps the folding-volume ambiguity in segmentDirFor
	//     alive. Every marker that gets written removes a directory from it.
	marker, present, merr := readSegmentDirMarker(queueDir)
	switch {
	case merr != nil:
		return nil, interfaces.DegradedFault("segment-open", filepath.Join(queueDir, segmentDirMarkerName),
			fmt.Sprintf("the ownership marker of the segment directory for queue %q is present but unusable, so the directory cannot be attributed safely", queueName),
			"this queue's messages are not checkpointed to cold storage; the source WAL file is NOT deleted, so nothing is lost, but the WAL will keep growing until this is fixed",
			merr)
	case present && marker != queueName:
		return nil, interfaces.DegradedFault("segment-open", queueDir,
			fmt.Sprintf("the segment directory selected for queue %q is owned by queue %q; refusing to write a second queue's records into it", queueName, marker),
			"this queue's messages are not checkpointed to cold storage; the source WAL file is NOT deleted, so nothing is lost, but the WAL will keep growing until this is fixed",
			nil)
	case !present:
		if werr := writeSegmentDirMarker(queueDir, queueName); werr != nil {
			return nil, interfaces.DegradedFault("segment-open", filepath.Join(queueDir, segmentDirMarkerName),
				fmt.Sprintf("the ownership marker for queue %q could not be written, so this directory cannot be proven to belong to it", queueName),
				"this queue's messages are not checkpointed to cold storage; the source WAL file is NOT deleted, so nothing is lost, but the WAL will keep growing until this is fixed",
				werr)
		}
	}

	// Batch ACK settings
	const ackChannelBuffer = 1000
	const ackBatchSize = 100

	segments := &QueueSegments{
		queueName:      queueName,
		dataDir:        queueDir,
		cfg:            sm.cfg,
		sealedSegments: make(map[uint64]*SegmentFile),
		ackBitmap:      roaring64.New(),
		ackChan:        make(chan uint64, ackChannelBuffer),
		ackBatchSize:   ackBatchSize,
		stopChan:       make(chan struct{}),
		metrics:        sm.metrics,
		logger:         sm.logger,
	}

	// Load existing segments before opening a new active segment
	segments.loadFaults = segments.loadExistingSegments()

	// review-4 N-7: loadFaults are drained only by RecoverFromSegments, so a
	// QueueSegments first constructed by the 5-minute checkpoint had its
	// classified faults collected into a slice nobody read — a fault producer
	// with no consumer. Logging them here gives every fault a consumer without
	// double-counting: the refusal message's artifact count still comes from
	// takeLoadFaults, not from log lines.
	for _, f := range segments.loadFaults {
		sm.logError("a segment file could not be fully interpreted",
			interfaces.LogField{Key: "queue", Value: queueName},
			interfaces.LogField{Key: "fault", Value: f.Error()})
	}

	// Create first segment
	if err := segments.openNextSegment(); err != nil {
		return nil, interfaces.DegradedFault("segment-open", queueDir,
			fmt.Sprintf("a new segment file could not be opened for queue %q", queueName),
			"this queue's messages are not checkpointed to cold storage; the source WAL file is NOT deleted, so nothing is lost, but the WAL will keep growing until this is fixed",
			err)
	}

	// Start background goroutines
	segments.wg.Add(2)
	go segments.compactionLoop()
	go segments.batchAckLoop()

	// Store and return
	actual, loaded := sm.queueSegments.LoadOrStore(queueName, segments)
	if loaded {
		segments.close()
		return actual.(*QueueSegments), nil
	}

	return segments, nil
}

// takeLoadFaults drains the faults recorded when this queue's directory was
// scanned. Draining rather than reading keeps a fault from being counted twice
// if recovery runs more than once in a process.
func (qs *QueueSegments) takeLoadFaults() []error {
	qs.loadFaultsMutex.Lock()
	defer qs.loadFaultsMutex.Unlock()
	out := qs.loadFaults
	qs.loadFaults = nil
	return out
}

// writeMessage writes a message to the current active segment
func (qs *QueueSegments) writeMessage(message *protocol.Message, offset uint64) error {
	qs.mutex.Lock()
	defer qs.mutex.Unlock()

	// WS-C review finding 3: the same poison gate as writeMessageBatchDurable.
	// A poisoned (diverged) active segment must never be appended to; retire
	// it and open a fresh one first, or refuse the write if no replacement can
	// be opened.
	if qs.currentSegment == nil || qs.segPoisoned != nil {
		if err := qs.replaceActiveSegmentLocked(); err != nil {
			return fmt.Errorf("segment unavailable: %w", err)
		}
	}

	// Serialize message
	msgBytes, err := serializeSegmentMessage(message, offset, qs.cfg.CRCDisabled)
	if err != nil {
		return fmt.Errorf("failed to serialize segment message at offset %d: %w", offset, err)
	}

	// Get current file position
	position := qs.currentSegment.fileSize.Load()

	// Write to file (hookable; default is f.Write). A short write without an
	// error is normalised to io.ErrShortWrite; on any failure the partial
	// bytes are truncated away or the segment retired (C2 — writeMessage has
	// no production caller today, but leaving the stale-fileSize trap in
	// place would be worse). It does not fsync, which is unchanged.
	n, err := segmentWrite(qs.currentSegment.file, msgBytes)
	if err == nil && n != len(msgBytes) {
		err = io.ErrShortWrite
	}
	if err != nil {
		qs.repairSegmentLocked(qs.currentSegment, position, "write", err)
		qs.assertSegmentSizeInvariantLocked()
		return fmt.Errorf("failed to write to segment: %w", err)
	}

	// Update index
	qs.currentIndex.mutex.Lock()
	qs.currentIndex.entries[offset] = position
	qs.currentIndex.mutex.Unlock()

	// Update metadata
	qs.currentSegment.fileSize.Add(int64(n))
	qs.currentSegment.messageCount.Add(1)
	if offset < qs.currentSegment.minOffset || qs.currentSegment.minOffset == 0 {
		qs.currentSegment.minOffset = offset
	}
	if offset > qs.currentSegment.maxOffset {
		qs.currentSegment.maxOffset = offset
	}

	// Check if we need to roll to a new segment. rollSegmentLocked opens the
	// next file first, so a failed open leaves the current segment active and
	// writable instead of sealed-read-only; a roll failure after this record
	// is on disk is logged, not returned.
	if qs.currentSegment.fileSize.Load() >= qs.cfg.SegmentSize {
		if rerr := qs.rollSegmentLocked(); rerr != nil {
			qs.logError("segment roll failed; the active segment is kept (its data is durable and it stays writable)",
				interfaces.LogField{Key: "queue", Value: qs.queueName},
				interfaces.LogField{Key: "error", Value: rerr.Error()})
		}
	}

	return nil
}

// readMessage reads a message from segments by offset
func (qs *QueueSegments) readMessage(offset uint64) (*protocol.Message, error) {
	// Try current segment first
	qs.mutex.Lock()
	currentSeg := qs.currentSegment
	currentIdx := qs.currentIndex
	qs.mutex.Unlock()

	if currentSeg != nil {
		currentIdx.mutex.RLock()
		position, found := currentIdx.entries[offset]
		currentIdx.mutex.RUnlock()

		if found {
			// The lock is held ACROSS the read, for the same reason as the
			// sealed branch below and against a different writer: sealSegment
			// does `file.Close(); file = readFile` on THIS SegmentFile while a
			// reader may already hold the pointer, having released qs.mutex
			// after the snapshot. Unsynchronized, that is a data race on
			// currentSeg.file AND — because the Close lands first — a reader
			// gets `file already closed`, which deliverMessage turns into a GAP
			// SKIP: the durable message is dropped, not retried. Measured at 32
			// readers: 8, 12 and 3 dropped reads in three 20-second runs
			// (review-4 B-3).
			currentSeg.mutex.RLock()
			msg, err := readSegmentMessageAt(currentSeg.file, position)
			currentSeg.mutex.RUnlock()
			if err != nil && qs.metrics != nil {
				qs.metrics.RecordSegmentReadError()
			}
			return msg, err
		}
	}

	// Try sealed segments
	qs.sealedMutex.RLock()
	defer qs.sealedMutex.RUnlock()

	for _, segment := range qs.sealedSegments {
		if offset >= segment.minOffset && offset <= segment.maxOffset {
			// The lock is held ACROSS the read, not just across the index
			// lookup. compactSegment swaps segment.file and segment.index
			// together; a reader that released the lock in between would pair a
			// position from one layout with a handle to the other and return a
			// different, wrong message — S-2's outcome reached by a data race
			// instead of by a failed rename. It is also a plain Go data race on
			// segment.file, which -race reports. compactSegment holds the write
			// lock only for the swap itself, not for the rewrite.
			segment.mutex.RLock()
			position, found := segment.index[offset]
			if !found {
				segment.mutex.RUnlock()
				continue
			}
			msg, err := readSegmentMessageAt(segment.file, position)
			segment.mutex.RUnlock()
			if err != nil && qs.metrics != nil {
				qs.metrics.RecordSegmentReadError()
			}
			return msg, err
		}
	}

	// Record as error since message wasn't found
	if qs.metrics != nil {
		qs.metrics.RecordSegmentReadError()
	}
	return nil, fmt.Errorf("message not in segments")
}

// sealSegment moves current segment to sealed segments.
// Returns an error if the segment file cannot be synced to disk.
func (qs *QueueSegments) sealSegment() error {
	if qs.currentSegment == nil {
		return nil
	}

	// Sync to disk — must succeed for durability before sealing
	if err := qs.currentSegment.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync segment before sealing: %w", err)
	}

	// Reopen the file read-only for sealed-segment reads BEFORE taking the
	// segment lock, so no file I/O happens inside it.
	readFile, err := os.Open(qs.currentSegment.path)

	// The index copy and the handle swap happen under ONE hold of
	// segment.mutex, which is the lock readMessage and collectAllMessages take
	// across their reads. Splitting them (index under the lock, Close+assign
	// outside it) is what made a concurrent reader see a CLOSED handle and drop
	// the message — review-4 B-3, the same shape as the compaction race one
	// branch below, on a different lifecycle path.
	qs.currentSegment.mutex.Lock()
	if qs.currentIndex != nil {
		qs.currentIndex.mutex.RLock()
		for offset, position := range qs.currentIndex.entries {
			qs.currentSegment.index[offset] = position
		}
		qs.currentIndex.mutex.RUnlock()
	}
	if err == nil {
		old := qs.currentSegment.file
		qs.currentSegment.file = readFile
		_ = old.Close()
	}
	qs.currentSegment.mutex.Unlock()

	// The reopen is an optimisation, not a correctness requirement: on failure
	// the segment stays sealed behind its original O_APPEND read-write handle,
	// which ReadAt serves correctly, so no data is at risk and sealing must not
	// be abandoned. But the error used to be discarded with no else, no log and
	// no metric (review-5 MINOR-6), so a process that had hit its fd limit sealed
	// every subsequent segment while leaking a writable handle each time and
	// told nobody. Reported here, and NOT returned, so the distinction between
	// "degraded" and "failed" stays honest.
	//
	// NOT METERED, deliberately, and not an oversight to be "restored". This
	// used to call RecordSegmentReadError, which folds a DESCRIPTOR problem at
	// seal time into the segment READ-error series, so an operator watching that
	// rate would diagnose a read fault that is not happening (review-7 MINOR-7).
	// A counter naming the real condition is worth having, but SegmentMetrics
	// has no such method and adding one reaches server/metrics.go and a test
	// double in a file Step 5's rebase also touches — real conflict risk for a
	// cosmetic gain. Registered for Step 7 in .notes/loop-2/fix-5.md. Until
	// then the Error log below carries the queue, the segment path and the
	// errno, which is the actionable half; a wrong number is worse than none.
	if err != nil {
		qs.logError("a sealed segment could not be reopened read-only; it stays sealed behind its read-write handle, which serves reads correctly but is not downgraded",
			interfaces.LogField{Key: "queue", Value: qs.queueName},
			interfaces.LogField{Key: "segment", Value: qs.currentSegment.path},
			interfaces.LogField{Key: "error", Value: err.Error()})
	}

	// Move to sealed segments
	qs.sealedMutex.Lock()
	qs.sealedSegments[qs.currentSegment.segmentNum] = qs.currentSegment
	qs.sealedMutex.Unlock()

	return nil
}

// openNextSegment creates and opens the next segment file (acquires qs.mutex).
// Use openNextSegmentLocked when the caller already holds qs.mutex.
func (qs *QueueSegments) openNextSegment() error {
	qs.mutex.Lock()
	defer qs.mutex.Unlock()
	return qs.openNextSegmentLocked()
}

// openNextSegmentLocked installs a fresh segment file as the active one.
// Precondition: caller must hold qs.mutex.
func (qs *QueueSegments) openNextSegmentLocked() error {
	seg, idx, err := qs.createSegmentFileLocked()
	if err != nil {
		return err
	}
	qs.currentSegment = seg
	qs.currentIndex = idx
	return nil
}

// createSegmentFileLocked creates a fresh segment file and returns it with a
// fresh index. Precondition: caller holds qs.mutex.
//
// WS-C C2: the number is STRICTLY MONOTONE (max of wall clock and
// lastSegmentNum+1, and lastSegmentNum is seeded from the highest number
// loaded at boot), so two numbers can never collide — and the file is opened
// O_EXCL, so even if one somehow does (a stray file, a number minted by a
// previous incarnation), the create FAILS instead of reopening the existing
// file in append mode with fileSize=0, which is the segment twin of the
// stale-offset bug C1 fixed in the WAL. On an EEXIST collision the number is
// bumped and retried, up to 16 times.
//
// The directory entry is made durable at creation (C4): the reliance point —
// the first CheckpointBatch == nil that leads to a WAL unlink — always comes
// later, so syncing here keeps the per-checkpoint cost flat.
func (qs *QueueSegments) createSegmentFileLocked() (*SegmentFile, *SegmentIndex, error) {
	num := uint64(time.Now().UnixNano())
	if num <= qs.lastSegmentNum {
		num = qs.lastSegmentNum + 1
	}
	for attempt := 0; attempt < 16; attempt++ {
		path := filepath.Join(qs.dataDir, fmt.Sprintf("%020d%s", num, SegmentFileExtension))
		f, err := segmentOpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR|os.O_APPEND, 0644)
		if err != nil {
			if os.IsExist(err) {
				num++ // never reopen an existing file with fileSize=0
				continue
			}
			return nil, nil, fmt.Errorf("failed to open segment file: %w", err)
		}
		if serr := syncDir(qs.dataDir); serr != nil {
			_ = f.Close()
			_ = os.Remove(path)
			return nil, nil, fmt.Errorf("segment dir fsync: %w", serr)
		}
		qs.lastSegmentNum = num
		return &SegmentFile{
			segmentNum: num,
			path:       path,
			file:       f,
			index:      make(map[uint64]int64),
		}, &SegmentIndex{entries: make(map[uint64]int64)}, nil
	}
	return nil, nil, fmt.Errorf("could not create a segment file in %s: 16 consecutive name collisions", qs.dataDir)
}

// rollSegmentLocked seals the current segment and installs a fresh one as the
// active segment. Precondition: caller holds qs.mutex.
//
// WS-C C2: the NEXT segment is created FIRST. On a creation failure the
// current segment stays ACTIVE and writable (oversized; SegmentSize is a soft
// limit). The previous shape sealed the current segment and then opened the
// next one, so a failed open left currentSegment pointing at a sealed
// READ-ONLY handle and every later write failed with EBADF.
func (qs *QueueSegments) rollSegmentLocked() error {
	next, nextIdx, err := qs.createSegmentFileLocked()
	if err != nil {
		return err // current stays active
	}
	if err := qs.sealSegment(); err != nil {
		_ = next.file.Close()
		_ = os.Remove(next.path)
		return err // current stays active (unsealed, writable)
	}
	qs.currentSegment = next
	qs.currentIndex = nextIdx
	return nil
}

// replaceActiveSegmentLocked retires the current segment WITHOUT sealing it
// (no Sync: its earlier batches were fdatasynced by their own
// writeMessageBatchDurable calls) and installs a fresh one. It is called with
// the current segment damaged (segPoisoned) or missing.
// Precondition: caller holds qs.mutex.
func (qs *QueueSegments) replaceActiveSegmentLocked() error {
	next, nextIdx, err := qs.createSegmentFileLocked()
	if err != nil {
		return err // stays poisoned; the next CheckpointBatch retries
	}
	if cur := qs.currentSegment; cur != nil {
		// Retire: copy the live index into the segment's own index under its
		// lock (as sealSegment does), try a read-only reopen (best effort, as
		// sealSegment does), and mark the damaged tail. The failed batch was
		// never indexed (indexing happens only after fsync), so the retired
		// segment's index covers only its durable prefix — but the damaged
		// tail's bytes remain in the file and are classified BENIGN at load:
		// nothing is ever appended after them, which is what turns tail
		// damage into interior damage.
		cur.mutex.Lock()
		if qs.currentIndex != nil {
			qs.currentIndex.mutex.RLock()
			for offset, position := range qs.currentIndex.entries {
				cur.index[offset] = position
			}
			qs.currentIndex.mutex.RUnlock()
		}
		if readFile, rerr := os.Open(cur.path); rerr == nil {
			old := cur.file
			cur.file = readFile
			_ = old.Close()
		}
		cur.damagedTail = qs.segPoisoned != nil
		cur.mutex.Unlock()

		qs.sealedMutex.Lock()
		qs.sealedSegments[cur.segmentNum] = cur
		qs.sealedMutex.Unlock()
	}
	qs.currentSegment = next
	qs.currentIndex = nextIdx
	qs.segPoisoned = nil
	return nil
}

// repairSegmentLocked repairs the active segment after a failed batch
// write/fsync: truncate back to the pre-batch size (fdatasync + Stat check).
// If the truncate fails, the segment is retired (damaged tail is benign;
// nothing confirmed is lost — everything before pre was fdatasynced by an
// earlier successful batch) and a fresh active segment takes over.
// Precondition: caller holds qs.mutex.
func (qs *QueueSegments) repairSegmentLocked(seg *SegmentFile, pre int64, op string, cause error) {
	err := segmentTruncate(seg.file, pre)
	if err == nil {
		seg.fileSize.Store(pre)
		return
	}
	qs.segPoisoned = fmt.Errorf("segment %s diverged after failed %s: %w", seg.path, op, err)
	qs.logError("segment write failed and could not be truncated; retiring the segment (its durable records stay readable)",
		interfaces.LogField{Key: "queue", Value: qs.queueName},
		interfaces.LogField{Key: "segment", Value: seg.path},
		interfaces.LogField{Key: "op", Value: op},
		interfaces.LogField{Key: "error", Value: err.Error()})
	_ = qs.replaceActiveSegmentLocked()
}

// compactionLoop periodically checks if compaction is needed
func (qs *QueueSegments) compactionLoop() {
	defer qs.wg.Done()

	ticker := time.NewTicker(qs.cfg.CompactionInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			qs.tryCompaction()

		case <-qs.stopChan:
			return
		}
	}
}

// tryCompaction checks if any segments need compaction.
//
// WHAT A PERMANENTLY UNCOMPACTABLE SEGMENT DOES, AND WHY (review-4 B-4).
// Step 4 stopped compactSegment from silently destroying a record it could not
// read — but the error went to `_ =` at this call site, and neither
// deletedCount nor messageCount was reset, so the segment was re-selected on
// every tick: 50 / 100 / 150 futile attempts at t=1/2/3s, measured, forever,
// each one re-reading the segment and writing a temp file it then removed.
// Converting silent corruption into an unbounded retry that also blocks the ack
// path is not an improvement.
//
// The condition is now BOUNDED, OBSERVABLE and NON-BLOCKING:
//
//   - BOUNDED: the segment is withdrawn from compaction for the lifetime of the
//     process, so it is attempted exactly ONCE. This is safe because compaction
//     is space reclamation ONLY — never a durability or correctness requirement
//     — while the retry is a full read of the segment plus a temp-file write
//     under qs.compactionMux. The cost of refusing is bounded (one segment is
//     not reclaimed); the cost of retrying is unbounded.
//   - OBSERVABLE: RecordSegmentCompactionFailure() is the alertable counter, and
//     the log line names the file and the failing record.
//   - NOT HIDDEN FOREVER: the block is per-process, not persisted. A restart
//     re-evaluates, and a data fault of this shape is classified FATAL by
//     loadSegmentFile at that restart, which gates the boot unless the operator
//     passes --unsafe-recovery. So "stop trying" cannot bury the problem.
//
// The block is keyed on ANY compaction failure rather than only on data faults.
// An environment failure (a full filesystem at temp-write time) would clear on
// its own, so blocking is pessimistic there — but the alternative is to
// distinguish two classes of error by inspection and retry one of them forever,
// which is the behaviour this finding is about. One rule, no arbitrary retry
// budget, and a restart is the reset.
func (qs *QueueSegments) tryCompaction() {
	qs.sealedMutex.RLock()
	segmentsToCompact := make([]*SegmentFile, 0)

	for _, segment := range qs.sealedSegments {
		if segment.compactionBlocked.Load() {
			continue
		}
		deletedCount := segment.deletedCount.Load()
		totalCount := segment.messageCount.Load()

		if totalCount > 0 {
			deletionRatio := float64(deletedCount) / float64(totalCount)
			if deletionRatio > qs.cfg.CompactionThreshold {
				segmentsToCompact = append(segmentsToCompact, segment)
			}
		}
	}
	qs.sealedMutex.RUnlock()

	// Compact segments
	for _, segment := range segmentsToCompact {
		if err := qs.compactSegment(segment); err != nil {
			segment.compactionBlocked.Store(true)
			if qs.metrics != nil {
				qs.metrics.RecordSegmentCompactionFailure()
			}
			qs.logError("segment WITHDRAWN from compaction: it cannot be rewritten and will not be re-attempted until this broker restarts. No data is lost — the file is left on disk untouched — but its acked records are not reclaimed.",
				interfaces.LogField{Key: "queue", Value: qs.queueName},
				interfaces.LogField{Key: "segment", Value: segment.path},
				interfaces.LogField{Key: "error", Value: err.Error()})
		}
	}
}

// compactSegment rewrites a segment excluding deleted messages.
//
// THIS FUNCTION IS OPERATING ON THE ONLY COPY OF THE DATA. performCheckpoint
// unlinks the source WAL file once CheckpointBatch succeeds, so from that
// instant the segment is not a cache of anything.
//
// FAILURE SEMANTICS, precisely (the previous blanket claim that "none of them
// proceeds to the rename" was false for two of its own paths — review-4 N-5, and
// it is the DOC that was wrong, not the code):
//
//   - Every failure UP TO AND INCLUDING os.Rename leaves the original file and
//     the original index EXACTLY as they were, removes the temp file, and
//     reports. Nothing observable changed.
//   - TWO paths return a non-nil error AFTER the rename has landed: a syncDir
//     failure (returned as syncErr at the very end, once the handle, index and
//     counters have all been swapped) and a failure to reopen the replaced file.
//     In both, the compacted file is already the segment on disk and the state
//     left behind is internally consistent, but "compactSegment returned an
//     error" must NOT be read as "nothing changed". tryCompaction only withdraws
//     the segment from future compaction on an error, which is correct either
//     way.
//
// Before Step 4 the copy loop was `if err == nil { ... if werr == nil { ... } }`
// with no else on either arm, and execution fell through to os.Rename
// regardless: a record that could not be read (bit rot, a torn write) or could
// not be written (a full filesystem) was silently omitted from the rewrite and
// then destroyed by the rename, with no error returned and no log line. The
// rename's own error was discarded too, which was worse than unreadable — the
// OLD file was reopened and the NEW index installed over it, so every lookup
// returned a different, wrong message.
func (qs *QueueSegments) compactSegment(segment *SegmentFile) error {
	qs.compactionMux.Lock()
	defer qs.compactionMux.Unlock()

	// Read all messages from old segment
	segment.mutex.RLock()
	oldIndex := make(map[uint64]int64, len(segment.index))
	for offset, position := range segment.index {
		oldIndex[offset] = position
	}
	segment.mutex.RUnlock()

	// DECIDE WHAT SURVIVES BEFORE TOUCHING THE FILESYSTEM.
	//
	// bitmapMutex used to be held (RLock) across the ENTIRE copy loop's reads
	// and writes. batchAckLoop.flush() needs bitmapMutex.Lock(), and once
	// ackChan (cap 1000) backs up SegmentManager.Acknowledge falls through to
	// applyAck, which also takes it — so a compaction of a large segment stalled
	// the broker's ACK PATH for its whole duration, once a minute, and with the
	// pre-fix unbounded retry that was permanent (review-4 B-4).
	//
	// The set of surviving offsets is a point-in-time decision, so it is taken
	// once, with no I/O inside the lock. An ack landing during the copy is
	// simply not applied in this round and is picked up by the next one — the
	// same guarantee the previous code gave, since Contains() was already racing
	// arbitrarily against concurrent applyAck calls.
	survivors := make([]uint64, 0, len(oldIndex))
	qs.bitmapMutex.RLock()
	for offset := range oldIndex {
		if !qs.ackBitmap.Contains(offset) {
			survivors = append(survivors, offset)
		}
	}
	qs.bitmapMutex.RUnlock()

	// Create new temporary segment
	tempPath := segment.path + segmentCompactSuffix
	tempFile, err := os.OpenFile(tempPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("segment compaction: cannot create %s: %w", tempPath, err)
	}

	// Write non-deleted messages to new segment
	newIndex := make(map[uint64]int64)
	var newPosition int64
	var copyErr error

	for _, offset := range survivors {
		oldPosition := oldIndex[offset]
		msg, rerr := readSegmentMessageAt(segment.file, oldPosition)
		if rerr != nil {
			copyErr = fmt.Errorf("segment compaction aborted: the record at offset %d (file position %d) in %s could not be read, and compacting past it would destroy it: %w",
				offset, oldPosition, segment.path, rerr)
			break
		}
		msgBytes, serErr := serializeSegmentMessage(msg, offset, qs.cfg.CRCDisabled)
		if serErr != nil {
			// Re-serializing an already-persisted message should never fail
			// (it was <=255-byte-bounded when first written).
			copyErr = fmt.Errorf("segment compaction aborted: the record at offset %d in %s could not be re-serialized: %w",
				offset, segment.path, serErr)
			break
		}
		n, werr := tempFile.Write(msgBytes)
		if werr != nil {
			copyErr = fmt.Errorf("segment compaction aborted: the record at offset %d could not be written to %s: %w",
				offset, tempPath, werr)
			break
		}
		newIndex[offset] = newPosition
		newPosition += int64(n)
	}

	if copyErr != nil {
		_ = tempFile.Close()
		_ = os.Remove(tempPath)
		return copyErr
	}

	// Sync the new file BEFORE it can become the segment. A discarded Sync
	// error means the rename can publish a file whose contents are not on the
	// platter.
	if serr := tempFile.Sync(); serr != nil {
		_ = tempFile.Close()
		_ = os.Remove(tempPath)
		return fmt.Errorf("segment compaction aborted: cannot fsync %s: %w", tempPath, serr)
	}
	// Exactly one Close on every path (it used to be closed twice: once by a
	// defer and once explicitly, so the deferred call returned EBADF).
	if cerr := tempFile.Close(); cerr != nil {
		_ = os.Remove(tempPath)
		return fmt.Errorf("segment compaction aborted: cannot close %s: %w", tempPath, cerr)
	}

	// Atomically replace old segment with compacted one.
	if rerr := os.Rename(tempPath, segment.path); rerr != nil {
		_ = os.Remove(tempPath)
		return fmt.Errorf("segment compaction aborted: cannot install %s over %s; the old file and its index are left untouched: %w",
			tempPath, segment.path, rerr)
	}

	// A rename is not durable until the DIRECTORY entry is fsynced; without
	// this a crash can leave the directory pointing at neither file. syncDir's
	// own doc says so, and this is its second required site.
	syncErr := syncDir(qs.dataDir)

	// Reopen segment file. If this fails the compacted file is already the
	// segment on disk, but qs's in-memory handle still refers to the (now
	// unlinked) old inode, which the OLD index describes correctly — so
	// leaving both in place is the consistent outcome, and the next boot reads
	// the compacted file with a freshly scanned index.
	newFile, err := os.Open(segment.path)
	if err != nil {
		return fmt.Errorf("segment compaction: %s was replaced but could not be reopened; the previous handle and index are still consistent: %w", segment.path, err)
	}

	// Update segment
	segment.mutex.Lock()
	_ = segment.file.Close()
	segment.file = newFile
	segment.index = newIndex
	segment.messageCount.Store(uint64(len(newIndex)))
	segment.deletedCount.Store(0)
	segment.fileSize.Store(newPosition)
	segment.mutex.Unlock()

	// Counted HERE, at the end, because the counter's name is "compactions" and
	// an operator reads it as work that happened. At the top of the function it
	// reported 150 compactions across 150 consecutive failures (review-4 B-4).
	if qs.metrics != nil {
		qs.metrics.RecordSegmentCompaction()
	}

	return syncErr
}

// segmentQuarantineCost is the operator-facing cost of abandoning ONE segment
// file. It is one string because the refusal message's claim that only the
// listed artifacts are discarded is only true if every producer says the same
// thing (canon rule 12).
const segmentQuarantineCost = "every checkpointed durable message in this segment file is abandoned, " +
	"including any that follow the damaged record and are themselves intact; the file is left on disk " +
	"UNTOUCHED, and every OTHER segment and queue is still recovered"

// loadExistingSegments scans this queue's segment directory and returns one
// classified fault per file that could not be fully interpreted. A fault on one
// file never abandons the directory: the file is quarantined (not registered,
// its records taken from nowhere) and the scan continues, mirroring
// RecoverFromWAL.
func (qs *QueueSegments) loadExistingSegments() []error {
	var faults []error

	entries, err := os.ReadDir(qs.dataDir)
	if err != nil {
		return []error{interfaces.DegradedFault("segment-open", qs.dataDir,
			fmt.Sprintf("the segment directory for queue %q could not be read", qs.queueName),
			"every checkpointed durable message for this queue is abandoned for as long as the directory stays unreadable; nothing on disk is modified, and every OTHER queue is still recovered",
			err)}
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()

		// A crash between "compaction wrote its temp file" and "compaction
		// renamed it" leaves this behind. It is a PARTIAL DUPLICATE of records
		// the original segment still holds — the original is untouched until
		// the rename — so removing it loses nothing, and leaving it leaked
		// forever because nothing else ever looked at these files.
		if strings.HasSuffix(name, segmentCompactSuffix) {
			_ = os.Remove(filepath.Join(qs.dataDir, name))
			continue
		}

		// review-4 N-3: the Step 4 sweep deleted writeIndexToDisk and the .idx
		// files it produced were already write-only — nothing has ever read one.
		// But every EXISTING deployment's segment directories hold one .idx per
		// sealed segment, and the sweep left them with no reaper, three lines
		// away from the one it added for .compact. They are removed here, and
		// nowhere else, so the constant lives in one place (canon rule 12).
		if strings.HasSuffix(name, segmentLegacyIndexSuffix) {
			_ = os.Remove(filepath.Join(qs.dataDir, name))
			continue
		}

		// The ownership marker is this directory's identity and is never a
		// segment. Named explicitly so no future reaper can sweep it.
		if name == segmentDirMarkerName {
			continue
		}

		// A crash between atomicWriteFile's create and its rename leaves the
		// marker's temp file behind. It is never read — the marker is only ever
		// read at its real name — so removing it loses nothing, and leaving it
		// would leak one per crash forever, which is exactly why the .compact
		// and .idx reapers above exist.
		if name == segmentDirMarkerName+TempFileExtension {
			_ = os.Remove(filepath.Join(qs.dataDir, name))
			continue
		}
		if !strings.HasSuffix(name, SegmentFileExtension) {
			continue
		}

		stem := strings.TrimSuffix(name, SegmentFileExtension)
		segmentNum, perr := strconv.ParseUint(stem, 10, 64)
		if perr != nil {
			// openNextSegmentLocked always writes %020d, so this is corruption
			// or operator action, never something this build produced. It used
			// to be the one silent `continue` left in a function that raises a
			// classified fault for every other uninterpretable artifact
			// (review-5 MINOR-7) — and a .seg file skipped without a word is
			// indistinguishable from one that was read.
			faults = append(faults, interfaces.DegradedFault("segment-open",
				filepath.Join(qs.dataDir, name),
				fmt.Sprintf("this file is named like a segment of queue %q but its name does not carry a segment number, so it cannot be placed in the offset order", qs.queueName),
				segmentQuarantineCost,
				perr))
			continue
		}

		if lerr := qs.loadSegmentFile(segmentNum, filepath.Join(qs.dataDir, name)); lerr != nil {
			faults = append(faults, lerr)
			continue
		}
		// Seed the monotone numbering with the highest loaded number so the
		// next created segment is strictly greater even if the wall clock has
		// stepped backwards across the restart (WS-C C2).
		if segmentNum > qs.lastSegmentNum {
			qs.lastSegmentNum = segmentNum
		}
	}

	return faults
}

// loadSegmentFile scans one segment file and registers it for reads.
//
// CLASSIFICATION. It is the WAL's (scanWALFile), applied to a tier that is
// authoritative for exactly the same reason — and deliberately NOT the
// derived-state policy this file used to apply:
//
//   - interior CRC failure, or a zero region followed by data  -> FATAL
//   - CRC failure on the last record / zero-filled tail / short
//     read at the tail                                         -> BENIGN
//   - the file cannot be opened or read                        -> DEGRADED
//
// AND IT NEVER TRUNCATES. The previous implementation used one `break` for
// every stop reason and then did file.Truncate(pos), so a single bad record in
// the middle physically destroyed every valid record after it, at boot, with
// no log, no metric and no count — on the only copy of the data. Nothing
// appends to a loaded segment (they are opened read-only and registered as
// sealed), so the truncation bought nothing.
//
// A FATAL or DEGRADED file is QUARANTINED: it is not registered, so no read
// can pair its bytes with an index, and the caller keeps scanning the rest of
// the directory.
func (qs *QueueSegments) loadSegmentFile(segmentNum uint64, segPath string) error {
	file, err := os.Open(segPath)
	if err != nil {
		return interfaces.DegradedFault("segment-open", segPath,
			"this segment file could not be opened and is quarantined for this boot",
			"every checkpointed durable message in this segment file is abandoned for as long as it stays unreadable; the file is left on disk UNTOUCHED, and every OTHER segment and queue is still recovered",
			err)
	}

	degraded := func(detail string, cause error) error {
		_ = file.Close()
		return interfaces.DegradedFault("segment-open", segPath, detail,
			"every checkpointed durable message in this segment file is abandoned for as long as it stays unreadable; the file is left on disk UNTOUCHED, and every OTHER segment and queue is still recovered",
			cause)
	}
	fatal := func(detail string) error {
		_ = file.Close()
		return interfaces.FatalFault("segment-scan", segPath, detail, segmentQuarantineCost, nil)
	}

	stat, err := file.Stat()
	if err != nil {
		return degraded("this segment file could not be stat'd and is quarantined for this boot", err)
	}
	fileSize := stat.Size()

	index := make(map[uint64]int64)
	var minOffset, maxOffset uint64
	var messageCount uint64
	var pos int64
	var scanFault error

	for {
		if pos+segmentRecordHeaderSize > fileSize {
			if pos < fileSize {
				scanFault = interfaces.BenignFault("segment-scan", segPath,
					fmt.Sprintf("truncated record header at byte offset %d; the trailing partial record was never fsynced, so no publisher was confirmed for it", pos))
			}
			break
		}

		header := make([]byte, segmentRecordHeaderSize)
		if _, rerr := file.ReadAt(header, pos); rerr != nil {
			return degraded(fmt.Sprintf("this segment file could not be read at byte offset %d and is quarantined for this boot", pos), rerr)
		}

		storedCRC := binary.BigEndian.Uint32(header[0:4])
		dataLen := binary.BigEndian.Uint32(header[4:8])

		// A zero region starting exactly on a record boundary. The segment
		// write path cannot emit a zero-length record (asserted at runtime by
		// TestSegmentScan_TheWritePathNeverProducesAZeroLengthRecord), so this
		// is never data.
		if walBlankRecordHeader(storedCRC, dataLen) {
			if walNothingIntelligibleFollows(file, pos, fileSize) {
				scanFault = interfaces.BenignFault("segment-scan", segPath,
					fmt.Sprintf("zero-filled tail from byte offset %d to end of file (%d bytes); those blocks never reached the platter, so nothing in them was ever fsynced or confirmed", pos, fileSize-pos))
				break
			}
			return fatal(fmt.Sprintf("a zero-filled region begins at byte offset %d and is FOLLOWED BY MORE DATA before end of file (%d bytes); this segment file cannot be interpreted", pos, fileSize))
		}

		if pos+segmentRecordHeaderSize+int64(dataLen) > fileSize {
			scanFault = interfaces.BenignFault("segment-scan", segPath,
				fmt.Sprintf("truncated record body at byte offset %d; the trailing partial record was never fsynced, so no publisher was confirmed for it", pos))
			break
		}

		data := make([]byte, dataLen)
		if _, rerr := file.ReadAt(data, pos+segmentRecordHeaderSize); rerr != nil {
			return degraded(fmt.Sprintf("this segment file could not be read at byte offset %d and is quarantined for this boot", pos+segmentRecordHeaderSize), rerr)
		}

		if storedCRC != 0 {
			h := crc32.NewIEEE()
			h.Write(header[4:8])
			h.Write(data)
			if h.Sum32() != storedCRC {
				next := pos + segmentRecordHeaderSize + int64(dataLen)
				if !walNothingIntelligibleFollows(file, next, fileSize) {
					return fatal(fmt.Sprintf("the record at byte offset %d failed its CRC32 check and is NOT the last record in the file — real data follows it (%d of %d bytes); this segment file cannot be interpreted",
						pos, fileSize-next, fileSize))
				}
				scanFault = interfaces.BenignFault("segment-scan", segPath,
					fmt.Sprintf("the last intelligible record in this file (byte offset %d) failed its CRC32 check and nothing but zeros follows it — a torn write; it was never fsynced, so no publisher was confirmed for it", pos))
				break
			}
		}

		// Extract the offset from the v4 payload to index the record: the payload
		// begins with [flags u16][queue u8-len][queue][offset u64], and segments
		// write an empty queue name. A record whose CRC is VALID but whose
		// payload cannot be located is not a torn tail — the bytes are what was
		// written — so it is fatal rather than a silent stop.
		if len(data) < 3 {
			return fatal(fmt.Sprintf("the record at byte offset %d passed its CRC32 check but is too short to carry a message payload (%d bytes); this segment file cannot be interpreted", pos, len(data)))
		}
		queueLen := int(data[2]) // data[0:2] = flags, data[2] = queue u8-len
		offStart := 3 + queueLen
		if offStart+8 > len(data) {
			return fatal(fmt.Sprintf("the record at byte offset %d passed its CRC32 check but its payload is malformed (offset field would run past the record); this segment file cannot be interpreted", pos))
		}
		offset := binary.BigEndian.Uint64(data[offStart : offStart+8])

		index[offset] = pos
		if minOffset == 0 || offset < minOffset {
			minOffset = offset
		}
		if offset > maxOffset {
			maxOffset = offset
		}
		messageCount++
		pos += segmentRecordHeaderSize + int64(dataLen)
	}

	seg := &SegmentFile{
		segmentNum: segmentNum,
		path:       segPath,
		file:       file,
		index:      index,
		minOffset:  minOffset,
		maxOffset:  maxOffset,
	}
	seg.messageCount.Store(messageCount)
	seg.fileSize.Store(pos)

	qs.sealedMutex.Lock()
	qs.sealedSegments[segmentNum] = seg
	qs.sealedMutex.Unlock()

	return scanFault
}

// collectAllMessages returns every record this queue's segments hold, together
// with a classified fault if any of them could not be read. A read error used
// to be dropped on the floor (`if err == nil`), which silently abandoned a
// checkpointed durable message whose source WAL file is already unlinked.
//
// WS-C C8 — CROSS-SEGMENT DEDUP. Duplicates across segment files arise
// legitimately (a partially-failed multi-queue checkpoint retries and rewrites
// a queue that already succeeded; a crash between segment fsync and WAL unlink
// leaves the copy plus a later re-checkpoint), so recovery collapses them by
// (queue, )offset — the same key the in-file index and the WAL-vs-segment
// dedup use. The NEWEST copy wins: sealed segments are visited in ascending
// segmentNum order and the ACTIVE segment last, and a seen offset is
// overwritten only on a successful read (a failed read of a newer copy must
// not erase an older readable copy). The output is sorted by offset ascending
// so it is deterministic (the sealed map's iteration order is not).
func (qs *QueueSegments) collectAllMessages() ([]*RecoveryMessage, error) {
	var messages []*RecoveryMessage
	var faults []error
	pos := map[uint64]int{} // offset -> index into messages

	read := func(seg *SegmentFile, offset uint64, position int64) {
		msg, err := readSegmentMessageAt(seg.file, position)
		if err != nil {
			faults = append(faults, interfaces.DegradedFault("segment-scan", seg.path,
				fmt.Sprintf("the record at offset %d (file position %d) is indexed but could not be read back", offset, position),
				"this message is abandoned; the file is left on disk UNTOUCHED, and every OTHER record, segment and queue is still recovered",
				err))
			return
		}
		if i, seen := pos[offset]; seen {
			messages[i] = &RecoveryMessage{ // newer wins (visited oldest first)
				QueueName: qs.queueName,
				Offset:    offset,
				Message:   msg,
			}
			return
		}
		pos[offset] = len(messages)
		messages = append(messages, &RecoveryMessage{
			QueueName: qs.queueName,
			Offset:    offset,
			Message:   msg,
		})
	}

	// Sealed segments in ascending segmentNum order: within one incarnation
	// (and across restarts, thanks to C2's monotone numbering seeded from the
	// highest loaded number) a higher number is a newer file.
	qs.sealedMutex.RLock()
	sealed := make([]*SegmentFile, 0, len(qs.sealedSegments))
	for _, seg := range qs.sealedSegments {
		sealed = append(sealed, seg)
	}
	qs.sealedMutex.RUnlock()
	sort.Slice(sealed, func(i, j int) bool { return sealed[i].segmentNum < sealed[j].segmentNum })

	for _, seg := range sealed {
		seg.mutex.RLock()
		for offset, position := range seg.index {
			read(seg, offset, position)
		}
		seg.mutex.RUnlock()
	}

	qs.mutex.Lock()
	currentSeg := qs.currentSegment
	currentIdx := qs.currentIndex
	qs.mutex.Unlock()

	if currentSeg != nil && currentIdx != nil {
		// Same lock discipline as the sealed loop above and as readMessage: the
		// read is inside currentSeg.mutex, so sealSegment's handle swap cannot
		// land between the index lookup and the ReadAt.
		//
		// BOUNDARY. review-4 named this read as the third member of B-3's class
		// and did not attempt to trigger it. It is not reachable in practice
		// today: the only caller is RecoverFromSegments, which constructs the
		// QueueSegments itself, so currentIdx.entries is empty and this loop has
		// zero iterations. It is fixed anyway because "currently unreachable" is
		// not a property of the code, and it costs one lock acquisition at boot.
		// The active segment is visited LAST so it beats every sealed copy.
		currentIdx.mutex.RLock()
		currentSeg.mutex.RLock()
		for offset, position := range currentIdx.entries {
			read(currentSeg, offset, position)
		}
		currentSeg.mutex.RUnlock()
		currentIdx.mutex.RUnlock()
	}

	// Deterministic output: sort by offset ascending (today's order was random
	// map order, which no consumer can depend on).
	sort.Slice(messages, func(i, j int) bool { return messages[i].Offset < messages[j].Offset })

	return messages, joinFaults(faults)
}

// RecoverFromSegments returns every checkpointed message in the data
// directory, keyed by the queue that owns it, together with a classified fault
// for anything that could not be interpreted.
//
// It honours the contract DisruptorStorage.GetRecoverableMessages states: a
// fatal fault is confined to the segment file that produced it, the scan
// continues, and everything else comes back. Before Step 4 this returned
// `result, nil` unconditionally — it swallowed its own ReadDir failure and
// every per-segment read error — so a segment tier that had become the ONLY
// copy of the data reported a clean "no cold data" for a directory it could
// not read.
func (sm *SegmentManager) RecoverFromSegments() (map[string][]*RecoveryMessage, error) {
	result := make(map[string][]*RecoveryMessage)
	var faults []error

	entries, err := os.ReadDir(sm.dataDir)
	if err != nil {
		return result, joinFaults([]error{interfaces.DegradedFault("segment-open", sm.dataDir,
			"the segments directory could not be read, so no checkpointed message in it can be recovered",
			"every message already checkpointed out of the WAL is abandoned; nothing on disk is modified",
			err)})
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		// The directory is opened AS FOUND and attributed to its owner from its
		// MARKER, so a directory this build would spell differently is still
		// read rather than stranded — and, critically, a directory whose NAME
		// happens to look like an encoding is not decoded. A marker that exists
		// but cannot be read is a fault, never "no marker": falling back would
		// attribute the directory to its own name, i.e. to the wrong queue,
		// which is exactly the silent loss review-4 B-1 measured.
		dirPath := filepath.Join(sm.dataDir, entry.Name())
		marker, present, merr := readSegmentDirMarker(dirPath)
		if merr != nil {
			faults = append(faults, interfaces.DegradedFault("segment-open",
				filepath.Join(dirPath, segmentDirMarkerName),
				"this segment directory's ownership marker exists but could not be read, so the directory cannot be attributed to a queue",
				"every checkpointed durable message in this directory is abandoned for as long as the marker stays unreadable; nothing on disk is modified, and every OTHER queue is still recovered",
				merr))
			continue
		}
		queueName := segmentDirOwner(entry.Name(), marker, present)

		qs, gerr := sm.getOrCreateQueueSegmentsAt(queueName, entry.Name())
		if gerr != nil {
			faults = append(faults, gerr)
			continue
		}
		faults = append(faults, qs.takeLoadFaults()...)

		messages, cerr := qs.collectAllMessages()
		if cerr != nil {
			faults = append(faults, cerr)
		}
		if len(messages) > 0 {
			result[queueName] = messages
		}
	}

	return result, joinFaults(faults)
}

// close stops background goroutines and closes files.
// It is idempotent: DisruptorStorage.Close reaches SegmentManager.Close, and a
// deferred Close in any caller reaches it again; the second close(qs.stopChan)
// used to panic during shutdown.
func (qs *QueueSegments) close() {
	if !qs.closed.CompareAndSwap(false, true) {
		return
	}
	close(qs.stopChan)
	qs.wg.Wait()

	qs.mutex.Lock()
	defer qs.mutex.Unlock()

	if qs.currentSegment != nil {
		_ = qs.currentSegment.file.Sync()
		_ = qs.currentSegment.file.Close()
	}

	qs.sealedMutex.Lock()
	defer qs.sealedMutex.Unlock()

	for _, segment := range qs.sealedSegments {
		_ = segment.file.Close()
	}
}

// Helper functions

// serializeSegmentMessage serializes a message into a segment record:
// [CRC][length][<message payload v4>]. The payload is produced by the SAME shared
// codec (appendMessagePayload) the WAL uses, so a segment record and a WAL message
// record carry byte-identical payloads and preserve the ENTIRE protocol.Message
// (all 14 optional basic.properties, not the pre-v4 subset), including x-death
// headers across compaction. Segments differ from the WAL only in the framing:
// no per-record type tag, and the payload's queue-name field is empty (the queue
// is known by the segment's directory).
//
// Returns an error (and a partial buffer the caller must discard) if the message
// contains a shortstr property longer than 255 bytes; the caller must not write a
// partial record.
func serializeSegmentMessage(message *protocol.Message, offset uint64, crcDisabled bool) ([]byte, error) {
	// ITER5 §3.6: segments are ALWAYS inline. A message that still carries a body
	// REFERENCE would serialize a bodyKindReference arm into a segment, where no
	// BodyBlock exists — a permanently dangling reference. Recovery/checkpoint
	// resolves and clears BodyRef before a message ever reaches here; this guard is
	// defense-in-depth: fail the checkpoint batch rather than persist a dangling
	// reference (mirrors the shortstr overflow guard).
	if len(message.BodyRef) > 0 {
		return nil, fmt.Errorf("refusing to serialize a body reference into a segment (offset %d): segments must be inline", offset)
	}
	totalSize := 8 + len(message.Exchange) + len(message.RoutingKey) + len(message.Body) + 256
	buf := make([]byte, 0, totalSize)

	// Reserve space for CRC and length.
	buf = append(buf, 0, 0, 0, 0) // CRC32 placeholder
	buf = append(buf, 0, 0, 0, 0) // Length placeholder

	// Shared v4 payload with an empty queue name (segments key by queue dir).
	buf, err := appendMessagePayload(buf, "", message, offset)
	if err != nil {
		return buf, err
	}

	// Calculate actual length (excluding CRC and length fields).
	dataLen := len(buf) - 8
	binary.BigEndian.PutUint32(buf[4:8], uint32(dataLen))

	if !crcDisabled {
		crc := crc32.ChecksumIEEE(buf[4:])
		binary.BigEndian.PutUint32(buf[0:4], crc)
	}

	return buf, nil
}

func readSegmentMessageAt(file *os.File, position int64) (*protocol.Message, error) {
	// Read header (CRC + length, 8 bytes)
	header := make([]byte, 8)
	_, err := file.ReadAt(header, position)
	if err != nil {
		return nil, err
	}

	// Parse header
	storedCRC := binary.BigEndian.Uint32(header[0:4])
	dataLen := binary.BigEndian.Uint32(header[4:8])

	// Read data portion
	data := make([]byte, dataLen)
	_, err = file.ReadAt(data, position+8)
	if err != nil {
		return nil, err
	}

	// Verify CRC over [length][data]. A zero stored CRC means CRC was disabled
	// at write time — skip verification.
	if storedCRC != 0 {
		h := crc32.NewIEEE()
		h.Write(header[4:8])
		h.Write(data)
		if h.Sum32() != storedCRC {
			return nil, fmt.Errorf("CRC mismatch")
		}
	}

	// Parse the shared v4 payload (queue name is empty and ignored; the offset is
	// carried on the reconstructed message's DeliveryTag).
	_, _, msg, ok := parseMessagePayload(data)
	if !ok {
		return nil, fmt.Errorf("invalid segment data: malformed message record")
	}
	// ITER5 §3.6: a segment must never carry a body reference (the companion of
	// the serializeSegmentMessage guard). Reject a 0x01 arm so a corrupt/foreign
	// segment can never yield a message with an unresolvable BodyRef.
	if len(msg.BodyRef) > 0 {
		return nil, fmt.Errorf("segment record carries a body reference (unsupported): segments must be inline")
	}
	return msg, nil
}
