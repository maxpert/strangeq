package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/maxpert/amqp-go/interfaces"
)

// Boot-time reconstruction of the shared WAL's derived state.
//
// THE DEFECT THIS FILE EXISTS TO CLOSE. Every piece of state the shared WAL
// needs in order to READ a record back — fileNum, oldFiles, the per-file offset
// bitmaps, offsetIndex — is written exclusively by the WRITE path (openNextFile,
// rollFile, flushBatch) and was reconstructed by nothing at boot. Recovery
// extracted messages (RecoverFromWAL) but never rebuilt the index that makes
// them readable, and fileNum restarted at zero so every incarnation reopened
// 00000000000000000001.wal with O_APPEND. Two consequences, both reproduced:
//
//   - C-1: a recovered record that does not fit in the in-memory ring (above the
//     spill threshold) had NO readable durable location — offsetIndex was empty,
//     oldFiles was empty, currentFileOffsets was empty — so its delivery tag was
//     claimable and unreadable and the dispatch plane silently gap-skipped it.
//   - C-2: when the reopened file rolled, rollFile registered it in oldFiles with
//     ONLY this incarnation's offsets, so tryDeleteOldFiles judged it fully acked
//     and os.Remove()d a file full of confirmed, unacknowledged durable records.
//
// rebuildBootState fixes the cause rather than either symptom: it enumerates the
// shared WAL directory once, at construction, and restores fileNum, oldFiles
// (with the COMPLETE physical offset inventory of each pre-existing file) and
// offsetIndex, so that a pre-existing file is both readable and correctly
// judged by the reclamation predicate. Because fileNum resumes past the highest
// existing stem, a new incarnation never appends to a file it did not create,
// which is what makes "oldFiles carries this incarnation's offsets" true again.
//
// COST: startup only. It runs once inside createSharedWAL, before any background
// goroutine is started and before any write can be accepted. Nothing on the
// write, fsync, ack or delivery path calls into this file.
//
// WHAT THIS FILE DELIBERATELY DOES NOT DO: it does not make acknowledgement
// durable. An earlier revision of this file persisted the WAL's ack bitmap to
// <sharedDir>/acks.snapshot and re-installed it at boot; that feature was
// WITHDRAWN. Acknowledgement in this broker is keyed by delivery tag, delivery
// tags are re-minted by design (broker/queue_dispatch.go FrontierReserve), and
// a persisted tag-keyed ack set therefore cannot distinguish "this ack cancels
// the record I just found" from "this ack cancels a record that is gone and a
// different, newer record happens to carry the same tag". Doing it safely needs
// per-record ack attribution (Artemis's Reclaimer criterion 2), which this
// architecture does not yet have. The full evidence — four criticals with
// measured loss numbers, the two doors the second cycle left open, and what a
// correct design would need — is in .notes/loop-2/deferred-ack-durability.md.
// Do not reintroduce a tag-keyed durable ack artifact without reading it.

// isWALFileName is THE predicate for "is this directory entry a shared-WAL
// segment", and it lives here, once (canon rule 12).
//
// It used to be two predicates that disagreed: rebuildBootState accepted only
// <digits>.wal via parseWALFileNum, while RecoverFromWAL accepted any *.wal —
// and the LOOSER one was the fatal one. So a .wal file with a non-numeric stem
// was invisible to the boot-state rebuild (no fileNum protection, no oldFiles
// entry, no offset inventory) yet was still scanned by recovery, where an
// unparseable header refused the boot. The refusal message advises moving the
// listed files aside; an operator who moved one aside as `damaged.wal` INSIDE
// the same directory re-armed the refusal through a path the rebuild could not
// even see. The message now says "out of the directory", and the two
// enumerations are one function.
func isWALFileName(e os.DirEntry) bool {
	if e.IsDir() {
		return false
	}
	_, ok := parseWALFileNum(e.Name())
	return ok
}

// parseWALFileNum extracts the %020d stem of a WAL file name. It returns false
// for any name that is not <digits>.wal, so a foreign file in the directory can
// never be mistaken for a WAL segment or move fileNum.
func parseWALFileNum(name string) (uint64, bool) {
	if filepath.Ext(name) != WALFileExtension {
		return 0, false
	}
	stem := strings.TrimSuffix(name, WALFileExtension)
	if stem == "" {
		return 0, false
	}
	for i := 0; i < len(stem); i++ {
		if stem[i] < '0' || stem[i] > '9' {
			return 0, false
		}
	}
	n, err := strconv.ParseUint(stem, 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

// walRecordIndexEntry is one indexed message record: the offset it carries and
// the file-relative byte position of its record header.
type walRecordIndexEntry struct {
	offset   uint64
	position int64
}

// indexWALFile performs a framing-level scan of one WAL file and reports where
// each message record lives, without deserializing message payloads.
//
// It is deliberately NOT scanWALFile: the boot index needs (offset, position)
// pairs and a shared-body flag, not materialized protocol.Messages, and paying
// for full property decode + body copies of every record on disk at startup
// would make boot cost proportional to the whole WAL in allocations as well as
// I/O. The record grammar walked here is byte-for-byte the one scanWALFile
// walks (CRC + length framing, per-record type tag, v4 payload prefix), so the
// two cannot disagree about record boundaries.
//
// IT APPLIES THE SAME THREE-WAY CLASSIFICATION AS scanWALFile, and that is
// load-bearing rather than tidy. rebuildBootState runs at storage CONSTRUCTION
// and refuses there, before RecoverFromWAL ever runs — so if this walk silently
// skipped a bad-CRC record (which it did), a directory with one unparseable
// file AND one interior CRC failure produced a refusal naming only the first.
// The refusal message states that its artifact list is complete; that claim is
// only true if every fault class is enumerated in the same pass.
//
// A torn tail (short read, zero-filled tail, or a CRC failure with nothing
// intelligible after it) ends the scan with no error, exactly as in
// scanWALFile: the trailing partial record was never fsynced, so no publisher
// was ever told it was durable.
func indexWALFile(path string) (entries []walRecordIndexEntry, hasSharedBody bool, err error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, false, err
	}
	defer file.Close()

	dataStart, err := walFileDataStart(file)
	if err != nil {
		return nil, false, err
	}
	if _, err := file.Seek(dataStart, io.SeekStart); err != nil {
		return nil, false, err
	}

	recStart := dataStart
	hdr := make([]byte, 8)
	for {
		if _, rerr := io.ReadFull(file, hdr); rerr != nil {
			break
		}
		crc := binary.BigEndian.Uint32(hdr[0:4])
		dataLen := binary.BigEndian.Uint32(hdr[4:8])

		// A zero-filled region is the end of this file's data, not a run of
		// zero-length records — see walBlankRecordHeader. Walking it 8 bytes at
		// a time would also make boot cost proportional to the zeroed tail.
		if walBlankRecordHeader(crc, dataLen) {
			if walNothingIntelligibleFollows(file, recStart, -1) {
				break
			}
			return nil, false, interfaces.FatalFault("wal-open", path,
				fmt.Sprintf("a zero-filled region begins at byte offset %d and is FOLLOWED BY MORE DATA before end of file; this file cannot be interpreted", recStart),
				"every durable message in this WAL file is abandoned; the file is left on disk untouched, and every OTHER WAL file in the directory is still recovered",
				nil)
		}

		data := make([]byte, dataLen)
		if _, rerr := io.ReadFull(file, data); rerr != nil {
			break
		}

		curRecStart := recStart
		recStart += 8 + int64(dataLen)

		if crc != 0 {
			h := crc32.NewIEEE()
			h.Write(hdr[4:8])
			h.Write(data)
			if crc != h.Sum32() {
				if walNothingIntelligibleFollows(file, recStart, -1) {
					break // torn tail: drop it and stop, nothing real follows
				}
				return nil, false, interfaces.FatalFault("wal-open", path,
					fmt.Sprintf("the record at byte offset %d failed its CRC32 check and is NOT the last record in the file — real data follows it; this file cannot be interpreted", curRecStart),
					"every durable message in this WAL file is abandoned, including any that follow the damaged record and are themselves intact; every OTHER WAL file in the directory is still recovered",
					nil)
			}
		}

		recType, payload := recordTypeAndPayload(data)
		switch recType {
		case WALRecordTypeBodyBlock:
			hasSharedBody = true
		case WALRecordTypeMessage:
			if offset, ok := peekRecordOffset(payload); ok {
				entries = append(entries, walRecordIndexEntry{offset: offset, position: curRecStart})
			}
		}
	}
	return entries, hasSharedBody, nil
}

// peekRecordOffset reads just the offset out of a v4 message payload.
//
// Layout (see appendMessagePayload, which is the single writer of this prefix):
//
//	[flags u16][queue u8-len + bytes][offset u64][...]
//
// Everything after the offset is irrelevant to the index, so it is not parsed.
func peekRecordOffset(payload []byte) (uint64, bool) {
	if len(payload) < 3 {
		return 0, false
	}
	qlen := int(payload[2])
	pos := 3 + qlen
	if pos+8 > len(payload) {
		return 0, false
	}
	return binary.BigEndian.Uint64(payload[pos : pos+8]), true
}

// rebuildBootState restores fileNum, oldFiles and offsetIndex from what is
// already on disk. It MUST be called from createSharedWAL before the
// first openNextFile() and before any background goroutine starts; at that
// point qw is not yet reachable from any other goroutine, so the fields it
// writes need no locking (the locks taken below are for uniformity with the
// rest of the type, not for a race that can occur here).
//
// A file whose header or grammar cannot be walked is still registered in
// oldFiles — with a NIL offset bitmap, which makes tryDeleteOldFiles' allAcked
// predicate false by construction, so an unreadable file can never be reclaimed
// on the strength of an inventory we do not have.
//
// STEP 3 — IT NOW FAILS THE BOOT on a per-file problem unless
// cfg.UnsafeRecovery is set. It used to `continue` past any per-file error,
// which meant a WAL file this build cannot parse was silently tolerated at
// construction and only surfaced later as a swallowed recovery error. This is
// the earliest point at which a data directory can be proven unrecoverable, and
// storage construction already aborts Build() — reusing that path rather than
// inventing a second refusal mechanism is deliberate.
//
// It restores no acknowledgement state: this incarnation's ackBitmap starts
// empty, exactly as it did before this file existed, and every physically
// present record is therefore recoverable and readable. See the file header for
// why durable acknowledgement was withdrawn.
func (qw *QueueWAL) rebuildBootState() error {
	entries, err := os.ReadDir(qw.dataDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("failed to enumerate shared WAL directory: %w", err)
	}

	type existingFile struct {
		num  uint64
		path string
	}
	var files []existingFile
	var maxNum uint64
	for _, e := range entries {
		if !isWALFileName(e) {
			continue
		}
		num, _ := parseWALFileNum(e.Name())
		if num > maxNum {
			maxNum = num
		}
		files = append(files, existingFile{num: num, path: filepath.Join(qw.dataDir, e.Name())})
	}

	// fileNum is the ONLY mutation of this counter outside openNextFile's
	// Add(1). Storing the highest existing stem makes the first Add(1) yield
	// maxNum+1, so this incarnation opens a file that did not exist before and
	// rollFile's "oldFiles carries this file's complete offset set" invariant
	// holds for every file this process creates.
	if maxNum > 0 {
		qw.fileNum.Store(maxNum)
	}

	var faults []*interfaces.RecoveryFault
	for _, f := range files {
		// durableSize -1 marks an inherited file: its whole content is
		// presumed intact (it was not damaged by a handled write error in
		// THIS incarnation), so checkpoints scan it whole (WS-C C1 §3.11).
		info := &walFileInfo{path: f.path, durableSize: -1}
		if st, serr := os.Stat(f.path); serr == nil {
			info.createdAt = st.ModTime()
		}

		recs, hasSharedBody, ierr := indexWALFile(f.path)
		if ierr != nil {
			// Unreadable/foreign/unsupported-version file: keep it, but with no
			// inventory, so nothing can conclude it is fully acknowledged.
			qw.oldFiles[f.num] = info
			faults = append(faults, classifyWALIndexError(f.path, ierr))
			continue
		}
		info.hasSharedBody = hasSharedBody

		offsets := roaring64.New()
		for _, rec := range recs {
			offsets.Add(rec.offset)
			qw.offsetIndex[rec.offset] = &offsetLocation{fileNum: f.num, filePosition: rec.position}
		}
		info.offsets = offsets
		qw.oldFiles[f.num] = info
	}

	gating := make([]*interfaces.RecoveryFault, 0, len(faults))
	for _, f := range faults {
		if f.Outcome.GatesBoot() {
			gating = append(gating, f)
		}
	}
	if len(gating) > 0 && !qw.cfg.UnsafeRecovery {
		return errors.New(interfaces.RecoveryRefusalMessage("WAL recovery", gating))
	}
	return nil
}

// classifyWALIndexError names the severity of a per-file boot-index failure at
// the site that knows what the failure means.
func classifyWALIndexError(path string, err error) *interfaces.RecoveryFault {
	// An error indexWALFile already classified keeps its own decision — the
	// site that knows the semantics owns the severity.
	if fs := interfaces.FaultsOf(err); len(fs) > 0 {
		return fs[0]
	}
	if errors.Is(err, ErrUnsupportedWALVersion) {
		return interfaces.FatalFault("wal-open", path,
			"this WAL file's framing version is not one this build can parse, so not one byte of it can be interpreted",
			"every durable message in this WAL file is abandoned; the file is left on disk untouched, the broker will never append to it, and every OTHER WAL file in the directory is still recovered",
			err)
	}
	return interfaces.DegradedFault("wal-open", path,
		"this WAL file could not be read and is quarantined for this boot",
		"every durable message in this WAL file is abandoned for as long as it stays unreadable; the file is left on disk untouched, and every OTHER WAL file in the directory is still recovered",
		err)
}
