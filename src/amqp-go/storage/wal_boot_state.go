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
// rollFile, commitBatch) and was reconstructed by nothing at boot. Recovery
// extracted messages (RecoverFromWAL) but never rebuilt the index that makes
// them readable, and fileNum restarted at zero so every incarnation reopened
// 00000000000000000001.wal with O_APPEND. Two consequences, both reproduced:
//
//   - C-1: a recovered record that does not fit in the in-memory ring (above the
//     spill threshold) had NO readable durable location — offsetIndex was empty,
//     oldFiles was empty, currentFileOffsets was empty — so its delivery tag was
//     claimable and unreadable and the dispatch plane silently gap-skipped it.
//   - C-2: when the reopened file rolled, rollFile registered it in oldFiles with
//     ONLY this incarnation's offsets, so tryReclaimOldFiles judged it fully acked
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

// indexWALFile builds the complete physical inventory of one WAL file. It
// shares scanWALFile's transaction rule: message records become live only when
// a matching Commit is present. Discarded uncommitted records and a torn tail
// are dead bytes, never live records.
func indexWALFile(fileNum uint64, path string) (inv *segmentInventory, hasSharedBody bool, err error) {
	inv = newSegmentInventory(0)
	defer func() {
		if err != nil {
			inv.valid = false
		}
	}()
	file, err := os.Open(path)
	if err != nil {
		return inv, false, err
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil {
		return inv, false, err
	}
	fileSize := stat.Size()

	dataStart, err := walFileDataStart(file)
	if err != nil {
		return inv, false, err
	}
	inv.durableBytes = fileSize
	if _, err := file.Seek(dataStart, io.SeekStart); err != nil {
		return inv, false, err
	}

	recStart := dataStart
	durableEnd := dataStart
	hdr := make([]byte, 8)
	inTx := false
	var txEpoch uint64
	var txBytes int64
	var txRecords []*segmentRecord

	discardTx := func() {
		inv.deadBytes.Add(txBytes)
		txBytes = 0
		txRecords = txRecords[:0]
		inTx = false
	}
	commitTx := func() {
		for _, r := range txRecords {
			r.committed = true
			inv.addRecord(r)
		}
		txBytes = 0
		txRecords = txRecords[:0]
		inTx = false
	}

	for {
		if _, rerr := io.ReadFull(file, hdr); rerr != nil {
			break
		}
		crc := binary.BigEndian.Uint32(hdr[0:4])
		dataLen := binary.BigEndian.Uint32(hdr[4:8])
		recordBytes := int64(8) + int64(dataLen)

		if walBlankRecordHeader(crc, dataLen) {
			if walNothingIntelligibleFollows(file, recStart, -1) {
				break
			}
			return inv, false, interfaces.FatalFault("wal-open", path,
				fmt.Sprintf("a zero-filled region begins at byte offset %d and is FOLLOWED BY MORE DATA before end of file; this file cannot be interpreted", recStart),
				"every durable message in this WAL file is abandoned; the file is left on disk untouched, and every OTHER WAL file in the directory is still recovered",
				nil)
		}

		data := make([]byte, dataLen)
		if _, rerr := io.ReadFull(file, data); rerr != nil {
			break
		}

		curRecStart := recStart
		recStart += recordBytes

		if crc != 0 {
			h := crc32.NewIEEE()
			h.Write(hdr[4:8])
			h.Write(data)
			if crc != h.Sum32() {
				if walNothingIntelligibleFollows(file, recStart, -1) {
					recStart = curRecStart // the whole bad tail record is dead
					break
				}
				return inv, false, interfaces.FatalFault("wal-open", path,
					fmt.Sprintf("the record at byte offset %d failed its CRC32 check and is NOT the last record in the file — real data follows it; this file cannot be interpreted", curRecStart),
					"every durable message in this WAL file is abandoned, including any that follow the damaged record and are themselves intact; every OTHER WAL file in the directory is still recovered",
					nil)
			}
		}

		durableEnd = recStart
		recType, payload := recordTypeAndPayload(data)
		switch recType {
		case WALRecordTypeTxBoundary:
			if len(payload) < walTxMarkerPayloadLen {
				if inTx {
					txBytes += recordBytes
				} else {
					inv.deadBytes.Add(recordBytes)
				}
				continue
			}
			kind := payload[0]
			epoch := binary.BigEndian.Uint64(payload[1:9])
			switch kind {
			case WALTxMarkerBegin:
				if inTx {
					discardTx()
				}
				inTx = true
				txEpoch = epoch
				txBytes = recordBytes
			case WALTxMarkerCommit:
				if inTx && epoch == txEpoch {
					commitTx()
				} else {
					if inTx {
						discardTx()
					}
					inv.deadBytes.Add(recordBytes)
				}
			default:
				if inTx {
					txBytes += recordBytes
				} else {
					inv.deadBytes.Add(recordBytes)
				}
			}

		case WALRecordTypeBodyBlock:
			hasSharedBody = true
			inv.addBlock(curRecStart, recordBytes)
			if inTx {
				txBytes += recordBytes
			}

		case WALRecordTypeMessage:
			queueName, offset, bodyRef, ok := peekRecordInventory(payload)
			if !ok {
				if inTx {
					txBytes += recordBytes
				} else {
					inv.deadBytes.Add(recordBytes)
				}
				continue
			}
			r := &segmentRecord{
				pos:       recordPos{segmentID: fileNum, position: curRecStart},
				queueName: queueName, deliveryTag: offset,
				bytes: recordBytes, bodyRef: bodyRef,
			}
			if inTx {
				txBytes += recordBytes
				txRecords = append(txRecords, r)
			} else {
				r.committed = true
				inv.addRecord(r)
			}
		default:
			if inTx {
				txBytes += recordBytes
			} else {
				inv.deadBytes.Add(recordBytes)
			}
		}
	}
	if inTx {
		discardTx()
	}
	// A body block with no surviving committed reference is physical garbage.
	// Its bytes are separate from transaction message bytes in the WAL format.
	for _, block := range inv.blocks {
		if block.refs.Load() == 0 {
			inv.deadBytes.Add(block.bytes)
		}
	}
	inv.deadBytes.Add(fileSize - durableEnd)
	return inv, hasSharedBody, nil
}

// peekRecordInventory reads only the structural prefix needed by the physical
// index, avoiding a full protocol.Message allocation for every boot record.
func peekRecordInventory(payload []byte) (queueName string, offset uint64, bodyRef int64, ok bool) {
	bodyRef = -1
	if len(payload) < 3 {
		return "", 0, bodyRef, false
	}
	qlen := int(payload[2])
	pos := 3 + qlen
	if pos+8 > len(payload) {
		return "", 0, bodyRef, false
	}
	queueName = string(payload[3 : 3+qlen])
	offset = binary.BigEndian.Uint64(payload[pos : pos+8])
	pos += 8
	// exchange and routing-key short strings, delivery mode, body kind
	for range 2 {
		if pos >= len(payload) {
			return "", 0, bodyRef, false
		}
		n := int(payload[pos])
		pos += 1 + n
		if pos > len(payload) {
			return "", 0, bodyRef, false
		}
	}
	if pos+2 > len(payload) {
		return "", 0, bodyRef, false
	}
	pos++ // delivery mode
	kind := payload[pos]
	pos++
	if kind == bodyKindReference {
		if pos+2 > len(payload) {
			return "", 0, bodyRef, false
		}
		n := int(binary.BigEndian.Uint16(payload[pos : pos+2]))
		pos += 2
		if n != 8 || pos+8 > len(payload) {
			return "", 0, bodyRef, false
		}
		bodyRef = int64(binary.BigEndian.Uint64(payload[pos : pos+8]))
	}
	return queueName, offset, bodyRef, true
}

// rebuildBootState restores fileNum, oldFiles and offsetIndex from what is
// already on disk. It MUST be called from createSharedWAL before the
// first openNextFile() and before any background goroutine starts; at that
// point qw is not yet reachable from any other goroutine, so the fields it
// writes need no locking (the locks taken below are for uniformity with the
// rest of the type, not for a race that can occur here).
//
// A file whose header or grammar cannot be walked is still registered in
// oldFiles — with a NIL offset bitmap, which makes tryReclaimOldFiles' allAcked
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
		info := &walFileInfo{path: f.path, durableSize: -1, inv: newSegmentInventory(0)}
		info.inv.valid = false
		if st, serr := os.Stat(f.path); serr == nil {
			info.createdAt = st.ModTime()
		}

		inv, hasSharedBody, ierr := indexWALFile(f.num, f.path)
		info.inv = inv
		if ierr != nil {
			// Unreadable/foreign/unsupported-version file: keep it, but with no
			// inventory, so nothing can conclude it is fully acknowledged.
			qw.oldFiles[f.num] = info
			faults = append(faults, classifyWALIndexError(f.path, ierr))
			continue
		}
		info.hasSharedBody = hasSharedBody

		offsets := roaring64.New()
		for _, rec := range inv.records {
			offsets.Add(rec.deliveryTag)
			qw.indexRecordLocked(rec)
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
