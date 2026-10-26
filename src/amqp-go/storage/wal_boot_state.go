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
	inv.durableBytes.Store(fileSize)
	if _, err := file.Seek(dataStart, io.SeekStart); err != nil {
		return inv, false, err
	}

	recStart := dataStart
	durableEnd := dataStart
	hdr := make([]byte, 8)
	inTx := false
	var txEpoch uint64
	var txBytes int64
	// txUnreadable counts the bytes of the records inside the open
	// transaction that this build cannot interpret (a newer format's
	// record). They are protected — counted as live — only if the
	// transaction COMMITS; a discarded transaction's bytes are dead.
	var txUnreadable int64
	var txRecords []*segmentRecord

	discardTx := func() {
		inv.deadBytes.Add(txBytes)
		txBytes = 0
		txUnreadable = 0
		txRecords = txRecords[:0]
		inTx = false
	}
	commitTx := func() {
		for _, r := range txRecords {
			r.committed = true
			inv.addRecord(r)
		}
		if txUnreadable > 0 {
			inv.unacked.Add(txUnreadable)
		}
		txBytes = 0
		txUnreadable = 0
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
			// THE SAME GRAMMAR THE SCAN USES. The index and the scan must
			// never disagree about a record's queue name, delivery tag or
			// body locator: when they did, a record could be indexed but
			// never scanned, and recovery dropped it silently.
			layout := parseWalPayload(payload)
			if !layout.ok {
				if layout.newerFormat {
					// DURABLE DATA THIS BUILD CANNOT INTERPRET. It is not
					// indexed (nothing can serve it), but its bytes are
					// counted as LIVE so the file can never be reclaimed —
					// and never compacted — while they are on disk: a newer
					// build could still read them, and deleting them would
					// be a silent loss of confirmed durable data.
					if inTx {
						txUnreadable += recordBytes
					} else {
						inv.unacked.Add(recordBytes)
					}
				}
				if inTx {
					txBytes += recordBytes
				} else if !layout.newerFormat {
					inv.deadBytes.Add(recordBytes)
				}
				continue
			}
			queueName, offset, bodyRef := layout.queueName, layout.offset, layout.bodyRef
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

// walPayloadLayout describes the structural prefix of a v4 message payload
// (the bytes AFTER the one-byte record-type tag).
//
// IT IS THE OUTPUT OF THE SINGLE GRAMMAR WALK (parseWalPayload). Every field
// here is read by that one walk, so the boot-time physical index and the
// recovery scan cannot disagree about where a field is.
type walPayloadLayout struct {
	// flags is the payload's presence bitmap (the u16 that opens the payload).
	flags uint16
	// queueName is the queue the record belongs to.
	queueName string
	// offset is the delivery tag the record carries.
	offset uint64
	// exchange and routingKey are the record's routing fields.
	exchange   string
	routingKey string
	// deliveryMode is the record's delivery mode.
	deliveryMode uint8
	// bodyKind is the inline|reference discriminator byte.
	bodyKind uint8
	// bodyRef is the file-relative position of the referenced BodyBlock, or
	// -1 when the record carries its body inline.
	bodyRef int64
	// locatorOff is the offset of the body locator WITHIN the payload, or
	// -1 when the record carries its body inline.
	locatorOff int
	// refLen is the length of the reference locator, or -1 when the record
	// carries its body inline.
	refLen int
	// bodyOff is the offset of the inline body WITHIN the payload, or -1
	// when the record does not carry an inline body.
	bodyOff int
	// bodyLen is the length of the inline body, or -1.
	bodyLen int
	// end is the offset WITHIN the payload just past the structural prefix:
	// the byte the first optional field starts at.
	end int
	// newerFormat is true when the record is DURABLE DATA THIS BUILD CANNOT
	// INTERPRET: either the reserved continuation bit is set (a future
	// format appended a second flags word, so nothing after the flags word
	// can be trusted), or the body kind is one this build does not know.
	//
	// Such a record must never be reclaimed (its bytes are still durable
	// data a newer build could read) and must never be dropped without a
	// fault (this build cannot serve it).
	newerFormat bool
	// ok is false when the payload is malformed, or when it is newerFormat.
	ok bool
}

// parseWalPayload walks the structural prefix of a v4 message payload: the
// presence bitmap, the queue name, the delivery tag, the exchange, the
// routing key, the delivery mode and the body arm.
//
// IT IS THE SINGLE WALK OF THAT GRAMMAR. The boot-time physical index
// (indexWALFile), the compaction's locator remapping and the recovery scan
// (parseMessagePayload) all go through it, so they cannot disagree about the
// queue name, the delivery tag, the body kind or where a body locator lives.
//
// WHAT IT REFUSES, AND WHY:
//
//   - A payload with the reserved CONTINUATION bit set. The bit means a
//     future format appended a second flags word; this build does not know
//     where that word sits, so NOTHING after the flags word — not the queue
//     name, not the delivery tag, not the body — can be trusted. The record
//     is marked newerFormat: it is durable data this build cannot interpret,
//     and it must not be reclaimed nor dropped silently.
//   - A payload whose body kind this build does not know. Same treatment: a
//     future body arm is durable data, not garbage.
//   - A truncated or malformed payload. That is garbage (dead bytes), which
//     is what the caller has always done with it.
//
// Every read is bounds-checked against len(payload); a short read returns
// ok=false rather than panicking or slicing out of bounds.
func parseWalPayload(payload []byte) walPayloadLayout {
	var out walPayloadLayout
	out.bodyRef = -1
	out.locatorOff = -1
	out.refLen = -1
	out.bodyOff = -1
	out.bodyLen = -1
	if len(payload) < 3 {
		return out
	}
	out.flags = binary.BigEndian.Uint16(payload[0:2])
	if out.flags&msgFlagContinuation != 0 {
		out.newerFormat = true
		return out
	}
	qlen := int(payload[2])
	pos := 3 + qlen
	if pos+8 > len(payload) {
		return out
	}
	out.queueName = string(payload[3 : 3+qlen])
	out.offset = binary.BigEndian.Uint64(payload[pos : pos+8])
	pos += 8
	// exchange and routing key, both u8-length-prefixed short strings.
	var ok bool
	if out.exchange, pos, ok = readShort(payload, pos); !ok {
		return out
	}
	if out.routingKey, pos, ok = readShort(payload, pos); !ok {
		return out
	}
	if pos+2 > len(payload) {
		return out
	}
	out.deliveryMode = payload[pos]
	pos++
	out.bodyKind = payload[pos]
	pos++
	switch out.bodyKind {
	case bodyKindInline:
		if pos+4 > len(payload) {
			return out
		}
		bodyLen := int(binary.BigEndian.Uint32(payload[pos : pos+4]))
		pos += 4
		if bodyLen < 0 || pos+bodyLen > len(payload) {
			return out
		}
		out.bodyOff = pos
		out.bodyLen = bodyLen
		pos += bodyLen
	case bodyKindReference:
		if pos+2 > len(payload) {
			return out
		}
		n := int(binary.BigEndian.Uint16(payload[pos : pos+2]))
		pos += 2
		// The locator of a body reference is EIGHT bytes: the referenced
		// BodyBlock's file-relative offset. Any other length is not a
		// record this build can resolve.
		if n != 8 || pos+8 > len(payload) {
			return out
		}
		out.locatorOff = pos
		out.refLen = n
		out.bodyRef = int64(binary.BigEndian.Uint64(payload[pos : pos+8]))
		pos += n
	default:
		// A body kind this build does not know: a future format's arm. The
		// record is durable data this build cannot interpret.
		out.newerFormat = true
		return out
	}
	out.end = pos
	out.ok = true
	return out
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
		// A crash between "the compaction copy is written" and "the copy is
		// installed" leaves a <digits>.wal.compact file behind. It is a partial
		// duplicate of a file that still holds every record, so removing it
		// loses nothing — and leaving it would leak one file per crash
		// forever, because nothing else ever looks at these names.
		if isWALCompactOrphan(e.Name()) {
			_ = os.Remove(filepath.Join(qw.dataDir, e.Name()))
			continue
		}
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
