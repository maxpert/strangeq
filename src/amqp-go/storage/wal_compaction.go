package storage

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"sort"
	"strings"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/maxpert/amqp-go/interfaces"
)

// WI7 — byte-ratio WAL compaction.
//
// The WAL tier keeps every acknowledged record's bytes on disk until the whole
// file is reclaimed (WI6). A sealed file whose acknowledged bytes are the
// MAJORITY of its durable bytes is mostly garbage, and that garbage is
// reclaimed only when the last live record in the file is acknowledged — which
// can be never. Compaction rewrites such a file with only its committed,
// unacknowledged records, so the space comes back NOW.
//
// The rewrite is a NEW FILE written beside the old one and installed with a
// rename, so a reader never observes a half-written file, and a failure before
// the rename leaves the old bytes AND the old index untouched.
//
// WHAT IS COPIED
//
//   - every committed, unacknowledged message record, byte for byte, except
//     that a shared-body locator is remapped to the block's new position and
//     the record's CRC is recomputed over the patched bytes;
//   - one BodyBlock per body still referenced by a copied record, emitted
//     BEFORE the record that references it (the co-location invariant both the
//     reader and the recovery scanner rely on: a reference is resolved only
//     when the block it points at has already been seen);
//   - NOTHING else. Transaction markers are not copied: the records they
//     bracket are committed, so they stand on their own without them.
//
// WHAT IS NOT COPIED
//
//   - acknowledged records (they are the garbage being reclaimed);
//   - uncommitted transaction records (a torn transaction is never
//     resurrected);
//   - body blocks no copied record references.

// DefaultWALCompactionThreshold is the share of a sealed WAL file's durable
// bytes that must be acknowledged before the file is compacted. The comparison
// is STRICT: exactly 50% dead does not compact.
const DefaultWALCompactionThreshold = 0.5

// walCompactSuffix is appended to a WAL file's path to name the temporary file
// a compaction writes before renaming it over the original.
const walCompactSuffix = ".compact"

// tryCompactOldFiles sweeps the sealed WAL files and compacts the ones whose
// acknowledged bytes exceed the configured share of their durable bytes.
//
// The sweep runs in the WAL's own cleanup goroutine: maintenance is called
// DIRECTLY from the cleanup tick, so there is no second ticker and no second
// goroutine to keep in step with the first.
//
// Every file is attempted AT MOST ONCE per sweep, and the claim is a
// compare-and-swap on the file's own record: two concurrent maintenance
// passes cannot compact the same file twice. A FAILED attempt leaves the claim
// set, so a file that cannot be compacted is not retried on every tick
// forever; a SUCCESSFUL attempt releases it, so a file that accumulates more
// dead bytes later can be compacted again.
func (qw *QueueWAL) tryCompactOldFiles() {
	qw.oldFilesMutex.RLock()
	nums := make([]uint64, 0, len(qw.oldFiles))
	for num := range qw.oldFiles {
		nums = append(nums, num)
	}
	qw.oldFilesMutex.RUnlock()
	// Oldest first: the oldest file has had the longest to accumulate dead
	// bytes, and compacting it first keeps the directory's size down while the
	// newer files are still being written.
	sort.Slice(nums, func(i, j int) bool { return nums[i] < nums[j] })

	for _, num := range nums {
		if err := qw.compactOldFile(num); err != nil {
			qw.logError("WAL file compaction failed; the file keeps its bytes and its index unchanged",
				interfaces.LogField{Key: "fileNum", Value: num},
				interfaces.LogField{Key: "error", Value: err.Error()})
		}
	}
}

// walCompactWrite writes b to f through the WAL write seam. A short write is
// an error: the copy would otherwise be silently truncated.
func walCompactWrite(f *os.File, b []byte) error {
	n, err := walWrite(f, b)
	if err != nil {
		return err
	}
	if n != len(b) {
		return fmt.Errorf("short write: %d of %d bytes", n, len(b))
	}
	return nil
}

// walRecordLocatorOffset returns the offset, within a whole framed record
// ([CRC 4][len 4][type 1][payload]), of the 8-byte body locator, and whether
// the record carries one at all.
//
// A framed record's payload starts at byte 9: 4 CRC bytes, 4 length bytes
// and the one-byte record-type tag.
func walRecordLocatorOffset(record []byte) (int, bool) {
	if len(record) < 9 {
		return 0, false
	}
	layout := parseWalPayload(record[9:])
	if !layout.ok || layout.locatorOff < 0 {
		return 0, false
	}
	return 9 + layout.locatorOff, true
}

// compactOldFile rewrites ONE sealed WAL file, keeping only its committed,
// unacknowledged records.
//
// The sequence is:
//
//  1. select the file (the caller has already established that it is
//     eligible);
//  2. write the compacted copy to a temporary file beside it and fsync it;
//  3. take the index write lock and the old-files lock, verify the file is
//     still registered, transfer any acknowledgement that landed while the
//     copy was being written, close the old file's cached handle, rename the
//     copy over the original, and publish the new layout.
//
// Every failure BEFORE the rename leaves the original bytes and the original
// index exactly as they were: nothing is published until the rename succeeds.
// A failure AFTER the rename (a failed directory fsync) still publishes the
// new layout, because the bytes on disk have already changed and an index
// that describes the OLD layout would be the wrong one.
func (qw *QueueWAL) compactOldFile(fileNum uint64) error {
	qw.oldFilesMutex.RLock()
	info, registered := qw.oldFiles[fileNum]
	qw.oldFilesMutex.RUnlock()
	if !registered {
		return nil
	}
	// The active file is never compacted: it is still being appended to.
	if fileNum == qw.fileNum.Load() {
		return nil
	}
	inv := info.inv
	if inv == nil || !inv.valid {
		return nil
	}
	// A fully acknowledged file is unlinked by the reclaim sweep, not
	// compacted into an empty file.
	if inv.unacked.Load() == 0 {
		return nil
	}
	// The selection is a strict byte-ratio comparison: exactly the threshold
	// is not enough.
	//
	// inv.deadBytes is the number of bytes in the file that no longer carry a
	// recoverable record (acknowledged records, and the unreferenced body
	// blocks), and inv.durableBytes is the file's durable size, so the ratio
	// is "how much of this file is garbage".
	threshold := qw.cfg.CompactionThreshold
	if threshold <= 0 {
		threshold = DefaultWALCompactionThreshold
	}
	if !(float64(inv.deadBytes.Load()) > threshold*float64(inv.durableBytes)) {
		return nil
	}
	// Claim the attempt with a compare-and-swap, so two concurrent
	// maintenance passes cannot compact the same file at once. A FAILED
	// attempt keeps the claim: compaction reads and rewrites a whole file,
	// so retrying a file that cannot be compacted would repeat that cost on
	// every tick forever. A SUCCESSFUL attempt releases the claim, so a file
	// that accumulates more dead bytes later can be compacted again.
	if !info.compactionAttempted.CompareAndSwap(false, true) {
		return nil
	}
	succeeded := false
	defer func() {
		if succeeded {
			info.compactionAttempted.Store(false)
		}
	}()

	// The inventory's maps are immutable once published, so the live and the
	// acknowledged records can be read without the index lock. The acked flag
	// is atomic and may flip while we work; a flip that lands before the
	// install is transferred below.
	type candidate struct {
		old *segmentRecord
		rec *segmentRecord
	}
	var live []*candidate
	var omitted []*segmentRecord
	for _, r := range inv.records {
		if !r.committed {
			continue
		}
		if r.acked.Load() {
			omitted = append(omitted, r)
			continue
		}
		live = append(live, &candidate{old: r})
	}
	if len(live) == 0 {
		return nil
	}
	// The records are emitted in their original order, so the newest copy of a
	// key is still the newest copy after the rewrite.
	sort.Slice(live, func(i, j int) bool {
		return live[i].old.pos.position < live[j].old.pos.position
	})

	src, err := os.Open(info.path)
	if err != nil {
		return fmt.Errorf("open source WAL file %s: %w", info.path, err)
	}
	defer src.Close()

	tmpPath := info.path + walCompactSuffix
	tmp, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("create compaction temp file %s: %w", tmpPath, err)
	}
	// A failure before the rename must leave nothing behind.
	installed := false
	defer func() {
		if !installed {
			_ = os.Remove(tmpPath)
		}
	}()

	newInv := newSegmentInventory(0)
	// oldBlockPos -> new position of a body block already emitted.
	blockPositions := make(map[int64]int64)
	var writeOff int64

	header := make([]byte, 0, WALHeaderSize)
	header = append(header, WALMagic...)
	header = append(header, WALFormatVersion)
	if err := walCompactWrite(tmp, header); err != nil {
		return fmt.Errorf("write compaction header: %w", err)
	}
	writeOff = int64(len(header))

	// The copy. A record's bytes are its WHOLE framed form
	// ([CRC 4][len 4][type 1][payload]), and its position is its offset in
	// the file, so the copy is a byte-for-byte move of those bytes; only a
	// body locator is patched, and the CRC is recomputed over the patched
	// bytes because it covers them.
	for _, c := range live {
		r := c.old
		raw := make([]byte, r.bytes)
		if _, err := src.ReadAt(raw, r.pos.position); err != nil {
			return fmt.Errorf("read record at %d of %s: %w", r.pos.position, info.path, err)
		}
		newBodyRef := int64(-1)
		if r.bodyRef >= 0 {
			// The body block is CO-LOCATED with the records that reference
			// it (one batch, one file), so it lives in this file at
			// r.bodyRef and is copied once no matter how many records
			// reference it.
			newBlockPos, emitted := blockPositions[r.bodyRef]
			if !emitted {
				block, ok := inv.blocks[r.bodyRef]
				if !ok {
					return fmt.Errorf("record %s/%d references body block at %d which is not in the inventory of %s",
						r.queueName, r.deliveryTag, r.bodyRef, info.path)
				}
				blockRaw := make([]byte, block.bytes)
				if _, err := src.ReadAt(blockRaw, r.bodyRef); err != nil {
					return fmt.Errorf("read body block at %d of %s: %w", r.bodyRef, info.path, err)
				}
				if err := walCompactWrite(tmp, blockRaw); err != nil {
					return fmt.Errorf("write body block: %w", err)
				}
				newBlockPos = writeOff
				writeOff += int64(len(blockRaw))
				blockPositions[r.bodyRef] = newBlockPos
				newInv.addBlock(newBlockPos, int64(len(blockRaw)))
			}
			// Remap the locator to the block's new position and recompute the
			// record's CRC, which covers the patched bytes.
			locOff, ok := walRecordLocatorOffset(raw)
			if !ok {
				return fmt.Errorf("record %s/%d of %s carries a body reference but no locator could be located",
					r.queueName, r.deliveryTag, info.path)
			}
			binary.BigEndian.PutUint64(raw[locOff:locOff+8], uint64(newBlockPos))
			crc := uint32(0)
			if !qw.cfg.CRCDisabled {
				crc = crc32.ChecksumIEEE(raw[4:])
			}
			binary.BigEndian.PutUint32(raw[0:4], crc)
			newBodyRef = newBlockPos
		}
		if err := walCompactWrite(tmp, raw); err != nil {
			return fmt.Errorf("write record %s/%d: %w", r.queueName, r.deliveryTag, err)
		}
		newPos := writeOff
		writeOff += int64(len(raw))
		c.rec = &segmentRecord{
			pos:         recordPos{segmentID: fileNum, position: newPos},
			queueName:   r.queueName,
			deliveryTag: r.deliveryTag,
			bytes:       int64(len(raw)),
			bodyRef:     newBodyRef,
			committed:   true,
		}
		newInv.addRecord(c.rec)
	}

	if err := walCompactionFsync(tmp); err != nil {
		return fmt.Errorf("fsync compaction temp file %s: %w", tmpPath, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close compaction temp file %s: %w", tmpPath, err)
	}

	// A test hook: the acknowledgement that must land between the copy and
	// the install is delivered through this hook, so the ordering is
	// deterministic and needs no timing.
	walCompactionBeforeInstall(fileNum)

	qw.offsetIndexMutex.Lock()
	defer qw.offsetIndexMutex.Unlock()
	qw.oldFilesMutex.Lock()
	defer qw.oldFilesMutex.Unlock()

	cur, ok := qw.oldFiles[fileNum]
	if !ok || cur != info {
		return fmt.Errorf("WAL file %d was reclaimed while it was being compacted", fileNum)
	}

	// Transfer the acknowledgements that landed while the copy was written.
	// A record that was copied and has since been acknowledged must NOT be
	// published as live: it would be delivered again after a restart, because
	// acknowledgements are process-volatile.
	for _, c := range live {
		if c.old.acked.Load() {
			c.rec.markAck()
		}
	}

	// The old file's cached handle points at the inode the rename replaces.
	// It is closed here and reopened lazily on the next read.
	qw.closeOldFileHandle(fileNum)

	if err := walRename(tmpPath, info.path); err != nil {
		return fmt.Errorf("install compacted WAL file %s: %w", info.path, err)
	}
	installed = true

	// The rename is not durable until the directory entry is fsynced. A
	// failure here does NOT invalidate the compaction: the rename has already
	// happened, and a crash that loses the rename leaves the OLD file in
	// place, which still holds every record.
	syncErr := syncDir(qw.dataDir)

	// Publish the new layout. The bytes on disk have already changed, so the
	// index MUST describe the new layout even if the directory fsync failed.
	newInv.durableBytes = writeOff
	info.inv = newInv
	info.durableSize = writeOff
	info.damagedTail = false
	info.hasSharedBody = len(blockPositions) > 0
	// The tag set of the file is the tag set of the records the file now
	// holds. A record that was acknowledged WHILE the copy was being written
	// is still physically in the file, so its tag stays in the set; a record
	// that was omitted is gone from the file, so its tag leaves it.
	offsets := roaring64.New()
	for _, rec := range newInv.records {
		offsets.Add(rec.deliveryTag)
	}
	info.offsets = offsets

	// The survivors move to their new positions; the records that were not
	// copied are no longer on disk at all.
	for _, c := range live {
		qw.replaceRecordLocked(c.old, c.rec)
	}
	for _, r := range omitted {
		qw.unindexRecordLocked(r)
	}

	succeeded = true
	if syncErr != nil {
		qw.logWarn("compacted WAL file was installed but its directory entry is not durable; a crash may leave the previous version of the file, which still holds every record",
			interfaces.LogField{Key: "file", Value: info.path},
			interfaces.LogField{Key: "error", Value: syncErr.Error()})
	}
	return nil
}

// isWALCompactOrphan reports whether name is the temporary file a WAL
// compaction writes before renaming it over the original: <digits>.wal.compact.
//
// A crash between "the copy is written" and "the copy is installed" leaves
// such a file behind. It is a partial duplicate of a file that still holds
// every record, so removing it loses nothing — and leaving it would leak one
// file per crash forever, because nothing else ever looks at these names.
func isWALCompactOrphan(name string) bool {
	if !strings.HasSuffix(name, WALFileExtension+walCompactSuffix) {
		return false
	}
	stem := strings.TrimSuffix(name, WALFileExtension+walCompactSuffix)
	if stem == "" {
		return false
	}
	for i := 0; i < len(stem); i++ {
		if stem[i] < '0' || stem[i] > '9' {
			return false
		}
	}
	return true
}
