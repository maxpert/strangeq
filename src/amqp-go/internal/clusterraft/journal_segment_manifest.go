package clusterraft

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
)

const (
	journalSegmentManifestName       = "segments.manifest"
	journalSegmentManifestTempPrefix = ".segments-manifest-"
	journalSegmentManifestMagic      = "SQJSEG04"
	journalSegmentManifestHeader     = len(journalSegmentManifestMagic) + 8 + 4
	journalSegmentManifestExtent     = 4 * 8
	journalSegmentManifestAnchor     = 3*8 + 4
	journalSegmentManifestTrailer    = 4
	maxJournalSegmentCount           = 1024
	maxJournalSegmentManifestBytes   = journalSegmentManifestHeader +
		maxJournalSegmentCount*journalSegmentManifestExtent + journalSegmentManifestAnchor +
		maxJournalCheckpointName + journalSegmentManifestTrailer
	minJournalSegmentFrameBytes = journalFrameHeaderSize + journalRecordHeaderSize + 1 + journalFrameTrailerSize
)

type journalSegmentExtent struct {
	id, firstSequence uint64
	base, sealedBytes int64 // Only the last extent is active and has sealedBytes == 0.
}

type journalSegmentManifest struct {
	extentBytes int64
	extents     []journalSegmentExtent
	checkpoint  journalSegmentCheckpointRef
}

func (m journalSegmentManifest) validate() error {
	if m.extentBytes < minJournalSegmentFrameBytes || m.extentBytes > journalGroupMaxFileBytes ||
		len(m.extents) == 0 || len(m.extents) > maxJournalSegmentCount {
		return fmt.Errorf("%w: invalid segment manifest bounds", errJournalCorrupt)
	}
	first := m.extents[0]
	if first.base != 0 {
		if first.base < 0 || first.firstSequence <= 1 || !m.checkpoint.valid() {
			return fmt.Errorf("%w: invalid retained journal origin or checkpoint", errJournalCorrupt)
		}
	} else if first.firstSequence != 1 {
		return fmt.Errorf("%w: invalid initial journal sequence", errJournalCorrupt)
	}
	base := first.base
	var lastID, lastSequence uint64
	for i, extent := range m.extents {
		// The selected index covers gaps in sealed history. Its replay
		// suffix always has contiguous offsets and frame sequences.
		gapAllowed := m.checkpoint.valid() && extent.base <= m.checkpoint.end
		if extent.id <= lastID || extent.base < base || (extent.base != base && !gapAllowed) || extent.firstSequence <= lastSequence ||
			extent.firstSequence == math.MaxUint64 {
			return fmt.Errorf("%w: invalid journal extent at position %d", errJournalCorrupt, i)
		}
		if i < len(m.extents)-1 {
			if extent.sealedBytes < minJournalSegmentFrameBytes || extent.sealedBytes > m.extentBytes ||
				extent.sealedBytes > math.MaxInt64-extent.base {
				return fmt.Errorf("%w: invalid sealed journal extent %d", errJournalCorrupt, extent.id)
			}
		} else if extent.sealedBytes != 0 {
			return fmt.Errorf("%w: journal has no final active extent", errJournalCorrupt)
		}
		base = extent.base + extent.sealedBytes
		lastID, lastSequence = extent.id, extent.firstSequence
	}
	if m.checkpoint != (journalSegmentCheckpointRef{}) {
		if !m.checkpoint.valid() {
			return fmt.Errorf("%w: invalid journal checkpoint descriptor", errJournalCorrupt)
		}
		_, err := m.cutoff(m.checkpoint.sequence, m.checkpoint.end)
		return err
	}
	return nil
}

// cutoff returns the first extent after an exact sealed prefix. A selected
// checkpoint may end at the retained origin, leaving no covered extents.
func (m journalSegmentManifest) cutoff(sequence uint64, end int64) (int, error) {
	first := 1
	if m.extents[0].base != 0 {
		first = 0
	}
	if sequence != 0 && sequence < math.MaxUint64 && len(m.extents) > first {
		for i, extent := range m.extents[first:] {
			if extent.base == end && extent.firstSequence == sequence+1 {
				return i + first, nil
			}
		}
	}
	return 0, fmt.Errorf("%w: journal checkpoint cutoff is not a sealed extent boundary", errJournalCorrupt)
}

func loadJournalSegmentManifest(dir string) (journalSegmentManifest, error) {
	path := filepath.Join(dir, journalSegmentManifestName)
	info, err := os.Lstat(path)
	if err != nil {
		return journalSegmentManifest{}, err
	}
	if !info.Mode().IsRegular() || info.Size() > int64(maxJournalSegmentManifestBytes) {
		return journalSegmentManifest{}, fmt.Errorf("%w: invalid segment manifest file", errJournalCorrupt)
	}
	file, err := os.Open(path)
	if err != nil {
		return journalSegmentManifest{}, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, int64(maxJournalSegmentManifestBytes)+1))
	if err != nil {
		return journalSegmentManifest{}, err
	}
	if len(data) < journalSegmentManifestHeader+journalSegmentManifestTrailer || len(data) > maxJournalSegmentManifestBytes {
		return journalSegmentManifest{}, fmt.Errorf("%w: invalid segment manifest header", errJournalCorrupt)
	}
	body, checksum := data[:len(data)-4], data[len(data)-4:]
	count := binary.BigEndian.Uint32(body[16:20])
	if count == 0 || count > maxJournalSegmentCount || binary.BigEndian.Uint32(checksum) != crc32.Checksum(body, journalChecksum) {
		return journalSegmentManifest{}, fmt.Errorf("%w: invalid segment manifest length or checksum", errJournalCorrupt)
	}
	end := journalSegmentManifestHeader + int(count)*journalSegmentManifestExtent
	if len(body) < end {
		return journalSegmentManifest{}, fmt.Errorf("%w: missing segment manifest extents", errJournalCorrupt)
	}
	m := journalSegmentManifest{extentBytes: int64(binary.BigEndian.Uint64(body[8:16])),
		extents: make([]journalSegmentExtent, count)}
	if string(body[:8]) != journalSegmentManifestMagic {
		return journalSegmentManifest{}, fmt.Errorf("%w: unknown segment manifest version", errJournalCorrupt)
	}
	if anchor := body[end:]; len(anchor) != 0 {
		if len(anchor) <= journalSegmentManifestAnchor || len(anchor) > journalSegmentManifestAnchor+maxJournalCheckpointName {
			return journalSegmentManifest{}, fmt.Errorf("%w: invalid segment manifest anchor length", errJournalCorrupt)
		}
		m.checkpoint = journalSegmentCheckpointRef{
			sequence: binary.BigEndian.Uint64(anchor[:8]), end: int64(binary.BigEndian.Uint64(anchor[8:16])),
			size: int64(binary.BigEndian.Uint64(anchor[16:24])), checksum: binary.BigEndian.Uint32(anchor[24:28]),
			name: string(anchor[28:]),
		}
	}
	for i := range m.extents {
		at := journalSegmentManifestHeader + i*journalSegmentManifestExtent
		m.extents[i] = journalSegmentExtent{
			id: binary.BigEndian.Uint64(body[at : at+8]), base: int64(binary.BigEndian.Uint64(body[at+8 : at+16])),
			sealedBytes: int64(binary.BigEndian.Uint64(body[at+16 : at+24])), firstSequence: binary.BigEndian.Uint64(body[at+24 : at+32]),
		}
	}
	return m, m.validate()
}

func (m journalSegmentManifest) publish(dir string, hooks journalSegmentHooks) error {
	if err := m.validate(); err != nil {
		return err
	}
	data := append([]byte(nil), journalSegmentManifestMagic...)
	data = binary.BigEndian.AppendUint64(data, uint64(m.extentBytes))
	data = binary.BigEndian.AppendUint32(data, uint32(len(m.extents)))
	for _, extent := range m.extents {
		data = binary.BigEndian.AppendUint64(data, extent.id)
		data = binary.BigEndian.AppendUint64(data, uint64(extent.base))
		data = binary.BigEndian.AppendUint64(data, uint64(extent.sealedBytes))
		data = binary.BigEndian.AppendUint64(data, extent.firstSequence)
	}
	if anchor := m.checkpoint; anchor.name != "" {
		data = binary.BigEndian.AppendUint64(data, anchor.sequence)
		data = binary.BigEndian.AppendUint64(data, uint64(anchor.end))
		data = binary.BigEndian.AppendUint64(data, uint64(anchor.size))
		data = binary.BigEndian.AppendUint32(data, anchor.checksum)
		data = append(data, anchor.name...)
	}
	data = binary.BigEndian.AppendUint32(data, crc32.Checksum(data, journalChecksum))

	tmp, err := os.CreateTemp(dir, journalSegmentManifestTempPrefix)
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	_, err = tmp.Write(data)
	if err == nil {
		err = hooks.syncFile(tmp)
	}
	closeErr := tmp.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	return hooks.publishManifest(func() error {
		if err := os.Rename(tmp.Name(), filepath.Join(dir, journalSegmentManifestName)); err != nil {
			return err
		}
		return hooks.syncDir(dir)
	})
}
