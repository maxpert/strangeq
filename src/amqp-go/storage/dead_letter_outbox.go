package storage

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"iter"
	"os"
	"path/filepath"
	"strings"

	"github.com/maxpert/amqp-go/protocol"
)

const (
	deadLetterPrefix    = "dead-letter-"
	deadLetterSuffix    = ".pending"
	deadLetterFormat    = "DLX1"
	deadLetterScanBatch = 64
)

// PendingDeadLetter contains an independent, inline copy of a dead letter.
// A nil Targets slice means routing has yet to resolve. A non-nil slice is
// the frozen set of target names that still need a durable delivery.
type PendingDeadLetter struct {
	Source  string
	Message *protocol.Message
	Targets []string
}

func (ds *StorageEngine) SupportsReliableDeadLetters() bool {
	return ds.wal != nil && !ds.wal.cfg.SyncDisabled
}

// StageDeadLetter commits the payload independently of the source record's
// lifetime. Its identifier cannot be reused by a future source delivery tag.
func (ds *StorageEngine) StageDeadLetter(source string, msg *protocol.Message) error {
	id := deadLetterPrefix + rand.Text() + deadLetterSuffix
	_, err := ds.UpdateDeadLetter(id, &PendingDeadLetter{Source: source, Message: msg})
	return err
}

// PendingDeadLetters scans the outbox in directory order, keeping a bounded
// batch of names in memory. Ending iteration or stopping closes the directory.
func (ds *StorageEngine) PendingDeadLetters(stop <-chan struct{}) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		dir, err := os.Open(ds.dataDir)
		if err != nil {
			yield("", err)
			return
		}
		defer dir.Close()
		for {
			select {
			case <-stop:
				return
			default:
			}
			entries, err := dir.ReadDir(deadLetterScanBatch)
			for _, entry := range entries {
				if !entry.IsDir() && isDeadLetterID(entry.Name()) && !yield(entry.Name(), nil) {
					return
				}
			}
			if err != nil {
				if err != io.EOF {
					yield("", err)
				}
				return
			}
		}
	}
}

func (ds *StorageEngine) ReadDeadLetter(id string) (*PendingDeadLetter, error) {
	if !isDeadLetterID(id) {
		return nil, fmt.Errorf("invalid pending dead-letter name %q", id)
	}
	data, err := os.ReadFile(filepath.Join(ds.dataDir, id))
	if err != nil {
		return nil, err
	}
	if len(data) < 12 || string(data[:4]) != deadLetterFormat {
		return nil, fmt.Errorf("invalid pending dead-letter format in %q", id)
	}
	content, checksum := data[:len(data)-4], data[len(data)-4:]
	if binary.BigEndian.Uint32(checksum) != crc32.ChecksumIEEE(content) {
		return nil, fmt.Errorf("pending dead-letter checksum mismatch in %q", id)
	}
	length := binary.BigEndian.Uint32(content[4:8])
	if uint64(length) > uint64(len(content)-8) {
		return nil, fmt.Errorf("invalid pending dead-letter routing data in %q", id)
	}
	end := 8 + int(length)
	var targets []string
	if err := json.Unmarshal(content[8:end], &targets); err != nil {
		return nil, fmt.Errorf("invalid pending dead-letter targets in %q: %w", id, err)
	}
	source, _, msg, ok := parseMessagePayload(content[end:])
	if !ok || len(msg.BodyRef) != 0 {
		return nil, fmt.Errorf("invalid pending dead-letter payload in %q", id)
	}
	return &PendingDeadLetter{Source: source, Message: msg, Targets: targets}, nil
}

// UpdateDeadLetter reports whether the replacement is visible to readers and
// returns any failure, including a sync error after the replacement is visible.
func (ds *StorageEngine) UpdateDeadLetter(id string, entry *PendingDeadLetter) (replaced bool, err error) {
	if !isDeadLetterID(id) {
		return false, fmt.Errorf("invalid pending dead-letter name %q", id)
	}
	if len(entry.Message.BodyRef) != 0 {
		return false, errors.New("pending dead-letter requires an inline body")
	}
	targets, err := json.Marshal(entry.Targets)
	if err != nil {
		return false, err
	}
	data := binary.BigEndian.AppendUint32([]byte(deadLetterFormat), uint32(len(targets)))
	data = append(data, targets...)
	data, err = appendMessagePayload(data, entry.Source, entry.Message, 0)
	if err != nil {
		return false, err
	}
	data = binary.BigEndian.AppendUint32(data, crc32.ChecksumIEEE(data))
	return replaceFileWithPolicy(filepath.Join(ds.dataDir, id), data, 0600, ds.metadataStore.pol)
}

func (ds *StorageEngine) RemoveDeadLetter(id string) error {
	if !isDeadLetterID(id) {
		return fmt.Errorf("invalid pending dead-letter name %q", id)
	}
	if err := os.Remove(filepath.Join(ds.dataDir, id)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return ds.metadataStore.pol.dir(ds.dataDir)
}

func isDeadLetterID(name string) bool {
	return filepath.Base(name) == name && strings.HasPrefix(name, deadLetterPrefix) && strings.HasSuffix(name, deadLetterSuffix)
}
