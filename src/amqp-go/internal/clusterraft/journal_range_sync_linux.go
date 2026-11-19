//go:build linux

package clusterraft

import (
	"errors"
	"os"
	"runtime"

	"golang.org/x/sys/unix"
)

type journalRangeSyncFile struct{ journalDataFile }

func wrapRangeSyncJournalFile(file *os.File) journalFile {
	return journalRangeSyncFile{journalDataFile{file}}
}

// writeAtSync uses full O_SYNC-equivalent durability for the written range.
// The sequential writer owns the descriptor and supplies an explicit offset.
func (f journalRangeSyncFile) writeAtSync(data []byte, offset int64) (int, error) {
	n, err := unix.Pwritev2(int(f.Fd()), [][]byte{data}, offset, unix.RWF_SYNC)
	runtime.KeepAlive(f.File)
	return n, err
}

func writeJournalFrame(file journalFile, data []byte, offset int64, syncRange bool) (int, bool, error) {
	if syncRange {
		if ranged, ok := file.(interface {
			writeAtSync([]byte, int64) (int, error)
		}); ok {
			n, err := ranged.writeAtSync(data, offset)
			// Never retry a partial write or hide an I/O/argument failure.
			// Raw Linux syscall errors can report -1 rather than zero bytes.
			unsupported := errors.Is(err, unix.ENOSYS) || errors.Is(err, unix.EOPNOTSUPP)
			if n > 0 || !unsupported {
				return n, err == nil && n == len(data), err
			}
		}
	}
	n, err := file.WriteAt(data, offset)
	return n, false, err
}
