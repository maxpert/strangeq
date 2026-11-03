//go:build unix

package storage

import (
	"io"
	"os"

	"golang.org/x/sys/unix"
)

// walWritevMaxIOV bounds the iovec count of ONE writev(2) call.
//
// IOV_MAX is 1024 on every Unix this build targets (Linux, Darwin, the BSDs);
// the kernel rejects a larger iovcnt with EINVAL, so the batch writer chunks
// its record pieces to stay under the limit. A record contributes at most
// THREE pieces (header, body, optional-property tail), so a chunk carries a
// few hundred records — the syscall overhead is negligible next to the bytes
// it moves.
const walWritevMaxIOV = 1024

// writevFile writes the concatenation of parts to f.
//
// The parts are handed to the kernel AS THEY ARE: the message body is passed
// BY REFERENCE, so the WAL never copies a body into a staging buffer. The
// bytes that land in the file are byte-for-byte what a single-buffer write of
// the concatenation would have produced.
//
// The pieces are written in order and in as few syscalls as the iovec limit
// allows. A short write is normalised to io.ErrShortWrite (Go's os.File.Write
// sets it itself, but a raw writev(2) does not), so the caller's
// truncate-and-repair path is taken either way.
func writevFile(f *os.File, parts [][]byte) (int, error) {
	total := 0
	for _, p := range parts {
		total += len(p)
	}
	if total == 0 {
		return 0, nil
	}

	fd := int(f.Fd())
	written := 0
	for start := 0; start < len(parts); {
		// The chunk holds at most walWritevMaxIOV pieces (the kernel
		// rejects a larger iovcnt with EINVAL).
		end := start + walWritevMaxIOV
		if end > len(parts) {
			end = len(parts)
		}
		n, err := unix.Writev(fd, parts[start:end])
		written += n
		if err != nil {
			return written, err
		}
		if n != chunkLen(parts[start:end]) {
			return written, io.ErrShortWrite
		}
		start = end
	}
	return written, nil
}

// chunkLen returns the total number of bytes in the given pieces.
func chunkLen(parts [][]byte) int {
	n := 0
	for _, p := range parts {
		n += len(p)
	}
	return n
}
