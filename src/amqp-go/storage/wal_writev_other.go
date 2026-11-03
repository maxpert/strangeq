//go:build !unix

package storage

import (
	"io"
	"os"
)

// writevFile writes the concatenation of parts to f.
//
// This is the non-Unix fallback (Windows): there is no writev(2), so the
// pieces are written one after another. The bytes that land in the file are
// byte-for-byte what a single-buffer write of the concatenation would have
// produced, and the message body is still passed BY REFERENCE — it is never
// copied into a staging buffer. The only cost is one syscall per piece.
func writevFile(f *os.File, parts [][]byte) (int, error) {
	written := 0
	for _, p := range parts {
		if len(p) == 0 {
			continue
		}
		n, err := f.Write(p)
		written += n
		if err != nil {
			return written, err
		}
		if n != len(p) {
			return written, io.ErrShortWrite
		}
	}
	return written, nil
}
