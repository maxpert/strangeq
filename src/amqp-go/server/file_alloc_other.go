//go:build !unix

package server

import "os"

// allocatedFileSize is the non-Unix fallback: there is no st_blocks in the
// Windows file info, so the apparent size is reported. Sparse allocation is
// also a Unix-only optimisation, so the two agree on the default path.
func allocatedFileSize(info os.FileInfo) int64 {
	return info.Size()
}
