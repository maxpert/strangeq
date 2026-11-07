//go:build unix

package server

import (
	"os"
	"syscall"
)

// allocatedFileSize returns the number of bytes a file actually occupies on
// disk: st_blocks * 512, the ALLOCATED size, not the apparent (i_size) size.
//
// A sparse ftruncate (or a barrier tail the drive has not materialised) makes a
// WAL file's apparent size larger than what it stores, so summing info.Size()
// over the WAL directory over-reports wal_size. st_blocks counts only what has
// actually been written.
func allocatedFileSize(info os.FileInfo) int64 {
	if st, ok := info.Sys().(*syscall.Stat_t); ok {
		return int64(st.Blocks) * 512
	}
	return info.Size()
}
