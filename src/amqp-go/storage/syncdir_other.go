//go:build !windows

package storage

import "os"

// syncDir opens the given directory and fsyncs it. On Linux/macOS/BSD a
// directory fsync is required after a rename so that the directory entry
// change (the rename) is itself durable across a crash or power loss — the
// file's data fsync alone does not guarantee the rename survived.
//
// This mirrors the build-tag split used by fdatasyncFile: the POSIX path here,
// a no-op on Windows where directory handles cannot be fsynced.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		_ = d.Close()
		return err
	}
	return d.Close()
}
