//go:build !windows

package storage

import (
	"errors"
	"os"
	"syscall"
)

// syncDirPlatform opens the given directory and fsyncs it. On
// Linux/macOS/BSD a directory fsync is required after a create, unlink or
// rename so that the directory entry change is itself durable across a crash
// or power loss — the file's data fsync alone does not guarantee the entry
// survived.
//
// ERRNO TOLERANCE (WS-C C4): EINVAL, EBADF and ENOTSUP/-EOPNOTSUPP from the
// directory Sync are reported as errDirSyncUnsupported (mapped to nil by the
// syncDir wrapper) rather than as failures. Some filesystems reject fsync on
// directory handles outright; treating that as fatal would block every WAL
// roll forever on them. This mirrors PostgreSQL's fsync_fname_ext.
//
// The actual Sync is behind an unexported func var so the errno mapping is
// unit-testable without a filesystem that really returns these errnos.
//
// This mirrors the build-tag split used by fdatasyncFile: the POSIX path
// here, a no-op on Windows where directory handles cannot be fsynced.
var syncDirPlatformSync = func(f *os.File) error { return f.Sync() }

func syncDirPlatform(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	serr := syncDirPlatformSync(d)
	cerr := d.Close()
	if serr != nil {
		if isUnsupportedDirSyncError(serr) {
			return errDirSyncUnsupported
		}
		return serr
	}
	return cerr
}

// isUnsupportedDirSyncError reports whether err is the errno class that means
// "this filesystem cannot fsync directory handles".
func isUnsupportedDirSyncError(err error) bool {
	return errors.Is(err, syscall.EINVAL) ||
		errors.Is(err, syscall.EBADF) ||
		errors.Is(err, syscall.ENOTSUP) ||
		errors.Is(err, syscall.EOPNOTSUPP)
}
