//go:build !darwin && !windows

package storage

import "os"

// syncDirPlatformSync is the directory fsync on the platforms that are neither
// macOS nor Windows: on Linux and the BSDs os.File.Sync is a plain fsync(2) on
// the directory handle, which enters the syscall and therefore does not park the
// P the way darwin's F_FULLFSYNC-through-runtime.fcntl does. (fdatasyncFile is
// NOT usable here: fdatasync(2) is not valid on a directory handle.)
//
// macOS overrides this in syncdir_darwin.go with fdatasyncFile. The variable
// must be defined by exactly one platform file per GOOS — it is the seam that
// syncDirPlatform and SetSyncDirForTest use.
var syncDirPlatformSync = func(f *os.File) error { return f.Sync() }
