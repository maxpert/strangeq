package storage

import (
	"errors"
	"sync/atomic"

	"github.com/maxpert/amqp-go/interfaces"
)

// syncDir makes a directory entry change (create, unlink, rename) durable.
//
// A directory entry must be durable before anything relies on that file for
// durability: for a WAL file the reliance point is the first confirm of a
// record in it, for a segment file it is the first CheckpointBatch == nil
// that leads to a WAL unlink. Syncing the directory WHEN THE FILE IS CREATED
// comes before both, so that is where every caller syncs (WS-C C4); the
// post-unlink sync here limits replay work and frees space, it is not a
// correctness requirement.
//
// The wrapper is the single hookable seam (SetSyncDirForTest) and where the
// "filesystem cannot fsync directories" errno class is mapped to success: a
// filesystem that rejects directory fsync must not block every WAL roll
// forever, so syncDirPlatform reports it as errDirSyncUnsupported and this
// wrapper maps that to nil after logging one WARN (if a logger was ever
// wired; storage without a logger stays silent).
func syncDir(dir string) error {
	if p := syncDirHook.Load(); p != nil {
		return (*p)(dir)
	}
	err := syncDirPlatform(dir)
	if errors.Is(err, errDirSyncUnsupported) {
		if syncDirUnsupportedWarned.CompareAndSwap(false, true) {
			if l := storageWarnLogger.Load(); l != nil {
				l.Warn("the filesystem does not support fsync on directory handles; "+
					"directory-entry changes (WAL/segment file creation, WAL unlink) are made durable "+
					"only by the filesystem's own ordering, not by an explicit barrier",
					interfaces.LogField{Key: "dir", Value: dir})
			}
		}
		return nil
	}
	return err
}

// errDirSyncUnsupported is the internal sentinel for the errno class
// (EINVAL/EBADF/ENOTSUP from a directory Sync) that means "this filesystem
// cannot fsync directories", not "the sync failed".
var errDirSyncUnsupported = errors.New("directory fsync unsupported by filesystem")

// syncDirUnsupportedWarned guards the one-time WARN above.
var syncDirUnsupportedWarned atomic.Bool

// atomicLogger is an atomically-swappable interfaces.Logger holder: Store is
// called once, after a manager's goroutines have started; Load is called only
// on cold failure paths. A nil (or never-stored) logger means "silent".
type atomicLogger struct {
	p atomic.Pointer[interfaces.Logger]
}

func (a *atomicLogger) Store(l interfaces.Logger) { a.p.Store(&l) }

func (a *atomicLogger) Load() interfaces.Logger {
	if p := a.p.Load(); p != nil {
		return *p
	}
	return nil
}

// storageWarnLogger is the package-level sink for the one-time
// directory-fsync warning. Wired (best effort) by the WAL and segment
// managers' SetLogger; nil = silent, which is the state of any storage
// constructed without a logger.
var storageWarnLogger atomicLogger
