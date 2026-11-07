//go:build darwin

package storage

import (
	"errors"
	"os"
	"sync/atomic"
	"syscall"

	"golang.org/x/sys/unix"

	"github.com/maxpert/amqp-go/interfaces"
)

// fdatasyncFile makes f's data durable on macOS by issuing fcntl(F_FULLFSYNC) —
// the same barrier os.File.Sync issues — through golang.org/x/sys/unix.
//
// DO NOT "SIMPLIFY" THIS BACK TO f.Sync(). It looks equivalent and is not.
// os.File.Sync on darwin reaches F_FULLFSYNC through
// internal/poll.(*FD).Fsync -> internal/syscall/unix.Fcntl -> runtime.fcntl,
// which is a bare libcCall with NO entersyscall. That is the observable,
// verifiable fact on THIS Go toolchain: the built storage.test binary's
// internal/syscall/unix.Fcntl disassembles to a CALL runtime.libcCall with no
// entersyscall anywhere on the path (contrast x/sys/unix.FcntlInt ->
// syscall.syscall -> runtime.entersyscall), and the stored STW trace confirms
// the consequence. A goroutine inside runtime.fcntl therefore stays _Grunning
// and KEEPS ITS P for the whole flush — 5–20 ms on an APFS SSD, seconds when
// the device stalls — it cannot be preempted (the SIGURG lands in libc, not at
// a Go safe point), and stopTheWorldWithSema / gcMarkDone block on that P.
// Measured on the F3 fsync benchmark cell: 22.5 % of wall time
// stopped-the-world, with 149 of 151 long STWs ending within 30 µs of the
// committer's F_FULLFSYNC returning.
//
// VERSION HISTORY (context only, NOT load-bearing): this linkname has been in
// place at least since Go 1.21. Whether Go 1.20's syscall.fcntl entered the
// syscall cannot be verified from this checkout, and nothing below depends on
// it — the fix rests only on the current toolchain's observed behaviour above.
//
// x/sys/unix.FcntlInt goes through syscall.syscall, which calls entersyscall:
// the P is handed off, sysmon can retake it, and STW proceeds while the flush
// runs. The fcntl itself — and therefore the durability barrier — is identical.
//
// EINTR is retried and ENOTSUP falls back to fsync(2), mirroring
// internal/poll.(*FD).Fsync on darwin (see Go issue #64215).
func fdatasyncFile(f *os.File) error {
	rc, err := f.SyscallConn()
	if err != nil {
		return err
	}
	var syncErr error
	cerr := rc.Control(func(fd uintptr) {
		syncErr = fullFsyncFD(fd)
	})
	if cerr != nil {
		return cerr
	}
	return syncErr
}

// fsyncFcntl issues fcntl(fd, cmd) — the raw ordered-sync command behind both
// the confirm fence (F_BARRIERFSYNC) and the full flush (F_FULLFSYNC). It goes
// through the walSyncCmdHook seam when one is installed (TEST-ONLY: the
// site-pin tests record WHICH command each durability site issues), and through
// x/sys/unix otherwise. The entersyscall/preemptibility rationale on
// fdatasyncFile above applies identically to both commands.
func fsyncFcntl(fd uintptr, cmd int) error {
	if p := walSyncCmdHook.Load(); p != nil {
		return (*p)(fd, cmd)
	}
	_, err := unix.FcntlInt(fd, cmd, 0)
	return err
}

// fullFsyncFD issues F_FULLFSYNC on fd, retrying EINTR and falling back to
// fsync(2) on ENOTSUP exactly like the standard library's darwin Fsync.
func fullFsyncFD(fd uintptr) error {
	for {
		err := fsyncFcntl(fd, unix.F_FULLFSYNC)
		switch {
		case err == nil:
			return nil
		case errors.Is(err, syscall.EINTR):
			continue
		case errors.Is(err, syscall.ENOTSUP):
			for {
				if ferr := unix.Fsync(int(fd)); !errors.Is(ferr, syscall.EINTR) {
					return ferr
				}
			}
		default:
			return err
		}
	}
}

// barrierSyncFile is the crash_model: barrier confirm primitive on macOS:
// fcntl(F_BARRIERFSYNC), issued through the same preemptible x/sys path as
// fdatasyncFile. It makes f's dirty pages and metadata durable at the drive
// (the fsync part) and then issues an ORDERING BARRIER instead of a drive
// write-cache flush. A process kill and an OS crash therefore cannot lose
// barrier-confirmed data; a sudden power loss can lose confirmed data still in
// the drive's volatile cache, so what survives is a prefix of the log in
// barrier order on drives that honour barriers. Only the two confirm sites use
// this; every other durability site keeps the full F_FULLFSYNC (fdatasyncFile).
func barrierSyncFile(f *os.File) error {
	rc, err := f.SyscallConn()
	if err != nil {
		return err
	}
	var syncErr error
	cerr := rc.Control(func(fd uintptr) {
		syncErr = barrierFsyncFD(fd)
	})
	if cerr != nil {
		return cerr
	}
	return syncErr
}

// barrierFsyncFD issues F_BARRIERFSYNC on fd, retrying EINTR. On EINVAL,
// ENOSYS or ENOTSUP — the drive/kernel does not implement the barrier — it
// FALLS BACK TO F_FULLFSYNC (fullFsyncFD), NEVER to plain fsync: plain fsync
// keeps the data but drops the ordering barrier, so it is weaker than the
// documented contract. F_FULLFSYNC is strictly stronger than the requested
// primitive, so the fallback is always safe. One WARN is emitted (if a logger
// was ever wired) so a device silently dropping to the slower primitive is
// visible.
func barrierFsyncFD(fd uintptr) error {
	for {
		err := fsyncFcntl(fd, unix.F_BARRIERFSYNC)
		switch {
		case err == nil:
			return nil
		case errors.Is(err, syscall.EINTR):
			continue
		case errors.Is(err, syscall.EINVAL),
			errors.Is(err, syscall.ENOSYS),
			errors.Is(err, syscall.ENOTSUP),
			errors.Is(err, syscall.EOPNOTSUPP):
			if barrierFallbackWarned.CompareAndSwap(false, true) {
				if l := storageWarnLogger.Load(); l != nil {
					l.Warn("fcntl(F_BARRIERFSYNC) is not supported on this filesystem or device; "+
						"the WAL confirm barrier falls back to F_FULLFSYNC (a full drive-cache flush, "+
						"which is stronger, not weaker)",
						interfaces.LogField{Key: "err", Value: err.Error()})
				}
			}
			return fullFsyncFD(fd)
		default:
			return err
		}
	}
}

// barrierFallbackWarned guards the one-time WARN above.
var barrierFallbackWarned atomic.Bool
