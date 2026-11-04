//go:build darwin

package storage

import (
	"errors"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
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

// fullFsyncFD issues F_FULLFSYNC on fd, retrying EINTR and falling back to
// fsync(2) on ENOTSUP exactly like the standard library's darwin Fsync.
func fullFsyncFD(fd uintptr) error {
	for {
		_, err := unix.FcntlInt(fd, unix.F_FULLFSYNC, 0)
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
