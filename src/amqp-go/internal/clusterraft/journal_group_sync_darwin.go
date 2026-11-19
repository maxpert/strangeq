package clusterraft

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

func syncJournalFile(file *os.File) error {
	conn, err := file.SyscallConn()
	if err != nil {
		return err
	}
	var syncErr error
	err = conn.Control(func(fd uintptr) {
		// Keep the full-flush durability of os.File.Sync while allowing the
		// scheduler to release its processor during a slow storage fence.
		for {
			_, syncErr = unix.FcntlInt(fd, unix.F_FULLFSYNC, 0)
			if !errors.Is(syncErr, unix.EINTR) {
				break
			}
		}
		if errors.Is(syncErr, unix.ENOTSUP) {
			for {
				syncErr = unix.Fsync(int(fd))
				if !errors.Is(syncErr, unix.EINTR) {
					break
				}
			}
		}
	})
	if err != nil {
		return err
	}
	return syncErr
}
