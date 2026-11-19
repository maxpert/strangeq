//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package clusterraft

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

func lockGroupJournal(file *os.File) error {
	if err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return fmt.Errorf("lock node raft journal: %w", err)
	}
	return nil
}
