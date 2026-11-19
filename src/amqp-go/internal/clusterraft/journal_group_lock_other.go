//go:build !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd

package clusterraft

import (
	"errors"
	"os"
)

func lockGroupJournal(*os.File) error {
	return errors.New("the experimental raft journal requires a supported file locking platform")
}
