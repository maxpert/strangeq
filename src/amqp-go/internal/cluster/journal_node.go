package cluster

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/maxpert/amqp-go/internal/clusterraft"
)

const (
	boltIdentityFile             = "identity.json"
	journalIdentityFile          = "journal-identity-v1.json"
	smallBodyJournalIdentityFile = "journal-small-body-identity-v1.json"
	clusterStorageModeFile       = "raft-storage-mode-v1"
	clusterProvisionTemp         = ".cluster-provision-"
	journalDirectory             = "raft-journal"
	journalProvisionedFile       = "journal-provisioned-v1"
)

type nodeJournalOwner interface {
	OpenGroup(clusterraft.Config) (*clusterraft.Group, error)
	GroupIDs() []uint64
	HasGroup(uint64) bool
	Failure() error
	Close() error
}

func (n *Node) storageIdentity() (name, mode string, err error) {
	name, mode = boltIdentityFile, "bolt"
	if n.cfg.ExperimentalRaftJournal {
		name, mode = journalIdentityFile, "journal"
	}
	if n.cfg.ExperimentalAckAndClaim {
		mode = "journal-ack-and-claim"
	}
	if n.cfg.ExperimentalAssignments {
		mode = "journal-assignments"
	}
	if n.cfg.ExperimentalJournalBodyCompression {
		mode = "journal-assignments-compressed-bodies"
	}
	if n.cfg.ExperimentalJournalSmallBodyCompression {
		name, mode = smallBodyJournalIdentityFile, "journal-assignments-small-compressed-bodies"
	}
	if n.cfg.ExperimentalRaftJournalDualStream {
		mode += "-two-stream-parity-v1"
	}
	if n.cfg.ExperimentalRaftJournalDualStream || n.cfg.ExperimentalJournalSmallBodyCompression {
		// Require the published mode for directories with a newer journal
		// identity or a two-stream layout.
		if exists, err := pathExists(n.path(name)); err != nil {
			return "", "", err
		} else if exists {
			if _, err := os.ReadFile(n.path(clusterStorageModeFile)); err != nil {
				return "", "", fmt.Errorf("journal lost its persistent storage mode: %w", err)
			}
		}
	}
	for _, other := range [...]string{boltIdentityFile, journalIdentityFile, smallBodyJournalIdentityFile} {
		if other == name {
			continue
		}
		if exists, err := pathExists(n.path(other)); err != nil {
			return "", "", err
		} else if exists {
			return "", "", fmt.Errorf("cluster storage mode %s conflicts with persistent %s", mode, other)
		}
	}
	if err := n.checkStorageModeClaim(mode); err != nil {
		return "", "", err
	}
	return name, mode, nil
}

func (n *Node) checkStorageModeClaim(mode string) error {
	prior, err := os.ReadFile(n.path(clusterStorageModeFile))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if string(prior) != mode {
		return fmt.Errorf("cluster storage mode %s conflicts with persistent mode %q", mode, prior)
	}
	return nil
}

func (n *Node) claimStorageMode(mode string) error {
	path := n.path(clusterStorageModeFile)
	if err := provisionFile(path, []byte(mode)); !errors.Is(err, os.ErrExist) {
		return err
	}
	// Two initializers may both find the directory empty. Only the first
	// link publishes the mode. A matching retry may finish provisioning it.
	if err := n.checkStorageModeClaim(mode); err != nil {
		return err
	}
	return syncStorageDirectory(n.cfg.Directory)
}

func freshStorageDirectory(entries []os.DirEntry) bool {
	for _, entry := range entries {
		if !entry.Type().IsRegular() || (entry.Name() != clusterStorageModeFile &&
			!strings.HasPrefix(entry.Name(), clusterProvisionTemp)) {
			return false
		}
	}
	return true
}

func (n *Node) prepareJournal() error {
	// Mode claims are the primary fence. Also reject incompatible authority
	// directories before opening storage if metadata was restored separately.
	incompatible := []string{"raft-journal-0", "raft-journal-1"}
	if n.cfg.ExperimentalRaftJournalDualStream {
		incompatible = []string{journalDirectory}
	}
	for _, name := range incompatible {
		if exists, err := pathExists(n.path(name)); err != nil {
			return err
		} else if exists {
			return fmt.Errorf("cluster storage mode conflicts with persistent %s", name)
		}
	}
	marker := n.path(journalProvisionedFile)
	raw, err := os.ReadFile(marker)
	provisioned := err == nil
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if provisioned && !bytes.Equal(raw, []byte{1}) {
		return errors.New("invalid persistent raft journal provision marker")
	}
	established := provisioned
	entries, err := os.ReadDir(n.cfg.Directory)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		name := entry.Name()
		if name == "initialized" || strings.HasPrefix(name, "opened-") || strings.HasPrefix(name, "retired-") {
			established = true
		}
		// A legacy per-group Bolt file is an incompatible authority even
		// if its old identity file was removed independently.
		if _, parseErr := strconv.ParseUint(name, 10, 64); parseErr == nil && entry.IsDir() {
			exists, err := clusterraft.Exists(n.path(name))
			if err != nil {
				return err
			}
			if exists {
				return fmt.Errorf("cluster storage mode journal conflicts with persistent Bolt group %s", name)
			}
		}
	}
	openJournal := clusterraft.OpenNodeJournal
	if n.cfg.ExperimentalRaftRangeSync {
		openJournal = clusterraft.OpenNodeJournalWithRangeSync
	}
	var owner nodeJournalOwner
	if n.cfg.ExperimentalRaftJournalDualStream {
		owner, err = clusterraft.OpenNodeJournalPairWithRangeSync(n.cfg.Directory, !established, n.cfg.Peer.ID)
	} else {
		owner, err = openJournal(n.path(journalDirectory), !established)
	}
	if err != nil {
		if established && errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("node lost its established persistent raft journal: %w", err)
		}
		return err
	}
	n.journal = owner
	if !provisioned {
		return durableFile(marker, []byte{1})
	}
	return nil
}

func (n *Node) storeExists(id uint64) (bool, error) {
	if n.journal != nil {
		return n.journal.HasGroup(id), nil
	}
	return clusterraft.Exists(n.store(id))
}
