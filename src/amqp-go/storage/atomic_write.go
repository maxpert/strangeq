package storage

import (
	"fmt"
	"os"
	"path/filepath"
)

// atomicWriteFile writes data to path so that any reader — this boot or the
// next one — sees either the previous contents or the complete new contents,
// and never a partial one.
//
// WHY THIS IS NOT os.WriteFile + syncDir. os.WriteFile O_CREATEs the target and
// then writes into it, so a failure part-way through (ENOSPC, EFBIG, EIO) and a
// crash between the write and the flush BOTH leave a short or zero-length file
// at the real path, with nothing left to distinguish it from a complete one.
// syncDir does not help: it makes the DIRECTORY ENTRY durable, which is exactly
// the half that was already fine, and never the file's contents. That shape is
// how review-5 CRITICAL-1 turned a full disk into a queue reading its own
// identity as the empty string on the next boot.
//
// Writing to a temp file, fsyncing THAT, and only then renaming means a
// half-written file never wears the real name: rename is atomic, so the target
// either does not exist yet or is complete. The final syncDir makes the rename
// itself survive power loss.
//
// SCOPE OF THIS CONSOLIDATION, stated because the first version of this comment
// overclaimed it as "the one implementation" and that was false (review-7
// MINOR-5). It covers exactly two callers: PersistentMetadataStore.atomicWrite,
// which delegates here rather than keeping a second copy, and the segment
// directory's ownership marker. review-5 CRITICAL-1's point was precisely that
// the metadata tier had this right while the segment tier, which holds the
// actual messages, did not.
//
// A third temp+rename with no f.Sync() and no syncDir used to exist in this
// package, carrying the same durability shape CRITICAL-1 was raised for. It was
// never reachable: nothing populated the state it checkpointed, so it could not
// write a file even in principle. Step 7 deleted that tier outright rather than
// fixing or folding it in here — the property to remember is that this helper
// has exactly the two callers named above, and a third appearing is a signal to
// delegate rather than to copy.
//
// It is a declare/boot-path helper, never the message hot path: there is
// exactly one file fsync and one directory fsync per call, no more than
// correctness requires.
func atomicWriteFile(path string, data []byte, perm os.FileMode) error {
	return atomicWriteFileWithPolicy(path, data, perm, newSyncPolicy(walConfirmBarrier).withSite(siteMetadataFile))
}

// atomicWriteFileWithPolicy is atomicWriteFile with an explicit crash-model
// sync policy: under barrier mode the temp-file fsync and the directory sync
// use the fence (this is a declare/topology path that CAN fire while the broker
// is serving traffic), under force_flush the full drive-cache flush. The atomic
// temp+rename shape is unchanged.
func atomicWriteFileWithPolicy(path string, data []byte, perm os.FileMode, pol syncPolicy) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// Any error below removes the temp file, so a failure never leaves a .tmp
	// behind and is never swallowed.
	tempPath := path + TempFileExtension
	f, err := os.OpenFile(tempPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, perm)
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(tempPath)
		return fmt.Errorf("failed to write temp file: %w", err)
	}
	if err := pol.file(f); err != nil {
		_ = f.Close()
		_ = os.Remove(tempPath)
		return fmt.Errorf("failed to fsync temp file: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tempPath)
		return fmt.Errorf("failed to close temp file: %w", err)
	}

	if err := os.Rename(tempPath, path); err != nil {
		_ = os.Remove(tempPath)
		return fmt.Errorf("failed to rename temp file: %w", err)
	}

	if err := pol.dir(dir); err != nil {
		return fmt.Errorf("failed to fsync directory: %w", err)
	}

	return nil
}
