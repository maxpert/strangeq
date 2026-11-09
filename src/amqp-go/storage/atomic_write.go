package storage

import (
	"fmt"
	"os"
	"path/filepath"
)

// atomicWriteFileWithPolicy writes data to path atomically with an explicit
// crash-model sync policy: under barrier mode the temp-file fsync and the
// directory sync use the fence (this is a declare/topology path that CAN fire
// while the broker is serving traffic), under force_flush the full drive-cache
// flush. The atomic
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
