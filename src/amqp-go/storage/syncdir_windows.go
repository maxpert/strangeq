//go:build windows

package storage

// syncDir is a no-op on Windows. Windows does not permit fsyncing a directory
// handle (CreateFile on a directory + FlushFileBuffers fails), and NTFS rename
// durability semantics differ from POSIX, so there is no portable directory
// fsync to perform here.
func syncDir(dir string) error {
	return nil
}
