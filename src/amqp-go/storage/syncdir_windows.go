//go:build windows

package storage

// syncDirPlatform is a no-op on Windows. Windows does not permit fsyncing a
// directory handle (CreateFile on a directory + FlushFileBuffers fails), and
// NTFS rename durability semantics differ from POSIX, so there is no portable
// directory fsync to perform here.
func syncDirPlatform(dir string) error {
	return nil
}

// syncDirPlatformFence mirrors syncDirPlatform: a no-op on Windows, which
// cannot fsync a directory handle. The fence is never selected here.
func syncDirPlatformFence(dir string) error {
	return nil
}
