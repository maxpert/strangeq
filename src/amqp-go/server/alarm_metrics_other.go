//go:build !linux

package server

// On non-Linux platforms (notably the darwin dev/test host) there is no clean,
// dependency-free way to read process RSS or a memory ceiling, so the SQ-12
// memory arm degrades gracefully: buildAlarmThresholds sees no detectable total
// memory, leaves the memory arm disarmed, and the monitor never trips a memory
// alarm. The disk arm (syscall.Statfs, see readDiskFreeBytes) works everywhere.
// Production runs on Linux, where readProcessRSSBytes reads /proc/self/statm.
// Tests inject deterministic samplers directly, so they are platform-independent.
func readProcessRSSBytes() (uint64, bool) { return 0, false }

func readTotalMemoryBytes() (uint64, bool) { return 0, false }
