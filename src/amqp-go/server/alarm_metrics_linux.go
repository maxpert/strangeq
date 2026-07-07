//go:build linux

package server

import (
	"bufio"
	"os"
	"strconv"
	"strings"
)

// readProcessRSSBytes returns the resident set size of the current process by
// reading /proc/self/statm (field 2 = resident pages) and multiplying by the
// page size. This is the *process* RSS — it includes off-heap allocations (WAL
// mmap, cgo, allocator fragmentation) that Go's runtime.MemStats.HeapInuse
// misses, which is why SQ-12 measures RSS rather than the Go heap.
func readProcessRSSBytes() (uint64, bool) {
	data, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0, false
	}
	fields := strings.Fields(string(data))
	if len(fields) < 2 {
		return 0, false
	}
	residentPages, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return 0, false
	}
	return residentPages * uint64(os.Getpagesize()), true
}

// readTotalMemoryBytes returns the memory ceiling the watermark is a fraction
// of: the cgroup memory limit when it is set below machine RAM (the container
// case, matching RabbitMQ's cgroup awareness), otherwise machine RAM from
// /proc/meminfo.
func readTotalMemoryBytes() (uint64, bool) {
	machine, machineOK := readMachineMemTotalBytes()
	limit, limitOK := readCgroupMemoryLimitBytes()

	switch {
	case limitOK && machineOK:
		if limit < machine {
			return limit, true
		}
		return machine, true
	case limitOK:
		return limit, true
	case machineOK:
		return machine, true
	default:
		return 0, false
	}
}

func readMachineMemTotalBytes() (uint64, bool) {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0, false
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "MemTotal:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) >= 2 {
			// MemTotal is reported in kB.
			if kb, err := strconv.ParseUint(fields[1], 10, 64); err == nil {
				return kb * 1024, true
			}
		}
	}
	return 0, false
}

// readCgroupMemoryLimitBytes reads the cgroup memory limit, preferring cgroup v2
// (memory.max) then falling back to v1 (memory.limit_in_bytes). An unset limit
// ("max" or the kernel's huge sentinel) reports "no limit".
func readCgroupMemoryLimitBytes() (uint64, bool) {
	// cgroup v2
	if data, err := os.ReadFile("/sys/fs/cgroup/memory.max"); err == nil {
		s := strings.TrimSpace(string(data))
		if s == "max" {
			return 0, false
		}
		if v, err := strconv.ParseUint(s, 10, 64); err == nil && v > 0 && v < cgroupNoLimitSentinel {
			return v, true
		}
		return 0, false
	}
	// cgroup v1
	if data, err := os.ReadFile("/sys/fs/cgroup/memory/memory.limit_in_bytes"); err == nil {
		s := strings.TrimSpace(string(data))
		if v, err := strconv.ParseUint(s, 10, 64); err == nil && v > 0 && v < cgroupNoLimitSentinel {
			return v, true
		}
	}
	return 0, false
}
