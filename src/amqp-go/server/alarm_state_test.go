package server

import (
	"testing"
	"unsafe"
)

// TestAlarmState_ZeroByDefault verifies a fresh Server reports no active alarm.
func TestAlarmState_ZeroByDefault(t *testing.T) {
	var s Server
	if got := s.AlarmState(); got != 0 {
		t.Fatalf("AlarmState() = %d, want 0", got)
	}
}

// TestAlarmState_Bits verifies the bit layout (bit0=memory, bit1=disk) that
// SQ-12 drives and the publish gate reads.
func TestAlarmState_Bits(t *testing.T) {
	var s Server

	s.alarmState.Store(AlarmMemory)
	if s.AlarmState()&AlarmMemory == 0 {
		t.Errorf("memory bit not observed via AlarmState()")
	}
	if s.AlarmState()&AlarmDisk != 0 {
		t.Errorf("disk bit set unexpectedly")
	}

	s.alarmState.Store(AlarmMemory | AlarmDisk)
	if s.AlarmState() != AlarmMemory|AlarmDisk {
		t.Errorf("AlarmState() = %d, want %d", s.AlarmState(), AlarmMemory|AlarmDisk)
	}

	s.alarmState.Store(0)
	if s.AlarmState() != 0 {
		t.Errorf("AlarmState() = %d, want 0 after clear", s.AlarmState())
	}
}

// TestAlarmState_PaddedFromHotCounters proves alarmState sits at least one cache
// line (64 bytes) beyond the last hot write counter (bytesSent) so the hot
// per-publish load of alarmState does not false-share with the per-message
// counter writes.
func TestAlarmState_PaddedFromHotCounters(t *testing.T) {
	var s Server
	const cacheLine = 64
	alarmOff := unsafe.Offsetof(s.alarmState)
	bytesSentOff := unsafe.Offsetof(s.bytesSent)
	if alarmOff < bytesSentOff+cacheLine {
		t.Fatalf("alarmState offset %d is within a cache line of bytesSent offset %d (need >= %d apart)",
			alarmOff, bytesSentOff, cacheLine)
	}
}
