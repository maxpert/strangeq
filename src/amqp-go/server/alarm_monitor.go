package server

import (
	"context"
	"syscall"
	"time"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/protocol"
	"go.uber.org/zap"
)

const (
	// defaultAlarmParkProbe bounds how long a paused reader parks before it
	// breaks out to probe socket liveness (SQ-12). It also caps the rate at
	// which a blocked-but-ignoring publisher could sneak a frame through the
	// reader-pause, so keep it modest.
	defaultAlarmParkProbe = 5 * time.Second

	// alarmControlWriteTimeout bounds the socket write of a connection.blocked /
	// connection.unblocked control frame so a wedged (non-reading) client can
	// never stall the monitor while it broadcasts.
	alarmControlWriteTimeout = 5 * time.Second

	// cgroupNoLimitSentinel: cgroup memory limits at/above this are treated as
	// "no limit" (kernels report a huge page-aligned sentinel for unlimited).
	cgroupNoLimitSentinel = uint64(1) << 62
)

// alarmThresholds holds the resolved absolute resource-alarm thresholds and the
// (injectable) samplers the monitor evaluates each tick. Built once from config
// via buildAlarmThresholds; the samplers default to the real /proc + Statfs
// readers, but tests substitute deterministic fakes to drive the hysteresis
// state machine and the reader-pause races without touching the host.
type alarmThresholds struct {
	interval time.Duration

	// Memory arm: process RSS >= memSetBytes trips the alarm; RSS <= memClearBytes
	// clears it (hysteresis). memSetBytes == 0 disables the memory arm.
	memSetBytes   uint64
	memClearBytes uint64

	// Disk arm: free < diskFloorBytes trips the alarm; free >= diskClearBytes
	// clears it (hysteresis). diskFloorBytes == 0 disables the disk arm.
	diskFloorBytes uint64
	diskClearBytes uint64

	sampleMemory   func() (rss uint64, ok bool)
	sampleDiskFree func() (free uint64, ok bool)
}

// buildAlarmThresholds resolves the resource-alarm configuration into absolute
// thresholds plus the production samplers, or returns nil when alarms are
// disabled OR both arms resolve to a zero threshold. A nil result means the
// monitor is never spawned and the broker does zero RSS/Statfs sampling — the
// unset zero-cost contract.
func buildAlarmThresholds(cfg *config.AMQPConfig) *alarmThresholds {
	ra := cfg.ResourceAlarms
	if !ra.Enabled {
		return nil
	}

	hysteresis := ra.Hysteresis
	if hysteresis < 0 || hysteresis >= 1 {
		hysteresis = 0
	}
	interval := time.Duration(ra.CheckIntervalMS) * time.Millisecond
	if interval <= 0 {
		interval = 2 * time.Second
	}

	t := &alarmThresholds{
		interval:       interval,
		sampleMemory:   readProcessRSSBytes,
		sampleDiskFree: func() (uint64, bool) { return 0, false },
	}

	// Memory arm: set at watermark*total, clear at watermark*(1-hysteresis)*total.
	// The ceiling is the cgroup limit (container) or machine RAM. If neither can
	// be detected (e.g. non-Linux hosts), the memory arm stays disabled.
	if ra.MemoryHighWatermark > 0 {
		if total, ok := readTotalMemoryBytes(); ok && total > 0 {
			t.memSetBytes = uint64(ra.MemoryHighWatermark * float64(total))
			t.memClearBytes = uint64(ra.MemoryHighWatermark * (1 - hysteresis) * float64(total))
		}
	}

	// Disk arm: trip below the floor, clear once free rises to floor*(1+hysteresis).
	if ra.DiskFreeLimitBytes > 0 {
		dataDir := cfg.Storage.Path
		if dataDir == "" {
			dataDir = "./data"
		}
		t.diskFloorBytes = uint64(ra.DiskFreeLimitBytes)
		t.diskClearBytes = uint64(float64(ra.DiskFreeLimitBytes) * (1 + hysteresis))
		t.sampleDiskFree = func() (uint64, bool) { return readDiskFreeBytes(dataDir) }
	}

	if t.memSetBytes == 0 && t.diskFloorBytes == 0 {
		return nil // nothing armed — stay dormant
	}
	return t
}

// evaluate computes the next alarm bitmask from one sample, applying per-arm
// hysteresis relative to the current bits. Pure and side-effect-free so the
// state machine is directly table-testable.
func (t *alarmThresholds) evaluate(cur uint32, rss uint64, rssOK bool, free uint64, freeOK bool) uint32 {
	next := cur

	if t.memSetBytes > 0 && rssOK {
		if cur&AlarmMemory != 0 {
			if rss <= t.memClearBytes {
				next &^= AlarmMemory
			}
		} else if rss >= t.memSetBytes {
			next |= AlarmMemory
		}
	}

	if t.diskFloorBytes > 0 && freeOK {
		if cur&AlarmDisk != 0 {
			if free >= t.diskClearBytes {
				next &^= AlarmDisk
			}
		} else if free < t.diskFloorBytes {
			next |= AlarmDisk
		}
	}

	return next
}

// startAlarmMonitor spawns the resource-alarm monitor goroutine iff alarms are
// enabled (s.alarm != nil). Returns true when it started one. When disabled it
// is a pure no-op: no goroutine, no memory/disk sampling (the unset zero-cost
// contract).
func (s *Server) startAlarmMonitor() bool {
	if s.alarm == nil {
		return false
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.Mutex.Lock()
	s.alarmCancel = cancel
	s.Mutex.Unlock()
	go s.runAlarmMonitor(ctx)
	return true
}

// runAlarmMonitor samples resources on a dedicated fast ticker (distinct from
// the 60s system-metrics ticker) and drives the global alarm bitmask.
func (s *Server) runAlarmMonitor(ctx context.Context) {
	ticker := time.NewTicker(s.alarm.interval)
	defer ticker.Stop()

	s.sampleAlarmsOnce() // evaluate immediately so an already-full disk blocks at once
	for {
		select {
		case <-ticker.C:
			s.sampleAlarmsOnce()
		case <-ctx.Done():
			return
		}
	}
}

// sampleAlarmsOnce samples only the armed resources, evaluates the hysteresis
// state machine, and applies the result. Disarmed resources are never sampled.
func (s *Server) sampleAlarmsOnce() {
	t := s.alarm
	if t == nil {
		return
	}

	var (
		rss, free     uint64
		rssOK, freeOK bool
	)
	if t.memSetBytes > 0 {
		rss, rssOK = t.sampleMemory()
	}
	if t.diskFloorBytes > 0 {
		free, freeOK = t.sampleDiskFree()
	}

	cur := s.alarmState.Load()
	s.applyAlarmBits(t.evaluate(cur, rss, rssOK, free, freeOK))
}

// applyAlarmBits transitions the global alarm bitmask and drives edge-triggered
// emission. Ordering is load-bearing for lost-wakeup freedom: alarmState is
// stored FIRST (Swap), so any reader that wakes and re-checks it observes the
// new value before it decides to re-park.
func (s *Server) applyAlarmBits(next uint32) {
	old := s.alarmState.Swap(next)
	if old == next {
		return
	}

	switch {
	case old == 0 && next != 0:
		// 0 -> nonzero: emit connection.blocked(reason) to opted-in clients.
		s.Log.Warn("resource alarm raised; blocking publishers",
			zap.String("reason", alarmReason(next)), zap.Uint32("alarm_bits", next))
		s.broadcastConnectionBlocked(alarmReason(next))
	case old != 0 && next == 0:
		// nonzero -> 0: emit connection.unblocked, then wake parked readers. The
		// store above already happened, so a woken reader re-checks and sees 0.
		s.Log.Info("resource alarm cleared; unblocking publishers")
		s.broadcastConnectionUnblocked()
		s.wakeParkedReaders()
	default:
		// old!=0 && next!=0: the active alarm SET changed while still blocked.
		// RabbitMQ does NOT re-emit connection.blocked, and readers stay parked
		// (still alarmed), so there is nothing to do.
	}
}

// alarmReason maps the active alarm bits to the human-readable connection.blocked
// reason shortstr. Provisional strings pending W7 verification against real
// RabbitMQ (forced via rabbitmqctl); RabbitMQ joins active reasons with " & "
// and does not re-emit when the set changes while already blocked.
func alarmReason(bits uint32) string {
	switch {
	case bits&AlarmMemory != 0 && bits&AlarmDisk != 0:
		return "low on memory & low on disk"
	case bits&AlarmMemory != 0:
		return "low on memory"
	case bits&AlarmDisk != 0:
		return "low on disk"
	default:
		return ""
	}
}

// snapshotConnections returns a point-in-time slice of live connections so the
// monitor can emit frames / poke wake channels without holding s.Mutex across a
// (bounded) socket write.
func (s *Server) snapshotConnections() []*protocol.Connection {
	s.Mutex.RLock()
	conns := make([]*protocol.Connection, 0, len(s.Connections))
	for _, c := range s.Connections {
		conns = append(conns, c)
	}
	s.Mutex.RUnlock()
	return conns
}

func (s *Server) broadcastConnectionBlocked(reason string) {
	for _, conn := range s.snapshotConnections() {
		s.notifyConnectionBlocked(conn, reason)
	}
}

func (s *Server) broadcastConnectionUnblocked() {
	for _, conn := range s.snapshotConnections() {
		s.notifyConnectionUnblocked(conn)
	}
}

// notifyConnectionBlocked sends connection.blocked to an opted-in client exactly
// once per block edge (AlarmNotified guards re-sends and resolves the race with
// a connection that joins mid-alarm). A no-op for clients that did not advertise
// the connection.blocked capability — SQ-12 must never emit to them.
func (s *Server) notifyConnectionBlocked(conn *protocol.Connection, reason string) {
	if !conn.ClientSupportsBlocked() {
		return
	}
	if conn.AlarmNotified.Swap(true) {
		return // already told this client it is blocked
	}
	m := &protocol.ConnectionBlockedMethod{Reason: reason}
	data, err := m.Serialize()
	if err != nil {
		s.Log.Error("serialize connection.blocked", zap.Error(err))
		return
	}
	frame := protocol.EncodeMethodFrame(10, protocol.ConnectionBlocked, data)
	if err := protocol.WriteFrameToConnectionWithDeadline(conn, frame, alarmControlWriteTimeout); err != nil {
		s.Log.Warn("failed to send connection.blocked",
			zap.String("connection_id", conn.ID), zap.Error(err))
	}
}

// notifyConnectionUnblocked sends connection.unblocked to an opted-in client
// exactly once, and only if it had previously been told it was blocked.
func (s *Server) notifyConnectionUnblocked(conn *protocol.Connection) {
	if !conn.ClientSupportsBlocked() {
		return
	}
	if !conn.AlarmNotified.Swap(false) {
		return // was never blocked-notified
	}
	m := &protocol.ConnectionUnblockedMethod{}
	data, err := m.Serialize()
	if err != nil {
		s.Log.Error("serialize connection.unblocked", zap.Error(err))
		return
	}
	frame := protocol.EncodeMethodFrame(10, protocol.ConnectionUnblocked, data)
	if err := protocol.WriteFrameToConnectionWithDeadline(conn, frame, alarmControlWriteTimeout); err != nil {
		s.Log.Warn("failed to send connection.unblocked",
			zap.String("connection_id", conn.ID), zap.Error(err))
	}
}

// wakeParkedReaders pokes every live connection's AlarmWake channel so a parked
// reader unwinds after the alarm clears. Non-blocking: the capacity-1 channel
// coalesces pokes, and the reader re-checks alarmState after receiving one.
func (s *Server) wakeParkedReaders() {
	for _, conn := range s.snapshotConnections() {
		if conn.AlarmWake == nil {
			continue
		}
		select {
		case conn.AlarmWake <- struct{}{}:
		default:
		}
	}
}

// alarmParkProbeInterval is the reader-park liveness re-arm interval, overridable
// for tests via s.alarmParkProbe.
func (s *Server) alarmParkProbeInterval() time.Duration {
	if s.alarmParkProbe > 0 {
		return s.alarmParkProbe
	}
	return defaultAlarmParkProbe
}

// parkReaderWhileAlarmed parks the connection's reader while a resource alarm is
// active, providing the TCP backpressure that throttles the publisher. It
// returns probing=true when it broke out on the bounded re-arm timer to probe
// socket liveness (rather than because the alarm fully cleared) so the caller
// bounds the ensuing read; false means the alarm cleared (normal resume) or the
// connection is tearing down.
//
// Lost-wakeup freedom: the loop re-checks the authoritative alarmState after
// every wake, so a coalesced or racing poke (the monitor stores alarmState=0
// before poking) can never leave the reader parked past a clear.
func (s *Server) parkReaderWhileAlarmed(conn *protocol.Connection) (probing bool) {
	probe := time.NewTimer(s.alarmParkProbeInterval())
	defer probe.Stop()

	for s.alarmState.Load() != 0 {
		select {
		case <-conn.AlarmWake:
			// Coalesced wake — re-check alarmState at the top of the loop.
		case <-conn.Done:
			return false
		case <-probe.C:
			// Bounded re-arm: break out to probe the socket so a dead peer is
			// detected even while blocked (critical when heartbeats are disabled
			// and the reader would otherwise park indefinitely).
			return true
		}
	}
	return false
}

// readDiskFreeBytes returns the free bytes available to an unprivileged process
// on the filesystem backing path. syscall.Statfs is available on both Linux and
// darwin, so this reader is platform-independent.
func readDiskFreeBytes(path string) (uint64, bool) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return 0, false
	}
	return stat.Bavail * uint64(stat.Bsize), true
}
