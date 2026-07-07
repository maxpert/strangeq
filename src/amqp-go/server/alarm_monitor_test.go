package server

import (
	"encoding/binary"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/protocol"
	"go.uber.org/zap"
)

// ============================================================================
// SQ-12 resource-alarm monitor: hysteresis state machine, dormancy, edge
// emission gated on the client's advertised connection.blocked capability, and
// the RabbitMQ no-re-emit-on-set-change rule.
// ============================================================================

// newAlarmServer builds a Server suitable for exercising the monitor's emission
// path: a live Connections map guarded by Mutex, a no-op logger, and default
// config.
func newAlarmServer() *Server {
	return &Server{
		Log:              zap.NewNop(),
		Config:           config.DefaultConfig(),
		MetricsCollector: &NoOpMetricsCollector{},
		Connections:      make(map[string]*protocol.Connection),
	}
}

// --- evaluate(): hysteresis state machine ---

func TestAlarmEvaluate_MemoryHysteresis(t *testing.T) {
	th := &alarmThresholds{
		memSetBytes:   1000,
		memClearBytes: 900, // 10% hysteresis band
	}

	cases := []struct {
		name string
		cur  uint32
		rss  uint64
		want uint32
	}{
		{"below set stays clear", 0, 999, 0},
		{"at set trips", 0, 1000, AlarmMemory},
		{"above set trips", 0, 5000, AlarmMemory},
		{"in band stays set", AlarmMemory, 950, AlarmMemory},
		{"just above clear stays set", AlarmMemory, 901, AlarmMemory},
		{"at clear clears", AlarmMemory, 900, 0},
		{"below clear clears", AlarmMemory, 100, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := th.evaluate(tc.cur, tc.rss, true, 0, false)
			if got != tc.want {
				t.Errorf("evaluate(cur=%d, rss=%d) = %d, want %d", tc.cur, tc.rss, got, tc.want)
			}
		})
	}
}

func TestAlarmEvaluate_DiskHysteresis(t *testing.T) {
	th := &alarmThresholds{
		diskFloorBytes: 1000,
		diskClearBytes: 1100, // clear once free rises 10% above the floor
	}

	cases := []struct {
		name string
		cur  uint32
		free uint64
		want uint32
	}{
		{"above floor stays clear", 0, 1000, 0},
		{"below floor trips", 0, 999, AlarmDisk},
		{"empty disk trips", 0, 0, AlarmDisk},
		{"in band stays set", AlarmDisk, 1050, AlarmDisk},
		{"just below clear stays set", AlarmDisk, 1099, AlarmDisk},
		{"at clear clears", AlarmDisk, 1100, 0},
		{"well above clears", AlarmDisk, 999999, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := th.evaluate(tc.cur, 0, false, tc.free, true)
			if got != tc.want {
				t.Errorf("evaluate(cur=%d, free=%d) = %d, want %d", tc.cur, tc.free, got, tc.want)
			}
		})
	}
}

// TestAlarmEvaluate_SampleFailurePreservesState verifies a failed sample (ok=false)
// never flaps the current bit — critical on hosts where a reader is unavailable.
func TestAlarmEvaluate_SampleFailurePreservesState(t *testing.T) {
	th := &alarmThresholds{memSetBytes: 1000, memClearBytes: 900, diskFloorBytes: 1000, diskClearBytes: 1100}

	if got := th.evaluate(AlarmMemory, 0, false, 0, false); got != AlarmMemory {
		t.Errorf("memory bit must persist across a failed sample: got %d", got)
	}
	if got := th.evaluate(AlarmDisk, 0, false, 0, false); got != AlarmDisk {
		t.Errorf("disk bit must persist across a failed sample: got %d", got)
	}
	if got := th.evaluate(0, 0, false, 0, false); got != 0 {
		t.Errorf("no bit should be set from failed samples: got %d", got)
	}
}

func TestAlarmEvaluate_BothArmsIndependent(t *testing.T) {
	th := &alarmThresholds{memSetBytes: 1000, memClearBytes: 900, diskFloorBytes: 500, diskClearBytes: 550}
	// memory over, disk fine -> memory only
	if got := th.evaluate(0, 2000, true, 999, true); got != AlarmMemory {
		t.Errorf("want memory-only, got %d", got)
	}
	// both over -> both
	if got := th.evaluate(0, 2000, true, 100, true); got != AlarmMemory|AlarmDisk {
		t.Errorf("want both, got %d", got)
	}
	// clearing memory while disk stays set
	if got := th.evaluate(AlarmMemory|AlarmDisk, 100, true, 100, true); got != AlarmDisk {
		t.Errorf("want disk-only after memory recovers, got %d", got)
	}
}

// --- alarmReason ---

func TestAlarmReason(t *testing.T) {
	cases := []struct {
		bits uint32
		want string
	}{
		{0, ""},
		{AlarmMemory, "low on memory"},
		{AlarmDisk, "low on disk"},
		{AlarmMemory | AlarmDisk, "low on memory & low on disk"},
	}
	for _, tc := range cases {
		if got := alarmReason(tc.bits); got != tc.want {
			t.Errorf("alarmReason(%d) = %q, want %q", tc.bits, got, tc.want)
		}
	}
}

// --- buildAlarmThresholds dormancy ---

func TestBuildAlarmThresholds_DisabledReturnsNil(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.ResourceAlarms.Enabled = false
	if got := buildAlarmThresholds(cfg); got != nil {
		t.Errorf("buildAlarmThresholds must return nil when disabled, got %+v", got)
	}
}

func TestBuildAlarmThresholds_ZeroThresholdsReturnsNil(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.ResourceAlarms.Enabled = true
	cfg.ResourceAlarms.MemoryHighWatermark = 0 // memory arm off
	cfg.ResourceAlarms.DiskFreeLimitBytes = 0  // disk arm off
	if got := buildAlarmThresholds(cfg); got != nil {
		t.Errorf("buildAlarmThresholds must return nil when both arms are zero, got %+v", got)
	}
}

func TestBuildAlarmThresholds_DiskArmResolves(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.ResourceAlarms.Enabled = true
	cfg.ResourceAlarms.MemoryHighWatermark = 0 // isolate the disk arm (works on all platforms)
	cfg.ResourceAlarms.DiskFreeLimitBytes = 50 * 1024 * 1024
	cfg.ResourceAlarms.Hysteresis = 0.1
	th := buildAlarmThresholds(cfg)
	if th == nil {
		t.Fatal("buildAlarmThresholds returned nil for an enabled disk arm")
	}
	if th.diskFloorBytes != 50*1024*1024 {
		t.Errorf("diskFloorBytes = %d, want %d", th.diskFloorBytes, 50*1024*1024)
	}
	wantClear := uint64(float64(th.diskFloorBytes) * 1.1)
	if th.diskClearBytes != wantClear {
		t.Errorf("diskClearBytes = %d, want %d", th.diskClearBytes, wantClear)
	}
}

// TestSampleAlarmsOnce_DisarmedResourcesNeverSampled proves the dormant/zero-cost
// contract at the sampling level: when a resource is disarmed, its sampler is
// never invoked.
func TestSampleAlarmsOnce_DisarmedResourcesNeverSampled(t *testing.T) {
	var memCalls, diskCalls atomic.Int64
	s := newAlarmServer()
	s.alarm = &alarmThresholds{
		interval:       time.Second,
		diskFloorBytes: 1000,
		diskClearBytes: 1100,
		// memory arm disarmed (memSetBytes == 0)
		sampleMemory:   func() (uint64, bool) { memCalls.Add(1); return 0, true },
		sampleDiskFree: func() (uint64, bool) { diskCalls.Add(1); return 5000, true },
	}

	s.sampleAlarmsOnce()

	if memCalls.Load() != 0 {
		t.Errorf("memory sampler called %d times though memory arm is disarmed", memCalls.Load())
	}
	if diskCalls.Load() != 1 {
		t.Errorf("disk sampler called %d times, want 1", diskCalls.Load())
	}
}

// --- edge emission gated on capability ---

// makeCapabilityConn builds a pipe-backed connection that advertises (or not)
// the connection.blocked capability, plus the client side to read from.
func makeCapabilityConn(id string, advertises bool) (*protocol.Connection, net.Conn) {
	clientConn, serverConn := net.Pipe()
	conn := protocol.NewConnection(serverConn)
	conn.ID = id
	if advertises {
		conn.ClientProperties = map[string]interface{}{
			"capabilities": map[string]interface{}{"connection.blocked": true},
		}
	}
	return conn, clientConn
}

// readMethod reads one method frame from clientConn (with a deadline) and returns
// its class and method IDs.
func readMethod(t *testing.T, clientConn net.Conn) (classID, methodID uint16, reason string) {
	t.Helper()
	_ = clientConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	frame, err := protocol.ReadFrame(clientConn)
	if err != nil {
		t.Fatalf("reading method frame: %v", err)
	}
	if frame.Type != protocol.FrameMethod {
		t.Fatalf("expected method frame, got type %d", frame.Type)
	}
	if len(frame.Payload) < 4 {
		t.Fatalf("method payload too short: %d", len(frame.Payload))
	}
	classID = binary.BigEndian.Uint16(frame.Payload[0:2])
	methodID = binary.BigEndian.Uint16(frame.Payload[2:4])
	if classID == 10 && methodID == protocol.ConnectionBlocked {
		m := &protocol.ConnectionBlockedMethod{}
		if err := m.Deserialize(frame.Payload[4:]); err == nil {
			reason = m.Reason
		}
	}
	return classID, methodID, reason
}

// expectNoClientFrame asserts the client side receives no frame within a short window.
func expectNoClientFrame(t *testing.T, clientConn net.Conn) {
	t.Helper()
	_ = clientConn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	buf := make([]byte, 64)
	if n, err := clientConn.Read(buf); err == nil {
		t.Fatalf("expected no frame, but read %d bytes", n)
	}
	_ = clientConn.SetReadDeadline(time.Time{})
}

func TestApplyAlarmBits_EmitsBlockedOnlyToOptedInClients(t *testing.T) {
	s := newAlarmServer()

	optIn, optInClient := makeCapabilityConn("opt-in", true)
	optOut, optOutClient := makeCapabilityConn("opt-out", false)
	defer optInClient.Close()
	defer optOutClient.Close()
	s.Connections[optIn.ID] = optIn
	s.Connections[optOut.ID] = optOut

	// The opted-in client must receive connection.blocked; the other must not.
	blockedReason := make(chan string, 1)
	go func() {
		class, method, reason := readMethod(t, optInClient)
		if class == 10 && method == protocol.ConnectionBlocked {
			blockedReason <- reason
		} else {
			blockedReason <- ""
		}
	}()
	noFrame := make(chan struct{})
	go func() {
		expectNoClientFrame(t, optOutClient)
		close(noFrame)
	}()

	s.applyAlarmBits(AlarmMemory)

	select {
	case r := <-blockedReason:
		if r != "low on memory" {
			t.Errorf("blocked reason = %q, want %q", r, "low on memory")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("opted-in client did not receive connection.blocked")
	}
	<-noFrame

	if !optIn.AlarmNotified.Load() {
		t.Error("opted-in connection AlarmNotified must be true after blocked")
	}
	if optOut.AlarmNotified.Load() {
		t.Error("opt-out connection must never be marked AlarmNotified")
	}
}

func TestApplyAlarmBits_EmitsUnblockedOnClear(t *testing.T) {
	s := newAlarmServer()
	optIn, optInClient := makeCapabilityConn("opt-in", true)
	defer optInClient.Close()
	s.Connections[optIn.ID] = optIn

	// Drain the blocked frame first.
	got := make(chan struct{})
	go func() {
		readMethod(t, optInClient) // blocked
		class, method, _ := readMethod(t, optInClient)
		if class == 10 && method == protocol.ConnectionUnblocked {
			close(got)
		}
	}()

	s.applyAlarmBits(AlarmMemory)
	s.applyAlarmBits(0)

	select {
	case <-got:
	case <-time.After(3 * time.Second):
		t.Fatal("opted-in client did not receive connection.unblocked on clear")
	}
	if optIn.AlarmNotified.Load() {
		t.Error("AlarmNotified must be false after unblocked")
	}
}

// TestApplyAlarmBits_NoReEmitWhenSetChangesWhileBlocked verifies the RabbitMQ
// rule: once blocked, a change in the active alarm SET (e.g. memory clears but
// disk trips) does NOT emit a second connection.blocked.
func TestApplyAlarmBits_NoReEmitWhenSetChangesWhileBlocked(t *testing.T) {
	s := newAlarmServer()
	optIn, optInClient := makeCapabilityConn("opt-in", true)
	defer optInClient.Close()
	s.Connections[optIn.ID] = optIn

	// Read exactly one blocked frame, then assert nothing else arrives while the
	// set churns (memory->both->disk), and finally an unblocked on full clear.
	firstBlocked := make(chan string, 1)
	secondFrame := make(chan [2]uint16, 1)
	go func() {
		class, method, reason := readMethod(t, optInClient)
		_ = class
		_ = method
		firstBlocked <- reason
		c2, m2, _ := readMethod(t, optInClient)
		secondFrame <- [2]uint16{c2, m2}
	}()

	s.applyAlarmBits(AlarmMemory) // 0 -> memory: emit blocked(memory)
	if r := <-firstBlocked; r != "low on memory" {
		t.Fatalf("first blocked reason = %q, want low on memory", r)
	}

	s.applyAlarmBits(AlarmMemory | AlarmDisk) // set change: must NOT re-emit
	s.applyAlarmBits(AlarmDisk)               // set change: must NOT re-emit
	s.applyAlarmBits(0)                       // full clear: emit unblocked

	select {
	case f := <-secondFrame:
		if f[0] != 10 || f[1] != protocol.ConnectionUnblocked {
			t.Fatalf("second frame = class %d method %d, want the unblocked frame (no re-emit of blocked)", f[0], f[1])
		}
	case <-time.After(3 * time.Second):
		t.Fatal("expected an unblocked frame after the churn")
	}
}
