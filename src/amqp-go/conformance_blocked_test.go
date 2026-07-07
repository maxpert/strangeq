//go:build conformance

package main

import (
	"encoding/binary"
	"net"
	"net/url"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/protocol"
)

// ----------------------------------------------------------------------------
// Case 7 — connection.blocked / unblocked + the negative-capability case.
//
// StrangeQ can only be forced into a resource alarm black-box via its DISK arm
// (the memory arm is disarmed on non-Linux hosts), so the end-to-end dual-target
// assertion uses the disk alarm. The memory + combined reason strings are locked
// against real RabbitMQ (TestConformance_RabbitMQAlarmReasons, rabbit-only) and
// compared to StrangeQ's alarmReason() in the white-box server conformance test
// (server/alarm_conformance_test.go).
// ----------------------------------------------------------------------------

// armDiskAlarmBroker returns a dual-target broker whose embedded StrangeQ side is
// configured so its DISK arm trips immediately (disk-free floor set absurdly high
// so real free space is always below it). For RabbitMQ the config is ignored and
// the caller forces the alarm via rabbitmqctl.
func armDiskAlarmBroker(t *testing.T) *confBroker {
	return newConfBrokerCfg(t, func(cfg *config.AMQPConfig) {
		cfg.ResourceAlarms.Enabled = true
		cfg.ResourceAlarms.MemoryHighWatermark = 0 // disarm the (platform-specific) memory arm
		cfg.ResourceAlarms.DiskFreeLimitBytes = 1 << 62
		cfg.ResourceAlarms.Hysteresis = 0.05
		cfg.ResourceAlarms.CheckIntervalMS = 150
	})
}

func forceDiskAlarm(t *testing.T, b *confBroker) {
	if b.target == "rabbitmq" {
		// disk free limit = 100x RAM → always above real free disk → disk alarm.
		rabbitctl(t, "set_disk_free_limit", "mem_relative", "100.0")
	}
}

func clearDiskAlarm(t *testing.T, b *confBroker) {
	if b.target == "rabbitmq" {
		rabbitctl(t, "set_disk_free_limit", rabbitCtlRestoreDisk)
	}
}

func TestConformance_ConnectionBlockedDiskAlarm(t *testing.T) {
	b := armDiskAlarmBroker(t)
	defer b.cleanup()
	forceDiskAlarm(t, b)
	defer clearDiskAlarm(t, b)

	conn, ch := b.dial(t)
	blocked := conn.NotifyBlocked(make(chan amqp.Blocking, 8))

	q := uniqueName("blk")
	declareDurable(t, ch, q, nil)
	// Become a publisher: StrangeQ blocks only publisher connections.
	_ = ch.PublishWithContext(t.Context(), "", q, false, false, amqp.Publishing{Body: []byte("p")})

	wantReason := expectByTarget(t, "disk-connection.blocked-reason", lockedRMQReasonDisk, strangeQReasonDisk)

	select {
	case bl := <-blocked:
		require.True(t, bl.Active, "expected a BLOCKED (Active) event")
		lockLog(t, "disk connection.blocked reason", bl.Reason)
		assert.Equal(t, wantReason, bl.Reason, "target=%s connection.blocked reason shortstr", confTarget())
	case <-time.After(6 * time.Second):
		t.Fatalf("target=%s: no connection.blocked within timeout", confTarget())
	}

	// Unblock lifecycle: RabbitMQ can be un-alarmed at runtime (black-box).
	// StrangeQ's config-armed disk alarm cannot be cleared at runtime black-box;
	// its unblock + reason strings are asserted in the white-box server test.
	if targetIsRabbit() {
		clearDiskAlarm(t, b)
		select {
		case bl := <-blocked:
			assert.False(t, bl.Active, "expected an UNBLOCKED event after clearing the alarm")
			lockLog(t, "connection.unblocked", bl)
		case <-time.After(8 * time.Second):
			t.Fatal("no connection.unblocked after clearing the RabbitMQ alarm")
		}
	}
}

// TestConformance_RabbitMQAlarmReasons forces RabbitMQ through memory, disk, and
// combined alarms and LOCKS the exact reason shortstrs (recorded in w7-report.md
// and mirrored by the white-box server conformance test). RabbitMQ-only.
func TestConformance_RabbitMQAlarmReasons(t *testing.T) {
	if !targetIsRabbit() {
		t.Skip("rabbit-only: locks the real RabbitMQ reason strings")
	}
	b := newConfBroker(t)
	defer b.cleanup()

	capture := func(label string, arm, disarm func()) string {
		conn, ch := b.dial(t)
		blocked := conn.NotifyBlocked(make(chan amqp.Blocking, 8))
		q := uniqueName("rmqalarm")
		declareDurable(t, ch, q, nil)
		arm()
		defer disarm()
		_ = ch.PublishWithContext(t.Context(), "", q, false, false, amqp.Publishing{Body: []byte("x")})
		select {
		case bl := <-blocked:
			lockLog(t, "RMQ reason "+label, bl.Reason)
			return bl.Reason
		case <-time.After(8 * time.Second):
			t.Fatalf("no connection.blocked for %s", label)
			return ""
		}
	}

	mem := capture("memory",
		func() { rabbitctl(t, "set_vm_memory_high_watermark", "0.0001") },
		func() { rabbitctl(t, "set_vm_memory_high_watermark", rabbitCtlRestoreMemHigh) })
	disk := capture("disk",
		func() { rabbitctl(t, "set_disk_free_limit", "mem_relative", "100.0") },
		func() { rabbitctl(t, "set_disk_free_limit", rabbitCtlRestoreDisk) })
	combined := capture("combined",
		func() {
			rabbitctl(t, "set_vm_memory_high_watermark", "0.0001")
			rabbitctl(t, "set_disk_free_limit", "mem_relative", "100.0")
		},
		func() {
			rabbitctl(t, "set_vm_memory_high_watermark", rabbitCtlRestoreMemHigh)
			rabbitctl(t, "set_disk_free_limit", rabbitCtlRestoreDisk)
		})

	t.Logf("LOCKED RabbitMQ reason strings: memory=%q disk=%q combined=%q", mem, disk, combined)
	// These are the values baked into the LOCKED-* constants; assert they still hold.
	assert.Equal(t, lockedRMQReasonMem, mem)
	assert.Equal(t, lockedRMQReasonDisk, disk)
	assert.Equal(t, lockedRMQReasonMemDisk, combined)
}

// ----------------------------------------------------------------------------
// Negative capability case — a client that did NOT advertise the
// connection.blocked capability must receive NO connection.blocked frame.
// amqp091-go always advertises it, so this is a hand-rolled raw AMQP client.
// ----------------------------------------------------------------------------

func TestConformance_NegativeCapabilityNoBlockedFrame(t *testing.T) {
	b := armDiskAlarmBroker(t)
	defer b.cleanup()
	forceDiskAlarm(t, b)
	defer clearDiskAlarm(t, b)

	// CONTROL: a capability-advertising client (amqp091 always advertises
	// connection.blocked) that publishes under the same active alarm MUST be
	// blocked. This proves the alarm is genuinely active in this window, so the
	// negative assertion below is meaningful rather than trivially true.
	ctlConn, ctlCh := b.dial(t)
	ctlBlocked := ctlConn.NotifyBlocked(make(chan amqp.Blocking, 4))
	ctlQ := uniqueName("negcap-control")
	declareDurable(t, ctlCh, ctlQ, nil)
	_ = ctlCh.PublishWithContext(t.Context(), "", ctlQ, false, false, amqp.Publishing{Body: []byte("p")})
	select {
	case bl := <-ctlBlocked:
		require.True(t, bl.Active, "control (capability-advertising) client must be BLOCKED under the active alarm")
		t.Logf("target=%s: control client correctly received connection.blocked (alarm is active)", confTarget())
	case <-time.After(6 * time.Second):
		t.Fatalf("target=%s: alarm not active — control client received no connection.blocked, so the negative case would be vacuous", confTarget())
	}

	u, err := url.Parse(b.uri)
	require.NoError(t, err)
	conn, err := net.DialTimeout("tcp", u.Host, 5*time.Second)
	require.NoError(t, err, "raw dial %s", u.Host)
	defer conn.Close()

	rc := &rawClient{t: t, conn: conn}
	rc.handshakeWithoutBlockedCapability()
	rc.openChannel(1)
	// Publish once so StrangeQ marks this a publisher connection (HasPublished);
	// the alarm is already active, so a capability-advertising client WOULD be
	// blocked here — this one must not be.
	rc.publish(1, "", uniqueName("negcap-nq"), []byte("p"))

	// Read frames for a window; assert NO connection.blocked (class 10 / method 60).
	deadline := time.Now().Add(3 * time.Second)
	_ = conn.SetReadDeadline(deadline)
	for {
		f, err := protocol.ReadFrame(conn)
		if err != nil {
			break // timeout / EOF → nothing more to read
		}
		if f.Type == protocol.FrameMethod && len(f.Payload) >= 4 {
			class := binary.BigEndian.Uint16(f.Payload[0:2])
			method := binary.BigEndian.Uint16(f.Payload[2:4])
			if class == 10 && method == uint16(protocol.ConnectionBlocked) {
				t.Fatalf("target=%s: received connection.blocked on a client that did NOT advertise the capability", confTarget())
			}
		}
	}
	t.Logf("target=%s: no connection.blocked frame delivered to non-advertising client (correct)", confTarget())
}

// ----------------------------------------------------------------------------
// Minimal raw AMQP 0-9-1 client — only enough to complete the handshake without
// the connection.blocked capability and to emit one basic.publish.
// ----------------------------------------------------------------------------

type rawClient struct {
	t    *testing.T
	conn net.Conn
}

func (rc *rawClient) writeFrame(f *protocol.Frame) {
	_ = rc.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	require.NoError(rc.t, protocol.WriteFrame(rc.conn, f), "write frame")
}

func (rc *rawClient) readFrame() *protocol.Frame {
	_ = rc.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	f, err := protocol.ReadFrame(rc.conn)
	require.NoError(rc.t, err, "read frame")
	return f
}

func (rc *rawClient) handshakeWithoutBlockedCapability() {
	// Protocol header: "AMQP" 0x00 0x00 0x09 0x01
	_ = rc.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	_, err := rc.conn.Write([]byte{'A', 'M', 'Q', 'P', 0x00, 0x00, 0x09, 0x01})
	require.NoError(rc.t, err, "write protocol header")

	rc.readFrame() // connection.start (10.10)

	// connection.start-ok WITHOUT the connection.blocked capability.
	startOK := &protocol.ConnectionStartOKMethod{
		ClientProperties: map[string]interface{}{
			"product":  "sq-w7-negcap-client",
			"platform": "go",
			"capabilities": map[string]interface{}{
				"consumer_cancel_notify": true,
				// connection.blocked deliberately OMITTED.
			},
		},
		Mechanism: "PLAIN",
		Response:  []byte("\x00guest\x00guest"),
		Locale:    "en_US",
	}
	sokData, err := startOK.Serialize()
	require.NoError(rc.t, err)
	rc.writeFrame(protocol.EncodeMethodFrameForChannel(0, 10, protocol.ConnectionStartOK, sokData))

	tuneFrame := rc.readFrame() // connection.tune (10.30)
	tune, err := protocol.ParseConnectionTune(tuneFrame.Payload[4:])
	require.NoError(rc.t, err, "parse connection.tune")

	// connection.tune-ok: echo channel-max + frame-max, heartbeat=0.
	tuneOK := make([]byte, 8)
	binary.BigEndian.PutUint16(tuneOK[0:2], tune.ChannelMax)
	binary.BigEndian.PutUint32(tuneOK[2:6], tune.FrameMax)
	binary.BigEndian.PutUint16(tuneOK[6:8], 0)
	rc.writeFrame(protocol.EncodeMethodFrameForChannel(0, 10, protocol.ConnectionTuneOK, tuneOK))

	// connection.open: vhost="/", reserved-1="", reserved-2 octet=0.
	openArgs := protocol.EncodeShortString("/")
	openArgs = append(openArgs, protocol.EncodeShortString("")...)
	openArgs = append(openArgs, 0x00)
	rc.writeFrame(protocol.EncodeMethodFrameForChannel(0, 10, protocol.ConnectionOpen, openArgs))

	rc.readFrame() // connection.open-ok (10.41)
}

func (rc *rawClient) openChannel(ch uint16) {
	rc.writeFrame(protocol.EncodeMethodFrameForChannel(ch, 20, protocol.ChannelOpen, protocol.EncodeShortString("")))
	rc.readFrame() // channel.open-ok (20.11)
}

func (rc *rawClient) publish(ch uint16, exchange, routingKey string, body []byte) {
	// basic.publish (60.40): reserved-1(uint16) + exchange(shortstr) + routing-key(shortstr) + bits(octet)
	args := make([]byte, 2)
	binary.BigEndian.PutUint16(args[0:2], 0)
	args = append(args, protocol.EncodeShortString(exchange)...)
	args = append(args, protocol.EncodeShortString(routingKey)...)
	args = append(args, 0x00) // mandatory=0, immediate=0
	rc.writeFrame(protocol.EncodeMethodFrameForChannel(ch, 60, protocol.BasicPublish, args))

	// content header (class 60, no properties) + body.
	rc.writeFrame(protocol.EncodeContentHeaderFrameForChannel(ch, 60, 0, uint64(len(body)), 0, nil))
	rc.writeFrame(protocol.EncodeBodyFrameForChannel(ch, body))
}
