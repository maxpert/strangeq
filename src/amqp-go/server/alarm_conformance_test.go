//go:build conformance

package server

import (
	"testing"
	"time"

	"github.com/maxpert/amqp-go/protocol"
)

// W7 conformance (white-box): lock StrangeQ's connection.blocked reason shortstrs
// against the values captured from real RabbitMQ 4.3.2 (see w7-report.md §7 and
// the rabbit-only TestConformance_RabbitMQAlarmReasons capture). The black-box
// dual-target suite can only trip StrangeQ's DISK arm on a non-Linux host (the
// memory arm is disarmed there), so the memory + combined strings are compared
// to RabbitMQ here, at the wire, via the real emission path.
//
// These are the RabbitMQ-locked reason strings (captured against RabbitMQ 4.3.2
// in W7). The combined string is "low on disk & memory": RabbitMQ stores the
// alarmed-by resources with lists:usort, so they always render in sorted atom
// order (disk before memory), regardless of which alarm tripped first.
const (
	rmqReasonMemory     = "low on memory"
	rmqReasonDisk       = "low on disk"
	rmqReasonMemAndDisk = "low on disk & memory"
)

// TestConformance_AlarmReasonStringsMatchRabbitMQ locks the pure bit→string
// mapping against RabbitMQ's captured reason shortstrs.
func TestConformance_AlarmReasonStringsMatchRabbitMQ(t *testing.T) {
	cases := []struct {
		bits uint32
		want string
	}{
		{AlarmMemory, rmqReasonMemory},
		{AlarmDisk, rmqReasonDisk},
		{AlarmMemory | AlarmDisk, rmqReasonMemAndDisk},
	}
	for _, c := range cases {
		if got := alarmReason(c.bits); got != c.want {
			t.Errorf("alarmReason(%#b) = %q, want (RabbitMQ-locked) %q", c.bits, got, c.want)
		}
	}
}

// TestConformance_BlockedUnblockedWirePath drives the full emission path for each
// alarm set and asserts the connection.blocked frame carries the RabbitMQ-locked
// reason shortstr, and that clearing emits connection.unblocked.
func TestConformance_BlockedUnblockedWirePath(t *testing.T) {
	cases := []struct {
		name string
		bits uint32
		want string
	}{
		{"memory", AlarmMemory, rmqReasonMemory},
		{"disk", AlarmDisk, rmqReasonDisk},
		{"combined", AlarmMemory | AlarmDisk, rmqReasonMemAndDisk},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := newAlarmServer()
			conn, client := makeCapabilityConn("wire-"+c.name, true)
			defer client.Close()
			conn.HasPublished.Store(true)
			s.Connections[conn.ID] = conn

			// blocked
			gotReason := make(chan string, 1)
			go func() {
				class, method, reason := readMethod(t, client)
				if class == 10 && method == protocol.ConnectionBlocked {
					gotReason <- reason
					return
				}
				gotReason <- ""
			}()
			s.applyAlarmBits(c.bits)
			select {
			case r := <-gotReason:
				if r != c.want {
					t.Errorf("connection.blocked reason = %q, want (RabbitMQ-locked) %q", r, c.want)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("no connection.blocked frame emitted")
			}

			// unblocked
			gotUnblock := make(chan uint16, 1)
			go func() {
				class, method, _ := readMethod(t, client)
				if class == 10 {
					gotUnblock <- method
					return
				}
				gotUnblock <- 0
			}()
			s.applyAlarmBits(0)
			select {
			case m := <-gotUnblock:
				if m != protocol.ConnectionUnblocked {
					t.Errorf("expected connection.unblocked (method %d), got method %d", protocol.ConnectionUnblocked, m)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("no connection.unblocked frame emitted after clearing the alarm")
			}
		})
	}
}
