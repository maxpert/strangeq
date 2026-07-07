package broker

import (
	"testing"
	"time"

	"github.com/maxpert/amqp-go/protocol"
)

// TestDeadLetterReasonConstants pins the exact wire strings RabbitMQ uses for
// the x-death "reason" field. These are part of the Wave 2 frozen contract.
// delivery_limit is deliberately NOT defined (reserved, not emitted).
func TestDeadLetterReasonConstants(t *testing.T) {
	if DeadLetterRejected != "rejected" {
		t.Errorf("DeadLetterRejected = %q, want rejected", DeadLetterRejected)
	}
	if DeadLetterExpired != "expired" {
		t.Errorf("DeadLetterExpired = %q, want expired", DeadLetterExpired)
	}
	if DeadLetterMaxLen != "maxlen" {
		t.Errorf("DeadLetterMaxLen = %q, want maxlen", DeadLetterMaxLen)
	}
}

// TestDeadLetterNilPolicy_ReturnsNilWithoutTouchingStorage pins the frozen
// DLX-seam contract for the no-DLX cases: a nil (or DLX-less) policy is a silent
// no-op that returns nil and never touches broker storage — so a zero-value
// broker is a valid receiver. The full-body behavior (republish, cycle filter,
// expiration strip) is covered in dead_letter_body_test.go against a real
// broker. See also TestDeadLetter_NilPolicyIsNoop.
func TestDeadLetterNilPolicy_ReturnsNilWithoutTouchingStorage(t *testing.T) {
	b := &StorageBroker{}
	msg := &protocol.Message{Exchange: "ex", RoutingKey: "rk", Body: []byte("x")}

	if err := b.deadLetter(nil, "src", msg, DeadLetterRejected); err != nil {
		t.Errorf("deadLetter(nil policy) = %v, want nil", err)
	}
	// A policy with no dead-letter-exchange configured is likewise a no-op: the
	// caller guard (p != nil && p.HasDeadLetterExchange) would normally prevent
	// this call, but the seam must be defensive and must not deref nil storage.
	if err := b.deadLetter(&QueuePolicy{}, "src", msg, DeadLetterExpired); err != nil {
		t.Errorf("deadLetter(DLX-less policy) = %v, want nil", err)
	}
}

// xDeathEntries extracts the x-death field array as a slice of field tables,
// failing the test if the header is missing or malformed.
func xDeathEntries(t *testing.T, headers map[string]interface{}) []map[string]interface{} {
	t.Helper()
	raw, ok := headers["x-death"]
	if !ok {
		t.Fatal("x-death header missing")
	}
	arr, ok := raw.([]interface{})
	if !ok {
		t.Fatalf("x-death is %T, want []interface{} (AMQP field array)", raw)
	}
	out := make([]map[string]interface{}, 0, len(arr))
	for i, e := range arr {
		tbl, ok := e.(map[string]interface{})
		if !ok {
			t.Fatalf("x-death[%d] is %T, want map[string]interface{} (field table)", i, e)
		}
		out = append(out, tbl)
	}
	return out
}

func assertEntry(t *testing.T, entry map[string]interface{}, queue string, reason DeadLetterReason, count int64) {
	t.Helper()
	if q, ok := entry["queue"].(string); !ok || q != queue {
		t.Errorf("entry queue = %v, want %q", entry["queue"], queue)
	}
	if r, ok := entry["reason"].(string); !ok || r != string(reason) {
		t.Errorf("entry reason = %v, want %q", entry["reason"], reason)
	}
	if c, ok := entry["count"].(int64); !ok || c != count {
		t.Errorf("entry count = %v (%T), want int64(%d)", entry["count"], entry["count"], count)
	}
}

// TestAnnotateXDeath_FirstDeath verifies the shape of a first-time x-death entry
// and that x-first-death-* is populated, with exact field names and types.
func TestAnnotateXDeath_FirstDeath(t *testing.T) {
	deathTime := time.Unix(1_700_000_000, 0).UTC()
	headers := annotateXDeath(nil, xDeathEvent{
		queue:       "orders",
		reason:      DeadLetterRejected,
		exchange:    "orders-ex",
		routingKeys: []string{"rk1"},
		deathTime:   deathTime,
	})

	entries := xDeathEntries(t, headers)
	if len(entries) != 1 {
		t.Fatalf("len(x-death) = %d, want 1", len(entries))
	}
	e := entries[0]
	assertEntry(t, e, "orders", DeadLetterRejected, 1)

	if ex, ok := e["exchange"].(string); !ok || ex != "orders-ex" {
		t.Errorf("exchange = %v, want orders-ex", e["exchange"])
	}
	ts, ok := e["time"].(time.Time)
	if !ok {
		t.Fatalf("time is %T, want time.Time", e["time"])
	}
	if ts.Unix() != deathTime.Unix() {
		t.Errorf("time = %v, want %v", ts.Unix(), deathTime.Unix())
	}
	rks, ok := e["routing-keys"].([]interface{})
	if !ok {
		t.Fatalf("routing-keys is %T, want []interface{} (field array)", e["routing-keys"])
	}
	if len(rks) != 1 {
		t.Fatalf("len(routing-keys) = %d, want 1", len(rks))
	}
	if s, ok := rks[0].(string); !ok || s != "rk1" {
		t.Errorf("routing-keys[0] = %v, want rk1", rks[0])
	}

	// x-first-death-* set on first death.
	if headers["x-first-death-reason"] != "rejected" {
		t.Errorf("x-first-death-reason = %v, want rejected", headers["x-first-death-reason"])
	}
	if headers["x-first-death-queue"] != "orders" {
		t.Errorf("x-first-death-queue = %v, want orders", headers["x-first-death-queue"])
	}
	if headers["x-first-death-exchange"] != "orders-ex" {
		t.Errorf("x-first-death-exchange = %v, want orders-ex", headers["x-first-death-exchange"])
	}
}

// TestAnnotateXDeath_OriginalExpirationOnlyForExpired verifies original-expiration
// is recorded only when reason==expired and only when a value is supplied.
func TestAnnotateXDeath_OriginalExpirationOnlyForExpired(t *testing.T) {
	// rejected: no original-expiration even if a value is passed.
	h := annotateXDeath(nil, xDeathEvent{
		queue:              "q",
		reason:             DeadLetterRejected,
		exchange:           "ex",
		originalExpiration: "60000",
	})
	if _, ok := xDeathEntries(t, h)[0]["original-expiration"]; ok {
		t.Error("original-expiration recorded for reason=rejected, want absent")
	}

	// expired with a value: recorded as a longstr.
	h = annotateXDeath(nil, xDeathEvent{
		queue:              "q",
		reason:             DeadLetterExpired,
		exchange:           "ex",
		originalExpiration: "60000",
	})
	if v, ok := xDeathEntries(t, h)[0]["original-expiration"].(string); !ok || v != "60000" {
		t.Errorf("original-expiration = %v, want \"60000\"", xDeathEntries(t, h)[0]["original-expiration"])
	}

	// expired with no value (queue-TTL-only expiry): not recorded.
	h = annotateXDeath(nil, xDeathEvent{
		queue:    "q",
		reason:   DeadLetterExpired,
		exchange: "ex",
	})
	if _, ok := xDeathEntries(t, h)[0]["original-expiration"]; ok {
		t.Error("original-expiration recorded for expired with no original value, want absent")
	}
}

// TestAnnotateXDeath_Aggregation verifies repeat dead-lettering from the same
// (queue,reason) increments count and moves the entry to the front, while a new
// pair prepends a fresh entry — so x-death[0] is always the most recent.
func TestAnnotateXDeath_Aggregation(t *testing.T) {
	base := time.Unix(1_700_000_000, 0).UTC()

	var h map[string]interface{}
	// 1. Q1/rejected -> [Q1r c1]
	h = annotateXDeath(h, xDeathEvent{queue: "Q1", reason: DeadLetterRejected, exchange: "e1", deathTime: base})
	// 2. Q1/rejected again -> [Q1r c2]
	h = annotateXDeath(h, xDeathEvent{queue: "Q1", reason: DeadLetterRejected, exchange: "e1", deathTime: base.Add(time.Second)})
	entries := xDeathEntries(t, h)
	if len(entries) != 1 {
		t.Fatalf("after 2 same-pair deaths len = %d, want 1", len(entries))
	}
	assertEntry(t, entries[0], "Q1", DeadLetterRejected, 2)

	// 3. Q2/expired -> prepended: [Q2e c1, Q1r c2]
	h = annotateXDeath(h, xDeathEvent{queue: "Q2", reason: DeadLetterExpired, exchange: "e2", deathTime: base.Add(2 * time.Second)})
	entries = xDeathEntries(t, h)
	if len(entries) != 2 {
		t.Fatalf("after new pair len = %d, want 2", len(entries))
	}
	assertEntry(t, entries[0], "Q2", DeadLetterExpired, 1)
	assertEntry(t, entries[1], "Q1", DeadLetterRejected, 2)

	// 4. Q1/rejected again -> moved to front with count 3: [Q1r c3, Q2e c1]
	h = annotateXDeath(h, xDeathEvent{queue: "Q1", reason: DeadLetterRejected, exchange: "e1", deathTime: base.Add(3 * time.Second)})
	entries = xDeathEntries(t, h)
	if len(entries) != 2 {
		t.Fatalf("after re-death of existing pair len = %d, want 2", len(entries))
	}
	assertEntry(t, entries[0], "Q1", DeadLetterRejected, 3)
	assertEntry(t, entries[1], "Q2", DeadLetterExpired, 1)

	// time on the moved-to-front entry reflects the most recent death.
	if ts := entries[0]["time"].(time.Time); ts.Unix() != base.Add(3*time.Second).Unix() {
		t.Errorf("aggregated entry time = %v, want %v", ts.Unix(), base.Add(3*time.Second).Unix())
	}
}

// TestAnnotateXDeath_DistinctReasonSameQueue verifies (queue,reason) is the
// aggregation key: the same queue with two different reasons yields two entries.
func TestAnnotateXDeath_DistinctReasonSameQueue(t *testing.T) {
	var h map[string]interface{}
	h = annotateXDeath(h, xDeathEvent{queue: "Q", reason: DeadLetterRejected, exchange: "e"})
	h = annotateXDeath(h, xDeathEvent{queue: "Q", reason: DeadLetterExpired, exchange: "e"})
	entries := xDeathEntries(t, h)
	if len(entries) != 2 {
		t.Fatalf("distinct reasons on same queue len = %d, want 2", len(entries))
	}
	assertEntry(t, entries[0], "Q", DeadLetterExpired, 1)
	assertEntry(t, entries[1], "Q", DeadLetterRejected, 1)
}

// TestAnnotateXDeath_FirstDeathSetOnce verifies x-first-death-* is written on
// the first death and never overwritten by later dead-letterings.
func TestAnnotateXDeath_FirstDeathSetOnce(t *testing.T) {
	var h map[string]interface{}
	h = annotateXDeath(h, xDeathEvent{queue: "first-q", reason: DeadLetterRejected, exchange: "first-ex"})
	h = annotateXDeath(h, xDeathEvent{queue: "second-q", reason: DeadLetterExpired, exchange: "second-ex"})
	h = annotateXDeath(h, xDeathEvent{queue: "third-q", reason: DeadLetterMaxLen, exchange: "third-ex"})

	if h["x-first-death-reason"] != "rejected" {
		t.Errorf("x-first-death-reason = %v, want rejected (set once)", h["x-first-death-reason"])
	}
	if h["x-first-death-queue"] != "first-q" {
		t.Errorf("x-first-death-queue = %v, want first-q (set once)", h["x-first-death-queue"])
	}
	if h["x-first-death-exchange"] != "first-ex" {
		t.Errorf("x-first-death-exchange = %v, want first-ex (set once)", h["x-first-death-exchange"])
	}
}

// TestAnnotateXDeath_PreexistingFirstDeathNotOverwritten verifies that when a
// message arrives already carrying x-first-death-reason (e.g. recovered from a
// prior death), annotateXDeath leaves the first-death fields untouched.
func TestAnnotateXDeath_PreexistingFirstDeathNotOverwritten(t *testing.T) {
	h := map[string]interface{}{
		"x-first-death-reason":   "expired",
		"x-first-death-queue":    "original-q",
		"x-first-death-exchange": "original-ex",
	}
	h = annotateXDeath(h, xDeathEvent{queue: "new-q", reason: DeadLetterRejected, exchange: "new-ex"})
	if h["x-first-death-reason"] != "expired" || h["x-first-death-queue"] != "original-q" || h["x-first-death-exchange"] != "original-ex" {
		t.Errorf("pre-existing x-first-death-* was overwritten: %v/%v/%v",
			h["x-first-death-reason"], h["x-first-death-queue"], h["x-first-death-exchange"])
	}
}

// TestAnnotateXDeath_WireEncodable proves the produced x-death uses only
// wire-legal AMQP field-table value types: EncodeFieldTable recursively rejects
// any unsupported type, so a clean encode confirms the field array of tables
// (int64 count, timestamp time, field-array routing-keys, longstr strings) is
// exactly the durable/wire shape SQ-10 and W2 depend on. A full decode
// round-trip is exercised by W2's durable-roundtrip and W7's conformance suite.
func TestAnnotateXDeath_WireEncodable(t *testing.T) {
	h := annotateXDeath(nil, xDeathEvent{
		queue:       "q",
		reason:      DeadLetterExpired,
		exchange:    "ex",
		routingKeys: []string{"a", "b"},
		deathTime:   time.Unix(1_700_000_000, 0).UTC(),
		// expired -> original-expiration recorded
		originalExpiration: "1000",
	})

	encoded, err := protocol.EncodeFieldTable(h)
	if err != nil {
		t.Fatalf("EncodeFieldTable rejected x-death (unsupported value type): %v", err)
	}
	if len(encoded) == 0 {
		t.Fatal("EncodeFieldTable produced empty output")
	}
}
