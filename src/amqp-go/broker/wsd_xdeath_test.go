package broker

import (
	"testing"
	"time"
)

// TestXDeath_RoutingKeys_Merge (T13.1; the name is kept from the brief but it
// asserts PRESERVE, D6/R13): on a repeat {queue, reason}, the EXISTING death
// entry is kept — count+1, routing-keys/exchange/first-time preserved
// verbatim, entry moved to the head. Pre-fix mechanism: mergeXDeath built the
// head entry from the CURRENT event and only carried the old count forward,
// discarding the prior routing-keys/exchange/time.
func TestXDeath_RoutingKeys_Merge(t *testing.T) {
	first := time.Unix(1_700_000_000, 0).UTC()
	second := time.Unix(1_700_000_600, 0).UTC()

	// First death: RK k1 from exchange ex1.
	headers := annotateXDeath(nil, xDeathEvent{
		queue:       "q",
		reason:      DeadLetterRejected,
		exchange:    "ex1",
		routingKeys: []string{"k1"},
		deathTime:   first,
	})

	// Second death of the same (queue, reason): the message came back with RK
	// REWRITTEN to k2 via x-dead-letter-routing-key, from the DLX exchange.
	headers = annotateXDeath(headers, xDeathEvent{
		queue:       "q",
		reason:      DeadLetterRejected,
		exchange:    "dlx",
		routingKeys: []string{"k2"},
		deathTime:   second,
	})

	entries := xDeathEntries(t, headers)
	if len(entries) != 1 {
		t.Fatalf("len(x-death) = %d, want 1 (repeat {queue,reason} aggregates)", len(entries))
	}
	e := entries[0]

	assertEntry(t, e, "q", DeadLetterRejected, 2)

	// D6: the PRIOR entry's fields are preserved VERBATIM — not merged.
	rks, ok := e["routing-keys"].([]interface{})
	if !ok {
		t.Fatalf("routing-keys is %T, want []interface{}", e["routing-keys"])
	}
	if len(rks) != 1 || rks[0] != "k1" {
		t.Errorf("routing-keys = %v, want [k1] (the FIRST death's keys, preserved per R13 — not merged)", rks)
	}
	if ex := e["exchange"]; ex != "ex1" {
		t.Errorf("exchange = %v, want ex1 (the FIRST death's exchange, preserved)", ex)
	}
	if ts, ok := e["time"].(time.Time); !ok || ts.Unix() != first.Unix() {
		t.Errorf("time = %v, want the FIRST death's time %v", e["time"], first.Unix())
	}

	// Head-insertion sanity: only one entry, at index 0, and it IS the
	// aggregated one (count 2).
	assertEntry(t, entries[0], "q", DeadLetterRejected, 2)
}

// TestXDeath_DifferentReasonsSeparateEntries (T13.2): (q, expired) then
// (q, rejected) are TWO entries, newest first.
func TestXDeath_DifferentReasonsSeparateEntries(t *testing.T) {
	first := time.Unix(1_700_000_000, 0).UTC()
	second := time.Unix(1_700_000_600, 0).UTC()

	headers := annotateXDeath(nil, xDeathEvent{
		queue:       "q",
		reason:      DeadLetterExpired,
		exchange:    "ex",
		routingKeys: []string{"k"},
		deathTime:   first,
	})
	headers = annotateXDeath(headers, xDeathEvent{
		queue:       "q",
		reason:      DeadLetterRejected,
		exchange:    "ex",
		routingKeys: []string{"k"},
		deathTime:   second,
	})

	entries := xDeathEntries(t, headers)
	if len(entries) != 2 {
		t.Fatalf("len(x-death) = %d, want 2 (different reasons are separate entries)", len(entries))
	}
	// Newest first (head-insertion).
	assertEntry(t, entries[0], "q", DeadLetterRejected, 1)
	assertEntry(t, entries[1], "q", DeadLetterExpired, 1)
}

// TestXDeath_RepeatKeepsOriginalExpiration: an expired repeat preserves the
// FIRST entry's original-expiration (R13: ttl preserved, not merged).
func TestXDeath_RepeatKeepsOriginalExpiration(t *testing.T) {
	headers := annotateXDeath(nil, xDeathEvent{
		queue:              "q",
		reason:             DeadLetterExpired,
		exchange:           "ex",
		routingKeys:        []string{"k"},
		originalExpiration: "1000",
	})
	headers = annotateXDeath(headers, xDeathEvent{
		queue:              "q",
		reason:             DeadLetterExpired,
		exchange:           "ex",
		routingKeys:        []string{"k"},
		originalExpiration: "999999",
	})

	entries := xDeathEntries(t, headers)
	if len(entries) != 1 {
		t.Fatalf("len(x-death) = %d, want 1", len(entries))
	}
	if v := entries[0]["original-expiration"]; v != "1000" {
		t.Errorf("original-expiration = %v, want \"1000\" (the FIRST death's value, preserved)", v)
	}
	assertEntry(t, entries[0], "q", DeadLetterExpired, 2)
}
