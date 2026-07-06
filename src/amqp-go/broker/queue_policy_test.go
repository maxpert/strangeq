package broker

import (
	"errors"
	"testing"
	"time"
)

func TestResolveQueuePolicy_NilAndEmptyArguments(t *testing.T) {
	for _, args := range []map[string]interface{}{nil, {}} {
		policy, err := ResolveQueuePolicy(args)
		if err != nil {
			t.Fatalf("expected no error for %v, got %v", args, err)
		}
		if policy != nil {
			t.Fatalf("expected nil policy for %v, got %+v", args, policy)
		}
	}
}

func TestResolveQueuePolicy_UnknownArgumentsIgnored(t *testing.T) {
	policy, err := ResolveQueuePolicy(map[string]interface{}{
		"x-queue-type":             "classic", // known to RabbitMQ, not to us — must be tolerated
		"x-custom-plugin":          int32(42),
		"some-random-key":          "value",
		"x-max-priority":           int32(10),
		"x-single-active-consumer": true,
	})
	if err != nil {
		t.Fatalf("unknown arguments must be ignored, got error: %v", err)
	}
	if policy != nil {
		t.Fatalf("expected nil policy when only unknown arguments present, got %+v", policy)
	}
}

func TestResolveQueuePolicy_ValidFullSet(t *testing.T) {
	policy, err := ResolveQueuePolicy(map[string]interface{}{
		"x-message-ttl":             int32(60000),
		"x-expires":                 int64(1800000),
		"x-dead-letter-exchange":    "dlx",
		"x-dead-letter-routing-key": "dead",
		"x-max-length":              int32(1000),
		"x-max-length-bytes":        int64(1048576),
		"x-overflow":                "reject-publish",
		"x-unknown-extension":       "ignored",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if policy == nil {
		t.Fatal("expected non-nil policy")
	}
	if !policy.HasMessageTTL || policy.MessageTTL != 60*time.Second {
		t.Errorf("MessageTTL = (%v, %v), want (60s, true)", policy.MessageTTL, policy.HasMessageTTL)
	}
	if !policy.HasQueueExpires || policy.QueueExpires != 30*time.Minute {
		t.Errorf("QueueExpires = (%v, %v), want (30m, true)", policy.QueueExpires, policy.HasQueueExpires)
	}
	if !policy.HasDeadLetterExchange || policy.DeadLetterExchange != "dlx" {
		t.Errorf("DeadLetterExchange = (%q, %v), want (dlx, true)", policy.DeadLetterExchange, policy.HasDeadLetterExchange)
	}
	if !policy.HasDeadLetterRoutingKey || policy.DeadLetterRoutingKey != "dead" {
		t.Errorf("DeadLetterRoutingKey = (%q, %v), want (dead, true)", policy.DeadLetterRoutingKey, policy.HasDeadLetterRoutingKey)
	}
	if !policy.HasMaxLength || policy.MaxLength != 1000 {
		t.Errorf("MaxLength = (%d, %v), want (1000, true)", policy.MaxLength, policy.HasMaxLength)
	}
	if !policy.HasMaxLengthBytes || policy.MaxLengthBytes != 1048576 {
		t.Errorf("MaxLengthBytes = (%d, %v), want (1048576, true)", policy.MaxLengthBytes, policy.HasMaxLengthBytes)
	}
	if policy.Overflow != OverflowRejectPublish {
		t.Errorf("Overflow = %v, want OverflowRejectPublish", policy.Overflow)
	}
}

func TestResolveQueuePolicy_DefaultOverflowIsDropHead(t *testing.T) {
	policy, err := ResolveQueuePolicy(map[string]interface{}{
		"x-max-length": int32(10),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if policy == nil {
		t.Fatal("expected non-nil policy")
	}
	if policy.Overflow != OverflowDropHead {
		t.Errorf("Overflow default = %v, want OverflowDropHead", policy.Overflow)
	}
}

// TestResolveQueuePolicy_NumericEncodings verifies every reasonable numeric
// Go type an AMQP table decoder (or a JSON metadata round-trip on recovery)
// can produce is accepted for integer arguments.
func TestResolveQueuePolicy_NumericEncodings(t *testing.T) {
	encodings := []struct {
		name  string
		value interface{}
	}{
		{"int", int(5000)},
		{"int8", int8(100)},
		{"int16", int16(5000)},
		{"int32", int32(5000)},
		{"int64", int64(5000)},
		{"uint", uint(5000)},
		{"uint8", uint8(100)},
		{"uint16", uint16(5000)},
		{"uint32", uint32(5000)},
		{"uint64", uint64(5000)},
		{"float32", float32(5000)},
		{"float64", float64(5000)}, // JSON metadata recovery produces float64
	}
	for _, enc := range encodings {
		t.Run(enc.name, func(t *testing.T) {
			policy, err := ResolveQueuePolicy(map[string]interface{}{
				"x-message-ttl": enc.value,
			})
			if err != nil {
				t.Fatalf("encoding %s rejected: %v", enc.name, err)
			}
			if policy == nil || !policy.HasMessageTTL {
				t.Fatalf("encoding %s: policy missing TTL", enc.name)
			}
			var want time.Duration
			switch enc.value.(type) {
			case int8, uint8:
				want = 100 * time.Millisecond
			default:
				want = 5 * time.Second
			}
			if policy.MessageTTL != want {
				t.Errorf("encoding %s: TTL = %v, want %v", enc.name, policy.MessageTTL, want)
			}
		})
	}
}

func TestResolveQueuePolicy_InvalidArguments(t *testing.T) {
	cases := []struct {
		name string
		args map[string]interface{}
	}{
		{"ttl wrong type string", map[string]interface{}{"x-message-ttl": "60000"}},
		{"ttl wrong type bool", map[string]interface{}{"x-message-ttl": true}},
		{"ttl negative", map[string]interface{}{"x-message-ttl": int32(-1)}},
		{"ttl negative int64", map[string]interface{}{"x-message-ttl": int64(-5000)}},
		{"ttl fractional float", map[string]interface{}{"x-message-ttl": float64(100.5)}},
		{"ttl uint64 overflow", map[string]interface{}{"x-message-ttl": uint64(1) << 63}},
		{"expires wrong type", map[string]interface{}{"x-expires": []byte("soon")}},
		{"expires zero", map[string]interface{}{"x-expires": int32(0)}},
		{"expires negative", map[string]interface{}{"x-expires": int32(-10)}},
		{"dlx wrong type", map[string]interface{}{"x-dead-letter-exchange": int32(1)}},
		{"dlrk wrong type", map[string]interface{}{"x-dead-letter-exchange": "dlx", "x-dead-letter-routing-key": int32(1)}},
		{"dlrk without dlx", map[string]interface{}{"x-dead-letter-routing-key": "dead"}},
		{"max-length wrong type", map[string]interface{}{"x-max-length": "1000"}},
		{"max-length negative", map[string]interface{}{"x-max-length": int32(-1)}},
		{"max-length-bytes wrong type", map[string]interface{}{"x-max-length-bytes": false}},
		{"max-length-bytes negative", map[string]interface{}{"x-max-length-bytes": int64(-1024)}},
		{"overflow wrong type", map[string]interface{}{"x-overflow": int32(1)}},
		{"overflow unknown value", map[string]interface{}{"x-overflow": "explode"}},
		{"overflow reject-publish-dlx unsupported", map[string]interface{}{"x-overflow": "reject-publish-dlx"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			policy, err := ResolveQueuePolicy(tc.args)
			if err == nil {
				t.Fatalf("expected error for %v, got policy %+v", tc.args, policy)
			}
			if !errors.Is(err, ErrInvalidQueueArgument) {
				t.Errorf("error must wrap ErrInvalidQueueArgument, got %v", err)
			}
			if policy != nil {
				t.Errorf("expected nil policy on error, got %+v", policy)
			}
		})
	}
}

func TestResolveQueuePolicy_ValidBoundaryValues(t *testing.T) {
	// TTL of 0 is valid per RabbitMQ (expire immediately); max-length 0 is valid.
	policy, err := ResolveQueuePolicy(map[string]interface{}{
		"x-message-ttl":      int32(0),
		"x-max-length":       int32(0),
		"x-max-length-bytes": int32(0),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !policy.HasMessageTTL || policy.MessageTTL != 0 {
		t.Errorf("TTL 0 should be valid and present, got (%v, %v)", policy.MessageTTL, policy.HasMessageTTL)
	}
	if !policy.HasMaxLength || policy.MaxLength != 0 {
		t.Errorf("MaxLength 0 should be valid and present, got (%d, %v)", policy.MaxLength, policy.HasMaxLength)
	}
	if !policy.HasMaxLengthBytes || policy.MaxLengthBytes != 0 {
		t.Errorf("MaxLengthBytes 0 should be valid and present, got (%d, %v)", policy.MaxLengthBytes, policy.HasMaxLengthBytes)
	}
}

func TestResolveQueuePolicy_DLXWithoutRoutingKey(t *testing.T) {
	policy, err := ResolveQueuePolicy(map[string]interface{}{
		"x-dead-letter-exchange": "dlx",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !policy.HasDeadLetterExchange || policy.DeadLetterExchange != "dlx" {
		t.Errorf("DeadLetterExchange = (%q, %v), want (dlx, true)", policy.DeadLetterExchange, policy.HasDeadLetterExchange)
	}
	if policy.HasDeadLetterRoutingKey {
		t.Error("DeadLetterRoutingKey should be absent")
	}
}

func TestResolveQueuePolicy_OverflowDropHeadExplicit(t *testing.T) {
	policy, err := ResolveQueuePolicy(map[string]interface{}{
		"x-overflow": "drop-head",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if policy == nil {
		t.Fatal("expected non-nil policy: x-overflow alone is a known argument")
	}
	if policy.Overflow != OverflowDropHead {
		t.Errorf("Overflow = %v, want OverflowDropHead", policy.Overflow)
	}
}
