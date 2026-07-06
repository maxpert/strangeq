package broker

// Queue policy resolution (SQ-7 scaffolding).
//
// AMQP 0.9.1 queue.declare carries optional x-arguments in an AMQP field
// table (map[string]interface{}). Wave 2 features (per-message TTL, dead
// lettering, max-length limiting) must not pay for map lookups and type
// assertions per message on the hot path, so the arguments are parsed and
// validated ONCE here, at declare time, into a typed QueuePolicy that is
// attached to the queue's runtime state (QueueState) behind an
// atomic.Pointer. The future hot-path cost is a single atomic load plus a
// nil check.
//
// This file implements resolution and validation only — no policy is
// enforced anywhere yet.

import (
	"errors"
	"fmt"
	"math"
	"time"
)

// ErrInvalidQueueArgument is returned when a known x-argument on
// queue.declare has an invalid type or value. Callers should handle this as
// a channel-level 406 PreconditionFailed per AMQP 0.9.1 semantics (RabbitMQ
// behaves the same way). Unknown x-arguments are ignored, matching RabbitMQ's
// tolerance for plugin-defined arguments.
var ErrInvalidQueueArgument = errors.New("invalid queue argument")

// OverflowBehavior selects what happens when a queue exceeds its configured
// x-max-length / x-max-length-bytes limit (enforcement lands with SQ-11).
type OverflowBehavior uint8

const (
	// OverflowDropHead drops the oldest message(s) to make room — RabbitMQ's
	// default behavior when x-overflow is absent.
	OverflowDropHead OverflowBehavior = iota
	// OverflowRejectPublish rejects new publishes while the queue is full.
	OverflowRejectPublish
)

func (o OverflowBehavior) String() string {
	switch o {
	case OverflowDropHead:
		return "drop-head"
	case OverflowRejectPublish:
		return "reject-publish"
	default:
		return fmt.Sprintf("OverflowBehavior(%d)", uint8(o))
	}
}

// QueuePolicy holds the parsed, validated queue x-arguments. A nil
// *QueuePolicy means "no policy" — queues declared without any known
// x-argument never allocate one, so the hot path stays a nil-pointer branch.
//
// Duration/limit fields are paired with Has* flags because zero is a valid
// configured value for several arguments (e.g. x-message-ttl: 0 means
// "expire immediately", distinct from "no TTL").
type QueuePolicy struct {
	// MessageTTL is the per-message time-to-live (x-message-ttl, milliseconds
	// on the wire). Valid only when HasMessageTTL is true. Enforced by SQ-9.
	MessageTTL    time.Duration
	HasMessageTTL bool

	// QueueExpires is the queue auto-expiry after disuse (x-expires,
	// milliseconds on the wire, must be positive). Valid only when
	// HasQueueExpires is true. Enforced by SQ-9.
	QueueExpires    time.Duration
	HasQueueExpires bool

	// DeadLetterExchange / DeadLetterRoutingKey configure dead lettering
	// (x-dead-letter-exchange / x-dead-letter-routing-key). Enforced by SQ-10.
	DeadLetterExchange      string
	HasDeadLetterExchange   bool
	DeadLetterRoutingKey    string
	HasDeadLetterRoutingKey bool

	// MaxLength / MaxLengthBytes cap queue depth (x-max-length /
	// x-max-length-bytes, non-negative). Enforced by SQ-11.
	MaxLength         int64
	HasMaxLength      bool
	MaxLengthBytes    int64
	HasMaxLengthBytes bool

	// Overflow selects the over-limit behavior (x-overflow). Defaults to
	// OverflowDropHead like RabbitMQ. Meaningful once SQ-11 lands.
	Overflow OverflowBehavior
}

// ResolveQueuePolicy parses and validates the known queue x-arguments from an
// AMQP field table. It returns (nil, nil) when no known argument is present
// (including nil/empty tables), a populated policy when at least one known
// argument is present and valid, and an error wrapping ErrInvalidQueueArgument
// when a known argument has an invalid type or value.
//
// Unknown arguments are ignored: RabbitMQ tolerates unknown x-args (plugins
// define their own), and rejecting them would break clients that set, e.g.,
// x-queue-type.
//
// This is the single resolution point: both the client declare path and the
// durable-queue recovery path route through StorageBroker.DeclareQueue, which
// calls this function.
func ResolveQueuePolicy(arguments map[string]interface{}) (*QueuePolicy, error) {
	if len(arguments) == 0 {
		return nil, nil
	}

	var policy QueuePolicy
	found := false

	if v, ok := arguments["x-message-ttl"]; ok {
		ttl, err := tableInt64(v)
		if err != nil || ttl < 0 {
			return nil, invalidArg("x-message-ttl", "a non-negative integer (milliseconds)", v)
		}
		policy.MessageTTL = time.Duration(ttl) * time.Millisecond
		policy.HasMessageTTL = true
		found = true
	}

	if v, ok := arguments["x-expires"]; ok {
		exp, err := tableInt64(v)
		if err != nil || exp <= 0 {
			return nil, invalidArg("x-expires", "a positive integer (milliseconds)", v)
		}
		policy.QueueExpires = time.Duration(exp) * time.Millisecond
		policy.HasQueueExpires = true
		found = true
	}

	if v, ok := arguments["x-dead-letter-exchange"]; ok {
		s, ok := v.(string)
		if !ok {
			return nil, invalidArg("x-dead-letter-exchange", "a string", v)
		}
		policy.DeadLetterExchange = s
		policy.HasDeadLetterExchange = true
		found = true
	}

	if v, ok := arguments["x-dead-letter-routing-key"]; ok {
		s, ok := v.(string)
		if !ok {
			return nil, invalidArg("x-dead-letter-routing-key", "a string", v)
		}
		if !policy.HasDeadLetterExchange {
			return nil, fmt.Errorf("%w: x-dead-letter-routing-key requires x-dead-letter-exchange to be set",
				ErrInvalidQueueArgument)
		}
		policy.DeadLetterRoutingKey = s
		policy.HasDeadLetterRoutingKey = true
		found = true
	}

	if v, ok := arguments["x-max-length"]; ok {
		n, err := tableInt64(v)
		if err != nil || n < 0 {
			return nil, invalidArg("x-max-length", "a non-negative integer", v)
		}
		policy.MaxLength = n
		policy.HasMaxLength = true
		found = true
	}

	if v, ok := arguments["x-max-length-bytes"]; ok {
		n, err := tableInt64(v)
		if err != nil || n < 0 {
			return nil, invalidArg("x-max-length-bytes", "a non-negative integer", v)
		}
		policy.MaxLengthBytes = n
		policy.HasMaxLengthBytes = true
		found = true
	}

	if v, ok := arguments["x-overflow"]; ok {
		s, ok := v.(string)
		if !ok {
			return nil, invalidArg("x-overflow", `"drop-head" or "reject-publish"`, v)
		}
		switch s {
		case "drop-head":
			policy.Overflow = OverflowDropHead
		case "reject-publish":
			policy.Overflow = OverflowRejectPublish
		default:
			return nil, invalidArg("x-overflow", `"drop-head" or "reject-publish"`, v)
		}
		found = true
	}

	if !found {
		return nil, nil
	}
	return &policy, nil
}

// invalidArg builds a PRECONDITION_FAILED-style error for a known x-argument
// with an invalid type or value.
func invalidArg(key, want string, got interface{}) error {
	return fmt.Errorf("%w: %s must be %s, got %v (%T)", ErrInvalidQueueArgument, key, want, got, got)
}

// errNotInteger is an internal sentinel from tableInt64; callers wrap it into
// an ErrInvalidQueueArgument with argument context.
var errNotInteger = errors.New("not an integer")

// tableInt64 coerces an AMQP field-table value to int64. Client libraries
// encode table integers as any of Go's integer widths depending on magnitude
// and encoder choices, and the durable-metadata JSON round-trip on recovery
// turns numbers into float64 — so all reasonable numeric encodings must be
// accepted. Floats are accepted only when integral; uint64 values above
// math.MaxInt64 are rejected as overflow.
func tableInt64(v interface{}) (int64, error) {
	switch n := v.(type) {
	case int:
		return int64(n), nil
	case int8:
		return int64(n), nil
	case int16:
		return int64(n), nil
	case int32:
		return int64(n), nil
	case int64:
		return n, nil
	case uint:
		if uint64(n) > math.MaxInt64 {
			return 0, errNotInteger
		}
		return int64(n), nil
	case uint8:
		return int64(n), nil
	case uint16:
		return int64(n), nil
	case uint32:
		return int64(n), nil
	case uint64:
		if n > math.MaxInt64 {
			return 0, errNotInteger
		}
		return int64(n), nil
	case float32:
		f := float64(n)
		// float64(math.MaxInt64) rounds up to 2^63, so >= rejects overflow.
		if f != math.Trunc(f) || f < math.MinInt64 || f >= math.MaxInt64 {
			return 0, errNotInteger
		}
		return int64(f), nil
	case float64:
		if n != math.Trunc(n) || n < math.MinInt64 || n >= math.MaxInt64 {
			return 0, errNotInteger
		}
		return int64(n), nil
	default:
		return 0, errNotInteger
	}
}
