package broker

import (
	"errors"
	"fmt"

	"github.com/maxpert/amqp-go/interfaces"
)

// DefaultVHost is used in RabbitMQ-shaped error texts. Vhost scoping is out of
// scope for WS-D; when it lands, callers pass the real vhost.
const DefaultVHost = "/"

// Broker semantic sentinels (WS-D). Each maps to exactly one AMQP channel-error
// code via the server's brokerErrorReply table (server/broker_errors.go,
// mapping M1-M11). errors/errors.go has codes only; the pre-existing sentinels
// live in interfaces/storage.go (ErrQueueNotFound, ErrExchangeNotFound, ...),
// storage_broker.go (ErrNoRoute, ErrQueueClosed, ErrExchangeTypeMismatch),
// queue_policy.go (ErrInvalidQueueArgument) and max_length.go
// (ErrMaxLengthExceeded).
var (
	ErrInequivalentArgs       = errors.New("inequivalent arg")         // 406
	ErrQueueInUse             = errors.New("queue in use")             // 406
	ErrQueueNotEmpty          = errors.New("queue not empty")          // 406
	ErrExchangeInUse          = errors.New("exchange in use")          // 406
	ErrQueueLocked            = errors.New("queue locked")             // 405
	ErrExchangeInternal       = errors.New("exchange is internal")     // 403
	ErrInvalidBindingArgument = errors.New("invalid binding argument") // 406
	ErrQueueLimitReached      = errors.New("queue limit reached")      // 406
	ErrExchangeLimitReached   = errors.New("exchange limit reached")   // 406
)

// SemanticError carries a RabbitMQ-shaped reply text (without the
// "CODE_NAME - " prefix, which the server mapping adds) plus the sentinel(s)
// it classifies as. errors.Is works through Unwrap() []error.
type SemanticError struct {
	Msg       string
	Sentinels []error
}

func (e *SemanticError) Error() string   { return e.Msg }
func (e *SemanticError) Unwrap() []error { return e.Sentinels }

func semErr(msg string, sentinels ...error) error {
	return &SemanticError{Msg: msg, Sentinels: sentinels}
}

func queueRS(name string) string { return fmt.Sprintf("queue '%s' in vhost '%s'", name, DefaultVHost) }
func exchangeRS(name string) string {
	return fmt.Sprintf("exchange '%s' in vhost '%s'", name, DefaultVHost)
}

func errNoQueue(name string) error {
	return semErr("no "+queueRS(name), interfaces.ErrQueueNotFound)
}
func errNoExchange(name string) error {
	return semErr("no "+exchangeRS(name), interfaces.ErrExchangeNotFound)
}

// QueueNotFoundError is exported for the server's non-storage-broker fallback
// (passive declare on a non-storage broker, §1.3/§1.4).
func QueueNotFoundError(name string) error    { return errNoQueue(name) }
func ExchangeNotFoundError(name string) error { return errNoExchange(name) }

// inequivalent builds the RabbitMQ-shaped redeclare-mismatch error (R7 text):
// "inequivalent arg '<k>' for <rs>: received <received> but current is <current>".
// It always classifies as ErrInequivalentArgs; extra sentinels (e.g.
// ErrExchangeTypeMismatch for the exchange type check) stay errors.Is-reachable.
func inequivalent(kind, name, key, received, current string, extra ...error) error {
	rs := queueRS(name)
	if kind == "exchange" {
		rs = exchangeRS(name)
	}
	return semErr(fmt.Sprintf("inequivalent arg '%s' for %s: received %s but current is %s",
		key, rs, received, current), append([]error{ErrInequivalentArgs}, extra...)...)
}
