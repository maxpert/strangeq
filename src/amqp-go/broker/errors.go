package broker

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
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

// fmtBool formats a flag field the way RabbitMQ's val/1 prints a bare value
// (R7): 'true' / 'false'.
func fmtBool(v bool) string { return "'" + strconv.FormatBool(v) + "'" }

// fmtBareVal prints a bare (non-table) value for an inequivalence message
// (R7 val/1): '<v>'.
func fmtBareVal(v string) string { return "'" + v + "'" }

// fmtArgVal formats a redeclare argument value per R7 val/1: absent → none;
// present → the value '<v>' of type '<t>', where <v> is the string, the
// decimal integer, or strconv.FormatFloat(f,'g',-1,64), and <t> is the AMQP
// type name (amqpTypeName).
func fmtArgVal(v interface{}, present bool) string {
	if !present {
		return "none"
	}
	return fmt.Sprintf("the value '%s' of type '%s'", argValueString(v), amqpTypeName(v))
}

// argValueString renders the value half of R7's typed val/1.
func argValueString(v interface{}) string {
	switch n := v.(type) {
	case string:
		return n
	case []byte:
		return string(n)
	case int8:
		return strconv.FormatInt(int64(n), 10)
	case uint8:
		return strconv.FormatUint(uint64(n), 10)
	case int16:
		return strconv.FormatInt(int64(n), 10)
	case uint16:
		return strconv.FormatUint(uint64(n), 10)
	case int32:
		return strconv.FormatInt(int64(n), 10)
	case uint32:
		return strconv.FormatUint(uint64(n), 10)
	case int64:
		return strconv.FormatInt(n, 10)
	case int:
		return strconv.FormatInt(int64(n), 10)
	case uint64:
		return strconv.FormatUint(n, 10)
	case uint:
		return strconv.FormatUint(uint64(n), 10)
	case float32:
		return strconv.FormatFloat(float64(n), 'g', -1, 64)
	case float64:
		return strconv.FormatFloat(n, 'g', -1, 64)
	case bool:
		return strconv.FormatBool(n)
	case time.Time:
		return strconv.FormatInt(n.Unix(), 10)
	case nil:
		return ""
	default:
		return fmt.Sprintf("%v", v)
	}
}

// amqpTypeName maps a decoded Go field value to its AMQP wire type name, per
// R7's printed type tags.
func amqpTypeName(v interface{}) string {
	switch v.(type) {
	case string:
		return "longstr"
	case []byte:
		return "binary"
	case int8:
		return "byte"
	case uint8:
		return "unsignedbyte"
	case int16:
		return "short"
	case uint16:
		return "unsignedshort"
	case int32:
		return "signedint"
	case uint32:
		return "unsignedint"
	case int64, int, uint64, uint:
		return "long"
	case float32:
		return "float"
	case float64:
		return "double"
	case bool:
		return "bool"
	case time.Time:
		return "timestamp"
	case map[string]interface{}, map[interface{}]interface{}:
		return "table"
	case []interface{}:
		return "array"
	case nil:
		return "void"
	case protocol.Decimal:
		return "decimal"
	default:
		return "unknown"
	}
}
