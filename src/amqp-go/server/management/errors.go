// Package management implements the StrangeQ management (control-plane) HTTP
// surface specified in .notes/specs/mgmt-01-server.md: a separate, off-by-
// default, loopback-by-default, auth-gated HTTP listener that serves the
// RabbitMQ-4.4-management-subset API.
//
// It is a CONSUMER of the broker: it holds a *server.Server and discovers the
// optional broker accessors (BOB-1/8/11) through structural interfaces, never
// importing broker internals. When an accessor is absent it reports `null` and
// sets stats_partial rather than fabricating a zero (FR-CONV-041).
package management

import (
	"encoding/json"
	"net/http"
	"strings"
)

// Error tokens (FR-CONV-030/031, PRD 11 §11.4).
const (
	errBadRequest       = "bad_request"
	errInequivalent     = "inequivalent"
	errUnauthorized     = "unauthorized"
	errAccessRefused    = "access_refused"
	errNotFound         = "not_found"
	errMethodNotAllowed = "method_not_allowed"
	errNotAcceptable    = "not_acceptable"
	errConflict         = "conflict"
	errPrecondition     = "precondition_failed"
	errPayloadTooLarge  = "payload_too_large"
	errUnsupportedMedia = "unsupported_media_type"
	errRateLimited      = "rate_limited"
	errInternal         = "internal_error"
	errNotImplemented   = "not_implemented"
	errUnavailable      = "unavailable"
)

// errorEnvelope is the wire shape of every error response.
type errorEnvelope struct {
	Error     string `json:"error"`
	Reason    string `json:"reason"`
	RequestID string `json:"request_id"`
}

// apiError carries a status + token + reason. Handlers return it; the
// middleware renders it. Reason text NEVER distinguishes unknown-user from
// wrong-password (AUTH-006) and never reveals whether a resource exists
// (AUTH-014/SEC-071) — that discipline lives at the construction sites.
type apiError struct {
	status int
	token  string
	reason string
	// allow is the Allow header value for a 405.
	allow string
	// headers are extra response headers to set before the body.
	headers map[string]string
}

func (e *apiError) Error() string { return e.token + ": " + e.reason }

func newAPIError(status int, token, reason string) *apiError {
	return &apiError{status: status, token: token, reason: reason}
}

type errorOption func(*apiError)

func withAllow(allow string) errorOption {
	return func(e *apiError) { e.allow = allow }
}

func withHeader(k, v string) errorOption {
	return func(e *apiError) {
		if e.headers == nil {
			e.headers = make(map[string]string)
		}
		e.headers[k] = v
	}
}

func apiErr(status int, token, reason string, opts ...errorOption) *apiError {
	e := &apiError{status: status, token: token, reason: reason}
	for _, o := range opts {
		o(e)
	}
	return e
}

// Common constructors, kept short because they are used on many paths.
func errUnauth(reason string) *apiError {
	return newAPIError(http.StatusUnauthorized, errUnauthorized, reason)
}
func errRefused(reason string) *apiError {
	return newAPIError(http.StatusForbidden, errAccessRefused, reason)
}
func errNotFoundMsg(reason string) *apiError {
	return newAPIError(http.StatusNotFound, errNotFound, reason)
}
func errBad(reason string) *apiError {
	return newAPIError(http.StatusBadRequest, errBadRequest, reason)
}
func errNotImpl(reason string) *apiError {
	return newAPIError(http.StatusNotImplemented, errNotImplemented, reason)
}
func errUnavailableMsg(reason string) *apiError {
	return newAPIError(http.StatusServiceUnavailable, errUnavailable, reason)
}

// sanitizeField strips ASCII control characters (including CR/LF) and caps the
// length of any attacker-controlled string that could reach a header, a log
// line, or an audit record (T-15/SEC-085). It never returns a value longer than
// max bytes.
func sanitizeField(v string, max int) string {
	if v == "" {
		return ""
	}
	var b strings.Builder
	b.Grow(len(v))
	for i := 0; i < len(v); i++ {
		c := v[i]
		if c < 0x20 || c == 0x7f {
			continue
		}
		b.WriteByte(c)
		if b.Len() >= max {
			break
		}
	}
	return b.String()
}

// marshalCompact is the single JSON encoder for the management surface
// (FR-CONV-046): compact, deterministic, HTML-escaping disabled so the bytes
// are exactly what a client sent/expects. encoding/json emits struct fields in
// declaration order and map keys sorted byte-wise, which is precisely the
// determinism the ETag depends on.
func marshalCompact(v any) ([]byte, error) {
	var b strings.Builder
	enc := json.NewEncoder(&encodeWriter{&b})
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	out := b.String()
	out = strings.TrimSuffix(out, "\n")
	return []byte(out), nil
}

// encodeWriter adapts strings.Builder to io.Writer.
type encodeWriter struct{ b *strings.Builder }

func (w *encodeWriter) Write(p []byte) (int, error) { return w.b.Write(p) }
