package management

import (
	"context"
	"net/http"
)

// ctxKey is the private context-key type for request-scoped values.
type ctxKey int

const (
	ctxKeyRequestID ctxKey = iota
	ctxKeyPrincipal
	ctxKeyEndpoint
	ctxKeyClient
)

func withRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, ctxKeyRequestID, id)
}

// requestID returns the correlation id for r, or "" when the middleware has not
// run (unit tests that call a handler directly).
func requestID(r *http.Request) string {
	if v, ok := r.Context().Value(ctxKeyRequestID).(string); ok {
		return v
	}
	return r.Header.Get("X-Request-Id")
}

func withPrincipal(ctx context.Context, p *Principal) context.Context {
	return context.WithValue(ctx, ctxKeyPrincipal, p)
}

// principalFrom returns the authenticated principal, or nil.
func principalFrom(r *http.Request) *Principal {
	if v, ok := r.Context().Value(ctxKeyPrincipal).(*Principal); ok {
		return v
	}
	return nil
}

func withEndpoint(ctx context.Context, label string) context.Context {
	return context.WithValue(ctx, ctxKeyEndpoint, label)
}

// endpointLabel returns the bounded route template for metrics (OPS-004).
func endpointLabel(r *http.Request) string {
	if v, ok := r.Context().Value(ctxKeyEndpoint).(string); ok {
		return v
	}
	return "other"
}

func withClient(ctx context.Context, c clientInfo) context.Context {
	return context.WithValue(ctx, ctxKeyClient, c)
}

func clientFrom(r *http.Request) clientInfo {
	if v, ok := r.Context().Value(ctxKeyClient).(clientInfo); ok {
		return v
	}
	return clientInfo{}
}
