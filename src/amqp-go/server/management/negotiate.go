package management

import (
	"encoding/base64"
	"net/http"
	"strings"
)

// base64String is the standard-base64 wire form for bytes (FR-CONV-047).
func base64String(b []byte) string { return base64.StdEncoding.EncodeToString(b) }

// acceptsJSON reports whether the request's Accept header permits
// application/json. An absent Accept (or */*) permits it; an Accept that names
// media types but excludes JSON ⇒ 406 (FR-CONV-010).
func acceptsJSON(r *http.Request) bool {
	accept := strings.TrimSpace(r.Header.Get("Accept"))
	if accept == "" {
		return true
	}
	for _, part := range strings.Split(accept, ",") {
		mt := strings.TrimSpace(part)
		if i := strings.Index(mt, ";"); i >= 0 {
			mt = strings.TrimSpace(mt[:i])
		}
		switch {
		case mt == "*/*", mt == "application/*":
			return true
		case strings.HasPrefix(mt, "application/json"):
			return true
		}
	}
	return false
}

// contentTypeIsJSON reports whether a request body declares JSON. An absent
// Content-Type is accepted for backward compatibility with clients that omit
// it on an empty body.
func contentTypeIsJSON(r *http.Request) bool {
	ct := r.Header.Get("Content-Type")
	if ct == "" {
		return true
	}
	if i := strings.Index(ct, ";"); i >= 0 {
		ct = ct[:i]
	}
	ct = strings.TrimSpace(strings.ToLower(ct))
	return ct == "application/json" || ct == "application/*"
}

// isMultipart reports whether the request is a multipart form (definitions
// import only).
func isMultipart(r *http.Request) bool {
	return strings.HasPrefix(strings.ToLower(r.Header.Get("Content-Type")), "multipart/form-data")
}

// negotiationError returns a 406/415 apiError, or nil when the request is
// acceptable. Definitions import additionally accepts multipart/form-data.
func negotiationError(r *http.Request, allowMultipart bool) *apiError {
	if !acceptsJSON(r) {
		return newAPIError(http.StatusNotAcceptable, errNotAcceptable,
			"only application/json is served by the management API")
	}
	if r.ContentLength != 0 || hasBodyMethod(r.Method) {
		if !contentTypeIsJSON(r) {
			if allowMultipart && isMultipart(r) {
				return nil
			}
			return newAPIError(http.StatusUnsupportedMediaType, errUnsupportedMedia,
				"request Content-Type must be application/json")
		}
	}
	return nil
}

func hasBodyMethod(method string) bool {
	switch method {
	case http.MethodPost, http.MethodPut, http.MethodPatch:
		return true
	}
	return false
}
