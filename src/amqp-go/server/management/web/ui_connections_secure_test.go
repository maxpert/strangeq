package web

import (
	"strings"
	"testing"

	"github.com/chromedp/chromedp"
)

// ddFor reads the <dd> that follows the <dt> whose text equals label, or
// "<no-<label>-row>" when absent.
func ddForJS(label string) string {
	return `(function(){
	  var dts = document.querySelectorAll('#content .props dt');
	  for (var i = 0; i < dts.length; i++) {
	    if (dts[i].textContent === ` + jsStr(label) + `) return dts[i].nextElementSibling.textContent;
	  }
	  return '<no-' + ` + jsStr(label) + ` + '-row>';
	})()`
}

func jsStr(s string) string {
	return "\"" + strings.ReplaceAll(s, "\"", "\\\"") + "\""
}

// TestUI_ConnectionsSecureIndicator proves the connections screen surfaces the
// transport honestly: a badge (with a text label, not colour alone) only on a
// secure row, and Yes/No plus the auth_mechanism on the detail page.
func TestUI_ConnectionsSecureIndicator(t *testing.T) {
	stub := newStub()
	stub.set(func(s *stubAPI) {
		s.connectionItems = []map[string]any{
			{
				"name": "c-tls", "user": "guest", "vhost": "/", "state": "running",
				"channels": 0, "peer_host": "10.0.0.1", "peer_port": 5671,
				"secure": true, "ssl": true, "auth_mechanism": "PLAIN",
			},
			{
				"name": "c-plain", "user": "guest", "vhost": "/", "state": "running",
				"channels": 0, "peer_host": "10.0.0.2", "peer_port": 5672,
				"secure": false, "ssl": false, "auth_mechanism": "PLAIN",
			},
		}
		s.connectionDetail = map[string]any{
			"name": "c-tls", "user": "guest", "vhost": "/", "state": "running",
			"channels": 0, "protocol": "AMQP 0-9-1", "peer_host": "10.0.0.1", "peer_port": 5671,
			"connected_at": "2026-11-08T00:00:00Z", "last_activity": "2026-11-08T00:00:01Z",
			"secure": true, "ssl": true, "auth_mechanism": "PLAIN",
		}
	})
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()

	mustDo(t, ctx, chromedp.Navigate(srv.URL+"/connections"), chromedp.WaitVisible(chromedp.CSS("#content tbody tr")))

	// Exactly one SSL badge, and it is on the TLS row.
	if n := evalInt(t, ctx, `document.querySelectorAll('#content tbody .ssl-badge').length`); n != 1 {
		t.Fatalf("the connections list must show exactly one SSL badge, found %d", n)
	}
	if n := evalInt(t, ctx, `document.querySelectorAll('#content tbody tr[data-key="c-tls"] .ssl-badge').length`); n != 1 {
		t.Fatalf("the SSL badge must sit on the secure row c-tls")
	}
	if n := evalInt(t, ctx, `document.querySelectorAll('#content tbody tr[data-key="c-plain"] .ssl-badge').length`); n != 0 {
		t.Fatalf("a non-TLS row must not carry an SSL badge")
	}
	// State is not conveyed by colour alone: the badge carries a text label.
	label := strings.TrimSpace(evalStr(t, ctx, `document.querySelector('#content tbody tr[data-key="c-tls"] .ssl-badge').textContent`))
	if label != "SSL" {
		t.Fatalf("SSL badge label = %q, want a visible \"SSL\" text label", label)
	}

	// Detail: the transport flag and the negotiated mechanism are shown.
	mustDo(t, ctx, chromedp.Navigate(srv.URL+"/connections/c-tls"), chromedp.WaitVisible(chromedp.CSS("#content .props")))
	if got := strings.TrimSpace(evalStr(t, ctx, ddForJS("SSL"))); got != "Yes" {
		t.Fatalf("detail SSL row = %q, want Yes on a TLS connection", got)
	}
	if got := strings.TrimSpace(evalStr(t, ctx, ddForJS("auth_mechanism"))); got != "PLAIN" {
		t.Fatalf("detail auth_mechanism row = %q, want PLAIN", got)
	}
}

// TestUI_ConnectionDetailHonestWhenNotSecure proves the detail never fabricates
// transport security: a plaintext connection reports No, and an unauthenticated
// one renders the em-dash convention for auth_mechanism.
func TestUI_ConnectionDetailHonestWhenNotSecure(t *testing.T) {
	stub := newStub()
	stub.set(func(s *stubAPI) {
		s.connectionDetail = map[string]any{
			"name": "c-plain", "user": "guest", "vhost": "/", "state": "running",
			"channels": 0, "protocol": "AMQP 0-9-1", "peer_host": "127.0.0.1", "peer_port": 5672,
			"connected_at": "2026-11-08T00:00:00Z", "last_activity": "2026-11-08T00:00:01Z",
			"secure": false, "ssl": false, "auth_mechanism": "",
		}
	})
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()

	mustDo(t, ctx, chromedp.Navigate(srv.URL+"/connections/c-plain"), chromedp.WaitVisible(chromedp.CSS("#content .props")))
	if got := strings.TrimSpace(evalStr(t, ctx, ddForJS("SSL"))); got != "No" {
		t.Fatalf("detail SSL row = %q, want No on a plaintext connection", got)
	}
	if got := strings.TrimSpace(evalStr(t, ctx, ddForJS("auth_mechanism"))); got != "\u2014" {
		t.Fatalf("detail auth_mechanism row = %q, want an em-dash when empty", got)
	}
}
