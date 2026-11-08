package web

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/chromedp/chromedp"
)

// TestUI_S13_DefinitionsContract is the static half of the S13 pins: the screen
// must issue a dry-run POST, expose the three outcome tables, disclose the
// non-atomic merge, and label the user/permission import unsupported.
func TestUI_S13_DefinitionsContract(t *testing.T) {
	js := string(readAsset(t, "app.js"))
	for _, want := range []string{
		`"/definitions?dry_run=true"`,
		`id: "definitions-file"`,
		`id: "definitions-apply"`,
		`id: "definitions-export"`,
		`"definitions-applied"`,
		`"definitions-skipped"`,
		`"definitions-failed"`,
		`id: "definitions-atomic-note"`,
		`Not supported (BOB-6)`,
		`opts.raw`,
	} {
		if !strings.Contains(js, want) {
			t.Errorf("app.js (S13) is missing %q", want)
		}
	}
	// The export note must reflect strangeq.excluded_sections.
	if !strings.Contains(js, "excluded_sections") {
		t.Error("app.js (S13) must surface strangeq.excluded_sections")
	}
}

// writeDefinitionsFile writes a small definitions document and returns its path.
func writeDefinitionsFile(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "definitions.json")
	body := `{"vhosts":[{"name":"/"}],"exchanges":[{"name":"events","type":"topic"}],"users":[{"name":"guest"}]}`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write definitions file: %v", err)
	}
	return path
}

// TestUI_S13_DefinitionsDryRunThenConfirm drives the S13 screen end to end: a
// file selection issues POST ?dry_run=true and renders the would-be tables, and
// Confirm import re-POSTs the SAME bytes and switches to the real result.
func TestUI_S13_DefinitionsDryRunThenConfirm(t *testing.T) {
	stub := newStub()
	stub.set(func(s *stubAPI) { s.requireLogin = true; s.wantCSRF = true })
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()

	login(t, ctx, srv.URL)

	file := writeDefinitionsFile(t)
	mustDo(t, ctx,
		chromedp.Navigate(srv.URL+"/definitions"),
		chromedp.WaitVisible(chromedp.CSS("#definitions-file")),
		chromedp.SetUploadFiles(chromedp.CSS("#definitions-file"), []string{file}),
		chromedp.WaitVisible(chromedp.CSS("#definitions-result-title")),
	)

	if !waitFor(t, 3*time.Second, func() bool { return stub.querySeen("definitions", "dry_run=true") }) {
		t.Fatalf("selecting a file must issue POST ?dry_run=true; posts=%v", stub.postQueriesFor("definitions"))
	}
	if title := evalStr(t, ctx, `document.getElementById('definitions-result-title').textContent`); !strings.Contains(title, "Dry-run") {
		t.Fatalf("preview title = %q, want a dry-run label", title)
	}
	for _, id := range []string{"definitions-applied", "definitions-skipped"} {
		if n := evalInt(t, ctx, `document.querySelectorAll('#`+id+` .data-table').length`); n != 1 {
			t.Fatalf("%s must render exactly one table (got %d)", id, n)
		}
	}
	// The failed section is always present; an empty one renders the empty
	// state rather than a table.
	if n := evalInt(t, ctx, `document.querySelectorAll('#definitions-failed').length`); n != 1 {
		t.Fatal("the failed section must always be rendered")
	}
	if txt := evalStr(t, ctx, `document.getElementById('definitions-atomic-note').textContent`); !strings.Contains(txt, "not atomic") {
		t.Fatalf("the non-atomic disclosure is missing: %q", txt)
	}
	if txt := evalStr(t, ctx, `document.getElementById('definitions-skipped').textContent`); !strings.Contains(txt, "BOB-6") {
		t.Fatalf("the skipped section must name the unsupported feature (BOB-6): %q", txt)
	}
	if stub.csrfSeen() != "csrf-token-abc" {
		t.Fatalf("the dry-run POST must carry the CSRF token, got %q", stub.csrfSeen())
	}
	body := stub.lastDefinitionsBody()
	if !strings.Contains(body, `"events"`) {
		t.Fatalf("the dry-run must send the file bytes verbatim, got %q", body)
	}

	// Confirm import: the same bytes go to the apply endpoint.
	mustDo(t, ctx, chromedp.Click(chromedp.CSS("#definitions-apply")))
	if !waitFor(t, 3*time.Second, func() bool {
		title, err := evalStrOK(ctx, `document.getElementById('definitions-result-title').textContent`)
		return err == nil && strings.Contains(title, "Import result")
	}) {
		t.Fatalf("Confirm import did not switch to the real result")
	}
	seenApply := false
	for _, q := range stub.postQueriesFor("definitions") {
		if !strings.Contains(q, "dry_run=") {
			seenApply = true
		}
	}
	if !seenApply {
		t.Fatalf("Confirm import must issue a POST without dry_run; posts=%v", stub.postQueriesFor("definitions"))
	}
}

// TestUI_S13_DefinitionsImportErrors surfaces the 413 and 429 reasons verbatim
// (FR-UI-107).
func TestUI_S13_DefinitionsImportErrors(t *testing.T) {
	cases := []struct {
		name   string
		status int
		want   string
	}{
		{"payload_too_large", 413, "payload_too_large"},
		{"rate_limited", 429, "rate_limited"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stub := newStub()
			stub.set(func(s *stubAPI) { s.defsImportStatus = tc.status })
			srv := appServer(t, stub)
			ctx, cancel := newTab(t)
			defer cancel()

			mustDo(t, ctx,
				chromedp.Navigate(srv.URL+"/definitions"),
				chromedp.WaitVisible(chromedp.CSS("#definitions-file")),
				chromedp.SetUploadFiles(chromedp.CSS("#definitions-file"), []string{writeDefinitionsFile(t)}),
				chromedp.WaitVisible(chromedp.CSS("#definitions-preview .notice.permission")),
			)
			txt := evalStr(t, ctx, `document.getElementById('definitions-preview').textContent`)
			if !strings.Contains(txt, tc.want) {
				t.Fatalf("the %s reason must surface verbatim, got %q", tc.want, txt)
			}
		})
	}
}
