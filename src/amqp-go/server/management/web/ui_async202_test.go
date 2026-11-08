package web

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/chromedp/chromedp"
)

// TestUI_Purge202_AsyncNotice (mgmt-01 §7.2 async purge): a 202 + Location from
// DELETE /api/queues/{vhost}/{name}/contents is NOT a purge of 0 messages. The
// SPA must render the "accepted as a background job" notice linking to the job
// endpoint, and must never show a "Purged …" count.
func TestUI_Purge202_AsyncNotice(t *testing.T) {
	stub := newStub()
	stub.set(func(s *stubAPI) {
		s.purgeStatus = http.StatusAccepted
		s.purgeLocation = "/api/strangeq/jobs/purge-1"
	})
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()

	mustDo(t, ctx,
		chromedp.Navigate(srv.URL+"/queues/orders"),
		chromedp.WaitVisible(chromedp.CSS("#purge-btn")),
		chromedp.Click(chromedp.CSS("#purge-btn")),
		chromedp.WaitVisible(chromedp.CSS("#modal-confirm")),
		chromedp.Click(chromedp.CSS("#purge-ack")),
		chromedp.Click(chromedp.CSS("#modal-confirm")),
		chromedp.WaitVisible(chromedp.CSS("#toast-root .notice.job-accepted")),
	)

	if !waitFor(t, 3*time.Second, func() bool {
		return stub.pathSeen("queues", "/api/queues/%2F/orders/contents")
	}) {
		t.Fatalf("the SPA did not issue the purge DELETE; paths=%v", stub.pathsFor("queues"))
	}

	txt := evalStr(t, ctx, `document.getElementById('toast-root').textContent`)
	if !strings.Contains(txt, "Accepted as a background job") {
		t.Fatalf("the 202 notice is missing: %q", txt)
	}
	if strings.Contains(txt, "Purged") {
		t.Fatalf("a 202 must not claim a purge count: %q", txt)
	}
	href := evalStr(t, ctx, `document.querySelector('#toast-root .notice.job-accepted a').getAttribute('href')`)
	if href != "/api/strangeq/jobs/purge-1" {
		t.Fatalf("the notice must link to the job endpoint, got %q", href)
	}
}

// TestUI_DefinitionsImport202_AsyncNotice (mgmt-05 async import): a 202 +
// Location from POST /api/definitions is not a completed merge. The SPA must
// render the async notice and never show "Import complete".
func TestUI_DefinitionsImport202_AsyncNotice(t *testing.T) {
	stub := newStub()
	stub.set(func(s *stubAPI) {
		s.defsImportStatus = http.StatusAccepted
		s.defsApplyLocation = "/api/strangeq/jobs/definitions-1"
	})
	srv := appServer(t, stub)
	ctx, cancel := newTab(t)
	defer cancel()

	mustDo(t, ctx,
		chromedp.Navigate(srv.URL+"/definitions"),
		chromedp.WaitVisible(chromedp.CSS("#definitions-file")),
		chromedp.SetUploadFiles(chromedp.CSS("#definitions-file"), []string{writeDefinitionsFile(t)}),
		chromedp.WaitVisible(chromedp.CSS("#definitions-result-title")),
		chromedp.Click(chromedp.CSS("#definitions-apply")),
		chromedp.WaitVisible(chromedp.CSS("#definitions-preview .notice.job-accepted")),
	)

	txt := evalStr(t, ctx, `document.getElementById('toast-root').textContent`)
	if strings.Contains(txt, "Import complete") {
		t.Fatalf("a 202 must not claim a completed import: %q", txt)
	}
	preview := evalStr(t, ctx, `document.getElementById('definitions-preview').textContent`)
	if !strings.Contains(preview, "Accepted as a background job") {
		t.Fatalf("the 202 import notice is missing: %q", preview)
	}
	href := evalStr(t, ctx, `document.querySelector('#definitions-preview .notice.job-accepted a').getAttribute('href')`)
	if href != "/api/strangeq/jobs/definitions-1" {
		t.Fatalf("the notice must link to the job endpoint, got %q", href)
	}
}
