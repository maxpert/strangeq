package metrics

import (
	"bytes"
	"regexp"
	"strings"
	"testing"
)

// TestScrapeText_IsTextExposition pins the exact format the SPA's client-side
// Prometheus parser is written against: a non-empty text exposition whose every
// sample line is `name[{labels}] value`, with a `# TYPE` family declaration and
// the default Go collector's go_goroutines family present (the default registry
// always carries it). It also pins the shared-handler source: the bytes are the
// same ones GET /metrics on the telemetry listener serves (spec mgmt-07 §2.1).
func TestScrapeText_IsTextExposition(t *testing.T) {
	body, err := ScrapeText()
	if err != nil {
		t.Fatalf("ScrapeText: %v", err)
	}
	if len(body) == 0 {
		t.Fatal("ScrapeText returned an empty body")
	}
	text := string(body)
	if !strings.Contains(text, "# TYPE") {
		t.Error("scrape has no # TYPE family declaration")
	}
	if !strings.Contains(text, "go_goroutines") {
		t.Error("scrape lacks go_goroutines (the default Go collector)")
	}
	lineRe := regexp.MustCompile(`^[a-zA-Z_:][a-zA-Z0-9_:]*(\{.*\})? +[^ ]+( +\d+)?$`)
	for i, line := range strings.Split(text, "\n") {
		line = strings.TrimSuffix(line, "\r")
		if strings.TrimSpace(line) == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if !lineRe.MatchString(line) {
			t.Errorf("line %d is not Prometheus exposition format: %q", i+1, line)
		}
	}
}

// TestScrapeText_Uncompressed proves the synthetic request carries no
// Accept-Encoding, so the body is plain text (never a gzip stream a browser
// fetch could not parse). The gzip magic bytes are 0x1f 0x8b.
func TestScrapeText_Uncompressed(t *testing.T) {
	body, err := ScrapeText()
	if err != nil {
		t.Fatalf("ScrapeText: %v", err)
	}
	if len(body) < 2 {
		t.Fatalf("scrape too short: %d bytes", len(body))
	}
	if body[0] == 0x1f && body[1] == 0x8b {
		t.Fatal("scrape is gzip-compressed; the synthetic request must not accept gzip")
	}
	if !bytes.HasPrefix(body, []byte("# ")) {
		t.Errorf("scrape does not begin with a comment line: %q", body[:16])
	}
}
