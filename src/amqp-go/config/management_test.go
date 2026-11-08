package config

import (
	"os"
	"strings"
	"testing"

	"golang.org/x/crypto/bcrypt"
)

// TestConfig_ManagementDefaults pins the safe defaults: the management surface
// is OFF, loopback, on 15672, with broker-auth inherited (spec §11 test 1).
func TestConfig_ManagementDefaults(t *testing.T) {
	cfg := DefaultConfig()
	m := cfg.Management
	if m.Enabled {
		t.Fatalf("management must be disabled by default (SEC-001), got Enabled=true")
	}
	if m.Address != "127.0.0.1" {
		t.Fatalf("management.address default = %q, want 127.0.0.1 (SEC-002)", m.Address)
	}
	if m.Port != 15672 {
		t.Fatalf("management.port default = %d, want 15672", m.Port)
	}
	if m.Authentication != ManagementAuthenticationInherit {
		t.Fatalf("management.authentication default = %q, want %q", m.Authentication, ManagementAuthenticationInherit)
	}
	if m.AllowUnauthenticated || m.AllowUnauthenticatedMutations || m.AllowConnectionClose {
		t.Fatalf("management allow_* flags must default false")
	}
	if m.Session.IdleTimeoutSeconds != 28800 || m.Session.MaxSessions != 1024 {
		t.Fatalf("session defaults wrong: %+v", m.Session)
	}
	if m.Limits.MaxPageSize != 500 || m.Limits.MaxInFlight != 64 || m.Limits.MaxResponseBytes != 4<<20 {
		t.Fatalf("limits defaults wrong: %+v", m.Limits)
	}
	if m.Async.MessageThreshold != 10000 || m.Async.EntityThreshold != 1000 || m.Async.MaxJobs != 128 {
		t.Fatalf("async defaults wrong: %+v", m.Async)
	}
	if m.Get.MaxCount != 100 {
		t.Fatalf("get.max_count default = %d, want 100", m.Get.MaxCount)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("defaults must validate: %v", err)
	}
}

// TestConfig_RefusesInheritAuthOffWithoutAllow: inherit + broker auth disabled
// without allow_unauthenticated must refuse AND name the exact key
// (AUTH-040/MIL-003, spec §11 test 2).
func TestConfig_RefusesInheritAuthOffWithoutAllow(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Management.Enabled = true
	cfg.Security.AuthenticationEnabled = false
	cfg.Management.Authentication = ManagementAuthenticationInherit

	err := cfg.Validate()
	if err == nil {
		t.Fatalf("expected refusal: inherit auth with broker auth disabled and no allow_unauthenticated")
	}
	if !strings.Contains(err.Error(), "management.allow_unauthenticated") {
		t.Fatalf("refusal must name management.allow_unauthenticated, got %q", err.Error())
	}

	// The opt-in makes it valid on loopback.
	cfg.Management.AllowUnauthenticated = true
	if err := cfg.Validate(); err != nil {
		t.Fatalf("allow_unauthenticated on loopback must validate: %v", err)
	}

	// ...but not on a non-loopback bind.
	cfg.Management.Address = "0.0.0.0"
	if err := cfg.Validate(); err == nil {
		t.Fatalf("allow_unauthenticated must require a loopback bind")
	}
}

// TestConfig_RefusesBasicWithBadHash (AUTH-003, spec §11 test 3).
func TestConfig_RefusesBasicWithBadHash(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Management.Enabled = true
	cfg.Management.Authentication = ManagementAuthenticationBasic

	cfg.Management.Basic.PasswordHash = "not-a-bcrypt-hash"
	if err := cfg.Validate(); err == nil {
		t.Fatalf("expected refusal for a non-bcrypt password_hash")
	}

	hash, err := bcrypt.GenerateFromPassword([]byte("secret"), bcrypt.MinCost)
	if err != nil {
		t.Fatalf("bcrypt: %v", err)
	}
	cfg.Management.Basic.PasswordHash = string(hash)
	cfg.Management.Basic.Username = "admin"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("a real bcrypt hash must validate: %v", err)
	}
}

// TestConfig_RefusesNoneOnNonLoopback (SEC-111, spec §11 test 4).
func TestConfig_RefusesNoneOnNonLoopback(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Management.Enabled = true
	cfg.Management.Authentication = ManagementAuthenticationNone
	cfg.Management.Address = "0.0.0.0"
	if err := cfg.Validate(); err == nil {
		t.Fatalf("authentication \"none\" on a non-loopback address must be refused")
	}
	cfg.Management.Address = "::1"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("authentication \"none\" on ::1 must validate: %v", err)
	}
}

// TestConfig_DisabledIgnoresBadManagement pins the fail-closed note: a disabled
// surface ignores bad management.* values rather than refusing to boot (§2).
func TestConfig_DisabledIgnoresBadManagement(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Management.Enabled = false
	cfg.Management.Authentication = "bogus-mode"
	cfg.Management.Port = 0
	cfg.Management.Basic.PasswordHash = "nonsense"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("disabled management must not fail validation: %v", err)
	}
}

// TestConfig_ManagementEnvBinding proves AMQP_MANAGEMENT_* flows through the
// existing env transform (OPS-030).
func TestConfig_ManagementEnvBinding(t *testing.T) {
	t.Setenv("AMQP_MANAGEMENT_PORT", "15673")
	t.Setenv("AMQP_MANAGEMENT_ENABLED", "true")
	t.Setenv("AMQP_MANAGEMENT_ADDRESS", "127.0.0.1")
	t.Setenv("AMQP_MANAGEMENT_AUTHENTICATION", "none")

	cfg, _, err := LoadConfig("")
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if !cfg.Management.Enabled {
		t.Fatalf("AMQP_MANAGEMENT_ENABLED did not bind")
	}
	if cfg.Management.Port != 15673 {
		t.Fatalf("AMQP_MANAGEMENT_PORT = %d, want 15673", cfg.Management.Port)
	}
	if cfg.Management.Authentication != "none" {
		t.Fatalf("AMQP_MANAGEMENT_AUTHENTICATION = %q", cfg.Management.Authentication)
	}
}

// TestConfig_RequireTagKnobIsUnknownKey pins OPS-030: there is deliberately no
// capability-requirement knob, so management.capabilities.require_tag must be
// reported as an unknown key and ignored.
func TestConfig_RequireTagKnobIsUnknownKey(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/cfg.yaml"
	body := "management:\n  enabled: false\n  capabilities:\n    require_tag: false\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	_, report, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	found := false
	for _, k := range report.UnknownKeys {
		if strings.Contains(k, "capabilities") {
			found = true
		}
	}
	if !found {
		t.Fatalf("management.capabilities.require_tag must be reported as an unknown key, got %v", report.UnknownKeys)
	}
}
