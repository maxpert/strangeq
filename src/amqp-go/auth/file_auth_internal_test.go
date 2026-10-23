package auth

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/crypto/bcrypt"
)

// TestAuth_UnknownUser_TimingDummy verifies that an unknown user pays the same
// bcrypt work as a known one (against a fixed dummy hash) and returns the same
// error text. The compare hook makes this a mechanism assertion, not a timing
// measurement.
func TestAuth_UnknownUser_TimingDummy(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "auth.json")
	userHash, err := bcrypt.GenerateFromPassword([]byte("pw"), bcrypt.MinCost)
	if err != nil {
		t.Fatalf("hash: %v", err)
	}
	body, err := json.Marshal(AuthFile{Users: []UserEntry{
		{Username: "alice", PasswordHash: string(userHash)},
	}})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := os.WriteFile(path, body, 0600); err != nil {
		t.Fatalf("write: %v", err)
	}

	f, err := NewFileAuthenticator(path)
	if err != nil {
		t.Fatalf("authenticator: %v", err)
	}

	type compareCall struct {
		hash []byte
		pw   []byte
	}
	var calls []compareCall
	f.compare = func(hash, pw []byte) error {
		calls = append(calls, compareCall{hash: append([]byte(nil), hash...), pw: append([]byte(nil), pw...)})
		return bcrypt.ErrMismatchedHashAndPassword
	}

	_, err = f.Authenticate("nobody", "pw")
	if err == nil || err.Error() != "invalid credentials" {
		t.Fatalf("unknown user error = %v, want invalid credentials", err)
	}
	if len(calls) != 1 {
		t.Fatalf("unknown user compare calls = %d, want 1 (dummy hash)", len(calls))
	}
	if string(calls[0].hash) != string(dummyHash()) {
		t.Fatalf("unknown user compared against %q, want dummyHash", calls[0].hash)
	}

	calls = nil
	_, err = f.Authenticate("alice", "wrong")
	if err == nil || err.Error() != "invalid credentials" {
		t.Fatalf("wrong password error = %v, want invalid credentials", err)
	}
	if len(calls) != 1 {
		t.Fatalf("known user compare calls = %d, want 1", len(calls))
	}
	if string(calls[0].hash) != string(userHash) {
		t.Fatalf("known user compared against the wrong hash")
	}
}
