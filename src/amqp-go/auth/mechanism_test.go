package auth

import (
	"testing"
)

func TestRegistryForMechanisms_Strict(t *testing.T) {
	t.Run("ANONYMOUS only", func(t *testing.T) {
		registry := RegistryForMechanisms([]string{"ANONYMOUS"})
		if _, err := registry.Get("PLAIN"); err == nil {
			t.Fatal("PLAIN must not be registered when authmechanisms lists only ANONYMOUS")
		}
		if _, err := registry.Get("ANONYMOUS"); err != nil {
			t.Fatalf("ANONYMOUS should be registered: %v", err)
		}
	})

	t.Run("unknown names are reported and skipped", func(t *testing.T) {
		registry, unknown := RegistryForMechanismsReport([]string{"PLAIN", "EXTERNAL"})
		if len(unknown) != 1 || unknown[0] != "EXTERNAL" {
			t.Fatalf("unknown = %v, want [EXTERNAL]", unknown)
		}
		if _, err := registry.Get("EXTERNAL"); err == nil {
			t.Fatal("EXTERNAL must not be registered")
		}
		if _, err := registry.Get("PLAIN"); err != nil {
			t.Fatalf("PLAIN should be registered: %v", err)
		}
	})

	t.Run("all-unknown list still falls back to PLAIN", func(t *testing.T) {
		// Spec §10.4: if the result is empty, register PLAIN. A config of
		// authmechanisms: [EXTERNAL] must not leave the broker advertising zero
		// mechanisms and refusing every login.
		registry, unknown := RegistryForMechanismsReport([]string{"EXTERNAL"})
		if len(unknown) != 1 || unknown[0] != "EXTERNAL" {
			t.Fatalf("unknown = %v, want [EXTERNAL]", unknown)
		}
		if _, err := registry.Get("PLAIN"); err != nil {
			t.Fatal("an all-unknown list must fall back to PLAIN so the broker can still authenticate")
		}
		if _, err := registry.Get("ANONYMOUS"); err == nil {
			t.Fatal("ANONYMOUS must not be implied by the fallback")
		}
	})

	t.Run("nil list defaults to PLAIN", func(t *testing.T) {
		registry, unknown := RegistryForMechanismsReport(nil)
		if len(unknown) != 0 {
			t.Fatalf("unknown = %v, want none", unknown)
		}
		if _, err := registry.Get("PLAIN"); err != nil {
			t.Fatalf("PLAIN should be the default: %v", err)
		}
		if _, err := registry.Get("ANONYMOUS"); err == nil {
			t.Fatal("ANONYMOUS must not be a default")
		}
	})

	t.Run("case insensitive", func(t *testing.T) {
		registry := RegistryForMechanisms([]string{"plain", "anonymous"})
		if _, err := registry.Get("PLAIN"); err != nil {
			t.Fatal("lowercase plain should register PLAIN")
		}
		if _, err := registry.Get("ANONYMOUS"); err != nil {
			t.Fatal("lowercase anonymous should register ANONYMOUS")
		}
	})

	t.Run("List is sorted", func(t *testing.T) {
		registry := RegistryForMechanisms([]string{"ANONYMOUS", "PLAIN"})
		got := registry.List()
		want := []string{"ANONYMOUS", "PLAIN"}
		if len(got) != len(want) {
			t.Fatalf("List() = %v, want %v", got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("List() = %v, want sorted %v", got, want)
			}
		}
	})
}
