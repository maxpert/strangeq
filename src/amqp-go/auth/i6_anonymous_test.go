package auth

import (
	"strings"
	"testing"

	"github.com/maxpert/amqp-go/interfaces"
)

func TestAnonymousMechanismIsLoopbackOnly(t *testing.T) {
	anonymous := &AnonymousMechanism{}
	user, err := anonymous.Authenticate(nil, nil)
	if err != nil {
		t.Fatalf("expected successful authentication, got error: %v", err)
	}
	if !user.LoopbackOnly {
		t.Error("ANONYMOUS mechanism user must have LoopbackOnly=true so remote connections are refused")
	}
}

func TestDefaultRegistryExcludesAnonymous(t *testing.T) {
	registry := DefaultRegistry()
	_, err := registry.Get("ANONYMOUS")
	if err == nil {
		t.Error("DefaultRegistry() must NOT include ANONYMOUS mechanism (security: opt-in only)")
	}
	_, err = registry.Get("PLAIN")
	if err != nil {
		t.Error("DefaultRegistry() must include PLAIN mechanism")
	}
}

func TestRegistryForMechanismsOptsInAnonymous(t *testing.T) {
	registry := RegistryForMechanisms([]string{"PLAIN", "ANONYMOUS"})
	_, err := registry.Get("ANONYMOUS")
	if err != nil {
		t.Error("RegistryForMechanisms with ANONYMOUS in list must register ANONYMOUS")
	}
	_, err = registry.Get("PLAIN")
	if err != nil {
		t.Error("RegistryForMechanisms must always register PLAIN")
	}

	registryNoAnon := RegistryForMechanisms([]string{"PLAIN"})
	_, err = registryNoAnon.Get("ANONYMOUS")
	if err == nil {
		t.Error("RegistryForMechanisms without ANONYMOUS in list must NOT register ANONYMOUS")
	}
}

func TestRegistryForMechanismsCaseInsensitive(t *testing.T) {
	registry := RegistryForMechanisms([]string{"plain", "anonymous"})
	_, err := registry.Get("ANONYMOUS")
	if err != nil {
		t.Error("RegistryForMechanisms should match mechanism names case-insensitively")
	}
	_, err = registry.Get("PLAIN")
	if err != nil {
		t.Error("RegistryForMechanisms should match mechanism names case-insensitively")
	}
}

func TestRegistryForMechanismsDefaultPlain(t *testing.T) {
	registry := RegistryForMechanisms(nil)
	_, err := registry.Get("PLAIN")
	if err != nil {
		t.Error("RegistryForMechanisms with nil/empty names must still register PLAIN as default")
	}
	_, err = registry.Get("ANONYMOUS")
	if err == nil {
		t.Error("RegistryForMechanisms with nil/empty names must NOT register ANONYMOUS")
	}
}

func TestRegistryForMechanismsStringOrdering(t *testing.T) {
	registry := RegistryForMechanisms([]string{"PLAIN", "ANONYMOUS"})
	str := registry.String()
	if !strings.Contains(str, "PLAIN") || !strings.Contains(str, "ANONYMOUS") {
		t.Errorf("RegistryForMechanisms String() should list both mechanisms, got %q", str)
	}
}

func TestAnonymousMechanismUserHasFullPermissions(t *testing.T) {
	anonymous := &AnonymousMechanism{}
	user, err := anonymous.Authenticate(nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if user.Username != "guest" {
		t.Errorf("expected username 'guest', got %q", user.Username)
	}
	found := false
	for _, vhp := range user.VHostPermissions {
		if vhp.VHost == "/" {
			found = true
			if vhp.Permission.Configure != ".*" || vhp.Permission.Write != ".*" || vhp.Permission.Read != ".*" {
				t.Error("ANONYMOUS guest should have full permissions on /")
			}
		}
	}
	if !found {
		t.Error("ANONYMOUS guest should have permission for / vhost")
	}
}

func TestAnonymousMechanismImplementsInterface(t *testing.T) {
	var _ interfaces.Authenticator = (*FileAuthenticator)(nil)
}
