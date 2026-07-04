package protocol

import (
	"sync"
	"testing"
)

func TestFallbackIDUnique(t *testing.T) {
	const goroutines = 100
	const perGoroutine = 1000
	var wg sync.WaitGroup
	ids := make(chan string, goroutines*perGoroutine)
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perGoroutine; j++ {
				ids <- fallbackID("conn")
			}
		}()
	}
	wg.Wait()
	close(ids)

	seen := make(map[string]struct{}, goroutines*perGoroutine)
	for id := range ids {
		if _, dup := seen[id]; dup {
			t.Errorf("duplicate ID generated: %s", id)
		}
		seen[id] = struct{}{}
	}
	if len(seen) != goroutines*perGoroutine {
		t.Errorf("expected %d unique IDs, got %d", goroutines*perGoroutine, len(seen))
	}
}

func TestFallbackIDFormat(t *testing.T) {
	id := fallbackID("amq.gen")
	if len(id) == 0 {
		t.Fatal("fallbackID returned empty string")
	}
}

func TestGenerateIDNoPanic(t *testing.T) {
	id := generateID()
	if id == "" {
		t.Fatal("generateID returned empty string")
	}
}

func TestGenerateQueueNameNoPanic(t *testing.T) {
	name := GenerateQueueName()
	if name == "" {
		t.Fatal("GenerateQueueName returned empty string")
	}
}
