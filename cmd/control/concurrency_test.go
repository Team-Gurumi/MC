package main

import (
	"runtime"
	"testing"
	"time"

	dhtnode "github.com/Team-Gurumi/MC/pkg/dht"
)

// Mock Node just enough to not panic
func newMockNode() *dhtnode.Node {
	// in reality this is hard to mock because dhtnode.Node is a struct
	// but the code only calls dhtnode.NewNode.
	// However, AnnounceManager takes *dhtnode.Node.
	// If we pass nil, it will panic in announceOnce.
	// So we can only test Enqueue concurrency if we don't actually run announceOnce or mock it.
	// We can't mock methods on a struct easily in Go if it's not an interface.
	// But we can check if Enqueue spawns goroutines fast.
	return nil
}

func TestAnnounceManager_Concurrency(t *testing.T) {
	// Prepare
	mgr := NewAnnounceManager(nil, "test", time.Second, time.Second)

	// Measure baseline goroutines
	base := runtime.NumGoroutine()

	// Action: Enqueue 10,000 times
	for i := 0; i < 10000; i++ {
		mgr.Enqueue("job-id")
	}

	// Measure after
	// Give a tiny bit of time for async run (if any)
	time.Sleep(100 * time.Millisecond)

	current := runtime.NumGoroutine()
	diff := current - base

	t.Logf("Goroutine diff: %d", diff)

	if diff > 100 {
		t.Errorf("Goroutine leak detected or unbounded spawn! Diff: %d", diff)
	} else {
		t.Log("Pass: Goroutine count is stable.")
	}
}
