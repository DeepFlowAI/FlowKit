package telecom

import (
	"context"
	"sync"
	"testing"
	"time"
)

type capturedEvent struct {
	method  string
	payload map[string]any
}

type captureSink struct {
	mu     sync.Mutex
	events []capturedEvent
}

func (c *captureSink) Emit(method string, payload map[string]any) {
	c.mu.Lock()
	c.events = append(c.events, capturedEvent{method: method, payload: payload})
	c.mu.Unlock()
}

func (c *captureSink) Methods() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, len(c.events))
	for i, e := range c.events {
		out[i] = e.method
	}
	return out
}

func (c *captureSink) Count(method string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, e := range c.events {
		if e.method == method {
			n++
		}
	}
	return n
}

func TestEvents_ApplySnapshotFiresRegistered(t *testing.T) {
	c := newTestCatalog()
	sink := &captureSink{}
	c.SetEventSink(sink)

	if _, err := c.ApplySnapshot(sampleSnapshot("p1", 1)); err != nil {
		t.Fatal(err)
	}
	if got := sink.Count(EventTrunkRegistered); got != 2 {
		t.Errorf("registered events: got %d want 2", got)
	}
	if got := sink.Count(EventTrunkDeregistered); got != 0 {
		t.Errorf("no deregistered on first apply, got %d", got)
	}
}

func TestEvents_ReapplySnapshotEmitsDiff(t *testing.T) {
	c := newTestCatalog()
	sink := &captureSink{}
	c.SetEventSink(sink)

	first := sampleSnapshot("p1", 1)
	if _, err := c.ApplySnapshot(first); err != nil {
		t.Fatal(err)
	}
	// Second snapshot drops *_out, adds *_extra.
	second := Snapshot{
		ProviderID: "p1",
		Revision:   2,
		LeaseTTL:   90 * time.Second,
		Trunks: []TrunkEntry{
			first.Trunks[0], // p1_in kept
			{
				ID:            "p1_extra",
				TrunkTypes:    []TrunkType{TrunkInbound},
				Status:        "enabled",
				PeerEndpoints: []PeerEndpoint{{IP: "198.51.100.5"}},
			},
		},
	}
	sink.mu.Lock()
	sink.events = nil
	sink.mu.Unlock()
	if _, err := c.ApplySnapshot(second); err != nil {
		t.Fatal(err)
	}
	if got := sink.Count(EventTrunkRegistered); got != 1 {
		t.Errorf("registered (new p1_extra): got %d want 1", got)
	}
	if got := sink.Count(EventTrunkDeregistered); got != 1 {
		t.Errorf("deregistered (dropped p1_out): got %d want 1", got)
	}
}

func TestEvents_RemoveProvider(t *testing.T) {
	c := newTestCatalog()
	sink := &captureSink{}
	c.SetEventSink(sink)

	if _, err := c.ApplySnapshot(sampleSnapshot("p1", 1)); err != nil {
		t.Fatal(err)
	}
	sink.mu.Lock()
	sink.events = nil
	sink.mu.Unlock()
	if _, err := c.RemoveProvider("p1"); err != nil {
		t.Fatal(err)
	}
	if got := sink.Count(EventTrunkDeregistered); got != 2 {
		t.Errorf("deregistered on RemoveProvider: got %d want 2", got)
	}
}

func TestEvents_SweeperEmitsStaleAndExpired(t *testing.T) {
	fc := &fakeClock{now: time.Date(2026, 5, 26, 12, 0, 0, 0, time.UTC)}
	c := NewCatalog(fc.Now)
	sink := &captureSink{}
	c.SetEventSink(sink)

	snap := sampleSnapshot("p1", 1)
	snap.LeaseTTL = 1 * time.Second
	snap.StaleAcceptable = 2 * time.Second
	if _, err := c.ApplySnapshot(snap); err != nil {
		t.Fatal(err)
	}
	sink.mu.Lock()
	sink.events = nil
	sink.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	go c.RunSweeper(ctx, SweeperConfig{Interval: 20 * time.Millisecond})

	// Step 1: into the stale window
	fc.Advance(2 * time.Second)
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) && sink.Count(EventLeaseStale) == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if got := sink.Count(EventLeaseStale); got != 1 {
		t.Errorf("lease.stale: got %d want 1", got)
	}

	// Step 2: past the stale window → dead
	fc.Advance(3 * time.Second)
	deadline = time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) && sink.Count(EventTrunkDeregistered) == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if got := sink.Count(EventTrunkDeregistered); got < 2 {
		t.Errorf("deregistered on lease_expired: got %d want >=2", got)
	}
	cancel()
}
