package telecom

import (
	"context"
	"sync"
	"testing"
	"time"
)

// fakeClock is a controllable clock for tests.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

func TestLease_StateTransitions(t *testing.T) {
	t0 := time.Date(2026, 5, 26, 12, 0, 0, 0, time.UTC)
	lease := &ProviderLease{
		ProviderID:      "p1",
		LeaseTTL:        90 * time.Second,
		StaleAcceptable: 30 * time.Second,
		LastHeartbeat:   t0,
		ExpiresAt:       t0.Add(90 * time.Second),
	}
	// Inside lease
	if lease.IsExpired(t0.Add(60 * time.Second)) {
		t.Error("should not be expired at t0+60s")
	}
	// Just expired, within stale window
	just := t0.Add(91 * time.Second)
	if !lease.IsExpired(just) {
		t.Error("expected expired at t0+91s")
	}
	if !lease.IsStale(just) {
		t.Error("expected stale at t0+91s")
	}
	if lease.IsDead(just) {
		t.Error("not yet dead at t0+91s")
	}
	// Past stale window
	dead := t0.Add(125 * time.Second)
	if !lease.IsDead(dead) {
		t.Error("expected dead at t0+125s")
	}
}

func TestLease_NeverExpires(t *testing.T) {
	lease := &ProviderLease{ProviderID: SystemProviderID, ExpiresAt: time.Time{}}
	for _, d := range []time.Duration{0, time.Hour, 24 * time.Hour, 365 * 24 * time.Hour} {
		now := time.Now().Add(d)
		if lease.IsExpired(now) || lease.IsStale(now) || lease.IsDead(now) {
			t.Errorf("system lease should never expire/stale/dead at +%v", d)
		}
	}
}

func TestRunSweeper_RemovesDeadProvider(t *testing.T) {
	fc := &fakeClock{now: time.Date(2026, 5, 26, 12, 0, 0, 0, time.UTC)}
	c := NewCatalog(fc.Now)

	snap := sampleSnapshot("p1", 1)
	snap.LeaseTTL = 1 * time.Second
	snap.StaleAcceptable = 1 * time.Second
	if _, err := c.ApplySnapshot(snap); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() {
		c.RunSweeper(ctx, SweeperConfig{Interval: 20 * time.Millisecond})
		close(done)
	}()

	// Advance past expiry + stale window
	fc.Advance(5 * time.Second)

	// Wait for sweeper to act
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if len(c.Trunks()) == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := len(c.Trunks()); got != 0 {
		t.Errorf("expected sweeper to remove dead provider's trunks, got %d", got)
	}

	cancel()
	<-done
}

func TestRunSweeper_KeepsLiveProvider(t *testing.T) {
	fc := &fakeClock{now: time.Date(2026, 5, 26, 12, 0, 0, 0, time.UTC)}
	c := NewCatalog(fc.Now)

	if _, err := c.ApplySnapshot(sampleSnapshot("p1", 1)); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	go c.RunSweeper(ctx, SweeperConfig{Interval: 20 * time.Millisecond})

	// Advance only 10s — well within 90s TTL
	fc.Advance(10 * time.Second)
	time.Sleep(100 * time.Millisecond)

	if got := len(c.Trunks()); got != 2 {
		t.Errorf("live provider should keep its trunks, got %d", got)
	}
}

func TestRunSweeper_KeepsSystemProvider(t *testing.T) {
	fc := &fakeClock{now: time.Date(2026, 5, 26, 12, 0, 0, 0, time.UTC)}
	c := NewCatalog(fc.Now)

	snap := BuildSystemSnapshot(SystemTrunkSettings{
		InboundServer: "203.0.113.10",
		InboundPort:   5060,
	})
	if _, err := c.ApplySnapshot(snap); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	go c.RunSweeper(ctx, SweeperConfig{Interval: 20 * time.Millisecond})

	// Advance years; system provider should still be alive
	fc.Advance(365 * 24 * time.Hour)
	time.Sleep(100 * time.Millisecond)

	if got := len(c.Trunks()); got != 1 {
		t.Errorf("system provider should never be swept, got %d trunks", got)
	}
}
