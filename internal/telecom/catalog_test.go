package telecom

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func newTestCatalog() *Catalog {
	clock := time.Date(2026, 5, 26, 12, 0, 0, 0, time.UTC)
	return NewCatalog(func() time.Time { return clock })
}

func sampleSnapshot(providerID string, revision int64) Snapshot {
	return Snapshot{
		ProviderID: providerID,
		Revision:   revision,
		LeaseTTL:   90 * time.Second,
		Trunks: []TrunkEntry{
			{
				ID:            providerID + "_in",
				TrunkTypes:    []TrunkType{TrunkInbound},
				Status:        "enabled",
				PeerEndpoints: []PeerEndpoint{{IP: "203.0.113.10", Port: 5060}},
			},
			{
				ID:         providerID + "_out",
				TrunkTypes: []TrunkType{TrunkOutbound},
				Status:     "enabled",
				Outbound:   OutboundProfile{Server: "sip.example.com", Port: 5060},
			},
		},
	}
}

func TestCatalog_ApplySnapshot_Basic(t *testing.T) {
	c := newTestCatalog()
	lease, err := c.ApplySnapshot(sampleSnapshot("p1", 1))
	if err != nil {
		t.Fatalf("ApplySnapshot: %v", err)
	}
	if lease.Revision != 1 {
		t.Errorf("revision: got %d want 1", lease.Revision)
	}
	if got := len(c.Trunks()); got != 2 {
		t.Errorf("trunk count: got %d want 2", got)
	}
}

func TestCatalog_ApplySnapshot_StaleRevision(t *testing.T) {
	c := newTestCatalog()
	if _, err := c.ApplySnapshot(sampleSnapshot("p1", 5)); err != nil {
		t.Fatal(err)
	}
	_, err := c.ApplySnapshot(sampleSnapshot("p1", 3))
	var sre *StaleRevisionError
	if !errors.As(err, &sre) {
		t.Fatalf("want StaleRevisionError, got %v", err)
	}
	if sre.CurrentRevision != 5 || sre.Submitted != 3 {
		t.Errorf("StaleRevisionError fields: %+v", sre)
	}
}

func TestCatalog_ApplySnapshot_Replaces(t *testing.T) {
	c := newTestCatalog()
	first := sampleSnapshot("p1", 1)
	first.Trunks = append(first.Trunks, TrunkEntry{
		ID:            "p1_extra",
		TrunkTypes:    []TrunkType{TrunkInbound},
		Status:        "enabled",
		PeerEndpoints: []PeerEndpoint{{IP: "203.0.113.20"}},
	})
	if _, err := c.ApplySnapshot(first); err != nil {
		t.Fatal(err)
	}
	if got := len(c.Trunks()); got != 3 {
		t.Fatalf("after first apply: %d trunks", got)
	}
	if _, err := c.ApplySnapshot(sampleSnapshot("p1", 2)); err != nil {
		t.Fatal(err)
	}
	if got := len(c.Trunks()); got != 2 {
		t.Errorf("after second apply: got %d want 2 (extra trunk should be gone)", got)
	}
	if c.GetTrunk("p1_extra") != nil {
		t.Error("p1_extra should have been removed")
	}
}

func TestCatalog_ApplySnapshot_TrunkIDConflict(t *testing.T) {
	c := newTestCatalog()
	if _, err := c.ApplySnapshot(sampleSnapshot("p1", 1)); err != nil {
		t.Fatal(err)
	}
	// p2 tries to claim p1_in
	steal := Snapshot{
		ProviderID: "p2",
		Revision:   1,
		LeaseTTL:   60 * time.Second,
		Trunks: []TrunkEntry{
			{
				ID:            "p1_in",
				TrunkTypes:    []TrunkType{TrunkInbound},
				Status:        "enabled",
				PeerEndpoints: []PeerEndpoint{{IP: "198.51.100.1"}},
			},
		},
	}
	_, err := c.ApplySnapshot(steal)
	var conflict *TrunkIDConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("want TrunkIDConflictError, got %v", err)
	}
	if conflict.ExistingProvider != "p1" {
		t.Errorf("existing provider: %s", conflict.ExistingProvider)
	}
}

func TestCatalog_Heartbeat_ExtendsLease(t *testing.T) {
	now := time.Date(2026, 5, 26, 12, 0, 0, 0, time.UTC)
	tick := now
	c := NewCatalog(func() time.Time { return tick })
	lease, err := c.ApplySnapshot(sampleSnapshot("p1", 1))
	if err != nil {
		t.Fatal(err)
	}
	original := lease.ExpiresAt
	tick = tick.Add(30 * time.Second)
	l2, err := c.Heartbeat("p1", 90*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !l2.ExpiresAt.After(original) {
		t.Errorf("heartbeat did not extend expiry: original=%v new=%v", original, l2.ExpiresAt)
	}
}

func TestCatalog_RemoveProvider(t *testing.T) {
	c := newTestCatalog()
	if _, err := c.ApplySnapshot(sampleSnapshot("p1", 1)); err != nil {
		t.Fatal(err)
	}
	removed, err := c.RemoveProvider("p1")
	if err != nil {
		t.Fatal(err)
	}
	if len(removed) != 2 {
		t.Errorf("removed: got %d want 2", len(removed))
	}
	if len(c.Trunks()) != 0 {
		t.Errorf("trunks not cleared: %d", len(c.Trunks()))
	}
	if _, err := c.RemoveProvider("p1"); err == nil {
		t.Error("second remove should fail with ProviderNotFoundError")
	}
}

func TestCatalog_LookupByPeer(t *testing.T) {
	c := newTestCatalog()
	if _, err := c.ApplySnapshot(sampleSnapshot("p1", 1)); err != nil {
		t.Fatal(err)
	}
	id, ok := c.LookupByPeer("203.0.113.10", 5060)
	if !ok || id != "p1_in" {
		t.Errorf("lookup: id=%q ok=%v want p1_in,true", id, ok)
	}
	if _, ok := c.LookupByPeer("198.51.100.1", 5060); ok {
		t.Error("unknown IP should return ok=false")
	}
}

func TestCatalog_ValidateSnapshot_Errors(t *testing.T) {
	c := newTestCatalog()
	cases := []Snapshot{
		{ProviderID: ""},
		{ProviderID: "p", Trunks: []TrunkEntry{{ID: "", TrunkTypes: []TrunkType{TrunkInbound}}}},
		{ProviderID: "p", Trunks: []TrunkEntry{
			{ID: "a", TrunkTypes: []TrunkType{TrunkInbound}, PeerEndpoints: []PeerEndpoint{{IP: "1.1.1.1"}}},
			{ID: "a", TrunkTypes: []TrunkType{TrunkInbound}, PeerEndpoints: []PeerEndpoint{{IP: "2.2.2.2"}}},
		}},
		{ProviderID: "p", Trunks: []TrunkEntry{{ID: "a", TrunkTypes: nil}}},
		{ProviderID: "p", Trunks: []TrunkEntry{{ID: "a", TrunkTypes: []TrunkType{TrunkInbound}}}}, // no peer endpoints
		{ProviderID: "p", Trunks: []TrunkEntry{{ID: "a", TrunkTypes: []TrunkType{TrunkOutbound}}}}, // outbound without server
	}
	for i, snap := range cases {
		if _, err := c.ApplySnapshot(snap); err == nil {
			t.Errorf("case[%d]: expected validation error, got nil", i)
		}
	}
}

func TestCatalog_Concurrent(t *testing.T) {
	c := newTestCatalog()
	if _, err := c.ApplySnapshot(sampleSnapshot("p1", 1)); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = c.LookupByPeer("203.0.113.10", 5060)
			_ = c.Trunks()
			_ = c.Providers()
		}()
	}
	wg.Wait()
}
