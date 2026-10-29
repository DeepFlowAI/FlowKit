package telecom

import (
	"testing"
)

func TestBuildSystemSnapshot_InboundOnly(t *testing.T) {
	snap := BuildSystemSnapshot(SystemTrunkSettings{
		InboundServer: "203.0.113.146",
		InboundPort:   5110,
		InboundUser:   "4000000001",
		InboundDID:    "4000000001",
	})
	if snap.ProviderID != SystemProviderID {
		t.Errorf("provider_id: %s", snap.ProviderID)
	}
	if snap.LeaseTTL != 0 {
		t.Errorf("lease_ttl: want 0 (never expire), got %v", snap.LeaseTTL)
	}
	if len(snap.Trunks) != 1 {
		t.Fatalf("trunks: got %d want 1 (inbound only)", len(snap.Trunks))
	}
	in := snap.Trunks[0]
	if in.ID != SystemInboundTrunkID {
		t.Errorf("inbound id: %s", in.ID)
	}
	if len(in.PeerEndpoints) != 1 || in.PeerEndpoints[0].IP != "203.0.113.146" || in.PeerEndpoints[0].Port != 5110 {
		t.Errorf("peer endpoint: %+v", in.PeerEndpoints)
	}
}

func TestBuildSystemSnapshot_OutboundWithFallback(t *testing.T) {
	snap := BuildSystemSnapshot(SystemTrunkSettings{
		InboundServer:  "203.0.113.146",
		InboundPort:    5110,
		InboundUser:    "4000000001",
		InboundPass:    "ipw",
		InboundRealm:   "carrier.example.com",
		InboundDID:     "4000000001",
		OutboundServer: "203.0.113.240",
		// OutboundPort omitted → fallback to inbound 5110? No, design says
		// outbound has its own default port; with InboundPort=5110 and
		// OutboundPort=0, expect 5110 fallback.
		OutboundDID:          "20260526",
		OutboundCalleePrefix: "035",
		AllowOriginate:       true,
	})
	if len(snap.Trunks) != 2 {
		t.Fatalf("trunks: got %d want 2 (in + out)", len(snap.Trunks))
	}
	var out *TrunkEntry
	for i := range snap.Trunks {
		if snap.Trunks[i].ID == SystemOutboundTrunkID {
			out = &snap.Trunks[i]
		}
	}
	if out == nil {
		t.Fatal("system_outbound not found")
	}
	if out.Outbound.Server != "203.0.113.240" {
		t.Errorf("server: %s", out.Outbound.Server)
	}
	if out.Outbound.Port != 5110 {
		t.Errorf("port: want 5110 (fallback to inbound), got %d", out.Outbound.Port)
	}
	if out.Outbound.User != "4000000001" {
		t.Errorf("user fallback: %s", out.Outbound.User)
	}
	if out.Outbound.DID != "20260526" {
		t.Errorf("did: %s", out.Outbound.DID)
	}
	if out.Outbound.CalleePrefix != "035" {
		t.Errorf("callee_prefix: %s", out.Outbound.CalleePrefix)
	}
}

func TestBuildSystemSnapshot_AllowOriginateOff(t *testing.T) {
	snap := BuildSystemSnapshot(SystemTrunkSettings{
		InboundServer:  "203.0.113.146",
		InboundPort:    5110,
		OutboundServer: "203.0.113.240",
		AllowOriginate: false,
	})
	if len(snap.Trunks) != 1 {
		t.Fatalf("with originate=false, expect inbound only; got %d", len(snap.Trunks))
	}
	if snap.Trunks[0].ID != SystemInboundTrunkID {
		t.Errorf("expected inbound, got %s", snap.Trunks[0].ID)
	}
}

func TestBuildSystemSnapshot_AllEmpty(t *testing.T) {
	snap := BuildSystemSnapshot(SystemTrunkSettings{})
	if len(snap.Trunks) != 0 {
		t.Errorf("empty settings → 0 trunks, got %d", len(snap.Trunks))
	}
}

func TestBuildSystemSnapshot_IntegrationWithCatalog(t *testing.T) {
	c := newTestCatalog()
	snap := BuildSystemSnapshot(SystemTrunkSettings{
		InboundServer:        "203.0.113.146",
		InboundPort:          5110,
		OutboundServer:       "203.0.113.240",
		OutboundPort:         5060,
		OutboundCalleePrefix: "035",
		AllowOriginate:       true,
	})
	lease, err := c.ApplySnapshot(snap)
	if err != nil {
		t.Fatalf("apply system snapshot: %v", err)
	}
	if !lease.ExpiresAt.IsZero() {
		t.Errorf("system lease should never expire, got %v", lease.ExpiresAt)
	}

	id, ok := c.LookupByPeer("203.0.113.146", 5110)
	if !ok || id != SystemInboundTrunkID {
		t.Errorf("lookup: got %q ok=%v", id, ok)
	}

	out := c.GetTrunk(SystemOutboundTrunkID)
	if out == nil || out.Outbound.Server != "203.0.113.240" {
		t.Errorf("outbound: %+v", out)
	}
}
