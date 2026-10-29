package telecom

import (
	"strings"
)

// SystemInboundTrunkID is the reserved trunk_id for the inbound half of the
// system Provider.
const SystemInboundTrunkID = "system_inbound"

// SystemOutboundTrunkID is the reserved trunk_id for the outbound half.
const SystemOutboundTrunkID = "system_outbound"

// SystemTrunkSettings is the env-derived input to BuildSystemSnapshot.
// Mirrors the SIP_TRUNK_* / SIP_TRUNK_OUTBOUND_* fields in flowkit.yaml.
type SystemTrunkSettings struct {
	// Inbound half (SIP_TRUNK_*)
	InboundServer string // becomes peer IP
	InboundPort   int    // becomes peer port
	InboundUser   string
	InboundPass   string
	InboundRealm  string
	InboundDID    string

	// Outbound half (SIP_TRUNK_OUTBOUND_*); empty fields fall back to inbound.
	OutboundServer       string
	OutboundPort         int
	OutboundUser         string
	OutboundPass         string
	OutboundRealm        string
	OutboundDID          string
	OutboundCalleePrefix string

	// AllowOriginate gates whether the outbound trunk is emitted at all.
	AllowOriginate bool
}

// BuildSystemSnapshot converts env-var settings into a Snapshot owned by the
// reserved "system" Provider. The lease never expires (LeaseTTL=0).
//
// Returns a Snapshot with zero, one, or two Trunks depending on which halves
// are configured:
//   - InboundServer empty → no system_inbound (no IP whitelist)
//   - OutboundServer empty OR AllowOriginate=false → no system_outbound
//   - both empty → returned snapshot has no Trunks; ApplySnapshot is a no-op
func BuildSystemSnapshot(s SystemTrunkSettings) Snapshot {
	snap := Snapshot{
		ProviderID: SystemProviderID,
		Revision:   1,
		LeaseTTL:   0, // never expires
	}

	if ip := strings.TrimSpace(s.InboundServer); ip != "" {
		snap.Trunks = append(snap.Trunks, TrunkEntry{
			ID:         SystemInboundTrunkID,
			TrunkTypes: []TrunkType{TrunkInbound},
			Status:     "enabled",
			PeerEndpoints: []PeerEndpoint{{
				IP:   ip,
				Port: s.InboundPort,
			}},
		})
	}

	if s.AllowOriginate {
		server := strings.TrimSpace(s.OutboundServer)
		if server == "" {
			server = strings.TrimSpace(s.InboundServer)
		}
		if server != "" {
			port := s.OutboundPort
			if port <= 0 {
				port = s.InboundPort
			}
			user := nonEmpty(s.OutboundUser, s.InboundUser)
			pass := nonEmpty(s.OutboundPass, s.InboundPass)
			realm := nonEmpty(s.OutboundRealm, s.InboundRealm)
			did := nonEmpty(s.OutboundDID, s.InboundDID)
			snap.Trunks = append(snap.Trunks, TrunkEntry{
				ID:         SystemOutboundTrunkID,
				TrunkTypes: []TrunkType{TrunkOutbound},
				Status:     "enabled",
				Outbound: OutboundProfile{
					Server:       server,
					Port:         port,
					User:         user,
					Pass:         pass,
					Realm:        realm,
					DID:          did,
					CalleePrefix: strings.TrimSpace(s.OutboundCalleePrefix),
				},
			})
		}
	}
	return snap
}

func nonEmpty(primary, fallback string) string {
	if v := strings.TrimSpace(primary); v != "" {
		return v
	}
	return strings.TrimSpace(fallback)
}
