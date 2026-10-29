package sip

import (
	"net"
	"strconv"
	"strings"
)

// TrunkResolver answers two questions for the SIP layer:
//   - inbound: which trunk_id matches this source (ip, port)?
//   - outbound (handled separately): which OutboundProfile to use for trunk_id?
//
// The telecom.Catalog is the canonical implementation; the interface stays
// in this package so the SIP layer does not import internal/telecom.
type TrunkResolver interface {
	LookupByPeer(ip string, port int) (trunkID string, ok bool)
}

// SetTrunkResolver wires the catalog-backed resolver used for inbound IP
// matching. If unset, every inbound INVITE is treated as having empty
// trunk_id (legacy single-trunk behavior).
func (s *Signal) SetTrunkResolver(r TrunkResolver) {
	s.mu.Lock()
	s.trunkResolver = r
	s.mu.Unlock()
}

// SetRejectUnknownPeer toggles whether inbound INVITEs from IPs that do not
// match any catalog trunk are rejected with 403. A bare Signal accepts every
// peer; the kernel config (telecom.inbound.reject_unknown_peer) defaults to true.
func (s *Signal) SetRejectUnknownPeer(reject bool) {
	s.mu.Lock()
	s.rejectUnknownPeer = reject
	s.mu.Unlock()
}

// resolveInboundTrunk parses the SIP message Source ("ip:port") and queries
// the resolver. Returns the trunk_id (may be empty) plus a flag indicating
// whether the resolver found a match.
func (s *Signal) resolveInboundTrunk(source string) (trunkID string, matched bool) {
	s.mu.Lock()
	resolver := s.trunkResolver
	s.mu.Unlock()
	if resolver == nil {
		return "", false
	}
	host, portStr, err := net.SplitHostPort(strings.TrimSpace(source))
	if err != nil {
		// Source may be IP-only on some transports.
		host = strings.TrimSpace(source)
		portStr = ""
	}
	port := 0
	if portStr != "" {
		if n, err := strconv.Atoi(portStr); err == nil {
			port = n
		}
	}
	return resolver.LookupByPeer(host, port)
}
