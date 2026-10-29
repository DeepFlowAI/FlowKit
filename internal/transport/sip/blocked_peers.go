package sip

import (
	"net"
	"strings"
)

// SetBlockedPeers rejects inbound INVITEs whose source IP is in the list (403).
func (s *Signal) SetBlockedPeers(ips []string) {
	m := make(map[string]struct{}, len(ips))
	for _, ip := range ips {
		ip = strings.TrimSpace(ip)
		if ip != "" {
			m[ip] = struct{}{}
		}
	}
	s.mu.Lock()
	s.blockedPeers = m
	s.mu.Unlock()
}

func peerHostFromSource(source string) string {
	host, _, err := net.SplitHostPort(strings.TrimSpace(source))
	if err != nil {
		return strings.TrimSpace(source)
	}
	return host
}

func (s *Signal) isBlockedPeer(host string) bool {
	if host == "" {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.blockedPeers) == 0 {
		return false
	}
	_, blocked := s.blockedPeers[host]
	return blocked
}
