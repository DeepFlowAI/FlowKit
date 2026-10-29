package sip

import (
	"net"
	"strings"

	"github.com/emiago/sipgo/sip"
)

// resolveCallerExtension reports the extension that placed this call, or "" if
// the INVITE did not come from a phone registered to us.
//
// Telling the two apart is what separates a customer dialling in over a
// carrier trunk from an agent dialling out on their desk phone: both arrive as
// an INVITE, and only the caller's identity distinguishes them.
//
// The From user-part is a claim, not proof — anything can write an extension
// number there. The proof is that the call arrives from the address that
// extension registered from, so a caller borrowing someone else's number is
// not recognised (and its call is then treated as an ordinary inbound one,
// never as licence to place calls on that extension's behalf).
//
// Only the host is compared. A handset commonly registers and calls through
// different NAT port mappings, so requiring the port to match would reject
// legitimate phones; sharing the public address is the meaningful signal.
func (s *Signal) resolveCallerExtension(req *sip.Request, peerSource string) string {
	if s.ua == nil {
		return ""
	}
	reg := s.ua.ExtensionRegistry()
	if reg == nil {
		return ""
	}
	from := req.From()
	if from == nil {
		return ""
	}
	aor := strings.TrimSpace(from.Address.User)
	if aor == "" {
		return ""
	}
	contact, ok := reg.Lookup(aor)
	if !ok {
		return ""
	}
	peerHost := peerHostFromSource(peerSource)
	if peerHost == "" || !sameSIPHost(contact.Host, peerHost) {
		return ""
	}
	return aor
}

// sameSIPHost compares two SIP host parts, tolerating the IPv6 forms that
// differ textually but address the same peer.
func sameSIPHost(a, b string) bool {
	a = strings.Trim(strings.TrimSpace(a), "[]")
	b = strings.Trim(strings.TrimSpace(b), "[]")
	if a == "" || b == "" {
		return false
	}
	if ipA, ipB := net.ParseIP(a), net.ParseIP(b); ipA != nil && ipB != nil {
		return ipA.Equal(ipB)
	}
	return strings.EqualFold(a, b)
}
