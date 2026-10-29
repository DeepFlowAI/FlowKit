package sip

import (
	"net"
	"strings"

	"github.com/emiago/sipgo/sip"
)

// headerSrcAddr is injected by a front-end SIP proxy (Kamailio) before it
// relays a request to flowkit. When present AND the UDP source is a trusted
// proxy (loopback), peer matching / registrar binding use this value instead
// of req.Source() — otherwise every proxied request would appear to come from
// 127.0.0.1 and trunk allow-lists / phone Contact bindings would break.
const headerSrcAddr = "X-Src-Addr"

// isTrustedProxySource reports whether source is a SIP front-end we trust to
// assert the original peer via X-Src-Addr. Today that is only loopback
// (Kamailio on the same host with network_mode: host).
func isTrustedProxySource(source string) bool {
	host := peerHostFromSource(source)
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// effectivePeerSource returns the far-end UDP address that should be used for
// trunk peer matching, blocked-peer checks, and registrar bindings.
//
// When the request arrived from a trusted proxy, prefer the X-Src-Addr header
// the proxy stamped with the original $si:$sp. Otherwise (or if the header is
// missing) fall back to req.Source(). External clients cannot spoof this:
// a forged X-Src-Addr is ignored unless the UDP source itself is loopback.
func effectivePeerSource(req *sip.Request) string {
	if req == nil {
		return ""
	}
	src := strings.TrimSpace(req.Source())
	if !isTrustedProxySource(src) {
		return src
	}
	if h := req.GetHeader(headerSrcAddr); h != nil {
		if v := strings.TrimSpace(h.Value()); v != "" {
			return v
		}
	}
	return src
}
