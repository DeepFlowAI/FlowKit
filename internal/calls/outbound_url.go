package calls

import (
	"fmt"
	"net/url"
	"strings"
)

// SetOutboundURLAllowedHosts restricts the hosts that RPC-supplied URLs
// (turn_url, audio_url, ws_url) may point at, so a client cannot make the
// kernel fetch arbitrary internal addresses. "example.com" matches that host
// only; ".example.com" matches any subdomain. Empty allows every host.
// Call before serving.
func (h *Handlers) SetOutboundURLAllowedHosts(hosts []string) {
	h.outboundHosts = nil
	for _, host := range hosts {
		if host = strings.ToLower(strings.TrimSpace(host)); host != "" {
			h.outboundHosts = append(h.outboundHosts, host)
		}
	}
}

func (h *Handlers) checkOutboundURL(raw string) error {
	if len(h.outboundHosts) == 0 {
		return nil
	}
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() == "" {
		return fmt.Errorf("invalid url %q", raw)
	}
	host := strings.ToLower(u.Hostname())
	for _, allowed := range h.outboundHosts {
		if strings.HasPrefix(allowed, ".") {
			if strings.HasSuffix(host, allowed) {
				return nil
			}
		} else if host == allowed {
			return nil
		}
	}
	return fmt.Errorf("url host %q is not in FLOWKIT_OUTBOUND_URL_ALLOWED_HOSTS", host)
}
