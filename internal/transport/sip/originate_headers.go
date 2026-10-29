package sip

import (
	"fmt"
	"strings"
)

// allowedOriginateHeader reports whether an extra SIP header may be appended
// to an outbound INVITE. Core dialog headers are always set by flowkit.
func allowedOriginateHeader(name string) bool {
	n := strings.ToLower(strings.TrimSpace(name))
	switch n {
	case "from", "to", "via", "call-id", "cseq", "contact",
		"content-type", "content-length", "max-forwards", "route",
		"record-route", "require", "supported", "allow", "user-agent":
		return false
	}
	if strings.HasPrefix(n, "x-") {
		return true
	}
	switch n {
	case "p-asserted-identity", "privacy", "diversion", "user-to-user":
		return true
	}
	return false
}

// FilterOriginateHeaders returns a copy containing only permitted headers.
func FilterOriginateHeaders(headers map[string]string) (map[string]string, error) {
	if len(headers) == 0 {
		return nil, nil
	}
	out := make(map[string]string, len(headers))
	for k, v := range headers {
		if !allowedOriginateHeader(k) {
			return nil, fmt.Errorf("originate: header not allowed: %s", k)
		}
		out[k] = v
	}
	return out, nil
}
