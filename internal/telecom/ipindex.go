package telecom

import (
	"net"
	"sort"
	"strings"
)

// IPIndex matches a source (ip, port) to a trunk_id using longest-prefix CIDR
// match, with lexicographic trunk_id tie-break for equal prefix lengths.
//
// Lookup is O(n) over registered entries; for the expected scale (≤ a few
// hundred entries) this is fine and keeps the code trivial.
type IPIndex struct {
	entries []ipEntry
}

type ipEntry struct {
	cidr      *net.IPNet // nil if Plain is set
	plainIP   net.IP     // non-nil for exact-IP entries
	port      int        // 0 = any port
	trunkID   string
	prefixLen int // /32 for plain IPs; CIDR mask bits otherwise
}

// NewIPIndex returns an empty index.
func NewIPIndex() *IPIndex {
	return &IPIndex{}
}

// Add registers an (ip, port) → trunk_id mapping.
// `ip` may be a plain IP ("203.0.113.10") or a CIDR ("203.0.113.0/24").
// `port` of 0 matches any port. Invalid ip strings are silently ignored.
func (idx *IPIndex) Add(ip string, port int, trunkID string) {
	raw := strings.TrimSpace(ip)
	if raw == "" || strings.TrimSpace(trunkID) == "" {
		return
	}
	if strings.ContainsRune(raw, '/') {
		_, cidr, err := net.ParseCIDR(raw)
		if err != nil {
			return
		}
		ones, _ := cidr.Mask.Size()
		idx.entries = append(idx.entries, ipEntry{
			cidr:      cidr,
			port:      port,
			trunkID:   trunkID,
			prefixLen: ones,
		})
		return
	}
	parsed := net.ParseIP(raw)
	if parsed == nil {
		return
	}
	bits := 32
	if parsed.To4() == nil {
		bits = 128
	}
	idx.entries = append(idx.entries, ipEntry{
		plainIP:   parsed,
		port:      port,
		trunkID:   trunkID,
		prefixLen: bits,
	})
}

// Lookup returns the best-matching trunk_id for (ip, port).
// Ordering: longest prefix first → among equal prefixes, trunk_id ascending.
func (idx *IPIndex) Lookup(ip string, port int) (string, bool) {
	target := net.ParseIP(strings.TrimSpace(ip))
	if target == nil {
		return "", false
	}
	var matches []ipEntry
	for _, e := range idx.entries {
		if !e.matches(target, port) {
			continue
		}
		matches = append(matches, e)
	}
	if len(matches) == 0 {
		return "", false
	}
	sort.Slice(matches, func(i, j int) bool {
		if matches[i].prefixLen != matches[j].prefixLen {
			return matches[i].prefixLen > matches[j].prefixLen
		}
		return matches[i].trunkID < matches[j].trunkID
	})
	return matches[0].trunkID, true
}

// Len returns the number of registered entries (for tests / metrics).
func (idx *IPIndex) Len() int { return len(idx.entries) }

func (e ipEntry) matches(target net.IP, port int) bool {
	if e.port != 0 && port != 0 && e.port != port {
		return false
	}
	if e.plainIP != nil {
		return e.plainIP.Equal(target)
	}
	if e.cidr != nil {
		return e.cidr.Contains(target)
	}
	return false
}
