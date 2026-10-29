package telecom

import (
	"strings"
)

// NormalizeE164 returns the input stripped of common SIP / display decoration:
//
//   - leading "+" and "tel:" prefix
//   - visual separators (space, "-", "(", ")", ".")
//
// The result is *not* validated as a real phone number. If the cleaned
// string consists entirely of ASCII digits, it is returned as-is (this is
// the E.164 numeric form). Otherwise the original input is returned
// unchanged — short codes, internal extensions, alphanumeric URIs, and
// any non-digit user part flow through untouched so the CC layer can
// decide what to do.
//
// Design rationale: FlowKit does NOT know which DIDs are legal. We only
// normalize *format*, not *semantics*, so CC always sees the same shape.
func NormalizeE164(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ""
	}
	work := trimmed
	if low := strings.ToLower(work); strings.HasPrefix(low, "tel:") {
		work = work[4:]
	}
	work = strings.TrimPrefix(work, "+")

	var b strings.Builder
	b.Grow(len(work))
	for _, r := range work {
		switch r {
		case ' ', '\t', '-', '(', ')', '.':
			continue
		default:
			b.WriteRune(r)
		}
	}
	cleaned := b.String()
	if cleaned == "" {
		return trimmed
	}
	for _, r := range cleaned {
		if r < '0' || r > '9' {
			return trimmed
		}
	}
	return cleaned
}
