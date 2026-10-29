package sip

import "regexp"

var (
	sipUUIDCallID         = regexp.MustCompile(`(?i)^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	sipNumericTrunkCallID = regexp.MustCompile(`^\d+-\d+-\d+$`)
)

// isPrimaryTrunkLeg reports whether an inbound INVITE is the media-bearing leg.
// CN PSTN trunks often spray parallel INVITEs: a numeric Call-ID + PCMU probe
// arrives first, while the UUID Call-ID + PCMA leg completes the dialog.
// Only that known probe pair is rejected — Asterisk/FreeSWITCH Call-IDs must
// not be dropped by the older "unknown shape ⇒ PCMA only" heuristic.
func isPrimaryTrunkLeg(sipCallID string, pt int) bool {
	if sipUUIDCallID.MatchString(sipCallID) {
		return true
	}
	if sipNumericTrunkCallID.MatchString(sipCallID) && pt == 0 {
		return false
	}
	return true
}
