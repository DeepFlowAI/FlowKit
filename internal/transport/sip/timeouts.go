package sip

import (
	"os"
	"strconv"
	"time"
)

const (
	defaultRTPIdleTimeout     = 60 * time.Second
	defaultInboundMaxDuration = 4 * time.Hour

	// byeTimeout bounds dialog Bye when the peer never answers (WriteBye wait).
	byeTimeout = 2 * time.Second
	// cancelAttemptTimeout bounds best-effort CANCEL after WaitAnswer ForceCancel.
	cancelAttemptTimeout = 500 * time.Millisecond
)

func rtpIdleTimeout() time.Duration {
	return durationEnv("SIP_RTP_IDLE_TIMEOUT", defaultRTPIdleTimeout)
}

func inboundMaxDuration() time.Duration {
	return durationEnv("SIP_INBOUND_MAX_DURATION", defaultInboundMaxDuration)
}

// maxInboundCalls returns the concurrent inbound SIP call cap. Zero means unlimited.
func maxInboundCalls() int {
	return intEnv("SIP_MAX_INBOUND_CALLS", 0)
}

// registerMaxExpiresSeconds returns the SIP_REGISTER_MAX_EXPIRES cap (in
// seconds) applied to REGISTER leases the registrar grants. The registrar is
// in-memory, so after a kernel restart it is empty until each phone
// re-REGISTERs; the default 120s cap bounds that recovery window. Set to 0
// to grant the requested lease as-is.
func registerMaxExpiresSeconds() int {
	return intEnv("SIP_REGISTER_MAX_EXPIRES", 120)
}

// idleKeepalive reports whether SIP MediaIO paces G.711 silence onto the RTP
// stream while there is no audio to send (SIP_IDLE_KEEPALIVE, default on).
// Carriers and phones expect a continuous media stream for the life of the
// call; a send hole flips far-end VAD/DTX state and resets jitter buffers, so
// the audio that follows the hole is heard as a stutter.
func idleKeepalive() bool {
	return boolEnv("SIP_IDLE_KEEPALIVE", true)
}

func durationEnv(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return fallback
	}
	return d
}

func intEnv(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		return fallback
	}
	return n
}

func boolEnv(key string, fallback bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return fallback
	}
	return b
}
