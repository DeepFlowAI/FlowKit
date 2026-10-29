package sip

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDurationEnv(t *testing.T) {
	t.Setenv("SIP_RTP_IDLE_TIMEOUT", "90s")
	require.Equal(t, 90*time.Second, rtpIdleTimeout())

	t.Setenv("SIP_RTP_IDLE_TIMEOUT", "bad")
	require.Equal(t, defaultRTPIdleTimeout, rtpIdleTimeout())

	t.Setenv("SIP_INBOUND_MAX_DURATION", "30m")
	require.Equal(t, 30*time.Minute, inboundMaxDuration())

	t.Setenv("SIP_MAX_INBOUND_CALLS", "10")
	require.Equal(t, 10, maxInboundCalls())

	t.Setenv("SIP_IDLE_KEEPALIVE", "0")
	require.False(t, idleKeepalive())

	t.Setenv("SIP_IDLE_KEEPALIVE", "bad")
	require.True(t, idleKeepalive())
}
