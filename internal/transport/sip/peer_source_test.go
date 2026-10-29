package sip

import (
	"testing"

	"github.com/emiago/sipgo/sip"
	"github.com/stretchr/testify/require"
)

func TestIsTrustedProxySource(t *testing.T) {
	require.True(t, isTrustedProxySource("127.0.0.1:5060"))
	require.True(t, isTrustedProxySource("[::1]:5070"))
	require.False(t, isTrustedProxySource("203.0.113.146:5110"))
	require.False(t, isTrustedProxySource("10.0.0.1:5060"))
	require.False(t, isTrustedProxySource(""))
}

func TestEffectivePeerSource_UsesHeaderFromLoopback(t *testing.T) {
	req := sip.NewRequest(sip.INVITE, sip.Uri{User: "100", Host: "example.com"})
	req.SetSource("127.0.0.1:5060")
	req.AppendHeader(sip.NewHeader(headerSrcAddr, "203.0.113.146:5110"))

	require.Equal(t, "203.0.113.146:5110", effectivePeerSource(req))
}

func TestEffectivePeerSource_IgnoresHeaderFromUntrusted(t *testing.T) {
	req := sip.NewRequest(sip.INVITE, sip.Uri{User: "100", Host: "example.com"})
	req.SetSource("192.0.2.43:5060")
	req.AppendHeader(sip.NewHeader(headerSrcAddr, "203.0.113.146:5110"))

	require.Equal(t, "192.0.2.43:5060", effectivePeerSource(req))
}

func TestEffectivePeerSource_FallsBackWhenHeaderMissing(t *testing.T) {
	req := sip.NewRequest(sip.INVITE, sip.Uri{User: "100", Host: "example.com"})
	req.SetSource("127.0.0.1:5070")

	require.Equal(t, "127.0.0.1:5070", effectivePeerSource(req))
}
