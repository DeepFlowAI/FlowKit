package sip

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// RFC 5737 documentation addresses and generic E.164-style fixtures — no production data.
const (
	testTrunkHost  = "203.0.113.10"
	testTrunkHost2 = "203.0.113.20"
	testDID        = "02188880000"
	testCallee     = "13800138000"
	testCalleePref = "021"
)

func TestResolveOriginate_DialString(t *testing.T) {
	s := &Signal{trunk: TrunkConfig{Server: testTrunkHost, Port: 5060, DID: testDID, CalleePrefix: testCalleePref}}
	target, from, _, err := s.resolveOriginate(testCallee, "", "")
	require.NoError(t, err)
	require.Equal(t, testCalleePref+testCallee, target.User)
	require.Equal(t, testTrunkHost, target.Host)
	require.Equal(t, 5060, target.Port)
	require.Equal(t, testDID, from)
}

func TestResolveOriginate_ShortVirtualNumber(t *testing.T) {
	s := &Signal{}
	s.SetOutboundResolver(func(id string) (TrunkConfig, bool) {
		if id == "trunk-vsp" {
			return TrunkConfig{Server: "127.0.0.1", Port: 5080}, true
		}
		return TrunkConfig{}, false
	})
	target, from, _, err := s.resolveOriginate("95002", "9000000", "trunk-vsp")
	require.NoError(t, err)
	require.Equal(t, "95002", target.User)
	require.Equal(t, "127.0.0.1", target.Host)
	require.Equal(t, 5080, target.Port)
	require.Equal(t, "9000000", from)
}

func TestResolveOriginate_FullURI(t *testing.T) {
	s := &Signal{trunk: TrunkConfig{Server: testTrunkHost, Port: 5060, DID: testDID, CalleePrefix: testCalleePref}}
	target, from, _, err := s.resolveOriginate("sip:"+testCallee+"@"+testTrunkHost+":5060", "456", "")
	require.NoError(t, err)
	require.Equal(t, testCallee, target.User)
	require.Equal(t, testTrunkHost, target.Host)
	require.Equal(t, 5060, target.Port)
	require.Equal(t, "456", from)
}

func TestApplyCalleePrefix_Idempotent(t *testing.T) {
	prefixed := testCalleePref + testCallee
	require.Equal(t, prefixed, applyCalleePrefix(testCalleePref, testCallee))
	require.Equal(t, prefixed, applyCalleePrefix(testCalleePref, prefixed))
}

func TestResolveOriginate_NoTrunk(t *testing.T) {
	s := &Signal{}
	_, _, _, err := s.resolveOriginate(testCallee, "", "")
	require.Error(t, err)
}

func TestTrunkHostPort_WithPortInServer(t *testing.T) {
	cfg := TrunkConfig{Server: testTrunkHost2 + ":5060", Port: 5060}
	host, port, err := trunkHostPort(cfg)
	require.NoError(t, err)
	require.Equal(t, testTrunkHost2, host)
	require.Equal(t, 5060, port)
}

func TestPickTrunkConfig_WithTrunkID(t *testing.T) {
	s := &Signal{}
	s.SetOutboundResolver(func(id string) (TrunkConfig, bool) {
		if id == "tr-foo" {
			return TrunkConfig{Server: testTrunkHost, Port: 5060}, true
		}
		return TrunkConfig{}, false
	})

	cfg, err := s.pickTrunkConfig("tr-foo")
	require.NoError(t, err)
	require.Equal(t, testTrunkHost, cfg.Server)

	_, err = s.pickTrunkConfig("tr-bar")
	require.ErrorIs(t, err, ErrTrunkNotFound)
}

func TestPickTrunkConfig_NotReady(t *testing.T) {
	s := &Signal{}
	s.SetOutboundResolver(func(id string) (TrunkConfig, bool) {
		return TrunkConfig{Server: ""}, true // resolver returns disabled trunk
	})
	_, err := s.pickTrunkConfig("tr-broken")
	require.ErrorIs(t, err, ErrTrunkNotReady)
}

func TestPickTrunkConfig_FallbackToStatic(t *testing.T) {
	s := &Signal{}
	s.SetTrunkConfig(TrunkConfig{Server: testTrunkHost, Port: 5060})
	// no resolver, empty trunk_id → static config
	cfg, err := s.pickTrunkConfig("")
	require.NoError(t, err)
	require.Equal(t, testTrunkHost, cfg.Server)

	// non-empty trunk_id without resolver → ErrTrunkNotFound (never silently fall back)
	_, err = s.pickTrunkConfig("tr-x")
	require.ErrorIs(t, err, ErrTrunkNotFound)
}

func TestPickTrunkConfig_ResolverDefaultBeatsStatic(t *testing.T) {
	s := &Signal{}
	s.SetTrunkConfig(TrunkConfig{Server: testTrunkHost})
	s.SetOutboundResolver(func(id string) (TrunkConfig, bool) {
		if id == "" {
			return TrunkConfig{Server: testTrunkHost2}, true
		}
		return TrunkConfig{}, false
	})
	cfg, err := s.pickTrunkConfig("")
	require.NoError(t, err)
	require.Equal(t, testTrunkHost2, cfg.Server, "resolver default should beat static fallback")
}
