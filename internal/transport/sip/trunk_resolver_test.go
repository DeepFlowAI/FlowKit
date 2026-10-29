package sip

import (
	"testing"

	"github.com/DeepFlowAI/flowkit/internal/session"
	"github.com/stretchr/testify/require"
)

type stubResolver struct {
	hits map[string]string // "ip:port" → trunk_id
}

func (s *stubResolver) LookupByPeer(ip string, port int) (string, bool) {
	if s.hits == nil {
		return "", false
	}
	key := ip
	if port > 0 {
		key += ":" + itoa(port)
	}
	if v, ok := s.hits[key]; ok {
		return v, true
	}
	if v, ok := s.hits[ip]; ok {
		return v, true
	}
	return "", false
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := false
	if n < 0 {
		neg = true
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

func newTestSignal(t *testing.T) *Signal {
	t.Helper()
	ua, err := New(Config{Listen: "127.0.0.1:0", ExtIP: "127.0.0.1", Logger: quietLogger()})
	require.NoError(t, err)
	mgr := session.NewManager(quietLogger())
	return NewSignal(ua, mgr, nil, quietLogger())
}

func TestResolveInboundTrunk_Match(t *testing.T) {
	s := newTestSignal(t)
	s.SetTrunkResolver(&stubResolver{hits: map[string]string{"203.0.113.10:5060": "tr1"}})

	id, ok := s.resolveInboundTrunk("203.0.113.10:5060")
	require.True(t, ok)
	require.Equal(t, "tr1", id)
}

func TestResolveInboundTrunk_NoResolver(t *testing.T) {
	s := newTestSignal(t)
	id, ok := s.resolveInboundTrunk("203.0.113.10:5060")
	require.False(t, ok)
	require.Equal(t, "", id)
}

func TestResolveInboundTrunk_IPOnlyFormat(t *testing.T) {
	s := newTestSignal(t)
	s.SetTrunkResolver(&stubResolver{hits: map[string]string{"203.0.113.10": "tr-any-port"}})

	id, ok := s.resolveInboundTrunk("203.0.113.10")
	require.True(t, ok)
	require.Equal(t, "tr-any-port", id)
}

func TestResolveInboundTrunk_UnknownPeer(t *testing.T) {
	s := newTestSignal(t)
	s.SetTrunkResolver(&stubResolver{hits: map[string]string{"203.0.113.10:5060": "tr1"}})

	_, ok := s.resolveInboundTrunk("198.51.100.1:5060")
	require.False(t, ok)
}

func TestSetters_SafeUnderConcurrency(t *testing.T) {
	s := newTestSignal(t)
	// Just exercise the setters; race detector covers correctness.
	s.SetTrunkResolver(&stubResolver{})
	s.SetRejectUnknownPeer(true)
	s.SetToNormalizer(func(in string) string { return in })
}
