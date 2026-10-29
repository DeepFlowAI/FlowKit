package sip

import (
	"strings"
	"sync"
	"testing"

	"github.com/DeepFlowAI/flowkit/internal/rpc"
	"github.com/DeepFlowAI/flowkit/internal/session"
	"github.com/emiago/sipgo/sip"
	"github.com/stretchr/testify/require"
)

// captureBroadcaster collects notifications under a mutex so tests can
// read len/contents from any goroutine.
type captureBroadcaster struct {
	mu            sync.Mutex
	notifications []*rpc.Notification
}

func (c *captureBroadcaster) BroadcastNotification(n *rpc.Notification) {
	c.mu.Lock()
	c.notifications = append(c.notifications, n)
	c.mu.Unlock()
}

func (c *captureBroadcaster) Count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.notifications)
}

func (c *captureBroadcaster) First() *rpc.Notification {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.notifications) == 0 {
		return nil
	}
	return c.notifications[0]
}

func (c *captureBroadcaster) hasMethod(method string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, n := range c.notifications {
		if n != nil && n.Method == method {
			return true
		}
	}
	return false
}

func TestNewSignal_WiresHandlers(t *testing.T) {
	ua, err := New(Config{Listen: "127.0.0.1:0", ExtIP: "127.0.0.1", Logger: quietLogger()})
	require.NoError(t, err)
	mgr := session.NewManager(quietLogger())
	br := &captureBroadcaster{}

	s := NewSignal(ua, mgr, br, quietLogger())
	require.Equal(t, 0, s.ActiveCalls())

	// Idempotent shutdown without active calls
	s.Shutdown()
	require.Equal(t, 0, s.ActiveCalls())
}

func TestIsPrimaryTrunkLeg(t *testing.T) {
	cases := []struct {
		callID string
		pt     int
		want   bool
	}{
		{"481cc9d6-cf72-123f-c08d-b4e9b8fac0c5", 8, true},
		{"39f7afc1-cf73-123f-c08d-b4e9b8fac0c5", 0, true},
		{"1448750201-309164253-1036032193", 0, false}, // CN probe
		{"1883807289-2017555194-583692242", 0, false},
		{"1448750201-309164253-1036032193", 8, true}, // numeric + PCMA is real
		{"unknown-call-id", 8, true},
		{"unknown-call-id", 0, true}, // Asterisk/FreeSWITCH shapes must pass
		{"1234567890@pbx.example.com", 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.callID, func(t *testing.T) {
			require.Equal(t, tc.want, isPrimaryTrunkLeg(tc.callID, tc.pt))
		})
	}
}

func TestIsInDialogInvite(t *testing.T) {
	req := sip.NewRequest(sip.INVITE, sip.Uri{User: "a", Host: "b"})
	req.AppendHeader(&sip.FromHeader{Address: sip.Uri{User: "a", Host: "b"}, Params: sip.NewParams()})
	to := &sip.ToHeader{Address: sip.Uri{User: "c", Host: "d"}, Params: sip.NewParams()}
	req.AppendHeader(to)
	require.False(t, isInDialogInvite(req))
	to.Params.Add("tag", "xyz")
	require.True(t, isInDialogInvite(req))
}

func TestSessionExpiresResponseHeaders(t *testing.T) {
	req := sip.NewRequest(sip.INVITE, sip.Uri{Host: "x"})
	require.Nil(t, sessionExpiresResponseHeaders(req))
	req.AppendHeader(sip.NewHeader("Session-Expires", "1800"))
	hs := sessionExpiresResponseHeaders(req)
	require.Len(t, hs, 1)
	require.Contains(t, hs[0].Value(), "1800")
	require.Contains(t, strings.ToLower(hs[0].Value()), "refresher=uas")
}

func TestStripURI_RemovesDisplayNameAndTag(t *testing.T) {
	cases := map[string]string{
		`"Alice" <sip:alice@example.com>;tag=abc123`: "sip:alice@example.com",
		`<sip:bob@example.com>`:                       "sip:bob@example.com",
		"sip:carol@example.com;tag=xyz":               "sip:carol@example.com",
		"sip:dave@example.com":                        "sip:dave@example.com",
	}
	for in, want := range cases {
		t.Run(in, func(t *testing.T) {
			require.Equal(t, want, stripURI(in))
		})
	}
}

func TestSignal_Shutdown_HangsUpActive(t *testing.T) {
	// We can't easily craft a real INVITE here without driving a full sipgo
	// client; the dialog test is in inbound_e2e_test.go (sipgo client →
	// flowkit UA → 200 OK → ACK → BYE). This unit test just confirms
	// Shutdown drains s.calls.
	ua, err := New(Config{Listen: "127.0.0.1:0", ExtIP: "127.0.0.1", Logger: quietLogger()})
	require.NoError(t, err)
	mgr := session.NewManager(quietLogger())
	s := NewSignal(ua, mgr, nil, quietLogger())

	// Manually populate as if INVITE had created an entry
	call := mgr.Create(session.DirectionInbound)
	s.mu.Lock()
	s.calls["fake-sip-id"] = &callState{flowCall: call}
	s.mu.Unlock()

	require.Equal(t, 1, s.ActiveCalls())
	s.Shutdown()
	require.Equal(t, 0, s.ActiveCalls())
	require.True(t, call.IsEnded(), "Shutdown should hang up tracked calls")
}

func TestSignal_PurgeEndedCalls_ReleasesInboundSlot(t *testing.T) {
	ua, err := New(Config{Listen: "127.0.0.1:0", ExtIP: "127.0.0.1", Logger: quietLogger()})
	require.NoError(t, err)
	mgr := session.NewManager(quietLogger())
	s := NewSignal(ua, mgr, nil, quietLogger())

	call := mgr.Create(session.DirectionInbound)
	s.mu.Lock()
	s.calls["stale-sip-id"] = &callState{flowCall: call}
	s.mu.Unlock()

	require.Equal(t, 1, s.ActiveCalls())
	_ = call.Hangup("test_end")
	s.purgeEndedCalls()
	require.Equal(t, 0, s.ActiveCalls())
}
