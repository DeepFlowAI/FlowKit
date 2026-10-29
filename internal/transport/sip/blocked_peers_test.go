package sip

import (
	"testing"

	"github.com/DeepFlowAI/flowkit/internal/session"
	"github.com/stretchr/testify/require"
)

func TestPeerHostFromSource(t *testing.T) {
	require.Equal(t, "192.0.2.43", peerHostFromSource("192.0.2.43:5060"))
	require.Equal(t, "10.0.0.1", peerHostFromSource("10.0.0.1"))
}

func TestSetBlockedPeers(t *testing.T) {
	ua, err := New(Config{Listen: "127.0.0.1:0", ExtIP: "127.0.0.1", Logger: quietLogger()})
	require.NoError(t, err)
	mgr := session.NewManager(quietLogger())
	s := NewSignal(ua, mgr, &captureBroadcaster{}, quietLogger())

	s.SetBlockedPeers([]string{"192.0.2.43", " 10.0.0.2 "})
	require.True(t, s.isBlockedPeer("192.0.2.43"))
	require.True(t, s.isBlockedPeer("10.0.0.2"))
	require.False(t, s.isBlockedPeer("203.0.113.146"))
	require.False(t, s.isBlockedPeer(""))
}
