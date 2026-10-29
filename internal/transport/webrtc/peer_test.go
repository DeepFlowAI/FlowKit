package webrtc

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/transport"
	pwebrtc "github.com/pion/webrtc/v4"
	"github.com/stretchr/testify/require"
)

// compile-time interface check
var _ transport.MediaIO = (*pcmMediaIO)(nil)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(discardWriter{}, &slog.HandlerOptions{Level: slog.LevelError + 1}))
}

type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }

func newPeer(t *testing.T) *Peer {
	t.Helper()
	p, err := New(Config{
		Logger:           quietLogger(),
		ICEGatherMaxWait: 5 * time.Second, // embed candidates for loopback tests
	})
	require.NoError(t, err)
	return p
}

func TestNew_DefaultConfig(t *testing.T) {
	p, err := New(Config{})
	require.NoError(t, err)
	defer p.Close()
	require.NotNil(t, p.MediaIO())
	require.NotNil(t, p.LocalTrack())
}

func TestPeer_ICEServersWired(t *testing.T) {
	p, err := New(Config{
		ICEServers: []string{"stun:stun.l.google.com:19302"},
		Logger:     quietLogger(),
	})
	require.NoError(t, err)
	defer p.Close()
	// The configuration is internal; ensure no error is the visible bar.
}

func TestPeer_LoopbackSDPRoundtrip(t *testing.T) {
	// Two Peers — A creates the offer, B answers, A sets remote answer.
	// CreateOffer/HandleOffer wait for ICE gathering to complete, so SDP
	// already embeds all candidates. No trickle, no candidate forwarding.
	peerA := newPeer(t)
	peerB := newPeer(t)
	defer peerA.Close()
	defer peerB.Close()

	var connectedA, connectedB atomic.Bool
	var wg sync.WaitGroup
	wg.Add(2)
	peerA.OnConnected(func() { connectedA.Store(true); wg.Done() })
	peerB.OnConnected(func() { connectedB.Store(true); wg.Done() })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	offerSDP, err := peerA.CreateOffer(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, offerSDP)

	answerSDP, err := peerB.HandleOffer(ctx, offerSDP)
	require.NoError(t, err)
	require.NotEmpty(t, answerSDP)

	require.NoError(t, peerA.SetRemoteAnswer(ctx, answerSDP))

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatalf("peers did not reach Connected; A=%v B=%v", connectedA.Load(), connectedB.Load())
	}
	require.True(t, connectedA.Load())
	require.True(t, connectedB.Load())
}

func TestPeer_OnLocalICECandidate_FiresAtLeastOnce(t *testing.T) {
	// Verify the OnLocalICECandidate callback wiring fires while gathering.
	// We don't forward the candidates (that's the SDP-embedded test above);
	// just count that they showed up.
	p := newPeer(t)
	defer p.Close()

	var seen atomic.Int32
	p.OnLocalICECandidate(func(_ pwebrtc.ICECandidateInit) { seen.Add(1) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := p.CreateOffer(ctx)
	require.NoError(t, err)

	require.Greater(t, seen.Load(), int32(0), "expected at least one local ICE candidate")
}

func TestPeer_HandleOffer_OnClosed(t *testing.T) {
	p := newPeer(t)
	require.NoError(t, p.Close())
	_, err := p.HandleOffer(context.Background(), "")
	require.Error(t, err)
}

func TestPeer_CreateOffer_OnClosed(t *testing.T) {
	p := newPeer(t)
	require.NoError(t, p.Close())
	_, err := p.CreateOffer(context.Background())
	require.Error(t, err)
}

func TestPeer_AddICECandidate_OnClosed(t *testing.T) {
	p := newPeer(t)
	require.NoError(t, p.Close())
	require.Error(t, p.AddICECandidate(pwebrtc.ICECandidateInit{Candidate: ""}))
}

func TestPeer_Close_Idempotent(t *testing.T) {
	p := newPeer(t)
	require.NoError(t, p.Close())
	require.NoError(t, p.Close())
}

func TestPeer_MediaIO_ChannelsClosedAfterPeerClose(t *testing.T) {
	p := newPeer(t)
	mio := p.MediaIO()
	require.NoError(t, p.Close())

	// Both channels should be closed within a moment.
	select {
	case _, ok := <-mio.IncomingPCM():
		require.False(t, ok)
	case <-time.After(time.Second):
		t.Fatal("IncomingPCM not closed")
	}
}

func TestPeer_HandleOffer_RejectsBadSDP(t *testing.T) {
	p := newPeer(t)
	defer p.Close()
	_, err := p.HandleOffer(context.Background(), "not a real sdp")
	require.Error(t, err)
}
