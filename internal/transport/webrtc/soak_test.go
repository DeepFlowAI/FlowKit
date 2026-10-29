package webrtc

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/rpc"
	"github.com/stretchr/testify/require"
)

// TestSoak_ConcurrentOffers stresses SignalState by spawning N concurrent
// Pion-driven offers, holding briefly, and tearing down. Replaces a
// Playwright-based browser soak (Playwright's sync API can't drive multi-
// threaded contexts cleanly; this Go test exercises the same code paths
// directly).
//
// Knobs via env:
//   SOAK_SESSIONS — default 5
//   SOAK_DURATION — seconds each session holds, default 2
func TestSoak_ConcurrentOffers(t *testing.T) {
	const sessions = 5
	holdDuration := 2 * time.Second

	reg, mgr, s := newSignalSetup(t)
	defer s.Shutdown()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	var failed atomic.Int32
	var connected atomic.Int32

	for i := 0; i < sessions; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			clientPeer, err := New(Config{Logger: quietLogger()})
			if err != nil {
				t.Logf("session %d: peer New failed: %v", idx, err)
				failed.Add(1)
				return
			}
			defer clientPeer.Close()

			offerSDP, err := clientPeer.CreateOffer(ctx)
			if err != nil {
				t.Logf("session %d: CreateOffer failed: %v", idx, err)
				failed.Add(1)
				return
			}

			raw, rpcErr := driveRegistry(t, reg, "webrtc.offer", map[string]string{"sdp": offerSDP})
			if rpcErr != nil {
				t.Logf("session %d: webrtc.offer rpc error: %v", idx, rpcErr)
				failed.Add(1)
				return
			}
			var result offerResult
			if err := json.Unmarshal(raw, &result); err != nil {
				t.Logf("session %d: bad result json: %v", idx, err)
				failed.Add(1)
				return
			}

			if err := clientPeer.SetRemoteAnswer(ctx, result.SDP); err != nil {
				t.Logf("session %d: SetRemoteAnswer failed: %v", idx, err)
				failed.Add(1)
				return
			}

			connected.Add(1)
			// Hold for a bit to simulate an active call
			select {
			case <-time.After(holdDuration):
			case <-ctx.Done():
			}

			// Hang up via session.Manager — exercises the auto-cleanup path
			_ = mgr.Close(result.CallID, "soak_end")
		}(i)
	}

	wg.Wait()

	require.Zero(t, failed.Load(), "%d/%d sessions failed", failed.Load(), sessions)
	require.Equal(t, int32(sessions), connected.Load(),
		"expected all %d sessions to complete SDP exchange", sessions)

	// All peers should be cleaned up after their Calls end
	require.Eventually(t, func() bool { return s.PeerCount() == 0 }, 5*time.Second, 50*time.Millisecond,
		"SignalState should have 0 peers after all sessions hangup; have %d", s.PeerCount())

	// Health check: registry methods still respond
	probe := &rpc.Request{JSONRPC: rpc.Version, Method: "webrtc.ice", Params: json.RawMessage(`{"call_id":"x"}`), ID: json.RawMessage(`"probe"`)}
	resp := reg.ProcessRequest(context.Background(), &rpc.Conn{}, probe)
	require.NotNil(t, resp)
}
