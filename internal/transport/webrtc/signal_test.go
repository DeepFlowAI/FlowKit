package webrtc

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/rpc"
	"github.com/DeepFlowAI/flowkit/internal/session"
	pwebrtc "github.com/pion/webrtc/v4"
	"github.com/stretchr/testify/require"
)

func newSignalSetup(t *testing.T) (*rpc.Registry, *session.Manager, *SignalState) {
	t.Helper()
	mgr := session.NewManager(quietLogger())
	reg := rpc.NewRegistry(quietLogger())
	// Hello gate is not relevant here — these methods are pre-hello-allowed
	// implicitly via AllowPreHello? No, they are NOT — for tests we'd hit
	// CodeNotReady. Register hello allowance so we can drive directly.
	reg.AllowPreHello("webrtc.offer")
	reg.AllowPreHello("webrtc.ice")

	factory := func() (*Peer, error) {
		return New(Config{Logger: quietLogger()})
	}
	s := RegisterSignalMethods(reg, factory, mgr, nil, quietLogger())
	return reg, mgr, s
}

// driveRegistry sends a request through the Registry and returns the result.
// Bypasses the WebSocket layer — exercises Registry.ProcessRequest directly.
func driveRegistry(t *testing.T, reg *rpc.Registry, method string, params any) (json.RawMessage, *rpc.Error) {
	t.Helper()
	req, err := rpc.NewRequest(method, params, "test-1")
	require.NoError(t, err)
	resp := reg.ProcessRequest(context.Background(), &rpc.Conn{}, req)
	if resp.Error != nil {
		return nil, resp.Error
	}
	return resp.Result, nil
}

func TestSignal_OfferCreatesCallAndPeer(t *testing.T) {
	reg, mgr, s := newSignalSetup(t)
	defer s.Shutdown()

	// Build a real Pion offer to feed in.
	clientPeer, err := New(Config{Logger: quietLogger()})
	require.NoError(t, err)
	defer clientPeer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	offer, err := clientPeer.CreateOffer(ctx)
	require.NoError(t, err)

	raw, rpcErr := driveRegistry(t, reg, "webrtc.offer", map[string]string{"sdp": offer})
	require.Nil(t, rpcErr)

	var result offerResult
	require.NoError(t, json.Unmarshal(raw, &result))
	require.NotEmpty(t, result.CallID)
	require.NotEmpty(t, result.SDP)

	// Manager has the new call
	require.Equal(t, 1, mgr.Count())
	require.Equal(t, 1, s.PeerCount())
}

func TestSignal_Offer_EmptySDP_Rejected(t *testing.T) {
	reg, _, s := newSignalSetup(t)
	defer s.Shutdown()

	_, rpcErr := driveRegistry(t, reg, "webrtc.offer", map[string]string{"sdp": ""})
	require.NotNil(t, rpcErr)
	require.Equal(t, rpc.CodeInvalidParams, rpcErr.Code)
}

func TestSignal_Offer_BadSDP_ReportsError(t *testing.T) {
	reg, _, s := newSignalSetup(t)
	defer s.Shutdown()

	_, rpcErr := driveRegistry(t, reg, "webrtc.offer", map[string]string{"sdp": "garbage"})
	require.NotNil(t, rpcErr)
	require.Equal(t, rpc.CodeWebRTCICEFailed, rpcErr.Code)
}

func TestSignal_ICE_BeforeCall_NotFound(t *testing.T) {
	reg, _, s := newSignalSetup(t)
	defer s.Shutdown()

	_, rpcErr := driveRegistry(t, reg, "webrtc.ice", map[string]any{
		"call_id":   "no-such-call",
		"candidate": pwebrtc.ICECandidateInit{Candidate: "candidate:1 1 udp 1 1.2.3.4 1 typ host"},
	})
	require.NotNil(t, rpcErr)
	require.Equal(t, rpc.CodeCallNotFound, rpcErr.Code)
}

func TestSignal_ICE_MissingCallID(t *testing.T) {
	reg, _, s := newSignalSetup(t)
	defer s.Shutdown()

	_, rpcErr := driveRegistry(t, reg, "webrtc.ice", map[string]any{
		"candidate": pwebrtc.ICECandidateInit{},
	})
	require.NotNil(t, rpcErr)
	require.Equal(t, rpc.CodeInvalidParams, rpcErr.Code)
}

func TestSignal_AutoCleanup_OnCallEnd(t *testing.T) {
	reg, mgr, s := newSignalSetup(t)
	defer s.Shutdown()

	// Create a real call via webrtc.offer
	clientPeer, err := New(Config{Logger: quietLogger()})
	require.NoError(t, err)
	defer clientPeer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	offer, err := clientPeer.CreateOffer(ctx)
	require.NoError(t, err)

	raw, rpcErr := driveRegistry(t, reg, "webrtc.offer", map[string]string{"sdp": offer})
	require.Nil(t, rpcErr)
	var result offerResult
	require.NoError(t, json.Unmarshal(raw, &result))

	require.Equal(t, 1, s.PeerCount())

	// Hang up the call → SignalState should clean up the peer
	require.NoError(t, mgr.Close(result.CallID, "test_hangup"))

	require.Eventually(t, func() bool { return s.PeerCount() == 0 }, 2*time.Second, 20*time.Millisecond)
}

func TestSignal_FactoryError(t *testing.T) {
	mgr := session.NewManager(quietLogger())
	reg := rpc.NewRegistry(quietLogger())
	reg.AllowPreHello("webrtc.offer")

	failingFactory := func() (*Peer, error) {
		return nil, errors.New("forced failure")
	}
	s := RegisterSignalMethods(reg, failingFactory, mgr, nil, quietLogger())
	defer s.Shutdown()

	_, rpcErr := driveRegistry(t, reg, "webrtc.offer", map[string]string{"sdp": "x"})
	require.NotNil(t, rpcErr)
	require.Equal(t, rpc.CodeWebRTCICEFailed, rpcErr.Code)
}
