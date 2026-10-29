package calls

import (
	"context"
	"testing"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/asr"
	asrmock "github.com/DeepFlowAI/flowkit/internal/asr/mock"
	"github.com/DeepFlowAI/flowkit/internal/rpc"
	"github.com/DeepFlowAI/flowkit/internal/session"
	"github.com/stretchr/testify/require"
)

type endpointTrackingASR struct {
	*asrmock.Mock
	endpointMs  int
	endpointRMS int
}

func (p *endpointTrackingASR) SetEndpointSilence(ms, rms int) {
	p.endpointMs = ms
	p.endpointRMS = rms
}

func TestHandleListen_StartsASR(t *testing.T) {
	reg, mgr, _ := newTestStack(t)
	call := mgr.Create(session.DirectionInbound)

	_, rpcErr := driveRPC(t, reg, "call.listen", map[string]string{
		"call_id": call.ID(),
	})
	require.Nil(t, rpcErr)
	// Second call is idempotent
	_, rpcErr = driveRPC(t, reg, "call.listen", map[string]string{
		"call_id": call.ID(),
	})
	require.Nil(t, rpcErr)
	require.NoError(t, call.Hangup("done"))
}

func TestHandleListen_AppliesPerCallEndpointPerformance(t *testing.T) {
	reg, mgr, h := newTestStack(t)
	call := mgr.Create(session.DirectionInbound)
	provider := &endpointTrackingASR{Mock: asrmock.New()}
	h.factories.ASR = func() (asr.Provider, error) { return provider, nil }

	_, rpcErr := driveRPC(t, reg, "call.listen", map[string]any{
		"call_id": call.ID(),
		"performance": map[string]any{
			"endpoint_silence_ms":    1000,
			"turn_detection_enabled": false,
		},
	})
	require.Nil(t, rpcErr)
	require.Equal(t, 1000, provider.endpointMs)
	cs := h.stateGet(call.ID())
	require.NotNil(t, cs)
	require.NotNil(t, cs.performance)
	require.NotNil(t, cs.performance.TurnDetectionEnabled)
	require.False(t, *cs.performance.TurnDetectionEnabled)
	require.NoError(t, call.Hangup("done"))
}

func TestHandleListen_StopsASROnCallEndWithoutHangupRPC(t *testing.T) {
	reg, mgr, h := newTestStack(t)
	call := mgr.Create(session.DirectionInbound)

	_, rpcErr := driveRPC(t, reg, "call.listen", map[string]string{
		"call_id": call.ID(),
	})
	require.Nil(t, rpcErr)

	require.NoError(t, call.Hangup("remote_bye"))

	require.Eventually(t, func() bool {
		h.mu.Lock()
		defer h.mu.Unlock()
		_, ok := h.callState[call.ID()]
		return !ok
	}, time.Second, 10*time.Millisecond, "listen cleanup should run when call ends without call.hangup RPC")
}

func TestHandleListen_RestartAfterStop(t *testing.T) {
	reg, mgr, h := newTestStack(t)
	call := mgr.Create(session.DirectionInbound)

	_, rpcErr := driveRPC(t, reg, "call.listen", map[string]string{"call_id": call.ID()})
	require.Nil(t, rpcErr)

	_, rpcErr = driveRPC(t, reg, "call.stop", map[string]string{
		"call_id": call.ID(), "target": "listen",
	})
	require.Nil(t, rpcErr)
	require.Eventually(t, func() bool {
		cs := h.stateGet(call.ID())
		return cs != nil && !cs.listening.Load()
	}, time.Second, 5*time.Millisecond, "listen stop should clear listening before restart")

	_, rpcErr = driveRPC(t, reg, "call.listen", map[string]string{"call_id": call.ID()})
	require.Nil(t, rpcErr)

	// call.stop keeps callState; a second listen must reuse it and must not
	// spawn another call.Done watcher (that was a goroutine leak + data race).
	h.mu.Lock()
	cs := h.callState[call.ID()]
	require.NotNil(t, cs)
	require.Equal(t, 1, cs.mediaWatchStarts, "exactly one media-end watcher per call")
	require.True(t, cs.listening.Load())
	require.NotNil(t, cs.asrProvider)
	h.mu.Unlock()

	require.NoError(t, call.Hangup("done"))
	require.Eventually(t, func() bool {
		return h.stateGet(call.ID()) == nil
	}, time.Second, 5*time.Millisecond, "hangup should drop callState via the single watcher")
}

// A call whose media has ended has a closed bus, and the bus refuses new
// subscribers. The audio pump used to dereference that nil, and a panic in a
// goroutine takes down every call in the process, not just this one.
func TestHandleListen_ClosedAudioBus_Errors(t *testing.T) {
	reg, mgr, _ := newTestStack(t)
	call := mgr.Create(session.DirectionInbound)
	// Straight to the state a hangup arriving mid-RPC leaves behind: the call is
	// still registered, its bus is not accepting subscribers.
	call.AudioBus().Close()

	_, rpcErr := driveRPC(t, reg, "call.listen", map[string]string{"call_id": call.ID()})
	require.NotNil(t, rpcErr)
	require.Equal(t, rpc.CodeCallStateInvalid, rpcErr.Code)
}

func TestHandleListen_NotFound(t *testing.T) {
	reg, _, _ := newTestStack(t)
	_, rpcErr := driveRPC(t, reg, "call.listen", map[string]string{
		"call_id": "ghost",
	})
	require.NotNil(t, rpcErr)
	require.Equal(t, rpc.CodeCallNotFound, rpcErr.Code)
}

func TestHandleListen_AdaptiveBargeIgnoresOnsetAndStopsOnConfirmedSpeech(t *testing.T) {
	reg, mgr, h := newTestStack(t)
	call := mgr.Create(session.DirectionInbound)

	_, rpcErr := driveRPC(t, reg, "call.listen", map[string]any{
		"call_id": call.ID(), "adaptive_barge_in": true,
	})
	require.Nil(t, rpcErr)
	playCtx, cancelPlay := context.WithCancel(context.Background())
	cs := h.stateGet(call.ID())
	require.NotNil(t, cs)
	cs.playMu.Lock()
	cs.playCancel = cancelPlay
	cs.playMu.Unlock()

	// Carrier answer noise may raise a raw VAD onset, then be discarded by the
	// shared ASR gate. Neither event is enough to cut audible playback.
	h.dispatch(call.ID(), "call.speech_start", nil)
	h.dispatch(call.ID(), "call.speech_discard", nil)
	require.NoError(t, playCtx.Err())

	h.dispatch(call.ID(), "call.speech_start", nil)
	h.dispatch(call.ID(), "call.speech_partial", map[string]any{"text": "等一下"})
	require.Eventually(t, func() bool { return playCtx.Err() != nil }, time.Second, 10*time.Millisecond)
	require.NoError(t, call.Hangup("done"))
}

func TestHandleListen_StopDetachesAdaptiveBarge(t *testing.T) {
	reg, mgr, h := newTestStack(t)
	call := mgr.Create(session.DirectionInbound)

	_, rpcErr := driveRPC(t, reg, "call.listen", map[string]any{
		"call_id": call.ID(), "adaptive_barge_in": true,
	})
	require.Nil(t, rpcErr)
	playCtx, cancelPlay := context.WithCancel(context.Background())
	cs := h.stateGet(call.ID())
	require.NotNil(t, cs)
	cs.playMu.Lock()
	cs.playCancel = cancelPlay
	cs.playMu.Unlock()

	_, rpcErr = driveRPC(t, reg, "call.stop", map[string]any{
		"call_id": call.ID(), "target": "listen",
	})
	require.Nil(t, rpcErr)
	h.dispatch(call.ID(), "call.speech_end", map[string]any{"text": "等一下"})
	require.NoError(t, playCtx.Err(), "stopped listener must not retain a barge subscription")

	_, rpcErr = driveRPC(t, reg, "call.stop", map[string]any{
		"call_id": call.ID(), "target": "play",
	})
	require.Nil(t, rpcErr)
	require.NoError(t, call.Hangup("done"))
}
