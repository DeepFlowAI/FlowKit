package calls

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/observability"
	"github.com/DeepFlowAI/flowkit/internal/recording"
	"github.com/DeepFlowAI/flowkit/internal/rpc"
	"github.com/DeepFlowAI/flowkit/internal/session"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

type bridgeParams struct {
	CallID      string `json:"call_id"`
	OtherCallID string `json:"other_call_id"`
	// UserCallID explicitly identifies the customer leg. This is required for
	// outbound customer calls, where SIP/WebRTC transport direction does not
	// describe the participant role. Omitted callers retain legacy inference.
	UserCallID string `json:"user_call_id,omitempty"`
	// Transcribe gates the live-transcript ASR for this bridge. Absent (nil)
	// or true keeps the default-on behavior; false disables ASR for this call.
	Transcribe *bool `json:"transcribe,omitempty"`
	// KeepaliveOnPeerHangup keeps the surviving leg alive when the other side
	// hangs up (consult A↔B: B leaving must not kill A's media). Default false
	// preserves the two-party customer↔agent behavior (peer hangup ends both).
	KeepaliveOnPeerHangup *bool `json:"keepalive_on_peer_hangup,omitempty"`
}

type bridgePair struct {
	a, b string
	stop chan struct{}
	once sync.Once
	wg   sync.WaitGroup

	keepaliveOnPeerHangup bool

	// asr holds the per-leg ASR streams when bridge transcription is on.
	asrMu sync.Mutex
	asr   []*bridgeASRSession
}

func (h *Handlers) handleBridge(ctx context.Context, conn *rpc.Conn, params json.RawMessage) (any, *rpc.Error) {
	var p bridgeParams
	if rpcErr := rpc.DecodeParams(params, &p); rpcErr != nil {
		return nil, rpcErr
	}
	if p.CallID == "" || p.OtherCallID == "" {
		return nil, rpc.ErrInvalidParams("call_id and other_call_id required")
	}
	if p.CallID == p.OtherCallID {
		return nil, rpc.ErrInvalidParams("cannot bridge a call to itself")
	}

	ctx, span := tracer.Start(ctx, "call.bridge")
	defer span.End()
	span.SetAttributes(
		attribute.String("call_id", p.CallID),
		attribute.String("other_call_id", p.OtherCallID),
	)

	callA, err := h.mgr.Get(p.CallID)
	if err != nil {
		span.SetStatus(codes.Error, "call_not_found")
		return nil, rpc.ErrCallNotFound(p.CallID)
	}
	if rpcErr := h.kernelExclusiveReject(conn, callA, "call.bridge"); rpcErr != nil {
		return nil, rpcErr
	}
	callB, err := h.mgr.Get(p.OtherCallID)
	if err != nil {
		span.SetStatus(codes.Error, "other_not_found")
		return nil, rpc.ErrCallNotFound(p.OtherCallID)
	}
	if rpcErr := h.kernelExclusiveReject(conn, callB, "call.bridge"); rpcErr != nil {
		return nil, rpcErr
	}

	userLeg, agentLeg := recording.PickBridgeLegs(callA, callB)
	if p.UserCallID != "" {
		switch p.UserCallID {
		case callA.ID():
			userLeg, agentLeg = callA, callB
		case callB.ID():
			userLeg, agentLeg = callB, callA
		default:
			return nil, rpc.ErrInvalidParams("user_call_id must match one bridged call")
		}
	}

	if callA.IsEnded() || callB.IsEnded() {
		return nil, rpc.ErrCallStateInvalid("call ended")
	}
	if callA.MediaIO() == nil || callB.MediaIO() == nil {
		return nil, rpc.ErrCallStateInvalid("media not ready on both legs")
	}
	if !bridgeableState(callA.State()) || !bridgeableState(callB.State()) {
		return nil, rpc.ErrCallStateInvalid(fmt.Sprintf("invalid state (%s ↔ %s)", callA.State(), callB.State()))
	}

	// Stop TTS/ASR on both legs before bridging (e.g. SIP LLM → human handoff).
	h.stopCallMedia(p.CallID, "all")
	h.stopCallMedia(p.OtherCallID, "all")
	// AEC3 is for cancelling local TTS from the mic uplink. During a
	// human↔human bridge the "render" reference becomes the other party,
	// which is not an acoustic echo path through the server — AEC3 then
	// injects musical noise / hiss on both legs. Drop it for the bridge.
	disableCaptureAEC(callA)
	disableCaptureAEC(callB)

	session.LinkConversation(callA, callB)

	h.bridgeMu.Lock()
	if peer, ok := h.bridgePeers[p.CallID]; ok {
		h.bridgeMu.Unlock()
		if peer == p.OtherCallID {
			return map[string]any{"ok": true, "call_id": p.CallID, "other_call_id": p.OtherCallID}, nil
		}
		return nil, rpc.ErrCallStateInvalid("call already bridged")
	}
	if peer, ok := h.bridgePeers[p.OtherCallID]; ok {
		h.bridgeMu.Unlock()
		return nil, rpc.ErrCallStateInvalid(fmt.Sprintf("other call already bridged to %s", peer))
	}

	stop := make(chan struct{})
	keepalive := p.KeepaliveOnPeerHangup != nil && *p.KeepaliveOnPeerHangup
	pair := &bridgePair{
		a: p.CallID, b: p.OtherCallID, stop: stop,
		keepaliveOnPeerHangup: keepalive,
	}
	h.bridgePeers[p.CallID] = p.OtherCallID
	h.bridgePeers[p.OtherCallID] = p.CallID
	h.bridgePairs[bridgeKey(p.CallID, p.OtherCallID)] = pair
	h.bridgeMu.Unlock()

	// Either leg may be a keepalive survivor being re-bridged (agent
	// reconnected after a browser refresh) — cancel its pending sweep.
	h.disarmKeepaliveSweep(p.CallID)
	h.disarmKeepaliveSweep(p.OtherCallID)

	h.stateFor(p.CallID, conn)
	h.stateFor(p.OtherCallID, conn)

	if err := callA.SetState(session.StateBridged); err != nil {
		h.removeBridge(p.CallID, p.OtherCallID)
		return nil, rpc.NewErrorf(rpc.CodeCallStateInvalid, "%v", err)
	}
	if err := callB.SetState(session.StateBridged); err != nil {
		_ = callA.SetState(session.StateActive)
		h.removeBridge(p.CallID, p.OtherCallID)
		return nil, rpc.NewErrorf(rpc.CodeCallStateInvalid, "%v", err)
	}

	pair.wg.Add(2)
	go h.runBridgeLeg(callA, callB, stop, &pair.wg)
	go h.runBridgeLeg(callB, callA, stop, &pair.wg)
	go h.watchBridgePair(pair)

	h.broadcast("call.bridged", p.CallID, map[string]any{
		"other_call_id":   p.OtherCallID,
		"conversation_id": callA.ConversationID(),
		"root_call_id":    callA.RootCallID(),
		"phase":           callA.Phase(),
	})
	h.broadcast("call.bridged", p.OtherCallID, map[string]any{
		"other_call_id":   p.CallID,
		"conversation_id": callB.ConversationID(),
		"root_call_id":    callB.RootCallID(),
		"phase":           callB.Phase(),
	})

	if h.bridgeObs != nil {
		h.bridgeObs.OnBridged(userLeg, agentLeg)
	}

	transcribe := p.Transcribe == nil || *p.Transcribe
	if h.bridgeASREnabled && transcribe {
		h.startBridgeASR(pair, userLeg, agentLeg)
	}

	observability.CallTraceCall(ctx, h.log, "rpc.bridge", callA,
		"other_call_id", p.OtherCallID,
	)
	return map[string]any{
		"ok":            true,
		"call_id":       p.CallID,
		"other_call_id": p.OtherCallID,
	}, nil
}

func (h *Handlers) handleUnbridge(ctx context.Context, conn *rpc.Conn, params json.RawMessage) (any, *rpc.Error) {
	var p callIDParams
	if rpcErr := rpc.DecodeParams(params, &p); rpcErr != nil {
		return nil, rpcErr
	}
	if p.CallID == "" {
		return nil, rpc.ErrInvalidParams("call_id required")
	}
	call, err := h.mgr.Get(p.CallID)
	if err != nil {
		return nil, rpc.ErrCallNotFound(p.CallID)
	}
	if rpcErr := h.kernelExclusiveReject(conn, call, "call.unbridge"); rpcErr != nil {
		return nil, rpcErr
	}

	h.bridgeMu.Lock()
	peer, ok := h.bridgePeers[p.CallID]
	h.bridgeMu.Unlock()
	if !ok {
		return map[string]any{"ok": true, "call_id": p.CallID}, nil
	}

	h.unbridgeCall(p.CallID)

	callA, _ := h.mgr.Get(p.CallID)
	callB, _ := h.mgr.Get(peer)
	if callA != nil && !callA.IsEnded() {
		if callA.State() == session.StateBridged {
			_ = callA.SetState(session.StateActive)
		}
		if callA.Direction() == session.DirectionInbound {
			callA.SetPhase(session.PhaseLLM)
		}
	}
	if callB != nil && !callB.IsEnded() {
		if callB.State() == session.StateBridged {
			_ = callB.SetState(session.StateActive)
		}
		if callB.Direction() == session.DirectionInbound {
			callB.SetPhase(session.PhaseLLM)
		}
	}

	h.broadcast("call.unbridged", p.CallID, map[string]any{"other_call_id": peer})
	h.broadcast("call.unbridged", peer, map[string]any{"other_call_id": p.CallID})
	if callA != nil {
		observability.CallTraceCall(ctx, h.log, "rpc.unbridge", callA, "other_call_id", peer)
	} else {
		observability.CallTrace(ctx, h.log, "rpc.unbridge", p.CallID, "other_call_id", peer)
	}
	return map[string]any{
		"ok":            true,
		"call_id":       p.CallID,
		"other_call_id": peer,
	}, nil
}

func bridgeableState(s session.State) bool {
	switch s {
	case session.StateAnswered, session.StateActive, session.StateBridged:
		return true
	default:
		return false
	}
}

func bridgeKey(a, b string) string {
	if a < b {
		return a + "|" + b
	}
	return b + "|" + a
}

func disableCaptureAEC(call *session.Call) {
	if call == nil {
		return
	}
	call.RemoveRenderSink("aec")
	call.SetCaptureProcessor(nil)
}

func (h *Handlers) runBridgeLeg(src, dst *session.Call, stop <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	sub := src.AudioBus().Subscribe()
	if sub == nil {
		return
	}
	defer src.AudioBus().Unsubscribe(sub)

	for {
		select {
		case <-stop:
			return
		case frame, ok := <-sub.Channel():
			if !ok {
				return
			}
			if src.IsEnded() || dst.IsEnded() {
				return
			}
			if err := dst.WriteOutgoing(frame.Samples); err != nil {
				if !errors.Is(err, session.ErrEnded) {
					h.log.Debug("bridge write failed", "from", src.ID(), "to", dst.ID(), "err", err)
				}
				return
			}
		}
	}
}

func (h *Handlers) watchBridgePair(pair *bridgePair) {
	callA, errA := h.mgr.Get(pair.a)
	callB, errB := h.mgr.Get(pair.b)
	// Hangup can race ahead of this goroutine: the peer may already be ended
	// (or removed from the manager map) before we enter the select. Apply
	// peer-gone / keepalive immediately instead of aborting and leaving the
	// survivor stuck in StateBridged.
	aGone := errA != nil || (callA != nil && callA.IsEnded())
	bGone := errB != nil || (callB != nil && callB.IsEnded())
	switch {
	case aGone && bGone:
		h.teardownBridge(pair.a, pair.b, "bridge_setup_failed")
		return
	case aGone:
		h.onBridgePeerGone(pair, pair.a, pair.b, callB, errB)
		return
	case bGone:
		h.onBridgePeerGone(pair, pair.b, pair.a, callA, errA)
		return
	}

	doneA := callA.Done()
	doneB := callB.Done()
	select {
	case <-doneA:
		h.onBridgePeerGone(pair, pair.a, pair.b, callB, errB)
	case <-doneB:
		h.onBridgePeerGone(pair, pair.b, pair.a, callA, errA)
	case <-pair.stop:
		// hangup closes a leg then calls unbridgeCall, which often wins this
		// select over Done(). Still apply peer-gone / keepalive semantics.
		h.onBridgeWatchStopped(pair)
	}
}

// onBridgeWatchStopped runs when the bridge is torn down via unbridge/hangup
// rather than the Done() path. If one leg already ended, mirror peer-gone
// behavior so keepalive consult bridges leave the survivor Active.
func (h *Handlers) onBridgeWatchStopped(pair *bridgePair) {
	callA, errA := h.mgr.Get(pair.a)
	callB, errB := h.mgr.Get(pair.b)
	aEnded := errA != nil || (callA != nil && callA.IsEnded())
	bEnded := errB != nil || (callB != nil && callB.IsEnded())
	if !aEnded && !bEnded {
		// Explicit call.unbridge while both legs are still up.
		return
	}
	switch {
	case aEnded && !bEnded:
		h.onBridgePeerGone(pair, pair.a, pair.b, callB, errB)
	case bEnded && !aEnded:
		h.onBridgePeerGone(pair, pair.b, pair.a, callA, errA)
	default:
		// Both already gone; hangup's synchronous restoreKeepaliveSurvivor
		// handles the consult survivor. Just ensure the pair is torn down.
		h.teardownBridge(pair.a, pair.b, "bridge_peer_gone")
	}
}

func (h *Handlers) onBridgePeerGone(
	pair *bridgePair,
	gone, survivor string,
	survivorCall *session.Call,
	survivorErr error,
) {
	h.teardownBridge(pair.a, pair.b, "bridge_peer_gone")
	if pair.keepaliveOnPeerHangup {
		// Consult-style bridge: tear audio only; leave the survivor up so the
		// controller can re-bridge them to the held customer.
		if survivorErr == nil && survivorCall != nil && !survivorCall.IsEnded() {
			h.restoreKeepaliveSurvivor(survivor, gone)
		}
		return
	}
	if survivorErr == nil && survivorCall != nil && !survivorCall.IsEnded() {
		_ = h.mgr.Close(survivor, "bridge_peer_gone")
	}
	h.broadcastCallHangup(pair.a, "bridge_peer_gone")
	h.broadcastCallHangup(pair.b, "bridge_peer_gone")
}

// keepaliveBridgePeer returns the bridged peer id when the given call is in a
// keepalive_on_peer_hangup bridge; otherwise "".
func (h *Handlers) keepaliveBridgePeer(callID string) string {
	h.bridgeMu.Lock()
	defer h.bridgeMu.Unlock()
	peer, ok := h.bridgePeers[callID]
	if !ok {
		return ""
	}
	pair, ok := h.bridgePairs[bridgeKey(callID, peer)]
	if !ok || !pair.keepaliveOnPeerHangup {
		return ""
	}
	return peer
}

// restoreKeepaliveSurvivor moves a consult survivor out of StateBridged after
// the peer hangs up. Idempotent when the watcher and hangup path both run.
func (h *Handlers) restoreKeepaliveSurvivor(survivor, gone string) {
	call, err := h.mgr.Get(survivor)
	if err != nil || call == nil || call.IsEnded() {
		return
	}
	if call.State() != session.StateBridged {
		return
	}
	// A late watch-stop for the old pair can land after the survivor was
	// already re-bridged to a new peer (blind transfer: the bridge to the
	// target wins the race, then the watcher for the torn-down initiator
	// pair runs). Demoting + arming here would close the live re-bridged
	// call when the sweep fires.
	if peer := h.bridgePeerOf(survivor); peer != "" && peer != gone {
		return
	}
	if err := call.SetState(session.StateActive); err != nil {
		return
	}
	h.broadcast("call.bridge_peer_left", survivor, map[string]any{
		"peer_call_id": gone,
		"reason":       "bridge_peer_gone",
	})
	h.armKeepaliveSweep(call)
}

// bridgePeerOf reports the call's current bridge peer, "" when unbridged.
func (h *Handlers) bridgePeerOf(callID string) string {
	h.bridgeMu.Lock()
	defer h.bridgeMu.Unlock()
	return h.bridgePeers[callID]
}

// SetKeepaliveSweep bounds how long a keepalive survivor leg may stay
// un-bridged before FlowKit closes it with reason "keepalive_timeout".
// FlowKit cannot distinguish "agent browser reconnecting" from "agent gone
// for good", so without this bound an orphaned customer leg leaks in
// StateActive forever. d <= 0 disables the sweep.
func (h *Handlers) SetKeepaliveSweep(d time.Duration) {
	h.keepaliveSweep = d
}

func (h *Handlers) armKeepaliveSweep(c *session.Call) {
	if h.keepaliveSweep <= 0 {
		return
	}
	h.keepaliveMu.Lock()
	if t, ok := h.keepaliveTimers[c.ID()]; ok {
		t.Stop()
	}
	sweep := h.keepaliveSweep
	h.keepaliveTimers[c.ID()] = time.AfterFunc(sweep, func() {
		h.keepaliveMu.Lock()
		delete(h.keepaliveTimers, c.ID())
		h.keepaliveMu.Unlock()
		if c.IsEnded() {
			return
		}
		// Re-bridged after arming (the disarm lost a race): the survivor is
		// no longer orphaned, so closing it would kill a live call.
		if c.State() == session.StateBridged {
			return
		}
		h.log.Warn("keepalive survivor not re-bridged in time; closing",
			"call_id", c.ID(), "sweep", sweep)
		if err := h.mgr.Close(c.ID(), "keepalive_timeout"); err == nil {
			h.broadcastCallHangup(c.ID(), "keepalive_timeout")
		}
	})
	h.keepaliveMu.Unlock()
}

func (h *Handlers) disarmKeepaliveSweep(callID string) {
	h.keepaliveMu.Lock()
	defer h.keepaliveMu.Unlock()
	if t, ok := h.keepaliveTimers[callID]; ok {
		t.Stop()
		delete(h.keepaliveTimers, callID)
	}
}

func (h *Handlers) teardownBridge(a, b, reason string) {
	h.bridgeMu.Lock()
	key := bridgeKey(a, b)
	pair, ok := h.bridgePairs[key]
	if ok {
		delete(h.bridgePairs, key)
	}
	// Only clear a peer index entry while it still points at this pair: a
	// late teardown for a stale pair (e.g. blind transfer — the leg already
	// re-bridged to a new peer) must not wipe the fresh re-bridge entry.
	if h.bridgePeers[a] == b {
		delete(h.bridgePeers, a)
	}
	if h.bridgePeers[b] == a {
		delete(h.bridgePeers, b)
	}
	h.bridgeMu.Unlock()

	if ok && pair != nil {
		pair.once.Do(func() { close(pair.stop) })
		h.stopBridgeASR(pair)
		pair.wg.Wait()
	}
	h.log.Debug("bridge torn down", "call_a", a, "call_b", b, "reason", reason)
	// Teardown often runs after one or both legs have ended; keep Values.
	if callA, err := h.mgr.Get(a); err == nil {
		observability.CallTraceCall(context.WithoutCancel(callA.Context()), h.log, "bridge.teardown", callA,
			"other_call_id", b, "reason", reason)
	} else {
		observability.CallTrace(context.Background(), h.log, "bridge.teardown", a,
			"other_call_id", b, "reason", reason)
	}
}

func (h *Handlers) removeBridge(a, b string) {
	h.teardownBridge(a, b, "bridge_aborted")
}

func (h *Handlers) unbridgeCall(callID string) {
	h.bridgeMu.Lock()
	peer, ok := h.bridgePeers[callID]
	h.bridgeMu.Unlock()
	if !ok {
		return
	}
	h.teardownBridge(callID, peer, "unbridge")
}
