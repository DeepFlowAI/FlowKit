package calls

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/DeepFlowAI/flowkit/internal/observability"
	"github.com/DeepFlowAI/flowkit/internal/rpc"
	"github.com/DeepFlowAI/flowkit/internal/session"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

type callIDParams struct {
	CallID string `json:"call_id"`
	Reason string `json:"reason,omitempty"`
}

func (h *Handlers) handleAnswer(ctx context.Context, conn *rpc.Conn, params json.RawMessage) (any, *rpc.Error) {
	var p callIDParams
	if rpcErr := rpc.DecodeParams(params, &p); rpcErr != nil {
		return nil, rpcErr
	}
	_, span := tracer.Start(ctx, "call.answer") // attribute.String avoids reflection (attribute.Any falls back to fmt.Sprintf)

	defer span.End()
	span.SetAttributes(attribute.String("call_id", p.CallID))

	call, err := h.mgr.Get(p.CallID)
	if err != nil {
		span.SetStatus(codes.Error, "call_not_found")
		return nil, rpc.ErrCallNotFound(p.CallID)
	}
	if rpcErr := h.kernelExclusiveReject(conn, call, "call.answer"); rpcErr != nil {
		return nil, rpcErr
	}
	h.stateFor(call.ID(), conn)

	// State machine: idle/ringing → answered. If already answered/active,
	// this is idempotent.
	cur := call.State()
	switch cur {
	case session.StateIdle, session.StateRinging:
		if err := call.SetState(session.StateAnswered); err != nil {
			return nil, rpc.NewErrorf(rpc.CodeCallStateInvalid, "%v", err)
		}
		// Transition to Active right after — for WebRTC, media is flowing
		// the moment the SDP exchange completes, which happened in
		// webrtc.offer. So Answered → Active is automatic here.
		if err := call.SetState(session.StateActive); err == nil {
			h.notify(call.ID(), "call.answered", nil)
			observability.CallTraceCall(ctx, h.log, "rpc.answer", call, "state", session.StateActive.String())
		}
	case session.StateAnswered, session.StateActive, session.StateBridged:
		// already answered — emit a synthetic notification so SDKs that
		// missed the original are kept consistent
		h.notify(call.ID(), "call.answered", nil)
		observability.CallTraceCall(ctx, h.log, "rpc.answer", call, "state", cur.String(), "idempotent", true)
	case session.StateEnded:
		return nil, rpc.ErrCallStateInvalid("call ended")
	}
	return map[string]bool{"ok": true}, nil
}

func (h *Handlers) broadcastCallHangup(callID, reason string) {
	if callID == "" {
		return
	}
	h.broadcast("call.hangup", callID, map[string]any{"reason": reason})
	h.dropState(callID)
}

func (h *Handlers) handleHangup(ctx context.Context, conn *rpc.Conn, params json.RawMessage) (any, *rpc.Error) {
	var p callIDParams
	if rpcErr := rpc.DecodeParams(params, &p); rpcErr != nil {
		return nil, rpcErr
	}
	if p.Reason == "" {
		p.Reason = "user_hangup"
	}
	call, err := h.mgr.Get(p.CallID)
	if err == nil {
		if rpcErr := h.kernelExclusiveReject(conn, call, "call.hangup"); rpcErr != nil {
			return nil, rpcErr
		}
		observability.CallTraceCall(ctx, h.log, "rpc.hangup", call, "reason", p.Reason)
	} else {
		observability.CallTrace(ctx, h.log, "rpc.hangup", p.CallID, "reason", p.Reason)
	}
	h.teardownExternalAgent(p.CallID)
	// Capture keepalive peer before Close/unbridge tears the pair down. The
	// bridge watcher can lose a race with hangup (goroutine not yet in select,
	// or peer already purged from the manager) and leave the survivor Bridged;
	// restore it synchronously here as the reliable path.
	keepalivePeer := h.keepaliveBridgePeer(p.CallID)
	if err := h.mgr.Close(p.CallID, p.Reason); err != nil {
		if errors.Is(err, session.ErrCallNotFound) {
			return nil, rpc.ErrCallNotFound(p.CallID)
		}
		return nil, rpc.NewErrorf(rpc.CodeInternalError, "%v", err)
	}
	h.unbridgeCall(p.CallID)
	if keepalivePeer != "" {
		h.restoreKeepaliveSurvivor(keepalivePeer, p.CallID)
	}
	h.broadcastCallHangup(p.CallID, p.Reason)
	return map[string]bool{"ok": true}, nil
}

func (h *Handlers) handleList(_ context.Context, _ *rpc.Conn, _ json.RawMessage) (any, *rpc.Error) {
	items := make([]map[string]any, 0)
	for _, c := range h.mgr.List() {
		if c.IsEnded() {
			continue
		}
		item := map[string]any{
			"call_id":         c.ID(),
			"conversation_id": c.ConversationID(),
			"root_call_id":    c.RootCallID(),
			"phase":           c.Phase(),
			"direction":       c.Direction().String(),
			"state":           c.State().String(),
			"from":            c.From(),
		}
		if requestID := c.RequestID(); requestID != "" {
			item["request_id"] = requestID
		}
		items = append(items, item)
	}
	return map[string]any{"calls": items}, nil
}

// watchCallMediaEnd stops ASR/TTS when the call ends for any reason (remote
// BYE, media EOF, max duration). RPC call.hangup also cleans up, but SIP
// hangup does not go through that path.
func (h *Handlers) watchCallMediaEnd(callID string, call *session.Call) {
	<-call.Done()
	h.stopCallMedia(callID, "all")
	h.dropState(callID)
}
