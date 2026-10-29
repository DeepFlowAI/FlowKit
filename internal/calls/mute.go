package calls

import (
	"context"
	"encoding/json"

	"github.com/DeepFlowAI/flowkit/internal/observability"
	"github.com/DeepFlowAI/flowkit/internal/rpc"
)

// Muting gates one leg's own uplink and nothing else: the party stays in the
// conversation and keeps hearing the other side. That is what separates it
// from hold, which suspends the call in both directions.
//
// A caller able to mute locally — a browser switching off its own track —
// should keep doing that, since it needs no round trip. This exists for the
// ones that cannot, such as an agent on a SIP handset whose mute button lives
// in a web console.

func (h *Handlers) handleMute(ctx context.Context, conn *rpc.Conn, params json.RawMessage) (any, *rpc.Error) {
	return h.setMute(ctx, conn, params, true)
}

func (h *Handlers) handleUnmute(ctx context.Context, conn *rpc.Conn, params json.RawMessage) (any, *rpc.Error) {
	return h.setMute(ctx, conn, params, false)
}

func (h *Handlers) setMute(ctx context.Context, conn *rpc.Conn, params json.RawMessage, muted bool) (any, *rpc.Error) {
	var p callIDParams
	if rpcErr := rpc.DecodeParams(params, &p); rpcErr != nil {
		return nil, rpcErr
	}
	call, err := h.mgr.Get(p.CallID)
	if err != nil {
		return nil, rpc.ErrCallNotFound(p.CallID)
	}
	method, event, trace := "call.unmute", "call.unmuted", "rpc.unmute"
	if muted {
		method, event, trace = "call.mute", "call.muted", "rpc.mute"
	}
	if rpcErr := h.kernelExclusiveReject(conn, call, method); rpcErr != nil {
		return nil, rpcErr
	}
	// Any state a call can be in still has a meaningful microphone, bridged
	// included — that is the case this was built for — so the only rejection
	// is a call that has ended. Repeating the same value is a no-op.
	if err := call.SetMuted(muted); err != nil {
		return nil, rpc.ErrCallStateInvalid("call ended")
	}
	observability.CallTraceCall(ctx, h.log, trace, call)
	h.broadcast(event, p.CallID, nil)
	return map[string]bool{"ok": true}, nil
}
