package calls

import (
	"context"
	"encoding/json"

	"github.com/DeepFlowAI/flowkit/internal/observability"
	"github.com/DeepFlowAI/flowkit/internal/rpc"
	"github.com/DeepFlowAI/flowkit/internal/session"
)

// SIPHoldController applies wire-level SIP hold (re-INVITE) for a flowkit call.
type SIPHoldController interface {
	SetCallHold(flowCallID string, hold bool) error
}

// SetSIPHoldController wires optional SIP hold signaling (inbound/outbound legs).
func (h *Handlers) SetSIPHoldController(c SIPHoldController) {
	h.sipHold = c
}

func (h *Handlers) handleHold(ctx context.Context, conn *rpc.Conn, params json.RawMessage) (any, *rpc.Error) {
	return h.setHold(ctx, conn, params, true)
}

func (h *Handlers) handleUnhold(ctx context.Context, conn *rpc.Conn, params json.RawMessage) (any, *rpc.Error) {
	return h.setHold(ctx, conn, params, false)
}

func (h *Handlers) setHold(ctx context.Context, conn *rpc.Conn, params json.RawMessage, hold bool) (any, *rpc.Error) {
	var p callIDParams
	if rpcErr := rpc.DecodeParams(params, &p); rpcErr != nil {
		return nil, rpcErr
	}
	call, err := h.mgr.Get(p.CallID)
	if err != nil {
		return nil, rpc.ErrCallNotFound(p.CallID)
	}
	method := "call.hold"
	if !hold {
		method = "call.unhold"
	}
	if rpcErr := h.kernelExclusiveReject(conn, call, method); rpcErr != nil {
		return nil, rpcErr
	}
	if call.IsEnded() {
		return nil, rpc.ErrCallStateInvalid("call ended")
	}
	st := call.State()
	if hold {
		if st == session.StateBridged {
			return nil, rpc.ErrCallStateInvalid("cannot hold while bridged")
		}
		if st == session.StateHeld {
			h.broadcastHold(p.CallID, true)
			return map[string]bool{"ok": true}, nil
		}
		if st != session.StateActive && st != session.StateAnswered {
			return nil, rpc.ErrCallStateInvalid("call not active")
		}
	} else {
		if st != session.StateHeld && !call.IsHeld() {
			return map[string]bool{"ok": true}, nil
		}
	}

	h.stopCallMedia(p.CallID, "all")

	if h.sipHold != nil {
		if err := h.sipHold.SetCallHold(p.CallID, hold); err != nil {
			return nil, rpc.NewErrorf(rpc.CodeInternalError, "%v", err)
		}
	}

	if hold {
		if err := call.SetHeld(true); err != nil {
			return nil, rpc.NewErrorf(rpc.CodeCallStateInvalid, "%v", err)
		}
		observability.CallTraceCall(ctx, h.log, "rpc.hold", call)
		h.broadcastHold(p.CallID, true)
	} else {
		if err := call.SetHeld(false); err != nil {
			return nil, rpc.NewErrorf(rpc.CodeCallStateInvalid, "%v", err)
		}
		observability.CallTraceCall(ctx, h.log, "rpc.unhold", call)
		h.broadcastHold(p.CallID, false)
	}
	return map[string]bool{"ok": true}, nil
}

func (h *Handlers) broadcastHold(callID string, held bool) {
	method := "call.unheld"
	if held {
		method = "call.held"
	}
	h.broadcast(method, callID, nil)
}
