package calls

import (
	"context"
	"encoding/json"

	"github.com/DeepFlowAI/flowkit/internal/observability"
	"github.com/DeepFlowAI/flowkit/internal/rpc"
)

type stopParams struct {
	CallID string `json:"call_id"`
	Target string `json:"target,omitempty"` // "play" | "listen" | "all"
}

func (h *Handlers) stopCallMedia(callID, target string) {
	if target == "" {
		target = "all"
	}
	h.mu.Lock()
	cs := h.callState[callID]
	h.mu.Unlock()
	if cs == nil {
		return
	}
	switch target {
	case "listen", "all":
		// Resolve the call before taking h.mu: the manager has its own lock, and
		// nesting the two would fix an order that callers must then respect.
		call, callErr := h.mgr.Get(callID)
		h.mu.Lock()
		sub := cs.asrSub
		provider := cs.asrProvider
		listenDialog := cs.listenDialog
		cs.asrSub = nil
		cs.asrProvider = nil
		cs.listenDialog = nil
		cs.listening.Store(false)
		cs.adaptiveListen.Store(false)
		h.mu.Unlock()
		if sub != nil && callErr == nil {
			call.AudioBus().Unsubscribe(sub)
		}
		if provider != nil {
			_ = provider.Stop()
		}
		if listenDialog != nil {
			listenDialog.close()
		}
	}
	if target == "play" || target == "all" {
		cs.playMu.Lock()
		cancel := cs.playCancel
		cs.playCancel = nil
		cs.playMu.Unlock()
		if cancel != nil {
			cancel()
		}
		h.stopOutgoingPlayback(callID)
		h.signalPlayWait(callID, "interrupted")
	}
}

func (h *Handlers) handleStop(ctx context.Context, conn *rpc.Conn, params json.RawMessage) (any, *rpc.Error) {
	var p stopParams
	if rpcErr := rpc.DecodeParams(params, &p); rpcErr != nil {
		return nil, rpcErr
	}
	call, callErr := h.mgr.Get(p.CallID)
	if callErr != nil {
		return nil, rpc.ErrCallNotFound(p.CallID)
	}
	if rpcErr := h.kernelExclusiveReject(conn, call, "call.stop"); rpcErr != nil {
		return nil, rpcErr
	}
	h.mu.Lock()
	cs := h.callState[p.CallID]
	h.mu.Unlock()
	if cs == nil {
		return nil, rpc.ErrCallNotFound(p.CallID)
	}
	target := p.Target
	if target == "" {
		target = "all"
	}
	if target == "all" {
		h.teardownExternalAgent(p.CallID)
	}
	h.stopCallMedia(p.CallID, target)
	observability.CallTraceCall(ctx, h.log, "rpc.stop", call, "target", target)
	return map[string]bool{"ok": true}, nil
}
