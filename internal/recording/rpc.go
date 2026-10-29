package recording

import (
	"context"
	"encoding/json"

	"github.com/DeepFlowAI/flowkit/internal/rpc"
	"github.com/DeepFlowAI/flowkit/internal/session"
)

type recordCallParams struct {
	CallID string `json:"call_id"`
}

// RegisterRPC wires call.record.* methods (always registered; disabled when m == nil).
func RegisterRPC(reg *rpc.Registry, m *Manager, mgr *session.Manager) {
	h := &rpcHandlers{m: m, mgr: mgr}
	reg.Register("call.record.start", h.handleStart)
	reg.Register("call.record.stop", h.handleStop)
	reg.Register("call.record.status", h.handleStatus)
}

type rpcHandlers struct {
	m   *Manager
	mgr *session.Manager
}

func (h *rpcHandlers) disabled() *rpc.Error {
	return rpc.NewErrorf(rpc.CodeInternalError, "recording is not enabled")
}

func (h *rpcHandlers) handleStart(_ context.Context, _ *rpc.Conn, params json.RawMessage) (any, *rpc.Error) {
	if h.m == nil {
		return nil, h.disabled()
	}
	var p recordCallParams
	if rpcErr := rpc.DecodeParams(params, &p); rpcErr != nil {
		return nil, rpcErr
	}
	call, err := h.mgr.Get(p.CallID)
	if err != nil {
		return nil, rpc.ErrCallNotFound(p.CallID)
	}
	if call.IsEnded() {
		return nil, rpc.ErrCallStateInvalid("call ended")
	}
	if err := h.m.StartCall(call); err != nil {
		return nil, rpc.NewErrorf(rpc.CodeInternalError, "%v", err)
	}
	return map[string]any{"ok": true}, nil
}

func (h *rpcHandlers) handleStop(_ context.Context, _ *rpc.Conn, params json.RawMessage) (any, *rpc.Error) {
	if h.m == nil {
		return nil, h.disabled()
	}
	var p recordCallParams
	if rpcErr := rpc.DecodeParams(params, &p); rpcErr != nil {
		return nil, rpcErr
	}
	if _, err := h.mgr.Get(p.CallID); err != nil {
		return nil, rpc.ErrCallNotFound(p.CallID)
	}
	url, err := h.m.StopCall(p.CallID)
	if err != nil {
		return map[string]any{"ok": false, "url": "", "reason": err.Error()}, nil
	}
	return map[string]any{"ok": true, "url": url}, nil
}

func (h *rpcHandlers) handleStatus(_ context.Context, _ *rpc.Conn, params json.RawMessage) (any, *rpc.Error) {
	if h.m == nil {
		return nil, h.disabled()
	}
	var p recordCallParams
	if rpcErr := rpc.DecodeParams(params, &p); rpcErr != nil {
		return nil, rpcErr
	}
	if _, err := h.mgr.Get(p.CallID); err != nil {
		return nil, rpc.ErrCallNotFound(p.CallID)
	}
	return h.m.Status(p.CallID), nil
}
