package calls

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/DeepFlowAI/flowkit/internal/rpc"
	"github.com/DeepFlowAI/flowkit/internal/session"
	"github.com/DeepFlowAI/flowkit/internal/transport"
)

type dtmfParams struct {
	CallID      string `json:"call_id"`
	Digits      string `json:"digits"`
	DurationMS  int    `json:"duration_ms,omitempty"`
}

func (h *Handlers) handleDTMF(_ context.Context, conn *rpc.Conn, params json.RawMessage) (any, *rpc.Error) {
	var p dtmfParams
	if rpcErr := rpc.DecodeParams(params, &p); rpcErr != nil {
		return nil, rpcErr
	}
	if p.Digits == "" {
		return nil, rpc.ErrInvalidParams("missing digits")
	}

	call, err := h.mgr.Get(p.CallID)
	if err != nil {
		return nil, rpc.ErrCallNotFound(p.CallID)
	}
	if rpcErr := h.kernelExclusiveReject(conn, call, "call.dtmf"); rpcErr != nil {
		return nil, rpcErr
	}
	if call.IsEnded() {
		return nil, rpc.ErrCallStateInvalid("call ended")
	}

	mio := call.MediaIO()
	if mio == nil {
		return nil, rpc.NewErrorf(rpc.CodeCallStateInvalid, "no media bound")
	}
	sender, ok := mio.(transport.DTMFSender)
	if !ok {
		return nil, rpc.ErrCallStateInvalid("dtmf not supported for this transport")
	}
	if err := sender.SendDTMF(p.Digits, p.DurationMS); err != nil {
		if errors.Is(err, session.ErrEnded) {
			return nil, rpc.ErrCallStateInvalid("call ended")
		}
		return nil, rpc.NewErrorf(rpc.CodeInternalError, "%v", err)
	}
	return map[string]bool{"ok": true}, nil
}
