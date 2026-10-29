package calls

import (
	"github.com/DeepFlowAI/flowkit/internal/rpc"
	"github.com/DeepFlowAI/flowkit/internal/session"
)

// SetKernelExclusive enables kernel-only control of inbound LLM-phase calls.
// External JSON-RPC clients (conn != nil) cannot answer, say, listen, stop,
// hangup, bridge, hold, or inject DTMF on those calls; programmatic handlers
// (conn == nil) used by InboundLLMAgent still work.
func (h *Handlers) SetKernelExclusive(v bool) {
	h.kernelExclusive.Store(v)
}

// KernelExclusive reports whether kernel-exclusive mode is on.
func (h *Handlers) KernelExclusive() bool {
	return h.kernelExclusive.Load()
}

func (h *Handlers) kernelExclusiveReject(conn *rpc.Conn, call *session.Call, method string) *rpc.Error {
	if conn == nil || call == nil || !h.kernelExclusive.Load() {
		return nil
	}
	if call.Direction() != session.DirectionInbound || call.Phase() != session.PhaseLLM {
		return nil
	}
	h.log.Info("rpc rejected (kernel_exclusive)",
		"method", method,
		"call_id", call.ID(),
		"conversation_id", call.ConversationID(),
	)
	return rpc.NewErrorf(rpc.CodeCallStateInvalid,
		"inbound call is owned by kernel auto-llm (disable SIP_KERNEL_EXCLUSIVE or SIP_KERNEL_AUTO_LLM to allow CC control)")
}
