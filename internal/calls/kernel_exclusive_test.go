package calls

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/DeepFlowAI/flowkit/internal/rpc"
	"github.com/DeepFlowAI/flowkit/internal/session"
	"github.com/DeepFlowAI/flowkit/internal/transport/fakeio"
	"github.com/stretchr/testify/require"
)

func TestKernelExclusive_RejectsWSCallControl(t *testing.T) {
	reg, mgr, h := newTestStack(t)
	h.SetKernelExclusive(true)

	for _, method := range []string{"call.answer", "call.say", "call.listen", "call.hangup"} {
		t.Run(method, func(t *testing.T) {
			call := mgr.Create(session.DirectionInbound)
			require.NoError(t, call.BindMediaIO(fakeio.New(64)))
			var params any
			switch method {
			case "call.say":
				params = map[string]string{"call_id": call.ID(), "text": "外部CC"}
			case "call.hangup":
				params = map[string]string{"call_id": call.ID(), "reason": "cc"}
			default:
				params = map[string]string{"call_id": call.ID()}
			}
			_, rpcErr := driveRPC(t, reg, method, params)
			require.NotNil(t, rpcErr, method)
			require.Equal(t, rpc.CodeCallStateInvalid, rpcErr.Code, method)
		})
	}
}

func TestKernelExclusive_AllowsOutboundWS(t *testing.T) {
	reg, mgr, h := newTestStack(t)
	h.SetKernelExclusive(true)
	call := mgr.Create(session.DirectionOutbound)
	require.NoError(t, call.BindMediaIO(fakeio.New(64)))

	_, rpcErr := driveRPC(t, reg, "call.say", map[string]string{
		"call_id": call.ID(), "text": "外呼",
	})
	require.Nil(t, rpcErr)
}

func TestKernelExclusive_AllowsInternalSay(t *testing.T) {
	_, mgr, h := newTestStack(t)
	h.SetKernelExclusive(true)
	call := mgr.Create(session.DirectionInbound)
	require.NoError(t, call.BindMediaIO(fakeio.New(64)))

	raw, _ := json.Marshal(sayParams{CallID: call.ID(), Text: "内核"})
	_, rpcErr := h.handleSay(context.Background(), nil, raw)
	require.Nil(t, rpcErr)
}
