package calls

import (
	"testing"

	"github.com/DeepFlowAI/flowkit/internal/rpc"
	"github.com/DeepFlowAI/flowkit/internal/session"
	"github.com/DeepFlowAI/flowkit/internal/transport/fakeio"
	"github.com/stretchr/testify/require"
)

type stubSIPHold struct {
	callID string
	hold   bool
	err    error
}

func (s *stubSIPHold) SetCallHold(flowCallID string, hold bool) error {
	s.callID = flowCallID
	s.hold = hold
	return s.err
}

func TestHandleHold_Unhold_State(t *testing.T) {
	reg, mgr, h := newTestStack(t)
	sip := &stubSIPHold{}
	h.SetSIPHoldController(sip)

	call := mgr.Create(session.DirectionInbound)
	require.NoError(t, call.BindMediaIO(fakeio.New(4)))
	require.NoError(t, call.SetState(session.StateRinging))
	require.NoError(t, call.SetState(session.StateAnswered))
	require.NoError(t, call.SetState(session.StateActive))

	_, rpcErr := driveRPC(t, reg, "call.hold", map[string]string{"call_id": call.ID()})
	require.Nil(t, rpcErr)
	require.Equal(t, session.StateHeld, call.State())
	require.True(t, call.IsHeld())
	require.Equal(t, call.ID(), sip.callID)
	require.True(t, sip.hold)

	_, rpcErr = driveRPC(t, reg, "call.unhold", map[string]string{"call_id": call.ID()})
	require.Nil(t, rpcErr)
	require.Equal(t, session.StateActive, call.State())
	require.False(t, call.IsHeld())
	require.False(t, sip.hold)
}

func TestHandleHold_WhileBridged_Rejected(t *testing.T) {
	reg, mgr, _ := newTestStack(t)
	call := mgr.Create(session.DirectionInbound)
	require.NoError(t, call.SetState(session.StateRinging))
	require.NoError(t, call.SetState(session.StateAnswered))
	require.NoError(t, call.SetState(session.StateActive))
	require.NoError(t, call.SetState(session.StateBridged))

	_, rpcErr := driveRPC(t, reg, "call.hold", map[string]string{"call_id": call.ID()})
	require.NotNil(t, rpcErr)
	require.Equal(t, rpc.CodeCallStateInvalid, rpcErr.Code)
}

func TestHandleHold_NotFound(t *testing.T) {
	reg, _, _ := newTestStack(t)
	_, rpcErr := driveRPC(t, reg, "call.hold", map[string]string{"call_id": "missing"})
	require.NotNil(t, rpcErr)
	require.Equal(t, rpc.CodeCallNotFound, rpcErr.Code)
}
