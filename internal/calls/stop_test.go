package calls

import (
	"testing"

	"github.com/DeepFlowAI/flowkit/internal/session"
	"github.com/stretchr/testify/require"
)

func TestHandleStop_NoActiveListen_NoOp(t *testing.T) {
	reg, mgr, _ := newTestStack(t)
	call := mgr.Create(session.DirectionInbound)

	// Initialise state by calling answer first
	_, _ = driveRPC(t, reg, "call.answer", map[string]string{"call_id": call.ID()})

	_, rpcErr := driveRPC(t, reg, "call.stop", map[string]string{
		"call_id": call.ID(), "target": "all",
	})
	require.Nil(t, rpcErr)
}

func TestHandleStop_NotFound(t *testing.T) {
	reg, _, _ := newTestStack(t)
	_, rpcErr := driveRPC(t, reg, "call.stop", map[string]string{"call_id": "ghost"})
	require.NotNil(t, rpcErr)
}
