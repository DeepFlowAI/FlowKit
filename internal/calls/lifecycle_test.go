package calls

import (
	"testing"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/rpc"
	"github.com/DeepFlowAI/flowkit/internal/session"
	"github.com/stretchr/testify/require"
)

func TestHandleAnswer_TransitionsToActive(t *testing.T) {
	reg, mgr, _ := newTestStack(t)
	call := mgr.Create(session.DirectionInbound)
	require.NoError(t, call.SetState(session.StateRinging))

	_, rpcErr := driveRPC(t, reg, "call.answer", map[string]string{"call_id": call.ID()})
	require.Nil(t, rpcErr)

	require.Equal(t, session.StateActive, call.State())
}

func TestHandleAnswer_NotFound(t *testing.T) {
	reg, _, _ := newTestStack(t)
	_, rpcErr := driveRPC(t, reg, "call.answer", map[string]string{"call_id": "no-such"})
	require.NotNil(t, rpcErr)
	require.Equal(t, rpc.CodeCallNotFound, rpcErr.Code)
}

func TestHandleAnswer_AfterEnded_Errors(t *testing.T) {
	reg, mgr, _ := newTestStack(t)
	call := mgr.Create(session.DirectionInbound)
	id := call.ID()
	require.NoError(t, call.Hangup("done"))

	// Wait for the manager's watcher to evict the entry.
	require.Eventually(t, func() bool {
		_, err := mgr.Get(id)
		return err != nil
	}, time.Second, 10*time.Millisecond)

	_, rpcErr := driveRPC(t, reg, "call.answer", map[string]string{"call_id": id})
	require.NotNil(t, rpcErr)
	// Manager auto-removes ended calls, so the user-visible error is
	// CallNotFound rather than CallStateInvalid. Both are valid outcomes;
	// CallNotFound is what we expose to keep the API consistent.
	require.Equal(t, rpc.CodeCallNotFound, rpcErr.Code)
}

func TestHandleHangup_ClosesCall(t *testing.T) {
	reg, mgr, _ := newTestStack(t)
	call := mgr.Create(session.DirectionInbound)
	id := call.ID()

	_, rpcErr := driveRPC(t, reg, "call.hangup", map[string]string{
		"call_id": id, "reason": "user_hangup",
	})
	require.Nil(t, rpcErr)
	require.True(t, call.IsEnded())

	// Manager removes it
	require.Eventually(t, func() bool {
		_, err := mgr.Get(id)
		return err != nil
	}, time.Second, 10*time.Millisecond)
}

func TestHandleHangup_NotFound(t *testing.T) {
	reg, _, _ := newTestStack(t)
	_, rpcErr := driveRPC(t, reg, "call.hangup", map[string]string{"call_id": "ghost"})
	require.NotNil(t, rpcErr)
	require.Equal(t, rpc.CodeCallNotFound, rpcErr.Code)
}
