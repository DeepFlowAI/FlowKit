package calls

import (
	"testing"

	"github.com/DeepFlowAI/flowkit/internal/rpc"
	"github.com/DeepFlowAI/flowkit/internal/session"
	"github.com/DeepFlowAI/flowkit/internal/transport/fakeio"
	"github.com/stretchr/testify/require"
)

func TestHandleMute_Unmute(t *testing.T) {
	reg, mgr, _ := newTestStack(t)

	call := mgr.Create(session.DirectionInbound)
	require.NoError(t, call.BindMediaIO(fakeio.New(4)))
	require.NoError(t, call.SetState(session.StateRinging))
	require.NoError(t, call.SetState(session.StateAnswered))
	require.NoError(t, call.SetState(session.StateActive))

	_, rpcErr := driveRPC(t, reg, "call.mute", map[string]string{"call_id": call.ID()})
	require.Nil(t, rpcErr)
	require.True(t, call.IsMuted())
	require.Equal(t, session.StateActive, call.State(), "muting must not change call state")

	// Repeating either direction is a no-op rather than an error: a console
	// re-sending its state on reconnect should not be treated as a fault.
	_, rpcErr = driveRPC(t, reg, "call.mute", map[string]string{"call_id": call.ID()})
	require.Nil(t, rpcErr)
	require.True(t, call.IsMuted())

	_, rpcErr = driveRPC(t, reg, "call.unmute", map[string]string{"call_id": call.ID()})
	require.Nil(t, rpcErr)
	require.False(t, call.IsMuted())
}

// The whole point of kernel-side mute is the bridged agent leg, so this is the
// case that must not be rejected — unlike hold, which is refused while bridged.
func TestHandleMute_WhileBridged_Allowed(t *testing.T) {
	reg, mgr, _ := newTestStack(t)
	call := mgr.Create(session.DirectionInbound)
	require.NoError(t, call.SetState(session.StateRinging))
	require.NoError(t, call.SetState(session.StateAnswered))
	require.NoError(t, call.SetState(session.StateActive))
	require.NoError(t, call.SetState(session.StateBridged))

	_, rpcErr := driveRPC(t, reg, "call.mute", map[string]string{"call_id": call.ID()})
	require.Nil(t, rpcErr)
	require.True(t, call.IsMuted())
	require.Equal(t, session.StateBridged, call.State())
}

func TestHandleMute_NotFound(t *testing.T) {
	reg, _, _ := newTestStack(t)
	_, rpcErr := driveRPC(t, reg, "call.mute", map[string]string{"call_id": "missing"})
	require.NotNil(t, rpcErr)
	require.Equal(t, rpc.CodeCallNotFound, rpcErr.Code)
}

func TestHandleMute_AfterHangup_Rejected(t *testing.T) {
	reg, mgr, _ := newTestStack(t)
	call := mgr.Create(session.DirectionInbound)
	require.NoError(t, call.SetState(session.StateRinging))
	require.NoError(t, call.SetState(session.StateAnswered))
	require.NoError(t, call.Hangup("test"))

	_, rpcErr := driveRPC(t, reg, "call.mute", map[string]string{"call_id": call.ID()})
	require.NotNil(t, rpcErr)
	// Which of the two errors comes back races the manager dropping the call:
	// the lookup fails once it is gone, and until then the ended check inside
	// SetMuted catches it. Pinning either one makes this test flaky; what the
	// call site actually relies on is that a finished call is never muted.
	require.True(t,
		rpcErr.Code == rpc.CodeCallNotFound || rpcErr.Code == rpc.CodeCallStateInvalid,
		"unexpected code %d", rpcErr.Code)
}
