package calls

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/room"
	"github.com/DeepFlowAI/flowkit/internal/rpc"
	"github.com/DeepFlowAI/flowkit/internal/session"
	"github.com/DeepFlowAI/flowkit/internal/transport/fakeio"
	"github.com/stretchr/testify/require"
)

// enableRoomStack wires the room.* methods on top of the standard test stack.
func enableRoomStack(t *testing.T) (*rpc.Registry, *session.Manager, *Handlers) {
	t.Helper()
	reg, mgr, h := newTestStack(t)
	for _, m := range []string{"room.create", "room.add_leg", "room.remove_leg", "room.replace_leg", "room.set_mix_gain", "room.close"} {
		reg.AllowPreHello(m)
	}
	h.EnableRooms(room.NewManager(mgr, quietLogger()), reg)
	return reg, mgr, h
}

func TestRoomRPC_DisabledReturnsMethodNotFound(t *testing.T) {
	reg, _, _ := newTestStack(t)
	reg.AllowPreHello("room.create")
	// room.create is not registered when rooms are disabled.
	_, rpcErr := driveRPC(t, reg, "room.create", map[string]any{})
	require.NotNil(t, rpcErr)
	require.Equal(t, rpc.CodeMethodNotFound, rpcErr.Code)
}

func TestRoomRPC_CreateAddCloseLifecycle(t *testing.T) {
	reg, mgr, _ := enableRoomStack(t)

	a := mgr.Create(session.DirectionInbound)
	b := mgr.Create(session.DirectionOutbound)
	activateCall(t, a, fakeio.New(8))
	activateCall(t, b, fakeio.New(8))

	raw, rpcErr := driveRPC(t, reg, "room.create", map[string]any{
		"leg_call_ids": []string{a.ID(), b.ID()},
	})
	require.Nil(t, rpcErr)
	var res map[string]any
	require.NoError(t, json.Unmarshal(raw, &res))
	roomID, _ := res["room_id"].(string)
	require.NotEmpty(t, roomID)

	// Add a third leg.
	c := mgr.Create(session.DirectionOutbound)
	activateCall(t, c, fakeio.New(8))
	_, rpcErr = driveRPC(t, reg, "room.add_leg", map[string]any{"room_id": roomID, "call_id": c.ID()})
	require.Nil(t, rpcErr)

	// Whisper gain (directional) OK.
	_, rpcErr = driveRPC(t, reg, "room.set_mix_gain", map[string]any{
		"room_id": roomID, "listener_call_id": b.ID(), "speaker_call_id": c.ID(), "gain": 0,
	})
	require.Nil(t, rpcErr)

	// Out-of-range gain rejected.
	_, rpcErr = driveRPC(t, reg, "room.set_mix_gain", map[string]any{
		"room_id": roomID, "listener_call_id": b.ID(), "speaker_call_id": c.ID(), "gain": 2,
	})
	require.NotNil(t, rpcErr)
	require.Equal(t, rpc.CodeInvalidParams, rpcErr.Code)

	// Whole-chain hangup.
	_, rpcErr = driveRPC(t, reg, "room.close", map[string]any{"room_id": roomID, "reason": "done"})
	require.Nil(t, rpcErr)
	require.Eventually(t, func() bool {
		return a.IsEnded() && b.IsEnded() && c.IsEnded()
	}, 2*time.Second, 10*time.Millisecond)
}

// A bridged pair moved into a room, then collapsed back to two legs, must be
// bridgeable again (conference participant left → transfer / re-bridge).
func TestRoomRPC_BridgedLegsCanBridgeAgainAfterRoom(t *testing.T) {
	reg, mgr, _ := enableRoomStack(t)

	customer := mgr.Create(session.DirectionInbound)
	agent := mgr.Create(session.DirectionOutbound)
	party := mgr.Create(session.DirectionOutbound)
	activateCall(t, customer, fakeio.New(8))
	activateCall(t, agent, fakeio.New(8))
	activateCall(t, party, fakeio.New(8))

	_, rpcErr := driveRPC(t, reg, "call.bridge", map[string]string{
		"call_id": customer.ID(), "other_call_id": agent.ID(),
	})
	require.Nil(t, rpcErr)

	raw, rpcErr := driveRPC(t, reg, "room.create", map[string]any{
		"leg_call_ids": []string{customer.ID(), agent.ID(), party.ID()},
	})
	require.Nil(t, rpcErr)
	require.Equal(t, session.StateActive, customer.State())
	require.Equal(t, session.StateActive, agent.State())
	var res map[string]any
	require.NoError(t, json.Unmarshal(raw, &res))
	roomID := res["room_id"].(string)

	for _, id := range []string{party.ID(), agent.ID()} {
		_, rpcErr = driveRPC(t, reg, "room.remove_leg", map[string]any{"room_id": roomID, "call_id": id})
		require.Nil(t, rpcErr)
	}

	_, rpcErr = driveRPC(t, reg, "call.bridge", map[string]string{
		"call_id": customer.ID(), "other_call_id": agent.ID(),
	})
	require.Nil(t, rpcErr)
	require.Equal(t, session.StateBridged, customer.State())
}

func TestRoomRPC_AddUnknownCall(t *testing.T) {
	reg, mgr, _ := enableRoomStack(t)
	a := mgr.Create(session.DirectionInbound)
	activateCall(t, a, fakeio.New(8))
	raw, rpcErr := driveRPC(t, reg, "room.create", map[string]any{"leg_call_ids": []string{a.ID()}})
	require.Nil(t, rpcErr)
	var res map[string]any
	require.NoError(t, json.Unmarshal(raw, &res))
	roomID := res["room_id"].(string)

	_, rpcErr = driveRPC(t, reg, "room.add_leg", map[string]any{"room_id": roomID, "call_id": "no-such"})
	require.NotNil(t, rpcErr)
	require.Equal(t, rpc.CodeCallNotFound, rpcErr.Code)
}
