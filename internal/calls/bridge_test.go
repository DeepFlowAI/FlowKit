package calls

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/session"
	"github.com/DeepFlowAI/flowkit/internal/transport/fakeio"
	"github.com/stretchr/testify/require"
)

func activateCall(t *testing.T, call *session.Call, io *fakeio.FakeIO) {
	t.Helper()
	require.NoError(t, call.BindMediaIO(io))
	require.NoError(t, call.SetState(session.StateRinging))
	require.NoError(t, call.SetState(session.StateAnswered))
	require.NoError(t, call.SetState(session.StateActive))
}

type bridgeRoleCapture struct {
	userID  string
	agentID string
}

func (c *bridgeRoleCapture) OnBridged(userLeg, agentLeg *session.Call) {
	c.userID = userLeg.ID()
	c.agentID = agentLeg.ID()
}

func (c *bridgeRoleCapture) OnLegReplaced(string, *session.Call) {}

func TestWriteOutgoing_WhenBridged(t *testing.T) {
	mgr := session.NewManager(quietLogger())
	t.Cleanup(func() { mgr.Shutdown("test_cleanup") })
	io := fakeio.New(8)
	done := make(chan struct{})
	go func() {
		io.CollectOutgoing()
		close(done)
	}()

	call := mgr.Create(session.DirectionInbound)
	activateCall(t, call, io)
	require.NoError(t, call.SetState(session.StateBridged))
	require.NoError(t, call.WriteOutgoing([]int16{1, 2, 3}))
	require.Eventually(t, func() bool {
		return len(io.Outgoing()) > 0
	}, time.Second, 10*time.Millisecond)
}

func TestBridge_ForwardsAudio(t *testing.T) {
	reg, mgr, _ := newTestStack(t)
	reg.AllowPreHello("call.bridge")

	ioA := fakeio.New(8)
	ioB := fakeio.New(8)
	go ioB.CollectOutgoing()

	callA := mgr.Create(session.DirectionInbound)
	callB := mgr.Create(session.DirectionOutbound)
	activateCall(t, callA, ioA)
	activateCall(t, callB, ioB)

	sipConv := callA.ConversationID()

	_, rpcErr := driveRPC(t, reg, "call.bridge", map[string]string{
		"call_id":       callA.ID(),
		"other_call_id": callB.ID(),
	})
	require.Nil(t, rpcErr)

	require.Equal(t, sipConv, callA.ConversationID())
	require.Equal(t, sipConv, callB.ConversationID())
	require.Equal(t, callA.ID(), callB.RootCallID())
	require.Equal(t, session.PhaseAgent, callA.Phase())

	frame := make([]int16, 320)
	for i := range frame {
		frame[i] = int16(i)
	}

	require.Eventually(t, func() bool {
		ioA.PushIncoming(frame)
		return len(ioB.Outgoing()) > 0
	}, 3*time.Second, 20*time.Millisecond)
}

func TestBridge_ExplicitUserLegOverridesTransportDirection(t *testing.T) {
	reg, mgr, h := newTestStack(t)
	reg.AllowPreHello("call.bridge")

	customer := mgr.Create(session.DirectionOutbound)
	agent := mgr.Create(session.DirectionInbound)
	activateCall(t, customer, fakeio.New(8))
	activateCall(t, agent, fakeio.New(8))

	capture := &bridgeRoleCapture{}
	h.SetBridgeObserver(capture)
	_, rpcErr := driveRPC(t, reg, "call.bridge", map[string]string{
		"call_id":       customer.ID(),
		"other_call_id": agent.ID(),
		"user_call_id":  customer.ID(),
	})
	require.Nil(t, rpcErr)
	require.Equal(t, customer.ID(), capture.userID)
	require.Equal(t, agent.ID(), capture.agentID)
}

func TestBridge_RejectsSameCall(t *testing.T) {
	reg, mgr, _ := newTestStack(t)
	reg.AllowPreHello("call.bridge")

	call := mgr.Create(session.DirectionInbound)
	activateCall(t, call, fakeio.New(4))

	_, rpcErr := driveRPC(t, reg, "call.bridge", map[string]string{
		"call_id":       call.ID(),
		"other_call_id": call.ID(),
	})
	require.NotNil(t, rpcErr)
}

func TestHandleUnbridge_RestoresActiveState(t *testing.T) {
	reg, mgr, _ := newTestStack(t)
	reg.AllowPreHello("call.unbridge")

	ioA := fakeio.New(8)
	ioB := fakeio.New(8)
	callA := mgr.Create(session.DirectionInbound)
	callB := mgr.Create(session.DirectionInbound)
	activateCall(t, callA, ioA)
	activateCall(t, callB, ioB)

	_, rpcErr := driveRPC(t, reg, "call.bridge", map[string]string{
		"call_id": callA.ID(), "other_call_id": callB.ID(),
	})
	require.Nil(t, rpcErr)
	require.Equal(t, session.StateBridged, callA.State())

	raw, rpcErr := driveRPC(t, reg, "call.unbridge", map[string]string{"call_id": callA.ID()})
	require.Nil(t, rpcErr)
	var res map[string]any
	require.NoError(t, json.Unmarshal(raw, &res))
	require.Equal(t, true, res["ok"])
	require.Equal(t, session.StateActive, callA.State())
	require.Equal(t, session.StateActive, callB.State())

	_, rpcErr = driveRPC(t, reg, "call.listen", map[string]string{"call_id": callA.ID()})
	require.Nil(t, rpcErr)
}

func TestBridge_KeepaliveOnPeerHangup(t *testing.T) {
	reg, mgr, _ := newTestStack(t)
	reg.AllowPreHello("call.bridge")
	reg.AllowPreHello("call.hangup")

	ioA := fakeio.New(8)
	ioB := fakeio.New(8)
	callA := mgr.Create(session.DirectionInbound)
	callB := mgr.Create(session.DirectionOutbound)
	activateCall(t, callA, ioA)
	activateCall(t, callB, ioB)

	_, rpcErr := driveRPC(t, reg, "call.bridge", map[string]any{
		"call_id":                  callA.ID(),
		"other_call_id":            callB.ID(),
		"keepalive_on_peer_hangup": true,
	})
	require.Nil(t, rpcErr)

	_, rpcErr = driveRPC(t, reg, "call.hangup", map[string]string{
		"call_id": callB.ID(),
		"reason":  "agent_hangup",
	})
	require.Nil(t, rpcErr)

	require.Eventually(t, func() bool {
		return callB.IsEnded() && !callA.IsEnded() && callA.State() == session.StateActive
	}, 2*time.Second, 20*time.Millisecond)
}

func TestBridge_KeepaliveSurvivorSweep(t *testing.T) {
	reg, mgr, h := newTestStack(t)
	reg.AllowPreHello("call.bridge")
	reg.AllowPreHello("call.hangup")
	h.SetKeepaliveSweep(80 * time.Millisecond)

	callA := mgr.Create(session.DirectionInbound)
	callB := mgr.Create(session.DirectionOutbound)
	activateCall(t, callA, fakeio.New(8))
	activateCall(t, callB, fakeio.New(8))

	_, rpcErr := driveRPC(t, reg, "call.bridge", map[string]any{
		"call_id":                  callA.ID(),
		"other_call_id":            callB.ID(),
		"keepalive_on_peer_hangup": true,
	})
	require.Nil(t, rpcErr)

	_, rpcErr = driveRPC(t, reg, "call.hangup", map[string]string{
		"call_id": callB.ID(),
		"reason":  "agent_hangup",
	})
	require.Nil(t, rpcErr)

	require.Eventually(t, func() bool {
		return !callA.IsEnded() && callA.State() == session.StateActive
	}, 2*time.Second, 20*time.Millisecond)
	require.Eventually(t, func() bool {
		return callA.IsEnded()
	}, 3*time.Second, 20*time.Millisecond, "sweep should close the orphaned survivor")
}

func TestBridge_KeepaliveSurvivorRebridgeDisarmsSweep(t *testing.T) {
	reg, mgr, h := newTestStack(t)
	reg.AllowPreHello("call.bridge")
	reg.AllowPreHello("call.hangup")
	h.SetKeepaliveSweep(150 * time.Millisecond)

	callA := mgr.Create(session.DirectionInbound)
	callB := mgr.Create(session.DirectionOutbound)
	activateCall(t, callA, fakeio.New(8))
	activateCall(t, callB, fakeio.New(8))

	_, rpcErr := driveRPC(t, reg, "call.bridge", map[string]any{
		"call_id":                  callA.ID(),
		"other_call_id":            callB.ID(),
		"keepalive_on_peer_hangup": true,
	})
	require.Nil(t, rpcErr)

	_, rpcErr = driveRPC(t, reg, "call.hangup", map[string]string{
		"call_id": callB.ID(),
		"reason":  "agent_hangup",
	})
	require.Nil(t, rpcErr)
	require.Eventually(t, func() bool {
		return !callA.IsEnded() && callA.State() == session.StateActive
	}, 2*time.Second, 20*time.Millisecond)

	// Agent reconnects on a fresh leg and re-bridges the survivor — the sweep
	// must disarm so the customer leg survives past the original deadline.
	callC := mgr.Create(session.DirectionOutbound)
	activateCall(t, callC, fakeio.New(8))
	_, rpcErr = driveRPC(t, reg, "call.bridge", map[string]any{
		"call_id":                  callA.ID(),
		"other_call_id":            callC.ID(),
		"keepalive_on_peer_hangup": true,
	})
	require.Nil(t, rpcErr)

	require.Never(t, func() bool {
		return callA.IsEnded()
	}, 400*time.Millisecond, 25*time.Millisecond)
}

func TestBridge_LateWatcherDoesNotDemoteRebridgeSurvivor(t *testing.T) {
	reg, mgr, h := newTestStack(t)
	reg.AllowPreHello("call.bridge")
	reg.AllowPreHello("call.hangup")
	h.SetKeepaliveSweep(150 * time.Millisecond)

	callA := mgr.Create(session.DirectionInbound)
	callB := mgr.Create(session.DirectionOutbound)
	activateCall(t, callA, fakeio.New(8))
	activateCall(t, callB, fakeio.New(8))

	_, rpcErr := driveRPC(t, reg, "call.bridge", map[string]any{
		"call_id":                  callA.ID(),
		"other_call_id":            callB.ID(),
		"keepalive_on_peer_hangup": true,
	})
	require.Nil(t, rpcErr)
	oldPair := h.bridgePairs[bridgeKey(callA.ID(), callB.ID())]
	require.NotNil(t, oldPair)

	// Blind-transfer race: the hangup path restores A synchronously, the
	// backend immediately re-bridges A to the target (callC), and only THEN
	// the old pair's watcher fires. It must not demote/re-arm A.
	_, rpcErr = driveRPC(t, reg, "call.hangup", map[string]string{
		"call_id": callB.ID(),
		"reason":  "transfer_release",
	})
	require.Nil(t, rpcErr)
	require.Eventually(t, func() bool {
		return !callA.IsEnded() && callA.State() == session.StateActive
	}, 2*time.Second, 20*time.Millisecond)

	callC := mgr.Create(session.DirectionOutbound)
	activateCall(t, callC, fakeio.New(8))
	_, rpcErr = driveRPC(t, reg, "call.bridge", map[string]any{
		"call_id":                  callA.ID(),
		"other_call_id":            callC.ID(),
		"keepalive_on_peer_hangup": true,
	})
	require.Nil(t, rpcErr)

	h.onBridgeWatchStopped(oldPair)

	require.Equal(t, session.StateBridged, callA.State(), "late watcher demoted the re-bridged survivor")
	require.Never(t, func() bool {
		return callA.IsEnded()
	}, 400*time.Millisecond, 25*time.Millisecond, "stale sweep closed the live re-bridged call")
}
