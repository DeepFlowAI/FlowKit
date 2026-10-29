package calls

import (
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/asr"
	asrmock "github.com/DeepFlowAI/flowkit/internal/asr/mock"
	"github.com/DeepFlowAI/flowkit/internal/room"
	"github.com/DeepFlowAI/flowkit/internal/rpc"
	"github.com/DeepFlowAI/flowkit/internal/session"
	"github.com/DeepFlowAI/flowkit/internal/transport/fakeio"
	"github.com/stretchr/testify/require"
)

func newRoomStack(t *testing.T) (*rpc.Registry, *session.Manager, *Handlers) {
	t.Helper()
	reg, mgr, h := newTestStack(t)
	h.EnableRooms(room.NewManager(mgr, quietLogger()), reg)
	for _, m := range []string{"call.bridge", "room.create", "room.add_leg", "room.remove_leg", "room.close"} {
		reg.AllowPreHello(m)
	}
	return reg, mgr, h
}

// transcriptCollector records the speaker of every transcript line dispatched
// under anchorID.
type transcriptCollector struct {
	mu       sync.Mutex
	speakers map[string]string // speaker → latest text
}

func (c *transcriptCollector) record(method string, params map[string]any) {
	if method != "call.bridge_transcript" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.speakers[params["speaker"].(string)] = params["text"].(string)
}

func (c *transcriptCollector) snapshot() map[string]string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]string, len(c.speakers))
	for k, v := range c.speakers {
		out[k] = v
	}
	return out
}

func (c *transcriptCollector) reset() {
	c.mu.Lock()
	c.speakers = map[string]string{}
	c.mu.Unlock()
}

// Adding a third party to a call moves both legs from a bridge into a mixing
// room, which tears the bridge — and its transcription — down. The live
// transcript has to survive that, or the agent's screen silently stops
// updating for the rest of the conference. The newcomer was never on the
// bridge and stays out of the transcript, keeping its two-speaker shape.
func TestRoom_TranscriptSurvivesAddingAThirdParty(t *testing.T) {
	reg, mgr, h := newRoomStack(t)
	h.bridgeASREnabled = true
	h.factories.ASR = func() (asr.Provider, error) {
		return asrmock.New(asrmock.Step{
			AfterSamples: 320,
			Type:         asr.EventFinal,
			Text:         "我要退款",
		}), nil
	}

	customer := mgr.Create(session.DirectionInbound) // user leg → anchor
	agent := mgr.Create(session.DirectionOutbound)
	participant := mgr.Create(session.DirectionOutbound)
	ioCustomer, ioAgent, ioParticipant := fakeio.New(8), fakeio.New(8), fakeio.New(8)
	for _, io := range []*fakeio.FakeIO{ioCustomer, ioAgent, ioParticipant} {
		go io.CollectOutgoing()
	}
	activateCall(t, customer, ioCustomer)
	activateCall(t, agent, ioAgent)
	activateCall(t, participant, ioParticipant)

	seen := &transcriptCollector{speakers: map[string]string{}}
	h.SubscribeCall(customer.ID(), seen.record)

	_, rpcErr := driveRPC(t, reg, "call.bridge", map[string]string{
		"call_id": customer.ID(), "other_call_id": agent.ID(),
	})
	require.Nil(t, rpcErr)

	frame := make([]int16, 320)
	pushAll := func() {
		ioCustomer.PushIncoming(frame)
		ioAgent.PushIncoming(frame)
		ioParticipant.PushIncoming(frame)
	}
	require.Eventually(t, func() bool {
		pushAll()
		got := seen.snapshot()
		return got["user"] != "" && got["agent"] != ""
	}, 3*time.Second, 20*time.Millisecond, "bridge should transcribe both legs")

	// Conference: both legs into a room, then the third party joins.
	seen.reset()
	raw, rpcErr := driveRPC(t, reg, "room.create", map[string]any{
		"leg_call_ids": []string{customer.ID(), agent.ID()},
	})
	require.Nil(t, rpcErr)
	var created map[string]any
	require.NoError(t, json.Unmarshal(raw, &created))
	roomID := created["room_id"].(string)

	_, rpcErr = driveRPC(t, reg, "room.add_leg", map[string]string{
		"room_id": roomID, "call_id": participant.ID(),
	})
	require.Nil(t, rpcErr)

	require.Eventually(t, func() bool {
		pushAll()
		got := seen.snapshot()
		return got["user"] != "" && got["agent"] != ""
	}, 3*time.Second, 20*time.Millisecond, "transcript must continue inside the room")

	require.Equal(t, map[string]string{"user": "我要退款", "agent": "我要退款"}, seen.snapshot(),
		"only the two original legs belong in the transcript")
}

// Legs leave rooms with no RPC behind them — the call simply ends, and the
// room destroys itself once it is down to a single leg with nothing left to
// mix. Both paths have to release the transcript: a leftover stream would keep
// transcribing a leg that a bridge is about to pick up again, duplicating
// every line from then on.
func TestRoom_TranscriptReleasedWhenTheRoomWindsDown(t *testing.T) {
	reg, mgr, h := newRoomStack(t)
	h.bridgeASREnabled = true
	h.factories.ASR = func() (asr.Provider, error) {
		return asrmock.New(asrmock.Step{
			AfterSamples: 320,
			Type:         asr.EventFinal,
			Text:         "好的",
		}), nil
	}

	customer := mgr.Create(session.DirectionInbound)
	agent := mgr.Create(session.DirectionOutbound)
	participant := mgr.Create(session.DirectionOutbound)
	ioCustomer, ioAgent, ioParticipant := fakeio.New(8), fakeio.New(8), fakeio.New(8)
	for _, io := range []*fakeio.FakeIO{ioCustomer, ioAgent, ioParticipant} {
		go io.CollectOutgoing()
	}
	activateCall(t, customer, ioCustomer)
	activateCall(t, agent, ioAgent)
	activateCall(t, participant, ioParticipant)

	_, rpcErr := driveRPC(t, reg, "call.bridge", map[string]string{
		"call_id": customer.ID(), "other_call_id": agent.ID(),
	})
	require.Nil(t, rpcErr)

	frame := make([]int16, 320)
	require.Eventually(t, func() bool {
		ioCustomer.PushIncoming(frame)
		ioAgent.PushIncoming(frame)
		return h.transcriptRoleCount() == 2
	}, 3*time.Second, 20*time.Millisecond)

	raw, rpcErr := driveRPC(t, reg, "room.create", map[string]any{
		"leg_call_ids": []string{customer.ID(), agent.ID()},
	})
	require.Nil(t, rpcErr)
	var created map[string]any
	require.NoError(t, json.Unmarshal(raw, &created))
	roomID := created["room_id"].(string)
	_, rpcErr = driveRPC(t, reg, "room.add_leg", map[string]string{
		"room_id": roomID, "call_id": participant.ID(),
	})
	require.Nil(t, rpcErr)
	require.Equal(t, 2, h.roomTranscriptCount(), "both original legs transcribe in the room")

	// The third party hangs up. Two legs still have each other to mix, so the
	// room lives on and both transcripts keep running.
	require.NoError(t, participant.Hangup("test"))
	require.Never(t, func() bool {
		return h.roomTranscriptCount() != 2
	}, 300*time.Millisecond, 50*time.Millisecond, "a three-to-two drop keeps the room running")

	// The agent hangs up too: one leg has nothing to mix, so the room destroys
	// itself and the surviving leg's transcript has to be released with it.
	require.NoError(t, agent.Hangup("test"))
	require.Eventually(t, func() bool {
		return h.roomTranscriptCount() == 0
	}, 2*time.Second, 20*time.Millisecond, "winding the room down must release every transcript")
}

// roomTranscriptCount reports how many legs currently hold a room transcript.
func (h *Handlers) roomTranscriptCount() int {
	h.roomASRMu.Lock()
	defer h.roomASRMu.Unlock()
	return len(h.roomASR)
}

// transcriptRoleCount reports how many legs the active bridges are transcribing.
func (h *Handlers) transcriptRoleCount() int {
	h.bridgeMu.Lock()
	pairs := make([]*bridgePair, 0, len(h.bridgePairs))
	for _, p := range h.bridgePairs {
		pairs = append(pairs, p)
	}
	h.bridgeMu.Unlock()
	n := 0
	for _, p := range pairs {
		p.asrMu.Lock()
		n += len(p.asr)
		p.asrMu.Unlock()
	}
	return n
}
