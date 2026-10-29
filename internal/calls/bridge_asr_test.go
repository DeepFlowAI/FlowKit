package calls

import (
	"sync"
	"testing"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/asr"
	asrmock "github.com/DeepFlowAI/flowkit/internal/asr/mock"
	"github.com/DeepFlowAI/flowkit/internal/session"
	"github.com/DeepFlowAI/flowkit/internal/transport/fakeio"
	"github.com/stretchr/testify/require"
)

// When bridge ASR is on, both legs of a bridge are transcribed and the text is
// dispatched as call.bridge_transcript under the user (inbound) leg's id, each
// line tagged with the speaker.
func TestBridge_DualLegASR_EmitsSpeakerTaggedTranscript(t *testing.T) {
	reg, mgr, h := newTestStack(t)
	reg.AllowPreHello("call.bridge")
	h.bridgeASREnabled = true
	// Leading comma (FunASR punctuation-model artifact) must be stripped.
	h.factories.ASR = func() (asr.Provider, error) {
		return asrmock.New(asrmock.Step{
			AfterSamples: 320,
			Type:         asr.EventFinal,
			Text:         "，我要退款",
		}), nil
	}

	ioA := fakeio.New(8)
	ioB := fakeio.New(8)
	go ioA.CollectOutgoing()
	go ioB.CollectOutgoing()

	callA := mgr.Create(session.DirectionInbound)  // user leg → anchor
	callB := mgr.Create(session.DirectionOutbound) // agent leg
	activateCall(t, callA, ioA)
	activateCall(t, callB, ioB)

	var mu sync.Mutex
	got := map[string]string{} // speaker → text
	h.SubscribeCall(callA.ID(), func(method string, params map[string]any) {
		if method != "call.bridge_transcript" {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		// Every line is reported under the anchor (user-leg) call_id.
		require.Equal(t, callA.ID(), params["call_id"])
		got[params["speaker"].(string)] = params["text"].(string)
	})

	_, rpcErr := driveRPC(t, reg, "call.bridge", map[string]string{
		"call_id":       callA.ID(),
		"other_call_id": callB.ID(),
	})
	require.Nil(t, rpcErr)

	frame := make([]int16, 320)
	require.Eventually(t, func() bool {
		ioA.PushIncoming(frame)
		ioB.PushIncoming(frame)
		mu.Lock()
		defer mu.Unlock()
		return got["user"] != "" && got["agent"] != ""
	}, 3*time.Second, 20*time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, "我要退款", got["user"])
	require.Equal(t, "我要退款", got["agent"])
}

// With the capability on but transcribe:false on the call, ASR never starts.
func TestBridge_NoASR_WhenTranscribeFalse(t *testing.T) {
	reg, mgr, h := newTestStack(t)
	reg.AllowPreHello("call.bridge")
	h.bridgeASREnabled = true
	h.factories.ASR = func() (asr.Provider, error) {
		return asrmock.New(asrmock.Step{
			AfterSamples: 320,
			Type:         asr.EventFinal,
			Text:         "我要退款",
		}), nil
	}

	ioA := fakeio.New(8)
	ioB := fakeio.New(8)
	go ioA.CollectOutgoing()
	go ioB.CollectOutgoing()

	callA := mgr.Create(session.DirectionInbound)
	callB := mgr.Create(session.DirectionOutbound)
	activateCall(t, callA, ioA)
	activateCall(t, callB, ioB)

	var seen int
	var mu sync.Mutex
	h.SubscribeCall(callA.ID(), func(method string, _ map[string]any) {
		if method == "call.bridge_transcript" {
			mu.Lock()
			seen++
			mu.Unlock()
		}
	})

	_, rpcErr := driveRPC(t, reg, "call.bridge", map[string]any{
		"call_id":       callA.ID(),
		"other_call_id": callB.ID(),
		"transcribe":    false,
	})
	require.Nil(t, rpcErr)

	frame := make([]int16, 320)
	for i := 0; i < 10; i++ {
		ioA.PushIncoming(frame)
		ioB.PushIncoming(frame)
		time.Sleep(10 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, 0, seen)
}

// With bridge ASR off (default), no transcript streams start.
func TestBridge_NoASR_WhenDisabled(t *testing.T) {
	reg, mgr, h := newTestStack(t)
	reg.AllowPreHello("call.bridge")
	require.False(t, h.bridgeASREnabled)
	h.factories.ASR = func() (asr.Provider, error) {
		return asrmock.New(asrmock.Step{
			AfterSamples: 320,
			Type:         asr.EventFinal,
			Text:         "我要退款",
		}), nil
	}

	ioA := fakeio.New(8)
	ioB := fakeio.New(8)
	go ioA.CollectOutgoing()
	go ioB.CollectOutgoing()

	callA := mgr.Create(session.DirectionInbound)
	callB := mgr.Create(session.DirectionOutbound)
	activateCall(t, callA, ioA)
	activateCall(t, callB, ioB)

	var seen int
	var mu sync.Mutex
	h.SubscribeCall(callA.ID(), func(method string, _ map[string]any) {
		if method == "call.bridge_transcript" {
			mu.Lock()
			seen++
			mu.Unlock()
		}
	})

	_, rpcErr := driveRPC(t, reg, "call.bridge", map[string]string{
		"call_id":       callA.ID(),
		"other_call_id": callB.ID(),
	})
	require.Nil(t, rpcErr)

	frame := make([]int16, 320)
	for i := 0; i < 10; i++ {
		ioA.PushIncoming(frame)
		ioB.PushIncoming(frame)
		time.Sleep(10 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, 0, seen)
}

// bridgeTranscriptLine is one dispatched call.bridge_transcript payload.
type bridgeTranscriptLine struct {
	text  string
	final bool
}

// collectUserBridgeTranscripts subscribes to the anchor call and appends every
// user-speaker bridge_transcript line in order.
func collectUserBridgeTranscripts(h *Handlers, anchorID string) func() []bridgeTranscriptLine {
	var mu sync.Mutex
	var lines []bridgeTranscriptLine
	h.SubscribeCall(anchorID, func(method string, params map[string]any) {
		if method != "call.bridge_transcript" || params["speaker"] != "user" {
			return
		}
		mu.Lock()
		lines = append(lines, bridgeTranscriptLine{
			text:  params["text"].(string),
			final: params["final"].(bool),
		})
		mu.Unlock()
	})
	return func() []bridgeTranscriptLine {
		mu.Lock()
		defer mu.Unlock()
		out := make([]bridgeTranscriptLine, len(lines))
		copy(out, lines)
		return out
	}
}

// FunASR-style single-char online increments must accumulate into a growing
// snapshot (each fragment alone would fail PlausibleUserSpeech); the offline
// final replaces that snapshot.
func TestBridgeASR_IncrementalPartialsAccumulate(t *testing.T) {
	reg, mgr, h := newTestStack(t)
	reg.AllowPreHello("call.bridge")
	h.bridgeASREnabled = true
	h.factories.ASR = func() (asr.Provider, error) {
		return asrmock.New(
			asrmock.Step{AfterSamples: 320, Type: asr.EventPartial, Text: "等"},
			asrmock.Step{AfterSamples: 640, Type: asr.EventPartial, Text: "一"},
			asrmock.Step{AfterSamples: 960, Type: asr.EventPartial, Text: "下"},
			asrmock.Step{AfterSamples: 1280, Type: asr.EventFinal, Text: "等一下"},
		), nil
	}

	ioA := fakeio.New(8)
	ioB := fakeio.New(8)
	go ioA.CollectOutgoing()
	go ioB.CollectOutgoing()

	callA := mgr.Create(session.DirectionInbound)
	callB := mgr.Create(session.DirectionOutbound)
	activateCall(t, callA, ioA)
	activateCall(t, callB, ioB)

	snapshot := collectUserBridgeTranscripts(h, callA.ID())

	_, rpcErr := driveRPC(t, reg, "call.bridge", map[string]string{
		"call_id":       callA.ID(),
		"other_call_id": callB.ID(),
	})
	require.Nil(t, rpcErr)

	frame := make([]int16, 320)
	require.Eventually(t, func() bool {
		ioA.PushIncoming(frame)
		ioB.PushIncoming(frame)
		got := snapshot()
		return len(got) >= 3 && got[len(got)-1].final
	}, 3*time.Second, 20*time.Millisecond)

	got := snapshot()
	require.Equal(t, []bridgeTranscriptLine{
		{text: "等一", final: false},
		{text: "等一下", final: false},
		{text: "等一下", final: true},
	}, got)
}

// Cumulative-style partials (each hypothesis contains the previous) must replace
// the accumulator, not append, so text is never duplicated.
func TestBridgeASR_CumulativePartialsDoNotDuplicate(t *testing.T) {
	reg, mgr, h := newTestStack(t)
	reg.AllowPreHello("call.bridge")
	h.bridgeASREnabled = true
	h.factories.ASR = func() (asr.Provider, error) {
		return asrmock.New(
			asrmock.Step{AfterSamples: 320, Type: asr.EventPartial, Text: "我要"},
			asrmock.Step{AfterSamples: 640, Type: asr.EventPartial, Text: "我要退"},
			asrmock.Step{AfterSamples: 960, Type: asr.EventPartial, Text: "我要退款"},
			asrmock.Step{AfterSamples: 1280, Type: asr.EventFinal, Text: "我要退款"},
		), nil
	}

	ioA := fakeio.New(8)
	ioB := fakeio.New(8)
	go ioA.CollectOutgoing()
	go ioB.CollectOutgoing()

	callA := mgr.Create(session.DirectionInbound)
	callB := mgr.Create(session.DirectionOutbound)
	activateCall(t, callA, ioA)
	activateCall(t, callB, ioB)

	snapshot := collectUserBridgeTranscripts(h, callA.ID())

	_, rpcErr := driveRPC(t, reg, "call.bridge", map[string]string{
		"call_id":       callA.ID(),
		"other_call_id": callB.ID(),
	})
	require.Nil(t, rpcErr)

	frame := make([]int16, 320)
	require.Eventually(t, func() bool {
		ioA.PushIncoming(frame)
		ioB.PushIncoming(frame)
		got := snapshot()
		return len(got) >= 4 && got[len(got)-1].final
	}, 3*time.Second, 20*time.Millisecond)

	got := snapshot()
	require.Equal(t, []bridgeTranscriptLine{
		{text: "我要", final: false},
		{text: "我要退", final: false},
		{text: "我要退款", final: false},
		{text: "我要退款", final: true},
	}, got)
}

// After a final the accumulator resets, so the next segment's partials do not
// carry text from the previous segment.
func TestBridgeASR_AccumulatorResetsAfterFinal(t *testing.T) {
	reg, mgr, h := newTestStack(t)
	reg.AllowPreHello("call.bridge")
	h.bridgeASREnabled = true
	h.factories.ASR = func() (asr.Provider, error) {
		return asrmock.New(
			asrmock.Step{AfterSamples: 320, Type: asr.EventPartial, Text: "你好"},
			asrmock.Step{AfterSamples: 640, Type: asr.EventFinal, Text: "你好"},
			asrmock.Step{AfterSamples: 960, Type: asr.EventPartial, Text: "再见"},
			asrmock.Step{AfterSamples: 1280, Type: asr.EventFinal, Text: "再见"},
		), nil
	}

	ioA := fakeio.New(8)
	ioB := fakeio.New(8)
	go ioA.CollectOutgoing()
	go ioB.CollectOutgoing()

	callA := mgr.Create(session.DirectionInbound)
	callB := mgr.Create(session.DirectionOutbound)
	activateCall(t, callA, ioA)
	activateCall(t, callB, ioB)

	snapshot := collectUserBridgeTranscripts(h, callA.ID())

	_, rpcErr := driveRPC(t, reg, "call.bridge", map[string]string{
		"call_id":       callA.ID(),
		"other_call_id": callB.ID(),
	})
	require.Nil(t, rpcErr)

	frame := make([]int16, 320)
	require.Eventually(t, func() bool {
		ioA.PushIncoming(frame)
		ioB.PushIncoming(frame)
		got := snapshot()
		return len(got) >= 4 && got[len(got)-1].final
	}, 3*time.Second, 20*time.Millisecond)

	got := snapshot()
	require.Equal(t, []bridgeTranscriptLine{
		{text: "你好", final: false},
		{text: "你好", final: true},
		{text: "再见", final: false},
		{text: "再见", final: true},
	}, got)
}
