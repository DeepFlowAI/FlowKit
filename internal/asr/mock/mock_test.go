package mock

import (
	"context"
	"testing"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/asr"
	"github.com/stretchr/testify/require"
)

// compile-time interface check
var _ asr.Provider = (*Mock)(nil)

func newSamples(n int) []int16 { return make([]int16, n) }

func TestMock_Lifecycle(t *testing.T) {
	m := New()
	require.NoError(t, m.Start(context.Background()))
	require.NoError(t, m.SendPCM(newSamples(320)))
	require.NoError(t, m.Stop())

	// Events channel closed
	_, ok := <-m.Events()
	require.False(t, ok)
}

func TestMock_SendBeforeStart_Errors(t *testing.T) {
	m := New()
	err := m.SendPCM(newSamples(10))
	require.ErrorIs(t, err, asr.ErrNotStarted)
}

func TestMock_SendAfterStop_Errors(t *testing.T) {
	m := New()
	require.NoError(t, m.Start(context.Background()))
	require.NoError(t, m.Stop())
	err := m.SendPCM(newSamples(10))
	require.ErrorIs(t, err, asr.ErrStopped)
}

func TestMock_EmitsScriptedEvents(t *testing.T) {
	m := New(
		Step{AfterSamples: 320, Type: asr.EventVADStart},
		Step{AfterSamples: 640, Type: asr.EventPartial, Text: "你"},
		Step{AfterSamples: 1280, Type: asr.EventPartial, Text: "你好"},
		Step{AfterSamples: 1920, Type: asr.EventFinal, Text: "你好，我要查订单"},
		Step{AfterSamples: 1920, Type: asr.EventVADEnd},
	)
	require.NoError(t, m.Start(context.Background()))

	// Feed PCM in 320-sample chunks (= 20 ms at 16 kHz)
	for i := 0; i < 6; i++ {
		require.NoError(t, m.SendPCM(newSamples(320)))
	}

	gotTypes := drain(t, m.Events(), 5)
	require.Equal(t, []asr.EventType{
		asr.EventVADStart,
		asr.EventPartial,
		asr.EventPartial,
		asr.EventFinal,
		asr.EventVADEnd,
	}, gotTypes)

	require.NoError(t, m.Stop())
}

func TestMock_SequenceMonotonic(t *testing.T) {
	m := New(
		Step{AfterSamples: 100, Type: asr.EventPartial, Text: "a"},
		Step{AfterSamples: 200, Type: asr.EventPartial, Text: "b"},
		Step{AfterSamples: 300, Type: asr.EventFinal, Text: "ab"},
	)
	require.NoError(t, m.Start(context.Background()))
	require.NoError(t, m.SendPCM(newSamples(300)))

	var prev int
	for i := 0; i < 3; i++ {
		select {
		case ev := <-m.Events():
			require.Greater(t, ev.Sequence, prev, "sequence must be monotonic")
			prev = ev.Sequence
		case <-time.After(time.Second):
			t.Fatalf("missing event %d", i)
		}
	}
	require.NoError(t, m.Stop())
}

func TestMock_NoStepsConsumed_BeforeThreshold(t *testing.T) {
	m := New(Step{AfterSamples: 1000, Type: asr.EventFinal, Text: "done"})
	require.NoError(t, m.Start(context.Background()))
	require.NoError(t, m.SendPCM(newSamples(500))) // not enough yet

	select {
	case ev := <-m.Events():
		t.Fatalf("unexpected event: %v", ev)
	case <-time.After(50 * time.Millisecond):
	}
	require.Equal(t, int64(500), m.SamplesReceived())
	require.NoError(t, m.Stop())
}

func TestMock_StopIsIdempotent(t *testing.T) {
	m := New()
	require.NoError(t, m.Start(context.Background()))
	require.NoError(t, m.Stop())
	require.NoError(t, m.Stop())
}

func TestMock_StartIsIdempotent(t *testing.T) {
	m := New()
	require.NoError(t, m.Start(context.Background()))
	require.NoError(t, m.Start(context.Background()))
}

func TestEventType_String(t *testing.T) {
	require.Equal(t, "partial", asr.EventPartial.String())
	require.Equal(t, "final", asr.EventFinal.String())
	require.Equal(t, "vad_start", asr.EventVADStart.String())
	require.Equal(t, "vad_end", asr.EventVADEnd.String())
	require.Equal(t, "error", asr.EventError.String())
	require.Equal(t, "unknown", asr.EventType(99).String())
}

func drain(t *testing.T, ch <-chan asr.Event, want int) []asr.EventType {
	t.Helper()
	out := make([]asr.EventType, 0, want)
	for i := 0; i < want; i++ {
		select {
		case ev := <-ch:
			out = append(out, ev.Type)
		case <-time.After(time.Second):
			t.Fatalf("only got %d / %d events", len(out), want)
		}
	}
	return out
}
