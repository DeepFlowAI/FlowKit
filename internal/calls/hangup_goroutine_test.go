package calls

import (
	"context"
	"testing"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/asr"
	asrmock "github.com/DeepFlowAI/flowkit/internal/asr/mock"
	"github.com/DeepFlowAI/flowkit/internal/media/bus"
	"github.com/DeepFlowAI/flowkit/internal/rpc"
	"github.com/DeepFlowAI/flowkit/internal/session"
	"github.com/DeepFlowAI/flowkit/internal/transport/fakeio"
	"github.com/DeepFlowAI/flowkit/internal/tts"
	"github.com/stretchr/testify/require"
)

// holdOpenTTS delivers one PCM chunk then keeps the stream open until ctx is
// cancelled. The first send is unbuffered, so it only completes once
// playChunks is ranging — and playChunks sets playing=true before ranging —
// making inPlayback a deterministic "mid-play" signal rather than a race on
// observing IsPlaying in a time window.
type holdOpenTTS struct {
	inPlayback chan struct{} // closed after playChunks accepts the first chunk
}

func (h *holdOpenTTS) Synthesize(ctx context.Context, text string, _ tts.Options) (<-chan tts.PCMChunk, error) {
	if text == "" {
		return nil, tts.ErrEmptyText
	}
	out := make(chan tts.PCMChunk)
	go func() {
		defer close(out)
		chunk := tts.PCMChunk{Samples: make([]int16, bus.SamplesPerFrame), Final: false}
		select {
		case out <- chunk:
			close(h.inPlayback)
		case <-ctx.Done():
			return
		}
		// Stay open so playChunks remains in the play slot (blocked on the
		// next range receive) until hangup cancels the synthesis context.
		<-ctx.Done()
	}()
	return out, nil
}

func (h *holdOpenTTS) Close() error { return nil }

// TestHangup_CancelsPerCallGoroutines starts play + ASR listen (typical
// per-call workers), hangs up, and asserts they reach a quiescent callState
// quickly once call.Context is cancelled.
func TestHangup_CancelsPerCallGoroutines(t *testing.T) {
	mgr := session.NewManager(quietLogger())
	t.Cleanup(func() { mgr.Shutdown("test_cleanup") })
	reg := rpc.NewRegistry(quietLogger())
	for _, m := range []string{"call.answer", "call.hangup", "call.say", "call.listen", "call.stop"} {
		reg.AllowPreHello(m)
	}
	hold := &holdOpenTTS{inPlayback: make(chan struct{})}
	h := Register(reg, mgr, ProviderFactories{
		ASR: func() (asr.Provider, error) { return asrmock.New(), nil },
		TTS: func() (tts.Provider, error) { return hold, nil },
	}, quietLogger())

	call := mgr.Create(session.DirectionInbound)
	io := fakeio.New(64)
	require.NoError(t, call.BindMediaIO(io))
	go io.CollectOutgoing()

	require.NoError(t, call.SetState(session.StateRinging))
	require.NoError(t, call.SetState(session.StateAnswered))
	require.NoError(t, call.SetState(session.StateActive))

	_, rpcErr := driveRPC(t, reg, "call.listen", map[string]string{"call_id": call.ID()})
	require.Nil(t, rpcErr)
	_, rpcErr = driveRPC(t, reg, "call.say", map[string]string{
		"call_id": call.ID(), "text": "hold-open",
	})
	require.Nil(t, rpcErr)

	select {
	case <-hold.inPlayback:
	case <-time.After(2 * time.Second):
		t.Fatal("playChunks never accepted the held-open TTS chunk")
	}
	require.True(t, h.IsPlaying(call.ID()), "must still be playing after inPlayback sync")

	cs := h.stateGet(call.ID())
	require.NotNil(t, cs)
	require.True(t, cs.listening.Load(), "listen must be active before hangup")

	hangupAt := time.Now()
	require.NoError(t, call.Hangup("test_hangup"))

	// watchCallMediaEnd → stopCallMedia + dropState; playCtx cancels with the
	// call tree so playChunks / ASR pumps exit without a separate Done watcher.
	require.Eventually(t, func() bool {
		return !h.IsPlaying(call.ID()) && h.stateGet(call.ID()) == nil
	}, 2*time.Second, 5*time.Millisecond,
		"play + callState must clear after hangup")

	elapsed := time.Since(hangupAt)
	t.Logf("hangup→per-call idle: %s", elapsed)
	require.Less(t, elapsed, 500*time.Millisecond,
		"per-call workers should exit well under half a second after hangup")
}
