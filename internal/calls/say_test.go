package calls

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/asr"
	asrmock "github.com/DeepFlowAI/flowkit/internal/asr/mock"
	"github.com/DeepFlowAI/flowkit/internal/media/bus"
	"github.com/DeepFlowAI/flowkit/internal/rpc"
	"github.com/DeepFlowAI/flowkit/internal/session"
	"github.com/DeepFlowAI/flowkit/internal/transport/fakeio"
	"github.com/DeepFlowAI/flowkit/internal/tts"
	ttsmock "github.com/DeepFlowAI/flowkit/internal/tts/mock"
	"github.com/stretchr/testify/require"
)

func TestHandleSay_WritesPCMOut(t *testing.T) {
	reg, mgr, _ := newTestStack(t)
	call := mgr.Create(session.DirectionInbound)

	// Bind a fakeio so we can observe outgoing samples
	io := fakeio.New(64)
	require.NoError(t, call.BindMediaIO(io))
	go io.CollectOutgoing()

	raw, rpcErr := driveRPC(t, reg, "call.say", map[string]string{
		"call_id": call.ID(), "text": "你好",
	})
	require.Nil(t, rpcErr)

	var result sayResult
	require.NoError(t, json.Unmarshal(raw, &result))
	require.NotEmpty(t, result.PlayID)

	// Total samples ≈ 2 chars × 800 samples/char = 1600. Playback is paced, so
	// wait for the whole stream rather than for the first frame.
	outgoingSamples := func() int {
		total := 0
		for _, f := range io.Outgoing() {
			total += len(f)
		}
		return total
	}
	require.Eventually(t, func() bool { return outgoingSamples() >= 1600 }, 2*time.Second, 20*time.Millisecond,
		"TTS samples must reach fakeio outgoing")
	require.Equal(t, 1600, outgoingSamples())

	require.NoError(t, call.Hangup("done"))
}

func TestHandleSay_PlayEndPlayedMSUsesSamples(t *testing.T) {
	// played_ms must be sample-written duration, not wall clock. A delayed
	// first TTS chunk makes wall_ms diverge so a wall-clock regression fails.
	mgr := session.NewManager(quietLogger())
	reg := rpc.NewRegistry(quietLogger())
	for _, m := range []string{"call.say"} {
		reg.AllowPreHello(m)
	}
	const synthDelay = 80 * time.Millisecond
	t.Cleanup(func() { mgr.Shutdown("test_cleanup") })
	h := Register(reg, mgr, ProviderFactories{
		ASR: func() (asr.Provider, error) { return asrmock.New(), nil },
		TTS: func() (tts.Provider, error) {
			return &delayedFirstChunkTTS{inner: ttsmock.New(), delay: synthDelay}, nil
		},
	}, quietLogger())

	call := mgr.Create(session.DirectionInbound)
	io := fakeio.New(64)
	require.NoError(t, call.BindMediaIO(io))
	go io.CollectOutgoing()

	var (
		mu      sync.Mutex
		playEnd map[string]any
	)
	unsub := h.SubscribeCall(call.ID(), func(method string, params map[string]any) {
		if method != "call.play_end" {
			return
		}
		cp := make(map[string]any, len(params))
		for k, v := range params {
			cp[k] = v
		}
		mu.Lock()
		playEnd = cp
		mu.Unlock()
	})
	defer unsub()

	_, rpcErr := driveRPC(t, reg, "call.say", map[string]string{
		"call_id": call.ID(), "text": "你好",
	})
	require.Nil(t, rpcErr)

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return playEnd != nil
	}, 2*time.Second, 10*time.Millisecond)

	mu.Lock()
	got := playEnd
	mu.Unlock()

	require.Equal(t, "completed", got["reason"])
	wantPlayed := int64(2*ttsmock.SamplesPerChar) * 1000 / int64(bus.SampleRate) // 100 ms
	require.Equal(t, wantPlayed, got["played_ms"])
	wallMS, ok := got["wall_ms"].(int64)
	require.True(t, ok, "wall_ms must be present on play_end")
	require.GreaterOrEqual(t, wallMS, synthDelay.Milliseconds())
	require.NotEqual(t, wantPlayed, wallMS)

	require.NoError(t, call.Hangup("done"))
}

func TestHeardPlaybackMs(t *testing.T) {
	first := time.Unix(100, 0)
	// Interrupted 166 ms after the first frame with ~2 s already queued: the
	// caller heard at most 166 ms, not the queued amount.
	require.Equal(t, int64(166), heardPlaybackMs(1989, false, first, first.Add(166*time.Millisecond)))
	// Interrupted after the queue drained: everything queued was heard.
	require.Equal(t, int64(500), heardPlaybackMs(500, false, first, first.Add(2*time.Second)))
	// Interrupted before any audio went out.
	require.Equal(t, int64(0), heardPlaybackMs(0, false, time.Time{}, first))
	// Completed playback keeps the full duration.
	require.Equal(t, int64(1989), heardPlaybackMs(1989, true, first, first.Add(time.Millisecond)))
}

// delayedFirstChunkTTS holds the first PCM chunk so playChunks wall time
// includes synthesis wait without changing how many samples are written.
type delayedFirstChunkTTS struct {
	inner *ttsmock.Mock
	delay time.Duration
}

func (d *delayedFirstChunkTTS) Synthesize(ctx context.Context, text string, opts tts.Options) (<-chan tts.PCMChunk, error) {
	ch, err := d.inner.Synthesize(ctx, text, opts)
	if err != nil {
		return nil, err
	}
	out := make(chan tts.PCMChunk, 4)
	go func() {
		defer close(out)
		first := true
		for chunk := range ch {
			if first {
				first = false
				select {
				case <-time.After(d.delay):
				case <-ctx.Done():
					return
				}
			}
			select {
			case out <- chunk:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}

func (d *delayedFirstChunkTTS) Close() error { return d.inner.Close() }

func TestHandleSay_PlayEndPlayedMSNoPerChunkTruncation(t *testing.T) {
	// 100 samples is not a multiple of 16: per-chunk int conversion yields 6 ms
	// each (truncating 6.25), so 16 chunks would report 96 ms instead of 100.
	const chunkSamples = 100
	const chunks = 16
	mgr := session.NewManager(quietLogger())
	t.Cleanup(func() { mgr.Shutdown("test_cleanup") })
	reg := rpc.NewRegistry(quietLogger())
	reg.AllowPreHello("call.say")
	h := Register(reg, mgr, ProviderFactories{
		ASR: func() (asr.Provider, error) { return asrmock.New(), nil },
		TTS: func() (tts.Provider, error) {
			return &fixedChunkTTS{chunkSamples: chunkSamples, chunks: chunks}, nil
		},
	}, quietLogger())

	call := mgr.Create(session.DirectionInbound)
	io := fakeio.New(64)
	require.NoError(t, call.BindMediaIO(io))
	go io.CollectOutgoing()

	var (
		mu      sync.Mutex
		playEnd map[string]any
	)
	unsub := h.SubscribeCall(call.ID(), func(method string, params map[string]any) {
		if method != "call.play_end" {
			return
		}
		cp := make(map[string]any, len(params))
		for k, v := range params {
			cp[k] = v
		}
		mu.Lock()
		playEnd = cp
		mu.Unlock()
	})
	defer unsub()

	_, rpcErr := driveRPC(t, reg, "call.say", map[string]string{
		"call_id": call.ID(), "text": "x",
	})
	require.Nil(t, rpcErr)

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return playEnd != nil
	}, 2*time.Second, 10*time.Millisecond)

	mu.Lock()
	got := playEnd
	mu.Unlock()

	totalSamples := int64(chunkSamples * chunks)
	wantPlayed := totalSamples * 1000 / int64(bus.SampleRate) // 100
	truncatedPerChunk := int64(chunks) * (int64(chunkSamples) * 1000 / int64(bus.SampleRate))
	require.Equal(t, "completed", got["reason"])
	require.Equal(t, wantPlayed, got["played_ms"])
	require.NotEqual(t, truncatedPerChunk, wantPlayed, "fixture must expose per-chunk truncation")

	require.NoError(t, call.Hangup("done"))
}

// fixedChunkTTS emits a fixed number of fixed-size PCM chunks (ignores text).
type fixedChunkTTS struct {
	chunkSamples int
	chunks       int
}

func (f *fixedChunkTTS) Synthesize(ctx context.Context, _ string, _ tts.Options) (<-chan tts.PCMChunk, error) {
	out := make(chan tts.PCMChunk, f.chunks)
	go func() {
		defer close(out)
		for i := 0; i < f.chunks; i++ {
			chunk := tts.PCMChunk{
				Samples: make([]int16, f.chunkSamples),
				Final:   i == f.chunks-1,
			}
			select {
			case out <- chunk:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}

func (f *fixedChunkTTS) Close() error { return nil }

func TestHandleSay_EmptyText_Errors(t *testing.T) {
	reg, mgr, _ := newTestStack(t)
	call := mgr.Create(session.DirectionInbound)

	_, rpcErr := driveRPC(t, reg, "call.say", map[string]string{
		"call_id": call.ID(), "text": "",
	})
	require.NotNil(t, rpcErr)
	require.Equal(t, rpc.CodeInvalidParams, rpcErr.Code)
}
