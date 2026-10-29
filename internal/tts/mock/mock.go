package mock

import (
	"context"
	"sync/atomic"

	"github.com/DeepFlowAI/flowkit/internal/tts"
)

// SamplesPerChar is the number of PCM samples the mock emits per character of
// input text. Roughly equivalent to ~50 ms / char at 16 kHz (a slow human
// speech rate), which makes mock output realistically-sized without using
// real audio fixtures.
const SamplesPerChar = 800

// ChunkSamples is the size of each emitted PCM chunk (~50 ms).
const ChunkSamples = 800

// Mock is an in-process tts.Provider. It emits zeroed PCM proportional to
// the input text length. Useful for SDK integration tests that need a
// believable streaming response without invoking a real TTS service.
type Mock struct {
	closed atomic.Bool

	// CallCount counts how many Synthesize invocations have completed
	// (successfully or not). Useful for assertions in higher-level tests.
	CallCount atomic.Int64
}

// New constructs a Mock.
func New() *Mock { return &Mock{} }

// Synthesize implements tts.Provider.
func (m *Mock) Synthesize(ctx context.Context, text string, _ tts.Options) (<-chan tts.PCMChunk, error) {
	if m.closed.Load() {
		return nil, tts.ErrClosed
	}
	if text == "" {
		return nil, tts.ErrEmptyText
	}

	totalSamples := len([]rune(text)) * SamplesPerChar
	out := make(chan tts.PCMChunk, 4)

	go func() {
		defer close(out)
		defer m.CallCount.Add(1)

		sent := 0
		for sent < totalSamples {
			n := ChunkSamples
			if sent+n > totalSamples {
				n = totalSamples - sent
			}
			chunk := tts.PCMChunk{
				Samples: make([]int16, n),
				Final:   sent+n >= totalSamples,
			}
			select {
			case out <- chunk:
				sent += n
			case <-ctx.Done():
				// Emit one final empty chunk so consumers can distinguish
				// "cancelled" from "completed naturally" by checking ctx.Err().
				return
			}
		}
	}()

	return out, nil
}

// Close implements tts.Provider.
func (m *Mock) Close() error {
	m.closed.Store(true)
	return nil
}
