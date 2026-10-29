package bus

import "time"

// SampleRate is the canonical PCM sample rate used throughout Flowkit's
// internal audio pipeline. Wire-level codecs (G.711 at 8 kHz, Opus at 48 kHz)
// are resampled to this rate before being placed on an AudioBus.
const SampleRate = 16000

// FrameDuration is the typical frame interval (20 ms). 16 kHz × 20 ms = 320
// samples per frame. This is used as a hint for tests and buffer sizing —
// the Bus itself does not enforce a particular frame length.
const FrameDuration = 20 * time.Millisecond

// SamplesPerFrame is 16 kHz × 20 ms.
const SamplesPerFrame = SampleRate * 20 / 1000

// Frame is one chunk of mono PCM audio. The Samples slice MUST NOT be
// retained or mutated by subscribers; for cross-goroutine handoff the Bus
// publishes the underlying slice by reference for zero-copy performance.
type Frame struct {
	// Samples are signed 16-bit little-endian PCM samples at SampleRate Hz.
	Samples []int16

	// Timestamp is the monotonic offset from session start (or any other
	// stable reference). Used by ASR/recording to align with the wall clock.
	Timestamp time.Duration

	// Source identifies the producer for debugging / routing decisions.
	// Values include "trunk", "webrtc", "tts", "file", and test fixtures.
	Source string
}
