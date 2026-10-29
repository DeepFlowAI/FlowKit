package tts

import (
	"context"
	"errors"
)

// PCMChunk is one block of synthesised audio. Streaming providers emit
// multiple chunks; one-shot providers emit one followed by Final=true.
type PCMChunk struct {
	// Samples is 16 kHz mono PCM (matching bus.SampleRate).
	Samples []int16
	// Final is true on the last chunk of a single synthesis call. Consumers
	// should not assume a non-Final chunk implies more chunks will follow if
	// the context is cancelled.
	Final bool
}

// Options can override per-call synthesis parameters. Nil/zero fields fall
// back to the provider's defaults (which the wiring layer typically reads
// from configuration).
type Options struct {
	Voice      string  // e.g. "siyue"
	SpeechRate int     // -500..500 (Aliyun-style)
	Volume     int     // 0..100
	Pitch      int     // -500..500
	SampleRate int     // 16000 default
	Format     string  // "pcm" (default) | "wav"
	Custom     map[string]any
	// LeadPadMs, when > 0, overrides the provider's default leading-silence pad
	// for this one synthesis (see cosyvoice LeadPadMs). Used to give the initial
	// greeting a long pad so its opening words survive the SIP answer→media
	// cut-through gap. 0 = use the provider default.
	LeadPadMs int
	// TailPadMs, when > 0, appends this much trailing silence after the last
	// speech samples. Keeps the far end's jitter buffer fed so the final
	// syllable plays out fully before the RTP stream goes quiet, instead of
	// being clipped when playback stops.
	TailPadMs int
	// PrebufferMs, when > 0, overrides the provider's startup audio prebuffer
	// for this synthesis (see cosyvoice PrebufferMs). 0 = use provider default.
	PrebufferMs int
}

// Provider is the contract every TTS backend implements. Unlike ASR, TTS is
// request/response in shape — one call to Synthesize, one stream of chunks
// back. Each call starts a fresh synthesis; there is no Start/Stop pair.
type Provider interface {
	// Synthesize converts text to PCM chunks. The returned channel is closed
	// when synthesis completes (the last chunk has Final=true) or when ctx
	// is cancelled.
	Synthesize(ctx context.Context, text string, opts Options) (<-chan PCMChunk, error)

	// Close releases any underlying network connections. After Close,
	// Synthesize returns ErrClosed.
	Close() error
}

// Common error sentinels.
var (
	ErrClosed      = errors.New("tts: provider closed")
	ErrEmptyText   = errors.New("tts: empty text")
	ErrCancelled   = errors.New("tts: synthesis cancelled")
	ErrUnsupported = errors.New("tts: unsupported option")
)
