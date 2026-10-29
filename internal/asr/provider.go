package asr

import (
	"context"
	"errors"
	"time"
)

// EventType enumerates ASR-level events. They map onto the wire-level
// JSON-RPC notifications (call.speech_*).
type EventType int

const (
	// EventVADStart fires when voice activity detection sees the user begin
	// speaking. Producers without VAD may omit it.
	EventVADStart EventType = iota
	// EventPartial carries an intermediate (non-final) recognition hypothesis.
	EventPartial
	// EventFinal carries the finalised recognition for one utterance.
	EventFinal
	// EventVADEnd fires when the user stops speaking. Producers without VAD
	// may omit it.
	EventVADEnd
	// EventError signals a provider-level failure. The Err field is set.
	EventError
)

func (t EventType) String() string {
	switch t {
	case EventVADStart:
		return "vad_start"
	case EventPartial:
		return "partial"
	case EventFinal:
		return "final"
	case EventVADEnd:
		return "vad_end"
	case EventError:
		return "error"
	}
	return "unknown"
}

// Event is the stream item carried on Provider.Events().
type Event struct {
	Type     EventType
	Text     string    // populated for Partial / Final
	Sequence int       // monotonic, scoped to the Provider instance
	At       time.Time // UTC
	Err      error     // populated for EventError
}

// Provider is the contract every ASR backend implements. A Provider instance
// is tied to one streaming session. Start a fresh Provider for each call.
//
// Lifecycle:
//
//	p := mock.New(...)        // construct
//	p.Start(ctx)              // open backend session
//	p.SendPCM(frame)          // feed 20 ms frames
//	for ev := range p.Events() { ... }
//	p.Stop()                  // closes Events channel
type Provider interface {
	// Start opens the backend session. Must be called exactly once before
	// SendPCM. Returning an error means the provider is unusable.
	Start(ctx context.Context) error

	// SendPCM feeds one frame of 16 kHz mono PCM. Implementations should be
	// safe for sequential calls from a single goroutine; concurrent use is
	// undefined.
	SendPCM(samples []int16) error

	// Events returns the read-only stream of recognition events. The channel
	// is closed after Stop returns.
	Events() <-chan Event

	// Stop closes the backend session and the Events channel. Idempotent.
	Stop() error
}

// Flusher is implemented by providers that can force the current utterance to
// finalize mid-stream and re-arm for the next one. FunASR has no server-side
// endpointing, so a VAD-driven consumer calls Flush the moment the speaker
// stops — yielding clean, low-latency sentence boundaries (what Aliyun's NLS
// does internally). Providers with their own endpointing may leave this absent.
type Flusher interface {
	Flush() error
}

// Common error sentinels providers may return from SendPCM.
var (
	ErrNotStarted = errors.New("asr: provider not started")
	ErrStopped    = errors.New("asr: provider stopped")
)
