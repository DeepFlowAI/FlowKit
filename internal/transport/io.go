// Package transport defines the audio I/O contract that every concrete
// transport (WebRTC, SIP, WS-PCM) implements. By hiding wire-level
// codecs and signalling behind this single interface, the Call and Provider
// layers can stay codec-agnostic.
package transport

import "context"

// Kind identifies which concrete transport backs a MediaIO. Use this for
// business decisions that differ by leg (e.g. server-side AEC only on SIP);
// do not infer the leg from optional-capability probes.
type Kind string

const (
	KindSIP    Kind = "sip"
	KindWebRTC Kind = "webrtc"
	KindWSPCM  Kind = "wspcm"
	// KindFake is for test doubles (fakeio). Never reuse a production Kind.
	KindFake Kind = "fake"
)

// MediaIO is one bidirectional 16 kHz mono PCM stream attached to a Call.
//
// Both directions move whole 20 ms frames (typically 320 int16 samples). The
// transport owns both channels: callers MUST NOT close OutgoingPCM directly
// — Close() releases both sides together.
//
// Concurrency: IncomingPCM may be read by exactly one goroutine; OutgoingPCM
// may be written by exactly one goroutine. Multi-consumer fan-out happens
// via the session-owned AudioBus, not here.
//
// Paced-playback methods (WaitPlaybackIdle through RestoreOutgoing) are
// required: every transport must embed the shared pacer kernel (or an
// equivalent). New transports MUST provide them — they are no longer optional
// probes. DTMF remains optional (see DTMFReceiver / DTMFSender); only SIP
// supports telephone-event today.
type MediaIO interface {
	// Kind reports which concrete transport this MediaIO is.
	Kind() Kind

	// IncomingPCM is the channel of frames received from the peer (mic side).
	// Closed when the transport ends, after which Close should also be called.
	IncomingPCM() <-chan []int16

	// OutgoingPCM accepts frames to send to the peer (speaker side). Owned
	// by the transport; do NOT close it from the caller side.
	OutgoingPCM() chan<- []int16

	// Close shuts the transport down, closing both channels. Idempotent.
	Close() error

	// WaitPlaybackIdle blocks until paced outgoing audio has drained.
	WaitPlaybackIdle(ctx context.Context) error

	// SetSentTap registers a callback invoked with each 16 kHz frame at the
	// instant it is sent (after pacing). Recorders and AEC use this for
	// correct playback timing. The tap must be non-blocking and must copy
	// samples it retains. Pass nil to clear.
	SetSentTap(fn func(samples []int16))

	// StopOutgoing discards queued / in-flight outgoing audio (barge-in).
	StopOutgoing()

	// SetPlaybackFeeding brackets a bursty source (TTS / file play). While
	// feeding is true the sender keeps the stream continuous (paced silence)
	// across underruns instead of tearing down. Idempotent; any goroutine.
	SetPlaybackFeeding(feeding bool)

	// PauseOutgoing freezes paced send without dropping the queue.
	// ResumeOutgoing continues from the same position.
	PauseOutgoing()
	ResumeOutgoing()

	// DuckOutgoing attenuates paced playback by gain in (0, 1] without
	// stopping or discarding queued audio. RestoreOutgoing returns to unity.
	DuckOutgoing(gain float64)
	RestoreOutgoing()
}

// DTMFHandler receives RFC 4733 telephone-event digits from the peer.
type DTMFHandler func(digit string)

// DTMFReceiver is optionally implemented by MediaIO transports that can
// detect inbound DTMF (SIP RFC 4733). WebRTC and WS-PCM do not implement it.
type DTMFReceiver interface {
	SetDTMFHandler(DTMFHandler)
}

// DTMFSender is optionally implemented by MediaIO transports that can emit
// RFC 4733 DTMF toward the peer. WebRTC and WS-PCM do not implement it.
type DTMFSender interface {
	SendDTMF(digits string, durationMS int) error
}
