package pacer

import (
	"context"
	"sync/atomic"
)

// Outgoing is an embeddable facade over *Pacer that exposes the transport
// package's paced-playback method names (OutgoingPCM, PauseOutgoing, …).
// Embedding it in a MediaIO satisfies — by method set alone, with no import of
// package transport — the paced-playback half of transport.MediaIO (not DTMF).
//
// Construction is two-step because the transport is typically also the Sink
// and must be addressable before the pacer can start:
//
//	m := &MediaIO{ /* sockets, incoming, … */ }
//	m.Outgoing.Bind(m, WithQueue(outBuf))
//	// … start incoming pump, return m
//
// Methods are safe before Bind: controls are no-ops, WaitPlaybackIdle returns
// ErrClosed, and OutgoingPCM returns nil (a send on a nil channel blocks; it
// never panics). Bind before any producer runs.
//
// Close is intentionally NOT part of the transport.MediaIO surface here: each
// transport's Close must also shut sockets / done channels. The embedder MUST
// call Outgoing.Close() from its own Close — otherwise the pacer send
// goroutine leaks.
type Outgoing struct {
	p atomic.Pointer[Pacer]
}

// Bind starts a paced sender on sink and attaches it to o. The first call wins;
// a racing second Bind closes the unused pacer so it cannot leak.
func (o *Outgoing) Bind(sink Sink, opts ...Option) {
	p := New(sink, opts...)
	if !o.p.CompareAndSwap(nil, p) {
		p.Close()
	}
}

// Close stops the underlying send goroutine. Idempotent. No-op if Bind was
// never called. Embedders must invoke this from their MediaIO.Close.
func (o *Outgoing) Close() {
	if p := o.p.Load(); p != nil {
		p.Close()
	}
}

// Pacer returns the bound kernel, or nil if Bind has not run. Intended for
// diagnostics (Snapshot); normal playback goes through the methods below.
func (o *Outgoing) Pacer() *Pacer { return o.p.Load() }

// PlaybackStats returns a call-lifetime diagnostics snapshot. The zero value
// is returned before Bind so transports can expose it safely during teardown.
func (o *Outgoing) PlaybackStats() Stats {
	if p := o.p.Load(); p != nil {
		return p.Snapshot()
	}
	return Stats{}
}

// OutgoingPCM implements the enqueue half of transport.MediaIO.
func (o *Outgoing) OutgoingPCM() chan<- []int16 {
	if p := o.p.Load(); p != nil {
		return p.OutgoingPCM()
	}
	return nil
}

// PauseOutgoing freezes paced send without dropping queued PCM.
func (o *Outgoing) PauseOutgoing() {
	if p := o.p.Load(); p != nil {
		p.Pause()
	}
}

// ResumeOutgoing continues after PauseOutgoing.
func (o *Outgoing) ResumeOutgoing() {
	if p := o.p.Load(); p != nil {
		p.Resume()
	}
}

// StopOutgoing discards queued / buffered outgoing audio (barge-in).
func (o *Outgoing) StopOutgoing() {
	if p := o.p.Load(); p != nil {
		p.Stop()
	}
}

// WaitPlaybackIdle blocks until there is no queued or in-flight outgoing audio.
// Returns ErrClosed if the pacer is closed or Bind has not run.
func (o *Outgoing) WaitPlaybackIdle(ctx context.Context) error {
	if p := o.p.Load(); p != nil {
		return p.WaitIdle(ctx)
	}
	return ErrClosed
}

// SetSentTap registers a send-time PCM tap; pass nil to clear.
func (o *Outgoing) SetSentTap(fn func([]int16)) {
	if p := o.p.Load(); p != nil {
		p.SetSentTap(fn)
	}
}

// DuckOutgoing attenuates paced frames by gain in (0, 1].
func (o *Outgoing) DuckOutgoing(gain float64) {
	if p := o.p.Load(); p != nil {
		p.Duck(gain)
	}
}

// RestoreOutgoing returns send gain to unity.
func (o *Outgoing) RestoreOutgoing() {
	if p := o.p.Load(); p != nil {
		p.Restore()
	}
}

// SetPlaybackFeeding brackets a bursty source so mid-burst underruns emit silence.
func (o *Outgoing) SetPlaybackFeeding(feeding bool) {
	if p := o.p.Load(); p != nil {
		p.SetFeeding(feeding)
	}
}
