// Package fakeio provides a controllable transport.MediaIO implementation
// for tests. It is in its own package so production builds never link it.
package fakeio

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/DeepFlowAI/flowkit/internal/transport"
)

// FakeIO is a transport.MediaIO controlled by tests. Tests push frames into
// IncomingCh to simulate incoming audio and read from OutgoingFrames to see
// what was sent. Each side has an independent sync.Once so partial closes
// (e.g. EndIncoming followed by Close) are safe.
//
// Paced-playback controls are no-ops (no send loop). SetSentTap is honored:
// CollectOutgoing invokes the registered tap as it drains each frame, so
// render sinks wired via Call.SetSentTap still receive reference audio.
type FakeIO struct {
	incoming chan []int16
	outgoing chan []int16

	mu     sync.Mutex
	outBuf [][]int16

	// tapMu guards sentTap against SetSentTap racing CollectOutgoing.
	tapMu   sync.Mutex
	sentTap func([]int16)

	// incomingMu serializes PushIncoming against closeIncoming so -race does
	// not flag closechan vs chansend when a pump goroutine outlives Close.
	incomingMu     sync.Mutex
	incomingClosed bool

	incomingOnce sync.Once
	doneOnce     sync.Once
	done         chan struct{}
	closed       atomic.Bool
}

// New constructs a FakeIO. bufSize controls the incoming/outgoing channel
// capacity; a value of 0 yields unbuffered channels (test must consume).
func New(bufSize int) *FakeIO {
	return &FakeIO{
		incoming: make(chan []int16, bufSize),
		outgoing: make(chan []int16, bufSize),
		done:     make(chan struct{}),
	}
}

// Kind implements transport.MediaIO.
func (f *FakeIO) Kind() transport.Kind { return transport.KindFake }

// IncomingPCM implements transport.MediaIO.
func (f *FakeIO) IncomingPCM() <-chan []int16 { return f.incoming }

// OutgoingPCM implements transport.MediaIO.
func (f *FakeIO) OutgoingPCM() chan<- []int16 { return f.outgoing }

// Close implements transport.MediaIO. Idempotent; safe after partial closes.
// The outgoing channel is intentionally NOT closed here: producers write to it
// via Call.WriteOutgoing, and closing it under them would race into a
// send-on-closed panic. Consumers stop on the done signal instead.
func (f *FakeIO) Close() error {
	// Signal done first so PushIncoming select can exit before we close the
	// incoming channel under incomingMu.
	f.doneOnce.Do(func() {
		f.closed.Store(true)
		close(f.done)
	})
	f.closeIncoming()
	return nil
}

// WaitPlaybackIdle implements transport.MediaIO. FakeIO has no pacer — always idle.
func (f *FakeIO) WaitPlaybackIdle(context.Context) error { return nil }

// SetSentTap implements transport.MediaIO. Pass nil to clear.
func (f *FakeIO) SetSentTap(fn func(samples []int16)) {
	f.tapMu.Lock()
	f.sentTap = fn
	f.tapMu.Unlock()
}

// StopOutgoing implements transport.MediaIO.
func (f *FakeIO) StopOutgoing() {}

// SetPlaybackFeeding implements transport.MediaIO.
func (f *FakeIO) SetPlaybackFeeding(bool) {}

// PauseOutgoing implements transport.MediaIO.
func (f *FakeIO) PauseOutgoing() {}

// ResumeOutgoing implements transport.MediaIO.
func (f *FakeIO) ResumeOutgoing() {}

// DuckOutgoing implements transport.MediaIO.
func (f *FakeIO) DuckOutgoing(float64) {}

// RestoreOutgoing implements transport.MediaIO.
func (f *FakeIO) RestoreOutgoing() {}

func (f *FakeIO) fireSentTap(samples []int16) {
	f.tapMu.Lock()
	tap := f.sentTap
	f.tapMu.Unlock()
	if tap != nil {
		// Tap must copy if it retains samples (same contract as paced transports).
		tap(samples)
	}
}

func (f *FakeIO) closeIncoming() {
	f.incomingOnce.Do(func() {
		f.incomingMu.Lock()
		defer f.incomingMu.Unlock()
		if !f.incomingClosed {
			close(f.incoming)
			f.incomingClosed = true
		}
	})
}

// PushIncoming feeds one frame as if it arrived from the peer. Blocks if
// the buffer is full. No-op if already closed.
func (f *FakeIO) PushIncoming(samples []int16) {
	f.incomingMu.Lock()
	defer f.incomingMu.Unlock()
	if f.incomingClosed || f.closed.Load() {
		return
	}
	select {
	case f.incoming <- samples:
	case <-f.done:
	}
}

// EndIncoming closes the incoming channel to simulate peer disconnect.
// Outgoing remains open until Close().
func (f *FakeIO) EndIncoming() {
	f.closeIncoming()
}

// CollectOutgoing drains the outgoing channel into the internal buffer and
// fires SetSentTap for each frame (enqueue-time stand-in for paced send).
// Test code calls Outgoing() to inspect what was sent.
//
// Since Close no longer closes the outgoing channel, this exits when done is
// signalled, but first drains any frames already buffered so writes that
// completed before Close are still collected.
//
// Run in a goroutine: go fake.CollectOutgoing()
func (f *FakeIO) CollectOutgoing() {
	for {
		select {
		case s := <-f.outgoing:
			f.fireSentTap(s)
			f.mu.Lock()
			f.outBuf = append(f.outBuf, s)
			f.mu.Unlock()
		case <-f.done:
			// Drain any frames buffered before Close, then stop.
			for {
				select {
				case s := <-f.outgoing:
					f.fireSentTap(s)
					f.mu.Lock()
					f.outBuf = append(f.outBuf, s)
					f.mu.Unlock()
				default:
					return
				}
			}
		}
	}
}

// Outgoing returns a snapshot of frames written by the consumer so far.
func (f *FakeIO) Outgoing() [][]int16 {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([][]int16, len(f.outBuf))
	copy(out, f.outBuf)
	return out
}
