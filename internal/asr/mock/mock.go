package mock

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/asr"
)

// Step is one scripted ASR event. The mock emits the event after it has
// received AfterSamples cumulative input samples via SendPCM.
type Step struct {
	AfterSamples int64
	Type         asr.EventType
	Text         string
}

// Mock is an in-process asr.Provider for tests and local development. It
// emits a pre-configured sequence of events as PCM frames arrive.
//
// Zero value is NOT useable — use New().
type Mock struct {
	steps []Step

	events    chan asr.Event
	started   atomic.Bool
	stopped   atomic.Bool
	closeOnce sync.Once

	mu       sync.Mutex
	received int64 // total samples seen via SendPCM
	cursor   int   // next step index
	seq      int   // monotonic event sequence
}

// New returns a Mock that will replay the given steps. Steps must be sorted
// by AfterSamples ascending (the mock does not sort them itself).
func New(steps ...Step) *Mock {
	return &Mock{
		steps:  steps,
		events: make(chan asr.Event, 32),
	}
}

// Start implements asr.Provider.
func (m *Mock) Start(_ context.Context) error {
	if !m.started.CompareAndSwap(false, true) {
		return nil // idempotent
	}
	return nil
}

// SendPCM implements asr.Provider. It counts cumulative samples and emits
// any scheduled events whose AfterSamples threshold has been crossed.
func (m *Mock) SendPCM(samples []int16) error {
	if !m.started.Load() {
		return asr.ErrNotStarted
	}
	if m.stopped.Load() {
		return asr.ErrStopped
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.received += int64(len(samples))
	for m.cursor < len(m.steps) && m.steps[m.cursor].AfterSamples <= m.received {
		if m.stopped.Load() {
			return asr.ErrStopped
		}
		step := m.steps[m.cursor]
		m.seq++
		ev := asr.Event{
			Type:     step.Type,
			Text:     step.Text,
			Sequence: m.seq,
			At:       time.Now().UTC(),
		}
		m.cursor++
		// Hold mu across the send so it cannot race Stop's close. The select
		// is non-blocking (default drops when the buffer is full), so the
		// lock is never held waiting on a consumer.
		select {
		case m.events <- ev:
		default:
			// Channel buffer (32) full — silently drop. Tests shouldn't rely
			// on more events than buffer can hold without a consumer.
		}
	}
	return nil
}

// Events implements asr.Provider.
func (m *Mock) Events() <-chan asr.Event { return m.events }

// Stop implements asr.Provider.
func (m *Mock) Stop() error {
	m.closeOnce.Do(func() {
		m.stopped.Store(true)
		m.mu.Lock()
		close(m.events)
		m.mu.Unlock()
	})
	return nil
}

// SamplesReceived returns the total number of PCM samples seen so far.
// Useful for asserting that the mock was fed enough data in a test.
func (m *Mock) SamplesReceived() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.received
}
