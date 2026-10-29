package bus

import (
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"
)

// SubscriberBufferSize is the per-subscriber channel capacity. When a
// subscriber falls behind, frames overflowing this buffer are dropped for
// that subscriber only — fast subscribers stay unaffected. 10 frames at 20 ms
// each = 200 ms tolerance window.
const SubscriberBufferSize = 10

// Subscriber is a single reader of an AudioBus. Each subscriber sees every
// published frame (delivered to its channel) unless its channel buffer is
// full, in which case that particular frame is dropped for it.
type Subscriber struct {
	id     string
	ch     chan Frame
	closed atomic.Bool

	// dropped counts how many frames were dropped because the buffer was full.
	dropped atomic.Uint64
}

// ID returns the unique subscriber identifier.
func (s *Subscriber) ID() string { return s.id }

// Channel returns the read-only frame channel. It is closed when the
// subscriber is closed or the parent Bus is closed.
func (s *Subscriber) Channel() <-chan Frame { return s.ch }

// Dropped returns the total number of frames dropped due to a full buffer.
func (s *Subscriber) Dropped() uint64 { return s.dropped.Load() }

// IsClosed reports whether the subscriber is no longer receiving frames.
func (s *Subscriber) IsClosed() bool { return s.closed.Load() }

// Bus fans audio frames out to N subscribers. Publishers never block on a
// slow subscriber: a full buffer just drops the frame for that subscriber.
type Bus struct {
	log *slog.Logger

	mu          sync.RWMutex
	subscribers map[string]*Subscriber

	closed atomic.Bool
	// totalDropped accumulates every per-subscriber overflow drop for the life
	// of the bus (survives Unsubscribe; unlike summing Dropped() on the map).
	totalDropped atomic.Uint64
}

// New constructs an empty Bus.
func New(log *slog.Logger) *Bus {
	if log == nil {
		log = slog.Default()
	}
	return &Bus{
		log:         log,
		subscribers: make(map[string]*Subscriber),
	}
}

// Subscribe registers a new subscriber and returns it. Subscribers added
// AFTER frames have been published do not receive those past frames.
// Returns nil if the bus is already closed.
func (b *Bus) Subscribe() *Subscriber {
	if b.closed.Load() {
		return nil
	}
	s := &Subscriber{
		id: uuid.NewString(),
		ch: make(chan Frame, SubscriberBufferSize),
	}
	b.mu.Lock()
	b.subscribers[s.id] = s
	b.mu.Unlock()
	return s
}

// Unsubscribe removes the subscriber and closes its channel. Idempotent.
func (b *Bus) Unsubscribe(s *Subscriber) {
	if s == nil {
		return
	}
	b.mu.Lock()
	delete(b.subscribers, s.id)
	b.mu.Unlock()
	if s.closed.CompareAndSwap(false, true) {
		close(s.ch)
	}
}

// Publish fans the frame out to every current subscriber. Non-blocking:
// frames are dropped per-subscriber whose buffer is full. After Close,
// Publish is a no-op.
func (b *Bus) Publish(f Frame) {
	if b.closed.Load() {
		return
	}
	// Send under RLock so a subscriber's channel cannot be closed mid-send:
	// Unsubscribe/Close both delete from the map under the write Lock before
	// closing the channel, so every subscriber still in the map here is open.
	b.mu.RLock()
	defer b.mu.RUnlock()
	for _, s := range b.subscribers {
		select {
		case s.ch <- f:
		default:
			s.dropped.Add(1)
			b.totalDropped.Add(1)
		}
	}
}

// SubscriberCount returns the current number of subscribers.
func (b *Bus) SubscriberCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.subscribers)
}

// DroppedTotal returns frames dropped across all subscribers since the bus was
// created. Safe under concurrent Publish / Subscribe / Unsubscribe.
func (b *Bus) DroppedTotal() uint64 {
	return b.totalDropped.Load()
}

// Close marks the bus as closed and shuts every subscriber down. Subsequent
// Publish calls become no-ops.
func (b *Bus) Close() {
	if !b.closed.CompareAndSwap(false, true) {
		return
	}
	b.mu.Lock()
	subs := b.subscribers
	b.subscribers = make(map[string]*Subscriber)
	b.mu.Unlock()
	for _, s := range subs {
		if s.closed.CompareAndSwap(false, true) {
			close(s.ch)
		}
	}
}
