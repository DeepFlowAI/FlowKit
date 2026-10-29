package session

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/observability"
	"github.com/google/uuid"
)

// ErrCallNotFound is returned by Get / Close when no call matches the id.
var ErrCallNotFound = errors.New("session: call not found")

// ErrShuttingDown is returned by Create once BeginShutdown has been called.
// The kernel uses it to translate to JSON-RPC code -32009.
var ErrShuttingDown = errors.New("session: shutting down")

const (
	// shutdownWatchersTimeout caps how long Shutdown waits for per-call watch
	// goroutines after hanging up. A stuck watcher must not block process exit.
	shutdownWatchersTimeout = 5 * time.Second

	// watchHangupEscapeTimeout bounds how long watch waits for Done after
	// requesting Hangup on root cancel. Hangup runs in another goroutine so a
	// wedged Hangup cannot pin the watcher (and thus Shutdown) forever.
	watchHangupEscapeTimeout = 5 * time.Second
)

// Manager owns the live Call map for a Flowkit instance. It assigns ids,
// auto-removes ended calls (via a watcher per call), and provides safe
// concurrent access.
//
// Lifecycle: NewManager creates a root context. Each Create derives a
// cancel-isolated Call context from that root (WithoutCancel — see CreateWithID).
// Shutdown Hangup-loops first (controlled), then cancels the root; watch selects
// on the root and Hangups as fallback so Done/IsEnded stay coupled.
type Manager struct {
	log *slog.Logger

	ctx    context.Context
	cancel context.CancelFunc

	mu           sync.RWMutex
	calls        map[string]*Call
	createHooks  []func(*Call)
	draining     atomic.Bool
	shutdownOnce sync.Once
	watchers     sync.WaitGroup

	// totalCreated counts every Call ever produced by this Manager — used by
	// /metrics' flowkit_calls_total counter. Increments only on success
	// (i.e. not when Create returns nil due to drain mode).
	totalCreated atomic.Int64
}

// AddCreateHook registers a callback invoked synchronously after each
// successful Create / CreateWithID. Used by optional subsystems (e.g. recording).
func (m *Manager) AddCreateHook(fn func(*Call)) {
	if fn == nil {
		return
	}
	m.mu.Lock()
	m.createHooks = append(m.createHooks, fn)
	m.mu.Unlock()
}

func (m *Manager) runCreateHooks(c *Call) {
	m.mu.RLock()
	hooks := append([]func(*Call){}, m.createHooks...)
	m.mu.RUnlock()
	for _, h := range hooks {
		h(c)
	}
}

// NewManager constructs an empty Manager with a live root context.
func NewManager(log *slog.Logger) *Manager {
	if log == nil {
		log = slog.Default()
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Manager{
		log:    log,
		ctx:    ctx,
		cancel: cancel,
		calls:  make(map[string]*Call),
	}
}

// Context returns the Manager root context. Cancelled by Shutdown (not by
// BeginShutdown alone) so graceful drain can finish active calls first.
func (m *Manager) Context() context.Context {
	return m.ctx
}

// BeginShutdown puts the manager into draining mode: Create returns nil,
// IsDraining starts reporting true. Idempotent.
// Existing calls continue to run until they hang up themselves or
// Shutdown(reason) force-closes them.
func (m *Manager) BeginShutdown() {
	m.draining.Store(true)
}

// IsDraining reports whether BeginShutdown has been called.
func (m *Manager) IsDraining() bool {
	return m.draining.Load()
}

// Create allocates a new Call with a fresh UUID and registers it. The Manager
// spawns a watcher that removes the call from its map once it reaches Ended.
//
// Returns nil if BeginShutdown / Shutdown has run.
func (m *Manager) Create(direction Direction) *Call {
	return m.CreateWithID(uuid.NewString(), direction)
}

// CreateWithID is like Create but uses the supplied id. If an entry with the
// same id already exists, it is closed and replaced (this should not happen
// in normal flow — log a warning).
//
// Returns nil if the manager is draining. The draining flag is re-checked under
// m.mu so a Create that passed the unlocked load cannot enter the map after
// BeginShutdown / Shutdown has flipped draining.
func (m *Manager) CreateWithID(id string, direction Direction) *Call {
	if m.draining.Load() {
		return nil
	}
	// WithoutCancel: parent cancel must not close Call.Done before Hangup sets
	// IsEnded. Force comes from watch selecting on Manager.Context and calling
	// Hangup — Values still inherit from the root.
	c := newCall(context.WithoutCancel(m.ctx), id, direction, m.log)

	m.mu.Lock()
	if m.draining.Load() {
		m.mu.Unlock()
		_ = c.Hangup("rejected")
		return nil
	}
	var replaced *Call
	if existing, dup := m.calls[id]; dup {
		m.log.Warn("session: replacing existing call with same id", "call_id", id)
		replaced = existing
	}
	m.calls[id] = c
	m.watchers.Add(1)
	m.mu.Unlock()

	// Hangup outside the map lock: closeEvents / hooks must not run while
	// holding m.mu (watchers and callbacks may need the same lock).
	if replaced != nil {
		_ = replaced.Hangup("replaced")
	}

	m.totalCreated.Add(1)
	observability.CallTraceCall(context.Background(), m.log, "call.created", c, "direction", direction.String())
	m.runCreateHooks(c)
	go m.watch(c)
	return c
}

// TotalCreated returns the lifetime count of successful Create calls.
// Used by /metrics' flowkit_calls_total counter.
func (m *Manager) TotalCreated() int64 {
	return m.totalCreated.Load()
}

// WaitDrain blocks until no calls are active or ctx is cancelled. Returns
// the remaining count when it returns. Used by graceful shutdown to give
// active calls a chance to finish before force-closing them.
func (m *Manager) WaitDrain(ctx context.Context) int {
	if m.Count() == 0 {
		return 0
	}
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return m.Count()
		case <-tick.C:
			if m.Count() == 0 {
				return 0
			}
		}
	}
}

// watch waits for the call to end and removes it from the map. Manager-owned
// goroutine; one per live call. Also watches the Manager root: on cancel it
// drives Hangup (no m.mu held) so shutdown force converges without breaking
// the Done/IsEnded coupling. The identity check (existing == c) ensures that
// a stale watcher cannot evict a replacement Call with the same id.
func (m *Manager) watch(c *Call) {
	defer m.watchers.Done()
	select {
	case <-c.Done():
		// Normal end (peer hangup, Close, Shutdown fast path, etc.).
	case <-m.ctx.Done():
		// Root cancelled. Hangup asynchronously: if it wedges before cancel(),
		// the escape timer still lets this watcher (and Shutdown) proceed.
		go func() { _ = c.Hangup("manager_shutdown") }()
		timer := time.NewTimer(watchHangupEscapeTimeout)
		select {
		case <-c.Done():
			timer.Stop()
		case <-timer.C:
			m.log.Warn("session: watch timed out waiting for Done after root cancel",
				"call_id", c.ID())
		}
	}
	m.mu.Lock()
	if existing, ok := m.calls[c.ID()]; ok && existing == c {
		delete(m.calls, c.ID())
	}
	m.mu.Unlock()
	m.log.Debug("session: call removed", "call_id", c.ID())
}

// Get returns the call with the given id, or ErrCallNotFound.
func (m *Manager) Get(id string) (*Call, error) {
	m.mu.RLock()
	c, ok := m.calls[id]
	m.mu.RUnlock()
	if !ok {
		return nil, ErrCallNotFound
	}
	return c, nil
}

// Close hangs up the call with the given id. Returns ErrCallNotFound if no
// such call exists. The watcher will remove the entry asynchronously once
// Hangup completes.
func (m *Manager) Close(id, reason string) error {
	c, err := m.Get(id)
	if err != nil {
		return err
	}
	return c.Hangup(reason)
}

// Count returns the number of live (non-Ended) calls. Cleaned-up entries are
// already gone, so this is an accurate count.
func (m *Manager) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.calls)
}

// List returns a snapshot of the live calls. The returned slice is safe to
// hold; the underlying Call pointers remain valid as long as not Hangup'd.
func (m *Manager) List() []*Call {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*Call, 0, len(m.calls))
	for _, c := range m.calls {
		out = append(out, c)
	}
	return out
}

// IDs returns the set of live call ids.
func (m *Manager) IDs() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]string, 0, len(m.calls))
	for id := range m.calls {
		out = append(out, id)
	}
	return out
}

// Shutdown force-ends the Manager: reject new creates, hang up every live
// call, cancel the root, and wait for watchers (bounded). Idempotent.
//
// Order:
//  1. draining — Create starts returning nil (re-checked under lock on insert).
//  2. Hangup loop (no m.mu) — controlled fast path over the List snapshot.
//  3. cancel root — fallback so watch Hangups any residue the loop missed;
//     WithoutCancel keeps Call.Done tied to Hangup/IsEnded.
//  4. Wait for watchers with a timeout so a stuck watcher cannot hang exit.
func (m *Manager) Shutdown(reason string) {
	m.shutdownOnce.Do(func() {
		m.draining.Store(true)
		for _, c := range m.List() {
			_ = c.Hangup(reason)
		}
		m.cancel()
		m.waitWatchers(shutdownWatchersTimeout)
	})
}

func (m *Manager) waitWatchers(timeout time.Duration) {
	done := make(chan struct{})
	go func() {
		m.watchers.Wait()
		close(done)
	}()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
		m.log.Warn("session: shutdown timed out waiting for watchers",
			"timeout", timeout.String(),
			"remaining_calls", m.Count())
	}
}
