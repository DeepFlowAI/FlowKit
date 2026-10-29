package room

import (
	"errors"
	"log/slog"
	"sync"

	"github.com/DeepFlowAI/flowkit/internal/session"
	"github.com/google/uuid"
)

// ErrRoomNotFound is returned when a room id is unknown.
var ErrRoomNotFound = errors.New("room: not found")

// Manager owns the live room map. It watches each leg's Call so a leg is
// pulled from its room when its call ends, and destroys a room once it drops
// below two legs (a room with 0 or 1 leg has nothing to mix).
type Manager struct {
	mgr *session.Manager
	log *slog.Logger

	mu    sync.RWMutex
	rooms map[string]*Room
	// legRoom maps a leg id → its room id, so a leg-end watcher can find and
	// detach the leg without scanning every room.
	legRoom map[string]string
	// onLegDetached is notified after a leg leaves a room.
	onLegDetached func(callID string)
	// onLegCallEnded is notified only when a leg's own call ends while in the
	// room (browser gone, network loss) — never for RPC-driven exits
	// (remove_leg / replace_leg / close / drain), which the caller already
	// knows about. Backends use it to release per-leg state that would
	// otherwise wait out a TTL (e.g. a supervisor monitor lock).
	onLegCallEnded func(callID string)
}

// OnLegDetached registers a callback fired once a leg has left a room, for any
// reason: removed, replaced, its call ended, or the room torn down. Whoever
// attached a per-leg resource to a room — a live transcript, say — needs a
// single place to release it, and the automatic exits have no RPC to hang off.
// The callback runs outside the manager's lock.
func (m *Manager) OnLegDetached(fn func(callID string)) {
	m.mu.Lock()
	m.onLegDetached = fn
	m.mu.Unlock()
}

// OnLegCallEnded registers a callback fired only from the leg-death watcher —
// i.e. the leg's call ended on its own while roomed. RPC-driven detaches do
// not fire it. The callback runs outside the manager's lock.
func (m *Manager) OnLegCallEnded(fn func(callID string)) {
	m.mu.Lock()
	m.onLegCallEnded = fn
	m.mu.Unlock()
}

func (m *Manager) legCallEnded(callID string) {
	m.mu.RLock()
	fn := m.onLegCallEnded
	m.mu.RUnlock()
	if fn != nil {
		fn(callID)
	}
}

func (m *Manager) legDetached(callIDs ...string) {
	if len(callIDs) == 0 {
		return
	}
	m.mu.RLock()
	fn := m.onLegDetached
	m.mu.RUnlock()
	if fn == nil {
		return
	}
	for _, id := range callIDs {
		fn(id)
	}
}

// NewManager constructs an empty room Manager. sessionMgr is used to force
// hang-ups on Close and to watch leg lifetimes.
func NewManager(sessionMgr *session.Manager, log *slog.Logger) *Manager {
	if log == nil {
		log = slog.Default()
	}
	return &Manager{
		mgr:     sessionMgr,
		log:     log,
		rooms:   make(map[string]*Room),
		legRoom: make(map[string]string),
	}
}

// Create makes a new empty room and returns it.
func (m *Manager) Create() *Room {
	id := "room-" + uuid.NewString()
	r := NewRoom(id, m.mgr, m.log)
	m.mu.Lock()
	m.rooms[id] = r
	m.mu.Unlock()
	return r
}

// Get returns the room with id, or ErrRoomNotFound.
func (m *Manager) Get(id string) (*Room, error) {
	m.mu.RLock()
	r, ok := m.rooms[id]
	m.mu.RUnlock()
	if !ok {
		return nil, ErrRoomNotFound
	}
	return r, nil
}

// AddLeg adds a call to a room and starts a watcher that detaches the leg
// when its call ends. Errors from the room (already-in-room, closed) are
// returned unchanged.
func (m *Manager) AddLeg(roomID string, call *session.Call) error {
	r, err := m.Get(roomID)
	if err != nil {
		return err
	}
	if err := r.AddLeg(call); err != nil {
		return err
	}
	m.trackLeg(r, call)
	return nil
}

// RemoveLeg detaches a leg by id and, if the room is now empty, destroys it.
func (m *Manager) RemoveLeg(roomID, callID string) error {
	r, err := m.Get(roomID)
	if err != nil {
		return err
	}
	if err := r.RemoveLeg(callID); err != nil {
		return err
	}
	m.forgetLeg(callID)
	m.legDetached(callID)
	m.destroyIfEmpty(r)
	return nil
}

// ReplaceLeg swaps oldID for newCall (takeover) and re-points the leg
// watchers accordingly.
func (m *Manager) ReplaceLeg(roomID, oldID string, newCall *session.Call) error {
	r, err := m.Get(roomID)
	if err != nil {
		return err
	}
	if err := r.ReplaceLeg(oldID, newCall); err != nil {
		return err
	}
	m.forgetLeg(oldID)
	m.trackLeg(r, newCall)
	m.legDetached(oldID)
	return nil
}

// SetMixGain forwards to the room.
func (m *Manager) SetMixGain(roomID, listenerID, speakerID string, gain float64) error {
	r, err := m.Get(roomID)
	if err != nil {
		return err
	}
	return r.SetMixGain(listenerID, speakerID, gain)
}

// Close tears down a room (whole-chain hangup) and removes it from the map.
func (m *Manager) Close(roomID, reason string) error {
	m.mu.Lock()
	r, ok := m.rooms[roomID]
	var detached []string
	if ok {
		delete(m.rooms, roomID)
		for legID, rid := range m.legRoom {
			if rid == roomID {
				delete(m.legRoom, legID)
				detached = append(detached, legID)
			}
		}
	}
	m.mu.Unlock()
	if !ok {
		return ErrRoomNotFound
	}
	r.Close(reason)
	m.legDetached(detached...)
	return nil
}

// Count returns the number of live rooms (for tests / metrics).
func (m *Manager) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.rooms)
}

// trackLeg records leg→room and spawns a watcher goroutine that detaches the
// leg when its call ends. The watcher exits on call end or room close.
func (m *Manager) trackLeg(r *Room, call *session.Call) {
	callID := call.ID()
	m.mu.Lock()
	m.legRoom[callID] = r.ID()
	m.mu.Unlock()

	go func() {
		select {
		case <-call.Done():
		case <-r.done:
			return
		}
		// Only detach if this leg still maps to this room (guards against a
		// ReplaceLeg/RemoveLeg having already moved on).
		m.mu.Lock()
		stillHere := m.legRoom[callID] == r.ID()
		if stillHere {
			delete(m.legRoom, callID)
		}
		m.mu.Unlock()
		if !stillHere {
			return
		}
		_ = r.RemoveLeg(callID)
		m.legDetached(callID)
		m.legCallEnded(callID)
		m.destroyIfEmpty(r)
	}()
}

func (m *Manager) forgetLeg(callID string) {
	m.mu.Lock()
	delete(m.legRoom, callID)
	m.mu.Unlock()
}

// destroyIfEmpty drops a room once fewer than two legs remain — there is
// nothing left to mix. Unlike an explicit room.close (whole-chain hangup),
// this does NOT hang up the surviving leg: it just detaches it and stops the
// mixing goroutines. Draining is idempotent, so a race between the leg
// watcher and an explicit RemoveLeg cannot double-destroy.
func (m *Manager) destroyIfEmpty(r *Room) {
	if r.LegCount() >= 2 {
		return
	}
	m.mu.Lock()
	_, live := m.rooms[r.ID()]
	var detached []string
	if live {
		delete(m.rooms, r.ID())
		for legID, rid := range m.legRoom {
			if rid == r.ID() {
				delete(m.legRoom, legID)
				detached = append(detached, legID)
			}
		}
	}
	m.mu.Unlock()
	if live {
		r.Drain()
		m.legDetached(detached...)
	}
}
