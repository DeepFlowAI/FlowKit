// Package room implements multi-party audio mixing "rooms" on top of the
// per-call AudioBus / WriteOutgoing plumbing. Each leg (a session.Call) is
// captured from its AudioBus; a per-listener mixer sums the other legs'
// latest frames — with a per-pair gain — and writes the result to the
// listener via WriteOutgoing.
//
// This unlocks three-party conferencing, barge-in (强插), whisper (耳语,
// directional gain), takeover (接管, ReplaceLeg), and whole-chain hangup
// (Close). Rooms reuse the exact bus/WriteOutgoing boundaries a two-leg
// bridge uses, so the wire-level codec transcoding to/from 16 kHz PCM at the
// MediaIO edge is shared.
package room

import (
	"errors"
	"log/slog"
	"sync"

	"github.com/DeepFlowAI/flowkit/internal/session"
)

var (
	// ErrLegNotFound is returned when a leg id is not part of the room.
	ErrLegNotFound = errors.New("room: leg not found")
	// ErrLegExists is returned when adding a leg already in the room.
	ErrLegExists = errors.New("room: leg already in room")
	// ErrClosed is returned when operating on a closed room.
	ErrClosed = errors.New("room: closed")
	// ErrGainRange is returned when a mix gain is outside [0, 1].
	ErrGainRange = errors.New("room: gain out of range [0,1]")
)

// closer hangs up a leg by id when the room tears the whole chain down.
// session.Manager satisfies it (Close(id, reason)).
type closer interface {
	Close(id, reason string) error
}

// roomLeg is one participant. Its capture goroutine writes the latest frame
// into slot; its mix goroutine reads every other leg's slot each tick.
type roomLeg struct {
	callID string
	call   *session.Call
	slot   *frameSlot

	stop chan struct{} // closed to stop this leg's capture+mix goroutines
	once sync.Once
	wg   sync.WaitGroup
}

// Room is a multi-party mixing room. All public methods are safe for
// concurrent use.
type Room struct {
	id     string
	log    *slog.Logger
	closer closer

	mu    sync.RWMutex
	legs  map[string]*roomLeg
	order []string
	// mix[listener][speaker] is the linear gain (0..1) applied to speaker's
	// audio in listener's downlink. A leg never hears itself.
	mix map[string]map[string]float64

	done      chan struct{}
	closeOnce sync.Once
}

// NewRoom constructs an empty room. closer force-hangs-up legs on Close.
func NewRoom(id string, closer closer, log *slog.Logger) *Room {
	if log == nil {
		log = slog.Default()
	}
	return &Room{
		id:     id,
		log:    log,
		closer: closer,
		legs:   make(map[string]*roomLeg),
		mix:    make(map[string]map[string]float64),
		done:   make(chan struct{}),
	}
}

// ID returns the room identifier.
func (r *Room) ID() string { return r.id }

// AddLeg adds a call to the room. The leg starts fully mutually audible with
// every existing leg (gain 1.0 in both directions). Returns ErrLegExists if
// the call is already in the room, ErrClosed if the room is closed.
func (r *Room) AddLeg(call *session.Call) error {
	if call == nil {
		return errors.New("room: nil call")
	}
	id := call.ID()

	r.mu.Lock()
	if r.isClosedLocked() {
		r.mu.Unlock()
		return ErrClosed
	}
	if _, dup := r.legs[id]; dup {
		r.mu.Unlock()
		return ErrLegExists
	}

	leg := &roomLeg{
		callID: id,
		call:   call,
		slot:   newFrameSlot(),
		stop:   make(chan struct{}),
	}
	r.legs[id] = leg
	r.order = append(r.order, id)

	// Default mix relations: 1.0 both ways with every existing leg.
	r.mix[id] = make(map[string]float64)
	for _, other := range r.order {
		if other == id {
			continue
		}
		r.mix[id][other] = 1.0
		r.mix[other][id] = 1.0
	}
	r.mu.Unlock()

	r.startLeg(leg)
	r.log.Debug("room leg added", "room_id", r.id, "call_id", id)
	return nil
}

// RemoveLeg stops and removes a leg. Its matrix row/column are deleted so no
// other leg keeps mixing it. Idempotent for absent legs. The room is NOT
// auto-closed here — the manager decides destruction policy (see Manager).
func (r *Room) RemoveLeg(callID string) error {
	r.mu.Lock()
	leg, ok := r.legs[callID]
	if !ok {
		r.mu.Unlock()
		return ErrLegNotFound
	}
	r.removeLegLocked(callID)
	r.mu.Unlock()

	r.stopLeg(leg)
	r.log.Debug("room leg removed", "room_id", r.id, "call_id", callID)
	return nil
}

// removeLegLocked deletes a leg's map entry, order entry, and matrix row +
// every other leg's column reference to it. Caller holds r.mu.
func (r *Room) removeLegLocked(callID string) {
	delete(r.legs, callID)
	for i, id := range r.order {
		if id == callID {
			r.order = append(r.order[:i], r.order[i+1:]...)
			break
		}
	}
	delete(r.mix, callID)
	for _, row := range r.mix {
		delete(row, callID)
	}
}

// ReplaceLeg atomically swaps oldID out for newCall, inheriting the old leg's
// mix relationships (so a supervisor takeover keeps the customer's audio path
// intact and uninterrupted). The new leg reuses the old leg's gain row/column.
func (r *Room) ReplaceLeg(oldID string, newCall *session.Call) error {
	if newCall == nil {
		return errors.New("room: nil call")
	}
	newID := newCall.ID()

	r.mu.Lock()
	if r.isClosedLocked() {
		r.mu.Unlock()
		return ErrClosed
	}
	oldLeg, ok := r.legs[oldID]
	if !ok {
		r.mu.Unlock()
		return ErrLegNotFound
	}
	if newID == oldID {
		r.mu.Unlock()
		return ErrLegExists
	}
	if _, dup := r.legs[newID]; dup {
		r.mu.Unlock()
		return ErrLegExists
	}

	// Inherit gains: rename oldID → newID in the matrix (row and every column).
	inRow := r.mix[oldID]
	delete(r.mix, oldID)
	newRow := make(map[string]float64, len(inRow))
	for spk, g := range inRow {
		if spk == oldID {
			continue
		}
		newRow[spk] = g
	}
	r.mix[newID] = newRow
	for listener, row := range r.mix {
		if listener == newID {
			continue
		}
		if g, has := row[oldID]; has {
			delete(row, oldID)
			row[newID] = g
		}
	}

	// Swap the leg entry + order slot.
	newLeg := &roomLeg{
		callID: newID,
		call:   newCall,
		slot:   newFrameSlot(),
		stop:   make(chan struct{}),
	}
	delete(r.legs, oldID)
	r.legs[newID] = newLeg
	for i, id := range r.order {
		if id == oldID {
			r.order[i] = newID
			break
		}
	}
	r.mu.Unlock()

	// Stop the old leg's goroutines (its downlink goes silent), start the new.
	r.stopLeg(oldLeg)
	r.startLeg(newLeg)
	r.log.Debug("room leg replaced", "room_id", r.id, "old", oldID, "new", newID)
	return nil
}

// SetMixGain sets the directional gain speaker→listener (whisper = target one
// listener at 1.0, mute for others at 0.0). gain must be in [0, 1]. Both legs
// must be present.
func (r *Room) SetMixGain(listenerID, speakerID string, gain float64) error {
	if gain < 0 || gain > 1 {
		return ErrGainRange
	}
	if listenerID == speakerID {
		return errors.New("room: a leg cannot hear itself")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.legs[listenerID]; !ok {
		return ErrLegNotFound
	}
	if _, ok := r.legs[speakerID]; !ok {
		return ErrLegNotFound
	}
	r.mix[listenerID][speakerID] = gain
	return nil
}

// Legs returns a snapshot of the current leg ids.
func (r *Room) Legs() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, len(r.order))
	copy(out, r.order)
	return out
}

// LegCount returns the number of legs currently in the room.
func (r *Room) LegCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.legs)
}

// Close tears the whole chain down: signals all goroutines to exit, force
// hangs-up every leg via the closer, and waits for goroutines to finish.
// Idempotent.
func (r *Room) Close(reason string) {
	r.closeOnce.Do(func() {
		close(r.done)

		r.mu.Lock()
		legs := make([]*roomLeg, 0, len(r.legs))
		for _, leg := range r.legs {
			legs = append(legs, leg)
		}
		r.legs = make(map[string]*roomLeg)
		r.order = nil
		r.mix = make(map[string]map[string]float64)
		r.mu.Unlock()

		// Stop and drain every mixing goroutine FIRST, so no goroutine calls
		// WriteOutgoing on a leg whose MediaIO the subsequent hangup closes.
		for _, leg := range legs {
			leg.once.Do(func() { close(leg.stop) })
		}
		for _, leg := range legs {
			leg.wg.Wait()
		}
		if r.closer != nil {
			for _, leg := range legs {
				_ = r.closer.Close(leg.callID, reason)
			}
		}
		r.log.Debug("room closed", "room_id", r.id, "reason", reason, "legs", len(legs))
	})
}

// Drain stops all mixing goroutines and clears room state WITHOUT hanging up
// any leg. Used when a room auto-destroys (drops below two legs): the
// surviving leg's call keeps running, it just stops being mixed. Idempotent.
func (r *Room) Drain() {
	r.closeOnce.Do(func() {
		close(r.done)

		r.mu.Lock()
		legs := make([]*roomLeg, 0, len(r.legs))
		for _, leg := range r.legs {
			legs = append(legs, leg)
		}
		r.legs = make(map[string]*roomLeg)
		r.order = nil
		r.mix = make(map[string]map[string]float64)
		r.mu.Unlock()

		for _, leg := range legs {
			leg.once.Do(func() { close(leg.stop) })
		}
		for _, leg := range legs {
			leg.wg.Wait()
		}
		r.log.Debug("room drained", "room_id", r.id, "legs", len(legs))
	})
}

func (r *Room) isClosedLocked() bool {
	select {
	case <-r.done:
		return true
	default:
		return false
	}
}

// stopLeg signals a single leg's goroutines to exit and waits for them. Safe
// to call outside r.mu (and required to be, since it blocks on wg.Wait).
func (r *Room) stopLeg(leg *roomLeg) {
	leg.once.Do(func() { close(leg.stop) })
	leg.wg.Wait()
}
