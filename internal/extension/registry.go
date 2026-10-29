// Package extension implements a trust-network SIP extension registrar. It
// records the Contact binding advertised by each REGISTER so that OpenDesk can
// originate a call to a registered softphone/handset by extension number
// (AOR) rather than by raw SIP URI.
//
// This is a "trust network" registrar: REGISTER is accepted without a digest
// challenge (the SIP UA continues to reply 200 OK unconditionally). Digest
// authentication is intentionally out of scope here.
package extension

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/emiago/sipgo/sip"
)

// Binding is one Address-of-Record → Contact mapping learned from REGISTER.
type Binding struct {
	AOR       string
	Contact   sip.Uri
	ExpiresAt time.Time
	LastSeen  time.Time
}

// BindingView is the read-only, string-rendered form returned by List / query
// (safe to hand to RPC callers without exposing the mutable sip.Uri).
type BindingView struct {
	Extension string    `json:"extension"`
	Contact   string    `json:"contact"`
	ExpiresAt time.Time `json:"expires_at"`
	LastSeen  time.Time `json:"last_seen"`
}

// Registry is the thread-safe set of active extension bindings.
type Registry struct {
	mu     sync.RWMutex
	now    func() time.Time
	aors   map[string]*Binding // AOR → binding
	events EventSink
}

// NewRegistry returns an empty Registry. Inject a clock for tests; nil uses
// time.Now.
func NewRegistry(clock func() time.Time) *Registry {
	if clock == nil {
		clock = time.Now
	}
	return &Registry{
		now:    clock,
		aors:   make(map[string]*Binding),
		events: noopSink{},
	}
}

// SetEventSink wires the event sink. Pass nil to disable events.
func (r *Registry) SetEventSink(sink EventSink) {
	r.mu.Lock()
	if sink == nil {
		r.events = noopSink{}
	} else {
		r.events = sink
	}
	r.mu.Unlock()
}

func (r *Registry) emit(method string, payload map[string]any) {
	r.mu.RLock()
	sink := r.events
	r.mu.RUnlock()
	if sink == nil {
		return
	}
	sink.Emit(method, payload)
}

// Register records (or refreshes) the binding for aor. A first-time
// registration emits extension.registered. A refresh with a changed contact
// emits extension.unregistered(reregister) for the old contact followed by
// extension.registered for the new one. A refresh with the same contact only
// extends the expiry and emits nothing.
func (r *Registry) Register(aor string, contact sip.Uri, expires time.Duration) {
	aor = strings.TrimSpace(aor)
	if aor == "" {
		return
	}

	type pendingEvent struct {
		method  string
		payload map[string]any
	}
	var pending []pendingEvent

	r.mu.Lock()
	now := r.now()
	newContact := contact.String()
	prev, existed := r.aors[aor]
	// A previously-stored binding that has already expired is treated as gone,
	// so re-registration after expiry emits a fresh registered event.
	prevLive := existed && now.Before(prev.ExpiresAt)

	r.aors[aor] = &Binding{
		AOR:       aor,
		Contact:   contact,
		ExpiresAt: now.Add(expires),
		LastSeen:  now,
	}

	switch {
	case !prevLive:
		pending = append(pending, pendingEvent{
			method:  EventExtensionRegistered,
			payload: map[string]any{"extension": aor, "contact": newContact},
		})
	case prev.Contact.String() != newContact:
		pending = append(pending, pendingEvent{
			method: EventExtensionUnregistered,
			payload: map[string]any{
				"extension": aor,
				"contact":   prev.Contact.String(),
				"reason":    ReasonReregister,
			},
		})
		pending = append(pending, pendingEvent{
			method:  EventExtensionRegistered,
			payload: map[string]any{"extension": aor, "contact": newContact},
		})
	}
	r.mu.Unlock()

	for _, ev := range pending {
		r.emit(ev.method, ev.payload)
	}
}

// Deregister removes the binding for aor and emits extension.unregistered with
// the given reason. No-op (no event) when aor is unknown.
func (r *Registry) Deregister(aor, reason string) {
	aor = strings.TrimSpace(aor)
	if aor == "" {
		return
	}
	r.mu.Lock()
	prev, ok := r.aors[aor]
	if ok {
		delete(r.aors, aor)
	}
	r.mu.Unlock()
	if !ok {
		return
	}
	r.emit(EventExtensionUnregistered, map[string]any{
		"extension": aor,
		"contact":   prev.Contact.String(),
		"reason":    reason,
	})
}

// Lookup returns the Contact URI for aor, or ok=false when the extension is
// unknown or its binding has expired.
func (r *Registry) Lookup(aor string) (sip.Uri, bool) {
	aor = strings.TrimSpace(aor)
	r.mu.RLock()
	defer r.mu.RUnlock()
	b, ok := r.aors[aor]
	if !ok || !r.now().Before(b.ExpiresAt) {
		return sip.Uri{}, false
	}
	return b.Contact, true
}

// LookupContact returns the string-rendered Contact URI for aor, or ok=false
// when unknown/expired. Convenience wrapper over Lookup for callers that want a
// plain SIP URI string (e.g. originate-by-extension).
func (r *Registry) LookupContact(aor string) (string, bool) {
	uri, ok := r.Lookup(aor)
	if !ok {
		return "", false
	}
	return uri.String(), true
}

// List returns a snapshot of all live (non-expired) bindings, sorted by
// extension for deterministic output.
func (r *Registry) List() []BindingView {
	r.mu.RLock()
	defer r.mu.RUnlock()
	now := r.now()
	out := make([]BindingView, 0, len(r.aors))
	for _, b := range r.aors {
		if !now.Before(b.ExpiresAt) {
			continue
		}
		out = append(out, BindingView{
			Extension: b.AOR,
			Contact:   b.Contact.String(),
			ExpiresAt: b.ExpiresAt,
			LastSeen:  b.LastSeen,
		})
	}
	sortBindings(out)
	return out
}

// RestoreResult counts how many dumped bindings were accepted or skipped.
type RestoreResult struct {
	Loaded  int
	Skipped int
}

// migrateMinLease is the minimum remaining TTL granted on Restore so a binding
// that was seconds from expiry on the old color does not vanish before the
// handset's next REGISTER after blue/green cutover.
const migrateMinLease = 120 * time.Second

// Restore installs bindings from a dump (e.g. blue/green cutover). Each loaded
// lease is extended to at least migrateMinLease, and extension.registered is
// emitted so OpenDesk (which may have raced a reconnect reconcile against the
// empty new color) flips the UI back to registered without waiting for the
// phone's next REGISTER.
func (r *Registry) Restore(views []BindingView) RestoreResult {
	var result RestoreResult
	if len(views) == 0 {
		return result
	}

	type pendingEvent struct {
		method  string
		payload map[string]any
	}
	var pending []pendingEvent

	r.mu.Lock()
	now := r.now()
	for _, v := range views {
		aor := strings.TrimSpace(v.Extension)
		contactStr := strings.TrimSpace(v.Contact)
		if aor == "" || contactStr == "" {
			result.Skipped++
			continue
		}
		if !v.ExpiresAt.After(now) {
			result.Skipped++
			continue
		}
		var contact sip.Uri
		if err := sip.ParseUri(contactStr, &contact); err != nil {
			result.Skipped++
			continue
		}
		lastSeen := v.LastSeen
		if lastSeen.IsZero() || lastSeen.After(now) {
			lastSeen = now
		}
		expiresAt := v.ExpiresAt
		if !expiresAt.After(now.Add(migrateMinLease)) {
			expiresAt = now.Add(migrateMinLease)
		}
		r.aors[aor] = &Binding{
			AOR:       aor,
			Contact:   contact,
			ExpiresAt: expiresAt,
			LastSeen:  lastSeen,
		}
		result.Loaded++
		pending = append(pending, pendingEvent{
			method:  EventExtensionRegistered,
			payload: map[string]any{"extension": aor, "contact": contact.String()},
		})
	}
	r.mu.Unlock()

	for _, ev := range pending {
		r.emit(ev.method, ev.payload)
	}
	return result
}

// RunSweeper deletes expired bindings at interval until ctx is cancelled,
// emitting extension.unregistered(expired) for each. The goroutine exits on
// ctx.Done(); the ticker is stopped on exit.
func (r *Registry) RunSweeper(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.sweepExpired()
		}
	}
}

func (r *Registry) sweepExpired() {
	type expired struct {
		aor     string
		contact string
	}
	var gone []expired

	r.mu.Lock()
	now := r.now()
	for aor, b := range r.aors {
		if !now.Before(b.ExpiresAt) {
			gone = append(gone, expired{aor: aor, contact: b.Contact.String()})
			delete(r.aors, aor)
		}
	}
	r.mu.Unlock()

	for _, e := range gone {
		r.emit(EventExtensionUnregistered, map[string]any{
			"extension": e.aor,
			"contact":   e.contact,
			"reason":    ReasonExpired,
		})
	}
}

func sortBindings(v []BindingView) {
	for i := 1; i < len(v); i++ {
		for j := i; j > 0 && v[j-1].Extension > v[j].Extension; j-- {
			v[j-1], v[j] = v[j], v[j-1]
		}
	}
}
