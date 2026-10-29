package telecom

// Event method names broadcast by the Catalog. CC clients subscribing to
// these can keep their trunk-aware UI / alerting in sync without polling.
const (
	EventTrunkRegistered   = "telecom.trunk.registered"
	EventTrunkDeregistered = "telecom.trunk.deregistered"
	EventLeaseStale        = "telecom.lease.stale"
)

// Reason values used in telecom.trunk.deregistered events.
const (
	ReasonRemoved      = "removed"        // explicit DELETE or snapshot drop
	ReasonLeaseExpired = "lease_expired"  // sweeper hard-deleted after stale window
)

// EventSink is the minimal interface the Catalog needs to push lifecycle
// notifications to subscribed clients. The rpc.Broadcaster type already
// satisfies this via BroadcastTelecomEvent (wired in main.go).
//
// Emit is fire-and-forget; implementations must not block.
type EventSink interface {
	Emit(method string, payload map[string]any)
}

// noopSink discards events; used when no sink has been wired (tests / dev).
type noopSink struct{}

func (noopSink) Emit(string, map[string]any) {}

// SetEventSink wires the event sink. Pass nil to disable events.
func (c *Catalog) SetEventSink(sink EventSink) {
	c.mu.Lock()
	if sink == nil {
		c.events = noopSink{}
	} else {
		c.events = sink
	}
	c.mu.Unlock()
}

func (c *Catalog) emit(method string, payload map[string]any) {
	c.mu.RLock()
	sink := c.events
	c.mu.RUnlock()
	if sink == nil {
		return
	}
	sink.Emit(method, payload)
}
