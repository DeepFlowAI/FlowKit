package extension

// Event method names broadcast by the Registry. CC clients subscribing to
// these can keep their extension-aware UI (softphone presence, "call the
// agent's handset" buttons) in sync without polling.
const (
	EventExtensionRegistered   = "extension.registered"
	EventExtensionUnregistered = "extension.unregistered"
)

// Reason values used in extension.unregistered events.
const (
	ReasonExpired    = "expired"    // sweeper removed a binding past its Expires
	ReasonRemoved    = "removed"    // explicit REGISTER with Expires: 0
	ReasonReregister = "reregister" // contact changed on a fresh REGISTER
)

// EventSink is the minimal interface the Registry needs to push lifecycle
// notifications to subscribed clients. The rpc.Broadcaster type satisfies
// this via a thin wrapper wired in main.go.
//
// Emit is fire-and-forget; implementations must not block.
type EventSink interface {
	Emit(method string, payload map[string]any)
}

// noopSink discards events; used when no sink has been wired (tests / dev).
type noopSink struct{}

func (noopSink) Emit(string, map[string]any) {}
