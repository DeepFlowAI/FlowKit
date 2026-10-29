package session

import "fmt"

// State enumerates the lifecycle stages of a Call.
type State int8

const (
	StateIdle     State = iota // newly created, no signaling yet
	StateRinging               // inbound INVITE received OR outbound INVITE sent
	StateAnswered              // 200 OK acknowledged
	StateActive                // media flowing (RTP / WebRTC datachannel up)
	StateHeld                  // media paused (hold music or local mute of both sides)
	StateBridged               // bridged to another call (agent transfer)
	StateEnded                 // terminal; no further transitions allowed
)

func (s State) String() string {
	switch s {
	case StateIdle:
		return "idle"
	case StateRinging:
		return "ringing"
	case StateAnswered:
		return "answered"
	case StateActive:
		return "active"
	case StateHeld:
		return "held"
	case StateBridged:
		return "bridged"
	case StateEnded:
		return "ended"
	}
	return fmt.Sprintf("unknown(%d)", s)
}

// Direction identifies whether the call originated locally (outbound) or
// arrived from a peer (inbound).
type Direction int8

const (
	DirectionInbound Direction = iota
	DirectionOutbound
)

func (d Direction) String() string {
	if d == DirectionOutbound {
		return "outbound"
	}
	return "inbound"
}

// transitions maps each legal source state to the set of allowed next states.
// Ended is terminal; anything else can transition to Ended.
var transitions = map[State]map[State]struct{}{
	StateIdle: {
		StateRinging:  {},
		StateAnswered: {}, // e.g. WebRTC direct accept without ringing phase
		StateEnded:    {},
	},
	StateRinging: {
		StateAnswered: {},
		StateEnded:    {},
	},
	StateAnswered: {
		StateActive: {},
		StateEnded:  {},
	},
	StateActive: {
		StateHeld:    {},
		StateBridged: {},
		StateEnded:   {},
	},
	StateHeld: {
		StateActive: {},
		StateEnded:  {},
	},
	StateBridged: {
		StateActive: {}, // un-bridge resumes the original leg
		StateEnded:  {},
	},
	StateEnded: {}, // terminal
}

// canTransition reports whether from → to is a legal state change.
func canTransition(from, to State) bool {
	if from == to {
		return false
	}
	allowed, ok := transitions[from]
	if !ok {
		return false
	}
	_, ok = allowed[to]
	return ok
}
