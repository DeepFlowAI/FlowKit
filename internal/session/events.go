package session

import "time"

// EventType enumerates the event flavours emitted by a Call.
type EventType string

const (
	EventStateChange   EventType = "state_change"
	EventRinging       EventType = "ringing"
	EventEarlyMedia    EventType = "early_media"
	EventAnswered      EventType = "answered"
	EventSpeechStart   EventType = "speech_start"
	EventSpeechPartial EventType = "speech_partial"
	EventSpeechEnd     EventType = "speech_end"
	EventPlayProgress  EventType = "play_progress"
	EventPlayEnd       EventType = "play_end"
	EventDTMF          EventType = "dtmf"
	EventHangup        EventType = "hangup"
	EventError         EventType = "error"
)

// Event is the tagged-union flowing through Call.Subscribe(). Consumers
// inspect Type to know which fields are populated. Producers should use the
// helper constructors below — they preset the timestamp and zero out
// inapplicable fields.
type Event struct {
	Type EventType
	At   time.Time

	// state_change
	From State
	To   State

	// speech_partial / speech_end
	Text     string
	Sequence int

	// play_progress / play_end
	PlayID    string
	PlayedMS  int64
	PlayCause string // "completed" | "stopped" | "interrupted"

	// dtmf
	Digit string

	// hangup / error
	Reason     string
	DurationMS int64
	ErrMessage string
}

func now() time.Time { return time.Now().UTC() }

// NewStateChange constructs an EventStateChange.
func NewStateChange(from, to State) Event {
	return Event{Type: EventStateChange, At: now(), From: from, To: to}
}

// NewSpeechPartial constructs an EventSpeechPartial.
func NewSpeechPartial(text string, seq int) Event {
	return Event{Type: EventSpeechPartial, At: now(), Text: text, Sequence: seq}
}

// NewSpeechEnd constructs an EventSpeechEnd.
func NewSpeechEnd(text string, durationMS int64) Event {
	return Event{Type: EventSpeechEnd, At: now(), Text: text, DurationMS: durationMS}
}

// NewPlayEnd constructs an EventPlayEnd. cause ∈ {"completed","stopped","interrupted"}.
func NewPlayEnd(playID, cause string, playedMS int64) Event {
	return Event{Type: EventPlayEnd, At: now(), PlayID: playID, PlayCause: cause, PlayedMS: playedMS}
}

// NewDTMF constructs an EventDTMF.
func NewDTMF(digit string) Event {
	return Event{Type: EventDTMF, At: now(), Digit: digit}
}

// NewHangup constructs an EventHangup with the call duration.
func NewHangup(reason string, durationMS int64) Event {
	return Event{Type: EventHangup, At: now(), Reason: reason, DurationMS: durationMS}
}

// NewError constructs an EventError.
func NewError(message string) Event {
	return Event{Type: EventError, At: now(), ErrMessage: message}
}
