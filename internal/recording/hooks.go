package recording

import "time"

// RecordingStarted describes the storage object opened for one recording.
type RecordingStarted struct {
	StorageKey string
	StartedAt  time.Time
}

// RecordingCompleted describes one finalized recording object.
type RecordingCompleted struct {
	URL           string
	StorageKey    string
	StartedAt     time.Time
	EndedAt       time.Time
	DurationMS    int64
	Partial       bool
	HeaderPatched bool
}

// Hooks are optional lifecycle callbacks (events, metrics).
type Hooks struct {
	OnStarted   func(RecordingStarted)
	OnCompleted func(RecordingCompleted)
}
