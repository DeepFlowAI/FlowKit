package recording

import "errors"

var (
	errRecordingNoBus     = errors.New("recording: call ended before recording could start")
	errRecordingStopped   = errors.New("recording: already stopped")
	errRecordingNotActive = errors.New("recording: not active")
)
