package calls

import "errors"

// ErrPlaybackInterrupted is returned by SaySync when playback was stopped
// early (barge-in or call.stop).
var ErrPlaybackInterrupted = errors.New("calls: playback interrupted")
