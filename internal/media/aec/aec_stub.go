//go:build !aec

// Package aec: no-op fallback compiled when the `aec` build tag is absent (host
// dev builds, CI without libwebrtc-audio-processing). New returns nil and all
// methods are safe on a nil receiver, so callers need no build-tag awareness.
package aec

// Available reports whether the native echo canceller is compiled in.
func Available() bool { return false }

// Options mirrors the cgo build's Options so callers compile on every platform.
type Options struct {
	NSEnabled     bool
	NSLevel       int
	AGCEnabled    bool
	StreamDelayMS int
}

// Canceller is an empty placeholder in stub builds.
type Canceller struct{}

// New always returns nil without the `aec` tag.
func New(Options) *Canceller { return nil }

// ProcessCapture passes mic audio through unchanged.
func (c *Canceller) ProcessCapture(mic []int16) []int16 { return mic }

// ProcessRender is a no-op.
func (c *Canceller) ProcessRender([]int16) {}

// Close is a no-op.
func (c *Canceller) Close() {}
