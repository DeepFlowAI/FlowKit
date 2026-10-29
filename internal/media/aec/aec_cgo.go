//go:build aec

// Package aec is a per-call acoustic echo canceller (WebRTC AEC3, plus optional
// noise suppression / AGC) used to remove the agent's own played TTS from the
// mic uplink before VAD/ASR see it. The native APM is C++; only this file crosses
// into cgo. Built under the `aec` tag (Docker image ships libwebrtc-audio-processing).
package aec

/*
#cgo pkg-config: webrtc-audio-processing-1
#include "apm_shim.h"
*/
import "C"

import (
	"sync"
	"unsafe"
)

const (
	sampleRate = 16000
	// frame10ms is APM's fixed processing unit at 16 kHz mono (10 ms).
	frame10ms = sampleRate / 100 // 160 samples
)

// Available reports whether the native echo canceller is compiled in.
func Available() bool { return true }

// Options configures a Canceller. AEC3 is always enabled (it is the point of
// this package); NS/AGC are independently toggleable for A/B tuning.
type Options struct {
	NSEnabled     bool
	NSLevel       int // 0..3 → low/moderate/high/veryHigh
	AGCEnabled    bool
	StreamDelayMS int // initial render↔capture delay hint
}

// Canceller wraps one per-call APM. The kernel moves 20 ms frames while APM wants
// 10 ms, so capture/render are rebuffered into 160-sample chunks. ProcessCapture
// and ProcessRender may run on separate goroutines (capture pump vs paced send);
// a mutex guards the shared native handle and buffers.
type Canceller struct {
	mu      sync.Mutex
	h       C.apm_handle
	delayMS C.int
	capBuf  []int16
	rendBuf []int16
	out10   []int16
	closed  bool
}

// New builds a Canceller, or nil if the native APM could not be created.
func New(opt Options) *Canceller {
	h := C.apm_create(
		C.int(sampleRate), C.int(1),
		C.int(1), // AEC3 on
		b2i(opt.NSEnabled),
		C.int(opt.NSLevel),
		b2i(opt.AGCEnabled),
	)
	if h == nil {
		return nil
	}
	return &Canceller{
		h:       h,
		delayMS: C.int(opt.StreamDelayMS),
		out10:   make([]int16, frame10ms),
	}
}

// ProcessCapture removes echo from mic PCM (16 kHz int16) and returns the cleaned
// samples. Only whole 10 ms chunks are emitted; a sub-frame remainder is buffered
// for the next call (kernel frames are 320, so the remainder is normally 0).
func (c *Canceller) ProcessCapture(mic []int16) []int16 {
	if c == nil {
		return mic
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return mic
	}
	c.capBuf = append(c.capBuf, mic...)
	n := len(c.capBuf) / frame10ms
	if n == 0 {
		return nil
	}
	out := make([]int16, 0, n*frame10ms)
	for i := 0; i < n; i++ {
		in := c.capBuf[i*frame10ms : (i+1)*frame10ms]
		if c.delayMS > 0 {
			C.apm_set_stream_delay(c.h, c.delayMS)
		}
		C.apm_process_capture(c.h,
			(*C.int16_t)(unsafe.Pointer(&in[0])),
			(*C.int16_t)(unsafe.Pointer(&c.out10[0])))
		out = append(out, c.out10...)
	}
	c.capBuf = compact(c.capBuf, n*frame10ms)
	return out
}

// ProcessRender feeds played (far-end) PCM as the echo reference.
func (c *Canceller) ProcessRender(played []int16) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	c.rendBuf = append(c.rendBuf, played...)
	n := len(c.rendBuf) / frame10ms
	for i := 0; i < n; i++ {
		in := c.rendBuf[i*frame10ms : (i+1)*frame10ms]
		C.apm_process_reverse(c.h, (*C.int16_t)(unsafe.Pointer(&in[0])))
	}
	c.rendBuf = compact(c.rendBuf, n*frame10ms)
}

// Close releases the native APM. Idempotent; later Process calls pass through.
func (c *Canceller) Close() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	c.closed = true
	C.apm_destroy(c.h)
	c.h = nil
}

func b2i(b bool) C.int {
	if b {
		return 1
	}
	return 0
}

// compact drops the first `consumed` samples, copying the remainder into a fresh
// slice so the backing array cannot grow unbounded across calls.
func compact(buf []int16, consumed int) []int16 {
	rem := buf[consumed:]
	if len(rem) == 0 {
		return buf[:0]
	}
	return append([]int16(nil), rem...)
}
