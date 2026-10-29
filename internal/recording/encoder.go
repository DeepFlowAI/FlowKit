package recording

import (
	"encoding/binary"
	"sync"
	"sync/atomic"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/media/bus"
)

// StereoEncoder interleaves remote (L) and local (R) mono PCM at 16 kHz.
// Slots are 20 ms (bus.SamplesPerFrame). A slot is emitted once a later slot
// arrives (deferring the trailing edge) or on Flush.
type StereoEncoder struct {
	mu       sync.Mutex
	nextEmit int64
	highSlot int64
	slots    map[int64]*slotBuf
	issues   atomic.Bool // late slot or same-side overwrite
	// Frames discarded because their slot was already emitted, per side.
	lateRemote atomic.Uint64
	lateLocal  atomic.Uint64
}

// LateFrames reports how many remote (L) and local (R) frames arrived after
// their slot was emitted and were dropped. A side whose timeline lags shows a
// large count here while the other side records normally.
func (e *StereoEncoder) LateFrames() (remote, local uint64) {
	return e.lateRemote.Load(), e.lateLocal.Load()
}

type slotBuf struct {
	l    []int16
	r    []int16
	hasL bool
	hasR bool
}

// NewStereoEncoder constructs an encoder.
func NewStereoEncoder() *StereoEncoder {
	return &StereoEncoder{
		slots: make(map[int64]*slotBuf),
	}
}

// HadIssues reports whether any frame was dropped due to alignment problems.
func (e *StereoEncoder) HadIssues() bool {
	return e.issues.Load()
}

// SlotIndex maps a wall-clock offset to a 20 ms slot.
func SlotIndex(ts time.Duration) int64 {
	return int64(ts / bus.FrameDuration)
}

// WriteRemote ingests incoming (left) audio. samples must not be retained by the caller.
func (e *StereoEncoder) WriteRemote(ts time.Duration, samples []int16) []byte {
	return e.writeSide(SlotIndex(ts), true, samples)
}

// WriteLocal ingests outgoing (right) audio.
func (e *StereoEncoder) WriteLocal(ts time.Duration, samples []int16) []byte {
	return e.writeSide(SlotIndex(ts), false, samples)
}

func (e *StereoEncoder) writeSide(slot int64, left bool, samples []int16) []byte {
	if len(samples) == 0 {
		return nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()

	if slot < e.nextEmit {
		e.issues.Store(true)
		if left {
			e.lateRemote.Add(1)
		} else {
			e.lateLocal.Add(1)
		}
		return nil
	}
	if slot > e.highSlot {
		e.highSlot = slot
	}
	sb := e.slots[slot]
	if sb == nil {
		sb = &slotBuf{}
		e.slots[slot] = sb
	}
	if left {
		if sb.hasL {
			e.issues.Store(true)
		}
		sb.l = samples
		sb.hasL = true
	} else {
		if sb.hasR {
			e.issues.Store(true)
		}
		sb.r = samples
		sb.hasR = true
	}
	return e.drainReadyLocked()
}

// emitHoldbackSlots defers emitting a slot until it has fallen this many slots
// behind the newest arrival. The local (agent/TTS) leg travels a longer path
// (tap → buffered channel → pump) than the steady remote RTP stream, so it
// reaches the encoder later; without a holdback the remote frames advance past
// the local slot and the local audio is dropped as "late" — leaving the agent
// channel silent. ~1s of holdback lets the lagging side arrive. Recording is
// not real-time, so this added latency is harmless (Flush drains the tail).
const emitHoldbackSlots = 50

func (e *StereoEncoder) drainReadyLocked() []byte {
	var out []byte
	for e.nextEmit <= e.highSlot {
		// Emit once both sides are present (timely pairing), or once the slot is
		// far enough behind that the lagging side is no longer expected.
		if e.bothSidesLocked(e.nextEmit) || e.highSlot-e.nextEmit >= emitHoldbackSlots {
			out = append(out, e.emitNextLocked()...)
			continue
		}
		break
	}
	return out
}

func (e *StereoEncoder) bothSidesLocked(slot int64) bool {
	sb, ok := e.slots[slot]
	return ok && sb.hasL && sb.hasR
}

func (e *StereoEncoder) emitNextLocked() []byte {
	sb, ok := e.slots[e.nextEmit]
	if !ok {
		e.nextEmit++
		return e.encodeSilenceSlot()
	}
	out := e.encodeSlot(sb)
	delete(e.slots, e.nextEmit)
	e.nextEmit++
	return out
}

func (e *StereoEncoder) encodeSlot(sb *slotBuf) []byte {
	n := bus.SamplesPerFrame
	out := make([]byte, n*wavChannels*2)
	for i := 0; i < n; i++ {
		var l, r int16
		if sb.hasL && i < len(sb.l) {
			l = sb.l[i]
		}
		if sb.hasR && i < len(sb.r) {
			r = sb.r[i]
		}
		off := i * 4
		binary.LittleEndian.PutUint16(out[off:], uint16(l))
		binary.LittleEndian.PutUint16(out[off+2:], uint16(r))
	}
	return out
}

func (e *StereoEncoder) encodeSilenceSlot() []byte {
	n := bus.SamplesPerFrame
	return make([]byte, n*wavChannels*2)
}

// Flush emits all remaining slots (including partial trailing slots).
func (e *StereoEncoder) Flush() []byte {
	e.mu.Lock()
	defer e.mu.Unlock()

	for slot := range e.slots {
		if slot > e.highSlot {
			e.highSlot = slot
		}
	}
	var out []byte
	for e.nextEmit <= e.highSlot || len(e.slots) > 0 {
		if _, ok := e.slots[e.nextEmit]; ok {
			out = append(out, e.emitNextLocked()...)
			continue
		}
		if e.nextEmit <= e.highSlot {
			out = append(out, e.emitNextLocked()...)
			continue
		}
		break
	}
	return out
}
