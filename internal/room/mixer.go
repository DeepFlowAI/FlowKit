package room

import (
	"sync"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/media/bus"
	"github.com/DeepFlowAI/flowkit/internal/session"
)

// frameSlot holds a small, bounded window of captured frames for one leg.
// WebRTC audio can arrive in short bursts, so retaining a few frames lets each
// listener consume them at the mixer's steady 20 ms cadence without dropping
// every other frame. The bounded window still caps latency when a listener
// falls behind.
//
// Every listener in the room reads the same speaker's slot, so reads must not
// consume: with one frame arriving per mixer tick, a listener that took the
// frame away would leave the others with nothing to mix. Freshness is tracked
// by sequence number instead — a speaker who has gone quiet stops advancing
// it, and their listeners contribute silence rather than repeat the last frame.
type frameSlot struct {
	mu     sync.Mutex
	frames [][]int16
	seq    uint64
}

const frameSlotCapacity = 4

func newFrameSlot() *frameSlot { return &frameSlot{} }

// set stores a copy of the latest frame under a new sequence number. The bus
// publishes slices by reference, so we copy to avoid retaining/mutating a
// shared buffer.
func (s *frameSlot) set(samples []int16) {
	cp := make([]int16, len(samples))
	copy(cp, samples)
	s.mu.Lock()
	s.seq++
	s.frames = append(s.frames, cp)
	if len(s.frames) > frameSlotCapacity {
		s.frames = s.frames[len(s.frames)-frameSlotCapacity:]
	}
	s.mu.Unlock()
}

// read returns the next retained frame after seen, plus its sequence number.
// Each listener keeps its own seen value, so all listeners can consume the
// same burst independently. If a listener falls outside the bounded window,
// it resumes from the oldest retained frame.
func (s *frameSlot) read(seen uint64) ([]int16, uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.frames) == 0 || seen >= s.seq {
		return nil, seen
	}
	oldestSeq := s.seq - uint64(len(s.frames)) + 1
	nextSeq := seen + 1
	if nextSeq < oldestSeq {
		nextSeq = oldestSeq
	}
	return s.frames[nextSeq-oldestSeq], nextSeq
}

// startLeg launches this leg's capture goroutine (bus → slot) and its mix
// goroutine (other slots → this leg's downlink). Both exit on the leg's stop
// channel, the room's done channel, or the call ending.
func (r *Room) startLeg(leg *roomLeg) {
	leg.wg.Add(2)
	go r.runCapture(leg)
	go r.runMix(leg)
}

// runCapture subscribes to the leg's AudioBus and writes each incoming frame
// into the leg's latest-frame slot.
func (r *Room) runCapture(leg *roomLeg) {
	defer leg.wg.Done()
	sub := leg.call.AudioBus().Subscribe()
	if sub == nil {
		return
	}
	defer leg.call.AudioBus().Unsubscribe(sub)

	done := leg.call.Done()
	for {
		select {
		case <-leg.stop:
			return
		case <-r.done:
			return
		case <-done:
			return
		case frame, ok := <-sub.Channel():
			if !ok {
				return
			}
			leg.slot.set(frame.Samples)
		}
	}
}

// runMix drives this leg's downlink on a 20 ms ticker: every tick it sums the
// other legs' latest frames (scaled by mix gain) and writes the result to the
// leg via WriteOutgoing. Ticker-driven output keeps a steady 20 ms cadence
// regardless of per-leg jitter, and a missing frame contributes silence.
func (r *Room) runMix(leg *roomLeg) {
	defer leg.wg.Done()
	ticker := time.NewTicker(bus.FrameDuration)
	defer ticker.Stop()

	done := leg.call.Done()
	// accum is a reusable Q15 accumulator (one extra headroom bit per source
	// is absorbed by soft-limiting below). The output frame, in contrast, must
	// be freshly allocated each tick: WriteOutgoing publishes the slice by
	// reference to the transport, so reusing one buffer would let a later tick
	// overwrite a frame still in flight.
	accum := make([]int32, bus.SamplesPerFrame)
	// Last sequence number mixed per speaker. Private to this listener, so one
	// listener's progress never affects what another still has left to mix.
	seen := make(map[string]uint64)

	for {
		select {
		case <-leg.stop:
			return
		case <-r.done:
			return
		case <-done:
			return
		case <-ticker.C:
			out := r.mixInto(leg, accum, seen)
			if out == nil {
				continue
			}
			if leg.call.IsEnded() {
				return
			}
			if err := leg.call.WriteOutgoing(out); err != nil {
				if err == session.ErrEnded {
					return
				}
				// Transient (no MediaIO yet, buffer): drop this tick, keep going.
			}
		}
	}
}

// mixInto sums every other leg's latest frame into accum (scaled by gain),
// soft-limits, and returns a freshly allocated int16 frame. Returns nil when
// there is nothing to mix. Caller reuses the accum buffer only.
func (r *Room) mixInto(leg *roomLeg, accum []int32, seen map[string]uint64) []int16 {
	// Snapshot the speakers and gains for this listener under the lock, then
	// read their slots without holding r.mu (slots have their own mutex).
	r.mu.RLock()
	row := r.mix[leg.callID]
	type src struct {
		id   string
		slot *frameSlot
		gain float64
	}
	srcs := make([]src, 0, len(row))
	for spkID, gain := range row {
		if gain <= 0 {
			continue
		}
		if spk, ok := r.legs[spkID]; ok {
			srcs = append(srcs, src{id: spkID, slot: spk.slot, gain: gain})
		}
	}
	r.mu.RUnlock()

	for i := range accum {
		accum[i] = 0
	}
	contributed := false
	for _, s := range srcs {
		frame, seq := s.slot.read(seen[s.id])
		seen[s.id] = seq
		if len(frame) == 0 {
			continue
		}
		contributed = true
		// Q15 gain: g15 = round(gain * 32768). sample*g15 >> 15 ≈ sample*gain.
		g15 := int32(s.gain*32768 + 0.5)
		limit := len(frame)
		if limit > len(accum) {
			limit = len(accum)
		}
		for i := 0; i < limit; i++ {
			accum[i] += (int32(frame[i]) * g15) >> 15
		}
	}
	if !contributed {
		return nil
	}
	out := make([]int16, len(accum))
	for i := range out {
		out[i] = softLimit(accum[i])
	}
	return out
}

// softLimit clamps a summed sample to int16 range. Hard clipping at the
// int16 bounds is the conventional mixer behavior; with per-leg gains ≤ 1 and
// few simultaneous talkers, sums rarely saturate, and clamping avoids the
// wrap-around distortion an overflowing int16 cast would produce.
func softLimit(v int32) int16 {
	if v > 32767 {
		return 32767
	}
	if v < -32768 {
		return -32768
	}
	return int16(v)
}
