package rtp

import (
	"sync"

	prtp "github.com/pion/rtp"
)

// JitterBuffer holds out-of-order RTP packets until either the in-sequence
// packet arrives or the buffer is shifted forward. Designed for telephony
// jitter (≤60 ms LAN, ≤200 ms WAN). For voice we tolerate dropping
// late-arrivals rather than stalling — the resulting audio gap is preferable
// to delay.
//
// Concurrency: Push is safe to call from one goroutine; Pop from another.
type JitterBuffer struct {
	cushion int // max packets buffered ahead of expected before forcing flush

	mu          sync.Mutex
	buf         map[uint16]*prtp.Packet
	expectedSeq uint16
	primed      bool
}

// NewJitterBuffer constructs a buffer with the given cushion. A cushion of
// 3 frames at 20 ms = 60 ms of reorder tolerance.
func NewJitterBuffer(cushion int) *JitterBuffer {
	if cushion < 1 {
		cushion = 1
	}
	return &JitterBuffer{
		cushion: cushion,
		buf:     make(map[uint16]*prtp.Packet),
	}
}

// Push adds an arriving packet and returns any packets that are now ready
// for delivery in sequence order (possibly the just-pushed one).
//
// Drop semantics:
//   - if seq < expectedSeq (and not a wrap), the packet is dropped silently
//     (late arrival)
//   - if len(buf) > cushion, we force-advance expectedSeq, discarding any
//     gap (lossy delivery)
//
// First call primes the buffer with whatever seq arrives.
func (j *JitterBuffer) Push(pkt *prtp.Packet) []*prtp.Packet {
	if pkt == nil {
		return nil
	}
	j.mu.Lock()
	defer j.mu.Unlock()

	if !j.primed {
		j.expectedSeq = pkt.SequenceNumber
		j.primed = true
	}

	// Late arrival: seq is in the "past" half of the 16-bit space relative
	// to expected. Use modular comparison to handle wrap-around.
	if seqLess(pkt.SequenceNumber, j.expectedSeq) {
		return nil
	}

	j.buf[pkt.SequenceNumber] = pkt

	// Force-advance if buffer is overrun.
	if len(j.buf) > j.cushion {
		j.expectedSeq = j.smallestSeqLocked()
	}

	// Drain everything in-sequence starting at expectedSeq.
	var out []*prtp.Packet
	for {
		p, ok := j.buf[j.expectedSeq]
		if !ok {
			break
		}
		delete(j.buf, j.expectedSeq)
		out = append(out, p)
		j.expectedSeq++
	}
	return out
}

// Flush returns and clears any remaining buffered packets, in sequence order.
// Useful at call teardown to drain whatever the buffer has.
func (j *JitterBuffer) Flush() []*prtp.Packet {
	j.mu.Lock()
	defer j.mu.Unlock()
	if len(j.buf) == 0 {
		return nil
	}
	// Order by seq starting from smallest known
	out := make([]*prtp.Packet, 0, len(j.buf))
	// Simple sweep — buffer is small (≤cushion)
	start := j.smallestSeqLocked()
	seq := start
	maxSweep := uint16(len(j.buf) * 2)
	for swept := uint16(0); swept < maxSweep && len(j.buf) > 0; swept++ {
		if p, ok := j.buf[seq]; ok {
			out = append(out, p)
			delete(j.buf, seq)
		}
		seq++
	}
	j.buf = make(map[uint16]*prtp.Packet)
	return out
}

// smallestSeqLocked returns the lowest sequence number in the buffer,
// using modular ordering relative to expectedSeq.
func (j *JitterBuffer) smallestSeqLocked() uint16 {
	first := true
	var min uint16
	for s := range j.buf {
		if first || seqLess(s, min) {
			min = s
			first = false
		}
	}
	return min
}

// seqLess reports whether a is "before" b in 16-bit sequence space. Uses
// the half-range trick so that 65534 < 65535 < 0 < 1 (wrap-friendly).
func seqLess(a, b uint16) bool {
	return int16(a-b) < 0
}
