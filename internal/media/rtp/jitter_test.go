package rtp

import (
	"testing"

	prtp "github.com/pion/rtp"
	"github.com/stretchr/testify/require"
)

func pkt(seq uint16) *prtp.Packet {
	return &prtp.Packet{Header: prtp.Header{SequenceNumber: seq, Timestamp: uint32(seq) * 160}}
}

func seqs(packets []*prtp.Packet) []uint16 {
	out := make([]uint16, len(packets))
	for i, p := range packets {
		out[i] = p.SequenceNumber
	}
	return out
}

func TestJitter_InOrderPassesThrough(t *testing.T) {
	j := NewJitterBuffer(3)

	require.Equal(t, []uint16{100}, seqs(j.Push(pkt(100))))
	require.Equal(t, []uint16{101}, seqs(j.Push(pkt(101))))
	require.Equal(t, []uint16{102}, seqs(j.Push(pkt(102))))
}

func TestJitter_ReorderedDeliversInSequence(t *testing.T) {
	j := NewJitterBuffer(3)

	require.Equal(t, []uint16{100}, seqs(j.Push(pkt(100))))

	// Arrive: 102 (out of order)
	require.Empty(t, j.Push(pkt(102)), "102 should buffer until 101 arrives")

	// Arrive: 101 — should release 101 then 102
	out := j.Push(pkt(101))
	require.Equal(t, []uint16{101, 102}, seqs(out))
}

func TestJitter_LateArrivalDropped(t *testing.T) {
	j := NewJitterBuffer(3)
	require.Equal(t, []uint16{100}, seqs(j.Push(pkt(100))))
	require.Equal(t, []uint16{101}, seqs(j.Push(pkt(101))))

	// 99 is "earlier" than expected (102) — drop
	require.Empty(t, j.Push(pkt(99)))
}

func TestJitter_OverflowForceAdvances(t *testing.T) {
	j := NewJitterBuffer(2) // small cushion to trigger force-advance easily
	// Prime
	require.Equal(t, []uint16{100}, seqs(j.Push(pkt(100))))

	// 101 is missing; 102, 103, 104 arrive
	require.Empty(t, j.Push(pkt(102)))
	require.Empty(t, j.Push(pkt(103)))
	// 4th pending (cushion=2 exceeded) → force-advance to smallest = 102
	out := j.Push(pkt(104))
	require.Contains(t, seqs(out), uint16(102), "should deliver 102 after force-advance")
}

func TestJitter_WrapAround(t *testing.T) {
	j := NewJitterBuffer(4)
	// Start near wrap point
	require.Equal(t, []uint16{65534}, seqs(j.Push(pkt(65534))))
	require.Equal(t, []uint16{65535}, seqs(j.Push(pkt(65535))))
	// 1 arrives before 0 — should buffer
	require.Empty(t, j.Push(pkt(1)))
	// 0 arrives — should deliver 0 then 1
	out := j.Push(pkt(0))
	require.Equal(t, []uint16{0, 1}, seqs(out))
}

func TestJitter_FlushReturnsAll(t *testing.T) {
	j := NewJitterBuffer(5)
	require.Equal(t, []uint16{100}, seqs(j.Push(pkt(100))))

	// Buffer 3 with a gap (101 missing)
	j.Push(pkt(102))
	j.Push(pkt(103))
	j.Push(pkt(104))

	out := j.Flush()
	got := seqs(out)
	require.Len(t, got, 3)
	require.Contains(t, got, uint16(102))
	require.Contains(t, got, uint16(103))
	require.Contains(t, got, uint16(104))
}

func TestJitter_NilPushIsSafe(t *testing.T) {
	j := NewJitterBuffer(3)
	require.Nil(t, j.Push(nil))
}

func TestSeqLess(t *testing.T) {
	require.True(t, seqLess(99, 100))
	require.False(t, seqLess(100, 99))
	require.False(t, seqLess(100, 100))
	// wrap: 65535 < 0
	require.True(t, seqLess(65535, 0))
	require.False(t, seqLess(0, 65535))
}
