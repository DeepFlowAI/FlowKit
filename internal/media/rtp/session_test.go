package rtp

import (
	"math"
	"testing"
	"time"

	prtp "github.com/pion/rtp"
	"github.com/stretchr/testify/require"
)

func TestSession_PackAt_ResumesTalkspurtOnWallClockTimeline(t *testing.T) {
	s := NewSession(PayloadTypePCMU, 160)
	start := time.Unix(1_700_000_000, 0)

	raw1, err := s.PackAt(make([]byte, 160), start)
	require.NoError(t, err)
	raw2, err := s.PackAt(make([]byte, 160), start.Add(20*time.Millisecond))
	require.NoError(t, err)
	raw3, err := s.PackAt(make([]byte, 160), start.Add(4020*time.Millisecond))
	require.NoError(t, err)

	var p1, p2, p3 prtp.Packet
	require.NoError(t, p1.Unmarshal(raw1))
	require.NoError(t, p2.Unmarshal(raw2))
	require.NoError(t, p3.Unmarshal(raw3))

	require.True(t, p1.Marker, "the first packet starts a talkspurt")
	require.False(t, p2.Marker, "contiguous audio stays in the same talkspurt")
	require.True(t, p3.Marker, "audio after silence starts a new talkspurt")
	require.Equal(t, p2.SequenceNumber+1, p3.SequenceNumber,
		"silence suppression does not consume RTP sequence numbers")
	require.Equal(t, p2.Timestamp+uint32(4*time.Second*8000/time.Second), p3.Timestamp,
		"the RTP sampling clock must advance while silence packets are suppressed")
}

func TestSession_PackAt_IgnoresShortSchedulingJitter(t *testing.T) {
	s := NewSession(PayloadTypePCMU, 160)
	start := time.Unix(1_700_000_000, 0)

	raw1, err := s.PackAt(make([]byte, 160), start)
	require.NoError(t, err)
	raw2, err := s.PackAt(make([]byte, 160), start.Add(40*time.Millisecond))
	require.NoError(t, err)
	raw3, err := s.PackAt(make([]byte, 160), start.Add(120*time.Millisecond))
	require.NoError(t, err)

	var p1, p2, p3 prtp.Packet
	require.NoError(t, p1.Unmarshal(raw1))
	require.NoError(t, p2.Unmarshal(raw2))
	require.NoError(t, p3.Unmarshal(raw3))

	require.False(t, p2.Marker, "ordinary scheduling jitter stays in the current talkspurt")
	require.Equal(t, p1.Timestamp+160, p2.Timestamp,
		"ordinary scheduling jitter keeps the nominal packet clock")
	require.True(t, p3.Marker, "a gap beyond the jitter allowance starts a new talkspurt")
	require.Equal(t, p2.Timestamp+uint32(80*time.Millisecond*8000/time.Second), p3.Timestamp)
}

func TestSession_PackAt_DoesNotRegressSamplingClock(t *testing.T) {
	s := NewSession(PayloadTypePCMU, 160)
	start := time.Unix(1_700_000_000, 0)

	raw1, err := s.PackAt(make([]byte, 160), start)
	require.NoError(t, err)
	raw2, err := s.PackAt(make([]byte, 160), start.Add(-time.Second))
	require.NoError(t, err)
	raw3, err := s.PackAt(make([]byte, 160), start.Add(20*time.Millisecond))
	require.NoError(t, err)

	var p1, p2, p3 prtp.Packet
	require.NoError(t, p1.Unmarshal(raw1))
	require.NoError(t, p2.Unmarshal(raw2))
	require.NoError(t, p3.Unmarshal(raw3))

	require.Equal(t, p1.Timestamp+160, p2.Timestamp)
	require.Equal(t, p2.Timestamp+160, p3.Timestamp,
		"an out-of-order wall-clock sample must not create a false silent gap")
	require.False(t, p2.Marker)
	require.False(t, p3.Marker)
}

func TestSession_PackAt_TimestampWrapsOnTalkspurtResume(t *testing.T) {
	s := NewSession(PayloadTypePCMU, 160)
	s.timestamp = math.MaxUint32 - 100
	start := time.Unix(1_700_000_000, 0)

	raw1, err := s.PackAt(make([]byte, 160), start)
	require.NoError(t, err)
	raw2, err := s.PackAt(make([]byte, 160), start.Add(4*time.Second))
	require.NoError(t, err)

	var p1, p2 prtp.Packet
	require.NoError(t, p1.Unmarshal(raw1))
	require.NoError(t, p2.Unmarshal(raw2))

	require.True(t, p2.Marker)
	require.Equal(t, p1.Timestamp+uint32(4*time.Second*8000/time.Second), p2.Timestamp,
		"RTP timestamps use normal uint32 wraparound")
}

func TestSession_TelephoneEventDoesNotAdvanceAudioClock(t *testing.T) {
	s := NewSession(PayloadTypePCMU, 160)
	start := time.Unix(1_700_000_000, 0)

	raw1, err := s.PackAt(make([]byte, 160), start)
	require.NoError(t, err)
	eventRaw, err := s.PackTelephoneEvent(BuildDTMFPayload(1, false, 10, 160), 96, true)
	require.NoError(t, err)
	raw2, err := s.PackAt(make([]byte, 160), start.Add(20*time.Millisecond))
	require.NoError(t, err)

	var p1, event, p2 prtp.Packet
	require.NoError(t, p1.Unmarshal(raw1))
	require.NoError(t, event.Unmarshal(eventRaw))
	require.NoError(t, p2.Unmarshal(raw2))

	require.Equal(t, p1.SequenceNumber+1, event.SequenceNumber)
	require.Equal(t, event.SequenceNumber+1, p2.SequenceNumber)
	require.Equal(t, p1.Timestamp+160, event.Timestamp)
	require.Equal(t, p1.Timestamp+160, p2.Timestamp)
	require.False(t, p2.Marker)
}

func TestSession_Pack_IncrementsSeqAndTimestamp(t *testing.T) {
	s := NewSession(PayloadTypePCMU, 160)

	raw1, err := s.Pack(make([]byte, 160))
	require.NoError(t, err)
	raw2, err := s.Pack(make([]byte, 160))
	require.NoError(t, err)
	raw3, err := s.Pack(make([]byte, 160))
	require.NoError(t, err)

	var p1, p2, p3 prtp.Packet
	require.NoError(t, p1.Unmarshal(raw1))
	require.NoError(t, p2.Unmarshal(raw2))
	require.NoError(t, p3.Unmarshal(raw3))

	// SSRC stable
	require.Equal(t, p1.SSRC, p2.SSRC)
	require.Equal(t, p2.SSRC, p3.SSRC)

	// Sequence increments by 1
	require.Equal(t, p1.SequenceNumber+1, p2.SequenceNumber)
	require.Equal(t, p2.SequenceNumber+1, p3.SequenceNumber)

	// Timestamp increments by 160 (per 20 ms @ 8 kHz)
	require.Equal(t, p1.Timestamp+160, p2.Timestamp)
	require.Equal(t, p2.Timestamp+160, p3.Timestamp)

	// Marker only on first packet
	require.True(t, p1.Marker)
	require.False(t, p2.Marker)
	require.False(t, p3.Marker)

	// Payload type
	require.Equal(t, PayloadTypePCMU, p1.PayloadType)
}

func TestSession_Pack_PayloadPreserved(t *testing.T) {
	s := NewSession(PayloadTypePCMU, 160)
	payload := []byte{0x01, 0x02, 0x03, 0xFF, 0xAA}
	raw, err := s.Pack(payload)
	require.NoError(t, err)

	var p prtp.Packet
	require.NoError(t, p.Unmarshal(raw))
	require.Equal(t, payload, []byte(p.Payload))
}

func TestUnpack_RejectsShortBuffer(t *testing.T) {
	_, err := Unpack(make([]byte, 5))
	require.Error(t, err)
}

func TestUnpack_DecodesValid(t *testing.T) {
	s := NewSession(PayloadTypePCMU, 160)
	raw, err := s.Pack([]byte{1, 2, 3})
	require.NoError(t, err)

	p, err := Unpack(raw)
	require.NoError(t, err)
	require.Equal(t, s.SSRC(), p.SSRC)
	require.Equal(t, []byte{1, 2, 3}, []byte(p.Payload))
}

func TestSession_SeqWrap(t *testing.T) {
	s := NewSession(PayloadTypePCMU, 160)
	// Force seq near wrap point
	s.seq = 65534
	raw1, _ := s.Pack(nil)
	raw2, _ := s.Pack(nil)
	raw3, _ := s.Pack(nil)

	var p1, p2, p3 prtp.Packet
	_ = p1.Unmarshal(raw1)
	_ = p2.Unmarshal(raw2)
	_ = p3.Unmarshal(raw3)

	require.Equal(t, uint16(65534), p1.SequenceNumber)
	require.Equal(t, uint16(65535), p2.SequenceNumber)
	require.Equal(t, uint16(0), p3.SequenceNumber, "sequence must wrap to 0")
}

func TestSession_SSRC_NonZero(t *testing.T) {
	for i := 0; i < 100; i++ {
		s := NewSession(PayloadTypePCMU, 160)
		require.NotEqual(t, uint32(0), s.SSRC(), "ssrc should not be zero")
	}
}
