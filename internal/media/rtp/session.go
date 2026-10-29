// Package rtp packages PCM payloads into RTP packets (and unpacks them back)
// for the SIP transport. We lean on github.com/pion/rtp for wire encoding;
// this package adds a per-call Session that tracks SSRC + sequence number
// + timestamp progression, plus a small jitter buffer for ingress.
//
// Designed for G.711 PCMU 8 kHz mono frames at 20 ms (160 bytes per packet).
// Higher sample rates / codecs would just pass a different PayloadType +
// SamplesPerFrame to NewSession.
package rtp

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"sync"
	"time"

	prtp "github.com/pion/rtp"
)

// PayloadTypePCMU is the static RTP payload type for G.711 μ-law per
// IANA assignments (RFC 3551 §6).
const PayloadTypePCMU uint8 = 0

// PayloadTypePCMA is G.711 A-law.
const PayloadTypePCMA uint8 = 8

const (
	defaultAudioClockRate = 8000
	talkspurtGapFrames    = 3
)

// Session tracks the outgoing RTP stream state for one call. Concurrent
// use is safe: Pack acquires an internal lock so the seq/timestamp pair
// stays consistent.
type Session struct {
	mu sync.Mutex

	payloadType uint8
	ssrc        uint32
	seq         uint16
	timestamp   uint32
	tsIncrement uint32
	clockRate   uint32
	framePeriod time.Duration

	haveAudio          bool
	lastAudioAt        time.Time
	lastAudioTimestamp uint32
}

// NewSession picks a random SSRC and starting sequence + timestamp.
// tsIncrement is the per-frame clock advance (160 for PCMU 20 ms at 8 kHz).
func NewSession(payloadType uint8, tsIncrement uint32) *Session {
	return NewSessionWithClock(payloadType, defaultAudioClockRate, tsIncrement)
}

// NewSessionWithClock creates a session with an explicit RTP sampling clock.
// The clock is needed to preserve the virtual media timeline across periods
// where silence packets are suppressed.
func NewSessionWithClock(payloadType uint8, clockRate, tsIncrement uint32) *Session {
	if clockRate == 0 {
		clockRate = defaultAudioClockRate
	}
	framePeriod := time.Duration(int64(time.Second) * int64(tsIncrement) / int64(clockRate))
	return &Session{
		payloadType: payloadType,
		ssrc:        randomUint32(),
		seq:         uint16(randomUint32() & 0xFFFF),
		timestamp:   randomUint32(),
		tsIncrement: tsIncrement,
		clockRate:   clockRate,
		framePeriod: framePeriod,
	}
}

// SSRC returns the synchronisation source identifier.
func (s *Session) SSRC() uint32 { return s.ssrc }

// Pack wraps the payload in an RTP packet, advancing seq + timestamp.
// Returns the wire-format bytes.
func (s *Session) Pack(payload []byte) ([]byte, error) {
	return s.PackAt(payload, time.Now())
}

// PackAt wraps an audio payload using sampledAt as the packet's sampling
// instant. During silence suppression no packets consume sequence numbers,
// but the RTP timestamp still follows the sampling clock. The first packet
// after a non-contiguous gap is marked as a new talkspurt so the receiver can
// re-converge its playout delay.
func (s *Session) PackAt(payload []byte, sampledAt time.Time) ([]byte, error) {
	s.mu.Lock()
	timestamp := s.timestamp
	marker := !s.haveAudio
	acceptedAt := sampledAt
	if s.haveAudio && !sampledAt.After(s.lastAudioAt) {
		acceptedAt = s.lastAudioAt
	}
	if s.haveAudio && sampledAt.After(s.lastAudioAt) {
		elapsed := sampledAt.Sub(s.lastAudioAt)
		if elapsed > talkspurtGapFrames*s.framePeriod {
			elapsedTicks := durationToClockTicks(elapsed, s.clockRate)
			if elapsedTicks < s.tsIncrement {
				elapsedTicks = s.tsIncrement
			}
			timestamp = s.lastAudioTimestamp + elapsedTicks
			marker = true
		}
	}
	pkt := prtp.Packet{
		Header: prtp.Header{
			Version:        2,
			PayloadType:    s.payloadType,
			SequenceNumber: s.seq,
			Timestamp:      timestamp,
			SSRC:           s.ssrc,
			Marker:         marker,
		},
		Payload: payload,
	}
	s.seq++
	s.timestamp = timestamp + s.tsIncrement
	s.haveAudio = true
	s.lastAudioAt = acceptedAt
	s.lastAudioTimestamp = timestamp
	s.mu.Unlock()
	return pkt.Marshal()
}

func durationToClockTicks(elapsed time.Duration, clockRate uint32) uint32 {
	seconds := elapsed / time.Second
	remainder := elapsed % time.Second
	ticks := uint64(seconds)*uint64(clockRate) +
		(uint64(remainder)*uint64(clockRate)+uint64(time.Second)/2)/uint64(time.Second)
	return uint32(ticks)
}

// PackTelephoneEvent wraps an RFC 4733 payload. Audio timestamp is not advanced.
func (s *Session) PackTelephoneEvent(payload []byte, payloadType uint8, marker bool) ([]byte, error) {
	return s.pack(payload, payloadType, marker, false)
}

func (s *Session) pack(payload []byte, payloadType uint8, marker bool, advanceTS bool) ([]byte, error) {
	s.mu.Lock()
	pkt := prtp.Packet{
		Header: prtp.Header{
			Version:        2,
			PayloadType:    payloadType,
			SequenceNumber: s.seq,
			Timestamp:      s.timestamp,
			SSRC:           s.ssrc,
			Marker:         marker,
		},
		Payload: payload,
	}
	s.seq++
	if advanceTS && payloadType == s.payloadType {
		s.timestamp += s.tsIncrement
	}
	s.mu.Unlock()
	return pkt.Marshal()
}

// Unpack decodes raw RTP wire bytes into a pion Packet. Returns an error
// if the bytes are malformed.
func Unpack(raw []byte) (*prtp.Packet, error) {
	if len(raw) < 12 {
		return nil, errors.New("rtp: packet shorter than 12-byte header")
	}
	p := &prtp.Packet{}
	if err := p.Unmarshal(raw); err != nil {
		return nil, err
	}
	return p, nil
}

func randomUint32() uint32 {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		// rand.Read on /dev/urandom should not fail in practice; fall back
		// to a constant for determinism if it does.
		return 0xDEAD0000
	}
	return binary.BigEndian.Uint32(b[:])
}
