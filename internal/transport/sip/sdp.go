package sip

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// SDP describes the audio-only m-line shape flowkit cares about. We
// deliberately ignore extensions (video, ICE, DTLS-SRTP fingerprints,
// crypto) beyond G.711 and telephone-event for DTMF.
type SDP struct {
	Origin      string // 'o=' line value
	ConnectIP   string // 'c=IN IP4 …' address
	AudioPort   int    // 'm=audio PORT RTP/AVP …'
	PayloadList []int  // payload types listed on the m= line
	telephonePT uint8  // RFC 4733 payload type from a=rtpmap, 0 if absent
}

// ParseSDP extracts the bits we need from raw SDP text. It does NOT verify
// the SDP is well-formed beyond what's required for media negotiation.
func ParseSDP(body string) (*SDP, error) {
	if body == "" {
		return nil, errors.New("sdp: empty body")
	}
	out := &SDP{}
	lines := strings.Split(strings.ReplaceAll(body, "\r\n", "\n"), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		switch {
		case strings.HasPrefix(line, "o="):
			out.Origin = strings.TrimPrefix(line, "o=")
		case strings.HasPrefix(line, "c="):
			parts := strings.Fields(strings.TrimPrefix(line, "c="))
			if len(parts) >= 3 && parts[0] == "IN" && parts[1] == "IP4" {
				out.ConnectIP = parts[2]
			}
		case strings.HasPrefix(line, "m=audio"):
			parts := strings.Fields(strings.TrimPrefix(line, "m="))
			// expected: ["audio", port, transport, pt1, pt2, ...]
			if len(parts) < 4 || parts[0] != "audio" {
				continue
			}
			port, err := strconv.Atoi(parts[1])
			if err != nil {
				return nil, fmt.Errorf("sdp: bad audio port %q: %w", parts[1], err)
			}
			out.AudioPort = port
			for _, p := range parts[3:] {
				pt, err := strconv.Atoi(p)
				if err == nil {
					out.PayloadList = append(out.PayloadList, pt)
				}
			}
		case strings.HasPrefix(line, "a=rtpmap:"):
			// a=rtpmap:96 telephone-event/8000
			val := strings.TrimPrefix(line, "a=rtpmap:")
			fields := strings.Fields(val)
			if len(fields) < 2 {
				continue
			}
			pt, err := strconv.Atoi(fields[0])
			if err != nil {
				continue
			}
			if strings.HasPrefix(strings.ToLower(fields[1]), "telephone-event/") {
				out.telephonePT = uint8(pt)
			}
		}
	}
	if out.AudioPort == 0 {
		return nil, errors.New("sdp: no m=audio line")
	}
	if out.ConnectIP == "" {
		return nil, errors.New("sdp: no c=IN IP4 line")
	}
	return out, nil
}

// HasPCMU returns true if the offer lists payload type 0 (G.711 μ-law).
func (s *SDP) HasPCMU() bool {
	for _, pt := range s.PayloadList {
		if pt == 0 {
			return true
		}
	}
	return false
}

// HasPCMA returns true if the offer lists payload type 8 (G.711 A-law).
func (s *SDP) HasPCMA() bool {
	for _, pt := range s.PayloadList {
		if pt == 8 {
			return true
		}
	}
	return false
}

// NegotiatedG711 picks the first G.711 codec in m=audio payload order.
// CN trunks often offer PCMA-only; Linphone typically lists PCMU first.
func (s *SDP) NegotiatedG711() (payloadType int, ok bool) {
	for _, pt := range s.PayloadList {
		if pt == 0 || pt == 8 {
			return pt, true
		}
	}
	return 0, false
}

// TelephoneEventPT returns the negotiated RFC 4733 payload type, or 0.
func (s *SDP) TelephoneEventPT() uint8 {
	return s.telephonePT
}

// BuildAnswerPCMU constructs a minimal SDP answer offering PCMU only.
func BuildAnswerPCMU(ourIP string, ourPort int, sessID int64) string {
	return BuildAnswerG711(ourIP, ourPort, sessID, 0, 0)
}

// BuildAnswerG711 constructs an SDP answer for G.711 (PCMU=0 or PCMA=8).
// telephoneEventPT is included in the m= line when non-zero; if zero, PT 101
// is offered so outbound DTMF can still be sent to peers that accept it.
func BuildAnswerG711(ourIP string, ourPort int, sessID int64, payloadType int, telephoneEventPT uint8) string {
	return buildAnswerG711(ourIP, ourPort, sessID, payloadType, telephoneEventPT, false)
}

// BuildAnswerG711Hold is like BuildAnswerG711 but marks the stream sendonly
// (RFC 3264 hold) for SIP re-INVITE.
func BuildAnswerG711Hold(ourIP string, ourPort int, sessID int64, payloadType int, telephoneEventPT uint8) string {
	return buildAnswerG711(ourIP, ourPort, sessID, payloadType, telephoneEventPT, true)
}

func buildAnswerG711(ourIP string, ourPort int, sessID int64, payloadType int, telephoneEventPT uint8, hold bool) string {
	if sessID == 0 {
		sessID = 1
	}
	codec := "PCMU"
	if payloadType == 8 {
		codec = "PCMA"
	}
	dtmfPT := int(telephoneEventPT)
	if dtmfPT == 0 {
		dtmfPT = 101
	}
	direction := "sendrecv"
	if hold {
		direction = "sendonly"
	}
	return fmt.Sprintf(
		"v=0\r\n"+
			"o=flowkit %d %d IN IP4 %s\r\n"+
			"s=flowkit\r\n"+
			"c=IN IP4 %s\r\n"+
			"t=0 0\r\n"+
			"m=audio %d RTP/AVP %d %d\r\n"+
			"a=rtpmap:%d %s/8000\r\n"+
			"a=rtpmap:%d telephone-event/8000\r\n"+
			"a=fmtp:%d 0-16\r\n"+
			"a=ptime:20\r\n"+
			"a=%s\r\n",
		sessID, sessID, ourIP, ourIP, ourPort, payloadType, dtmfPT,
		payloadType, codec, dtmfPT, dtmfPT, direction,
	)
}
