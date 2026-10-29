package rtp

import (
	"errors"
	"fmt"
)

// PayloadTypeTelephoneEvent is the commonly used static PT for RFC 4733
// telephone-event when dynamically assigned PT is unavailable.
const PayloadTypeTelephoneEvent uint8 = 101

// DTMF event codes per RFC 4733 §3.2.
const (
	dtmfEvent0 = 0
	dtmfEventStar = 10
	dtmfEventHash = 11
)

// ParseDTMFEnd inspects a telephone-event RTP payload and returns the digit
// when the end bit (E) is set. Returns ("", false) for non-terminal packets
// or malformed payloads.
func ParseDTMFEnd(payload []byte) (digit string, ok bool) {
	if len(payload) < 4 {
		return "", false
	}
	event := payload[0]
	end := payload[1]&0x80 != 0
	if !end {
		return "", false
	}
	d, err := eventToDigit(event)
	if err != nil {
		return "", false
	}
	return d, true
}

// BuildDTMFPayload constructs a 4-byte RFC 4733 telephone-event payload.
func BuildDTMFPayload(event byte, end bool, volume byte, duration uint16) []byte {
	flags := volume & 0x3f
	if end {
		flags |= 0x80
	}
	return []byte{event, flags, byte(duration >> 8), byte(duration)}
}

// DigitToEvent maps a single DTMF character to its RFC 4733 event code.
func DigitToEvent(digit string) (byte, error) {
	if len(digit) != 1 {
		return 0, fmt.Errorf("dtmf: expected one character, got %q", digit)
	}
	switch digit[0] {
	case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
		return digit[0] - '0', nil
	case '*':
		return dtmfEventStar, nil
	case '#':
		return dtmfEventHash, nil
	case 'A', 'a':
		return 12, nil
	case 'B', 'b':
		return 13, nil
	case 'C', 'c':
		return 14, nil
	case 'D', 'd':
		return 15, nil
	default:
		return 0, fmt.Errorf("dtmf: invalid digit %q", digit)
	}
}

func eventToDigit(event byte) (string, error) {
	switch {
	case event <= 9:
		return string(rune('0' + event)), nil
	case event == dtmfEventStar:
		return "*", nil
	case event == dtmfEventHash:
		return "#", nil
	case event >= 12 && event <= 15:
		return string(rune('A' + (event - 12))), nil
	default:
		return "", errors.New("dtmf: unknown event code")
	}
}
