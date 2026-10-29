package rtp

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDigitToEvent_RoundTrip(t *testing.T) {
	for _, d := range []string{"0", "1", "5", "9", "*", "#", "A", "D"} {
		ev, err := DigitToEvent(d)
		require.NoError(t, err)
		got, err := eventToDigit(ev)
		require.NoError(t, err)
		require.Equal(t, stringsToUpper(d), got)
	}
}

func stringsToUpper(s string) string {
	if len(s) == 1 && s[0] >= 'a' && s[0] <= 'z' {
		return string(s[0] - 32)
	}
	return s
}

func TestParseDTMFEnd_RequiresEndBit(t *testing.T) {
	payload := BuildDTMFPayload(1, false, 10, 640)
	_, ok := ParseDTMFEnd(payload)
	require.False(t, ok)

	payload = BuildDTMFPayload(1, true, 10, 640)
	digit, ok := ParseDTMFEnd(payload)
	require.True(t, ok)
	require.Equal(t, "1", digit)
}

func TestBuildDTMFPayload_EndBit(t *testing.T) {
	payload := BuildDTMFPayload(dtmfEventHash, true, 10, 800)
	require.Equal(t, byte(dtmfEventHash), payload[0])
	require.True(t, payload[1]&0x80 != 0)
	require.Equal(t, byte(10), payload[1]&0x3f)
	require.Equal(t, uint16(800), uint16(payload[2])<<8|uint16(payload[3]))
}

func TestDigitToEvent_Invalid(t *testing.T) {
	_, err := DigitToEvent("x")
	require.Error(t, err)
}
