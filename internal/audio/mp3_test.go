package audio

import (
	_ "embed"
	"testing"

	"github.com/stretchr/testify/require"
)

// tone.mp3 is a tiny (~2 KB) 0.2s 440 Hz stereo MP3 at 44.1 kHz, generated with
// ffmpeg (libmp3lame). It exercises the real go-mp3 decode → down-mix → resample
// path end to end.
//
//go:embed testdata/tone.mp3
var toneMP3 []byte

func TestDecodeToPCM16k_MP3_DecodesToMono16k(t *testing.T) {
	out, err := DecodeToPCM16k(toneMP3)
	require.NoError(t, err)
	require.NotEmpty(t, out)

	// ~0.2s of 16 kHz mono audio ≈ 3200 samples. Allow slack for codec
	// warm-up/flush; just assert it is in a plausible range, not exact.
	require.Greater(t, len(out), 1000)
	require.Less(t, len(out), 6000)

	// A 440 Hz tone must contain non-silent samples.
	var nonZero bool
	for _, s := range out {
		if s != 0 {
			nonZero = true
			break
		}
	}
	require.True(t, nonZero, "decoded tone should not be all silence")
}

func TestDecodeMP3_EmptyData(t *testing.T) {
	_, err := decodeMP3(nil)
	require.Error(t, err)
}

func TestDecodeMP3_GarbageDoesNotPanic(t *testing.T) {
	// ID3 header followed by junk: decoder should error, not panic.
	require.NotPanics(t, func() {
		_, _ = decodeMP3([]byte("ID3\x04\x00\x00\x00\x00\x00\x00garbage payload"))
	})
	require.NotPanics(t, func() {
		_, _ = decodeMP3([]byte{0xFF, 0xFB, 0x90, 0x00, 0x11, 0x22})
	})
}

func TestDecodeToPCM16k_MP3_RoutedNotUnsupported(t *testing.T) {
	// ID3-tagged and raw-frame MP3 headers must dispatch to decodeMP3 (which
	// errors on truncated input) rather than returning ErrUnsupportedFormat.
	_, err := DecodeToPCM16k([]byte("ID3\x03\x00\x00\x00\x00\x00\x00"))
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrUnsupportedFormat)

	_, err = DecodeToPCM16k([]byte{0xFF, 0xFB, 0x90, 0x00})
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrUnsupportedFormat)
}
