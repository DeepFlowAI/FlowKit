package audio

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/DeepFlowAI/flowkit/internal/media/bus"
	"github.com/stretchr/testify/require"
)

// buildWAV constructs a minimal PCM16 WAV for the given samples, channel count
// and sample rate. samples are interleaved when channels == 2.
func buildWAV(t *testing.T, samples []int16, channels, sampleRate int) []byte {
	t.Helper()
	var buf bytes.Buffer
	dataBytes := len(samples) * 2
	byteRate := sampleRate * channels * 2
	blockAlign := channels * 2

	buf.WriteString("RIFF")
	_ = binary.Write(&buf, binary.LittleEndian, uint32(36+dataBytes))
	buf.WriteString("WAVE")

	buf.WriteString("fmt ")
	_ = binary.Write(&buf, binary.LittleEndian, uint32(16))
	_ = binary.Write(&buf, binary.LittleEndian, uint16(1)) // PCM
	_ = binary.Write(&buf, binary.LittleEndian, uint16(channels))
	_ = binary.Write(&buf, binary.LittleEndian, uint32(sampleRate))
	_ = binary.Write(&buf, binary.LittleEndian, uint32(byteRate))
	_ = binary.Write(&buf, binary.LittleEndian, uint16(blockAlign))
	_ = binary.Write(&buf, binary.LittleEndian, uint16(16)) // bits/sample

	buf.WriteString("data")
	_ = binary.Write(&buf, binary.LittleEndian, uint32(dataBytes))
	for _, s := range samples {
		_ = binary.Write(&buf, binary.LittleEndian, s)
	}
	return buf.Bytes()
}

func TestDecodeWAV_Mono16k_PassesThrough(t *testing.T) {
	in := []int16{100, -100, 200, -200, 300}
	wav := buildWAV(t, in, 1, bus.SampleRate)

	out, err := DecodeToPCM16k(wav)
	require.NoError(t, err)
	require.Equal(t, in, out)
}

func TestDecodeWAV_StereoDownmix(t *testing.T) {
	// Interleaved L/R: (10,20),(30,50) → mono averages (15),(40).
	in := []int16{10, 20, 30, 50}
	wav := buildWAV(t, in, 2, bus.SampleRate)

	out, err := DecodeToPCM16k(wav)
	require.NoError(t, err)
	require.Equal(t, []int16{15, 40}, out)
}

func TestDecodeWAV_ResamplesTo16k(t *testing.T) {
	// 8 kHz source → 16 kHz output is twice as long.
	in := make([]int16, 100)
	for i := range in {
		in[i] = int16(i)
	}
	wav := buildWAV(t, in, 1, 8000)

	out, err := DecodeToPCM16k(wav)
	require.NoError(t, err)
	require.Equal(t, 200, len(out))
}

func TestDecodeToPCM16k_Garbage_Errors(t *testing.T) {
	_, err := DecodeToPCM16k([]byte("not audio at all"))
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrUnsupportedFormat))
}

func TestDecodeWAV_Float_Unsupported(t *testing.T) {
	// Hand-build a WAV with format tag 3 (IEEE float).
	wav := buildWAV(t, []int16{1, 2}, 1, bus.SampleRate)
	// fmt audioFormat is at offset 20 (12 RIFF/WAVE + 8 chunk header).
	binary.LittleEndian.PutUint16(wav[20:22], 3)
	_, err := DecodeToPCM16k(wav)
	require.ErrorIs(t, err, ErrUnsupportedFormat)
}
