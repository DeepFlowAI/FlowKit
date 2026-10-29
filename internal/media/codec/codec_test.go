package codec

import (
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

// rmsErrorDB computes the RMS error between two int16 slices in decibels
// relative to full-scale (negative numbers are good; -40 dB is excellent).
func rmsErrorDB(a, b []int16) float64 {
	if len(a) != len(b) {
		return math.Inf(1)
	}
	var sumSq float64
	for i := range a {
		d := float64(a[i]) - float64(b[i])
		sumSq += d * d
	}
	if sumSq == 0 {
		return math.Inf(-1)
	}
	rms := math.Sqrt(sumSq / float64(len(a)))
	return 20 * math.Log10(rms/32768)
}

// --- μ-law ---

func TestPCMU_Roundtrip_ZeroLossy(t *testing.T) {
	original := []int16{0, 1, -1, 100, -100, 4096, -4096, 32000, -32000, 32767, -32768}
	encoded := EncodePCMU(original)
	require.Equal(t, len(original), len(encoded))
	decoded := DecodePCMU(encoded)
	require.Equal(t, len(original), len(decoded))

	// μ-law is lossy; check the per-sample max absolute error is bounded.
	for i := range original {
		got := int(decoded[i])
		want := int(original[i])
		diff := got - want
		if diff < 0 {
			diff = -diff
		}
		require.LessOrEqual(t, diff, 4096, "sample %d: got %d want %d", i, got, want)
	}
}

func TestPCMU_RoundtripRMS_AcceptableForSpeech(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	original := make([]int16, 8000) // 1 second @ 8 kHz
	for i := range original {
		// generate a 1 kHz sine + a bit of noise (speech-like)
		t := float64(i) / 8000
		sample := math.Sin(2*math.Pi*1000*t)*0.5 + (rng.Float64()-0.5)*0.05
		original[i] = int16(sample * 30000)
	}
	encoded := EncodePCMU(original)
	decoded := DecodePCMU(encoded)
	err := rmsErrorDB(original, decoded)
	require.Less(t, err, -25.0, "μ-law RMS error should be < -25 dB for speech-like input, got %.1f", err)
}

func TestPCMU_KnownValue_Zero(t *testing.T) {
	// μ-law zero codeword is 0xFF (after inversion of 0x00)
	require.Equal(t, byte(0xFF), encodeMuLawSample(0))
	require.Equal(t, int16(0), muLawDecodeSample(0xFF))
}

// --- A-law ---

func TestPCMA_Roundtrip_ZeroLossy(t *testing.T) {
	original := []int16{0, 1, -1, 100, -100, 4096, -4096, 32000, -32000}
	encoded := EncodePCMA(original)
	decoded := DecodePCMA(encoded)
	for i := range original {
		got := int(decoded[i])
		want := int(original[i])
		diff := got - want
		if diff < 0 {
			diff = -diff
		}
		require.LessOrEqual(t, diff, 4096, "sample %d: got %d want %d", i, got, want)
	}
}

// --- Resampling ---

func TestUpsample8To16_Length(t *testing.T) {
	in := []int16{1, 2, 3}
	out := Upsample8To16(in)
	require.Len(t, out, 6)
	// odd-indexed should be averaged
	require.Equal(t, int16(1), out[0])
	require.Equal(t, int16((1+2)/2), out[1])
	require.Equal(t, int16(2), out[2])
	require.Equal(t, int16((2+3)/2), out[3])
	require.Equal(t, int16(3), out[4])
	require.Equal(t, int16(3), out[5]) // last sample held
}

func TestUpsample8To16_Empty(t *testing.T) {
	require.Nil(t, Upsample8To16(nil))
	require.Nil(t, Upsample8To16([]int16{}))
}

func TestDownsample16To8_Length(t *testing.T) {
	in := make([]int16, 320)
	for i := range in {
		in[i] = 1000
	}
	out := Downsample16To8(in)
	require.Len(t, out, 160)
	for _, sample := range out[32:] {
		require.InDelta(t, 1000, sample, 1,
			"a settled low-frequency/DC signal should retain unity gain")
	}
}

func TestDownsample16To8_OddInput_DropsLast(t *testing.T) {
	in := []int16{1, 3, 5}
	out := Downsample16To8(in)
	require.Len(t, out, 1)
}

func TestDownsample16To8_SuppressesFrequenciesAboveTelephoneNyquist(t *testing.T) {
	const sampleRate = 16000
	in := make([]int16, sampleRate)
	for i := range in {
		in[i] = int16(math.Sin(2*math.Pi*6000*float64(i)/sampleRate) * 30000)
	}

	out := Downsample16To8(in)
	var inEnergy, outEnergy float64
	for _, sample := range in {
		inEnergy += float64(sample) * float64(sample)
	}
	for _, sample := range out {
		outEnergy += float64(sample) * float64(sample)
	}
	inRMS := math.Sqrt(inEnergy / float64(len(in)))
	outRMS := math.Sqrt(outEnergy / float64(len(out)))

	require.Less(t, outRMS/inRMS, 0.02,
		"frequencies above 4 kHz must be filtered instead of aliasing into the telephone band")
}

func TestDownsampler16To8_PreservesFilterStateAcrossTransportFrames(t *testing.T) {
	in := make([]int16, 320*4)
	for i := range in {
		in[i] = int16((math.Sin(2*math.Pi*700*float64(i)/16000) +
			0.25*math.Sin(2*math.Pi*3100*float64(i)/16000)) * 12000)
	}

	wholeResampler := NewDownsampler16To8()
	want := wholeResampler.Process(in)
	framedResampler := NewDownsampler16To8()
	var got []int16
	for offset := 0; offset < len(in); offset += 320 {
		got = append(got, framedResampler.Process(in[offset:offset+320])...)
	}

	require.Equal(t, want, got,
		"20 ms transport boundaries must not reset the anti-alias filter")
}

func TestResample_Roundtrip_RMSBounded(t *testing.T) {
	// 16k → 8k → 16k roundtrip should preserve speech-band content.
	rng := rand.New(rand.NewSource(1))
	in := make([]int16, 1600) // 100 ms @ 16 kHz
	for i := range in {
		t := float64(i) / 16000
		sample := math.Sin(2*math.Pi*440*t) * 0.5
		sample += (rng.Float64() - 0.5) * 0.02
		in[i] = int16(sample * 30000)
	}
	down := Downsample16To8(in)
	up := Upsample8To16(down)
	require.Len(t, up, len(in))
	const filterDelay16k = (downsampleTaps - 3) / 2
	err := rmsErrorDB(in[:len(in)-filterDelay16k], up[filterDelay16k:])
	require.Less(t, err, -15.0, "resample roundtrip RMS err should be < -15 dB, got %.1f", err)
}

func TestG711AndResample_FullPath_RMSBounded(t *testing.T) {
	// PCM 16k → downsample → PCMU → wire → PCMU → upsample → PCM 16k
	// Use a continuous sine wave; per-sample error is noisy in a tiny signal
	// but RMS over a long signal converges.
	in := make([]int16, 16000) // 1 second
	for i := range in {
		t := float64(i) / 16000
		in[i] = int16(math.Sin(2*math.Pi*440*t) * 20000)
	}
	down8 := Downsample16To8(in)
	encoded := EncodePCMU(down8)
	decoded := DecodePCMU(encoded)
	up16 := Upsample8To16(decoded)
	require.Len(t, up16, len(in))

	const filterDelay16k = (downsampleTaps - 3) / 2
	err := rmsErrorDB(in[:len(in)-filterDelay16k], up16[filterDelay16k:])
	require.Less(t, err, -15.0,
		"full PCM→μ-law→PCM roundtrip RMS error should be < -15 dB, got %.1f", err)
}
