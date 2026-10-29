package codec

import "math"

// Resampling between 8 kHz and 16 kHz (the rates Flowkit's internal pipeline
// and G.711 wire format need). Downsampling is band-limited so frequencies
// above the telephone Nyquist limit do not fold back into audible speech.

// Upsample8To16 takes an 8 kHz PCM frame and returns a 16 kHz frame twice as
// long. Linear interpolation between adjacent samples.
func Upsample8To16(in []int16) []int16 {
	if len(in) == 0 {
		return nil
	}
	out := make([]int16, len(in)*2)
	for i := 0; i < len(in); i++ {
		out[i*2] = in[i]
		var next int16
		if i+1 < len(in) {
			next = in[i+1]
		} else {
			next = in[i]
		}
		// average without overflow: shift each operand right first
		out[i*2+1] = int16((int32(in[i]) + int32(next)) / 2)
	}
	return out
}

const (
	downsampleTaps      = 63
	downsampleCutoffHz  = 3700.0
	downsampleInputRate = 16000.0
)

var downsampleCoefficients = makeDownsampleCoefficients()

// Downsampler16To8 is a stateful band-limited 2:1 resampler. Keeping the FIR
// history across calls prevents 20 ms transport frame boundaries from becoming
// filter discontinuities.
type Downsampler16To8 struct {
	history [downsampleTaps]float64
	write   int
	phase   int
}

// NewDownsampler16To8 creates a resampler whose history starts at silence.
func NewDownsampler16To8() *Downsampler16To8 { return &Downsampler16To8{} }

// Process filters 16 kHz PCM before decimating it to 8 kHz. An odd trailing
// input sample is retained as phase for the next call.
func (r *Downsampler16To8) Process(in []int16) []int16 {
	if len(in) == 0 {
		return nil
	}
	out := make([]int16, 0, (r.phase+len(in))/2)
	for _, sample := range in {
		r.history[r.write] = float64(sample)
		r.write = (r.write + 1) % downsampleTaps
		r.phase++
		if r.phase < 2 {
			continue
		}
		r.phase = 0

		var filtered float64
		idx := r.write
		for _, coefficient := range downsampleCoefficients {
			idx--
			if idx < 0 {
				idx = downsampleTaps - 1
			}
			filtered += coefficient * r.history[idx]
		}
		out = append(out, clampInt16(filtered))
	}
	return out
}

// Downsample16To8 performs one self-contained conversion. Streaming callers
// should own a Downsampler16To8 and reuse it across frames.
func Downsample16To8(in []int16) []int16 {
	return NewDownsampler16To8().Process(in)
}

func makeDownsampleCoefficients() [downsampleTaps]float64 {
	var coefficients [downsampleTaps]float64
	cutoff := downsampleCutoffHz / downsampleInputRate
	center := float64(downsampleTaps-1) / 2
	var sum float64
	for i := range coefficients {
		x := float64(i) - center
		var sinc float64
		if x == 0 {
			sinc = 2 * cutoff
		} else {
			sinc = math.Sin(2*math.Pi*cutoff*x) / (math.Pi * x)
		}
		window := 0.42 - 0.5*math.Cos(2*math.Pi*float64(i)/float64(downsampleTaps-1)) +
			0.08*math.Cos(4*math.Pi*float64(i)/float64(downsampleTaps-1))
		coefficients[i] = sinc * window
		sum += coefficients[i]
	}
	for i := range coefficients {
		coefficients[i] /= sum
	}
	return coefficients
}

func clampInt16(sample float64) int16 {
	sample = math.Round(sample)
	if sample > math.MaxInt16 {
		return math.MaxInt16
	}
	if sample < math.MinInt16 {
		return math.MinInt16
	}
	return int16(sample)
}

// ResampleLinear converts in from srcRate to dstRate using linear
// interpolation between adjacent samples. It handles arbitrary rate pairs
// (e.g. 16 kHz ↔ 24 kHz for external voice agents). For voice this is
// adequate — see the package note on the mildness of these ratios. Returns in
// unchanged when the rates match or are non-positive.
func ResampleLinear(in []int16, srcRate, dstRate int) []int16 {
	if len(in) == 0 || srcRate == dstRate || srcRate <= 0 || dstRate <= 0 {
		return in
	}
	outLen := int(int64(len(in)) * int64(dstRate) / int64(srcRate))
	if outLen <= 0 {
		return nil
	}
	out := make([]int16, outLen)
	ratio := float64(srcRate) / float64(dstRate)
	for i := range out {
		pos := float64(i) * ratio
		idx := int(pos)
		frac := pos - float64(idx)
		s0 := int32(in[idx])
		s1 := s0
		if idx+1 < len(in) {
			s1 = int32(in[idx+1])
		}
		out[i] = int16(s0 + int32(frac*float64(s1-s0)))
	}
	return out
}
