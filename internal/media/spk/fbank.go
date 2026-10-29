package spk

import (
	"math"
	"sync"
)

// Kaldi-compatible 80-band log-mel filterbank — the front-end the CAM++ speaker
// model was trained with, replicating torchaudio.compliance.kaldi.fbank at its
// defaults (25 ms window, 10 ms shift, Povey window, pre-emphasis 0.97, power
// spectrum, 80 triangular mel bands over 20..8000 Hz, natural log) followed by
// per-utterance mean subtraction.
//
// Like the Whisper front-end in internal/media/turn, the transform is a direct
// real DFT over a precomputed basis rather than an FFT: it runs once per
// utterance over a couple of hundred frames, where the model inference itself
// costs far more.
const (
	fbSampleRate = 16000
	fbWindow     = 400 // 25 ms
	fbShift      = 160 // 10 ms
	fbPadded     = 512 // window size rounded up to a power of two
	fbFreq       = fbPadded/2 + 1
	fbMels       = 80
	fbLowFreq    = 20.0
	fbHighFreq   = 8000.0
	fbPreemph    = 0.97
	// torchaudio's floor on mel energies before the log.
	fbEpsilon = 1.1920928955078125e-07
)

var (
	fbInitOnce sync.Once
	poveyWin   [fbWindow]float64
	melBank    [fbMels][fbFreq]float64
	dftCos     [fbFreq][fbWindow]float64
	dftSin     [fbFreq][fbWindow]float64
)

func fbInit() {
	fbInitOnce.Do(func() {
		// Povey window: a symmetric Hann raised to 0.85.
		for n := 0; n < fbWindow; n++ {
			h := 0.5 - 0.5*math.Cos(2*math.Pi*float64(n)/float64(fbWindow-1))
			poveyWin[n] = math.Pow(h, 0.85)
		}
		// Real-DFT basis. Only the first fbWindow samples of the 512-point frame
		// are non-zero, so the basis needs no more than that.
		for k := 0; k < fbFreq; k++ {
			for n := 0; n < fbWindow; n++ {
				a := 2 * math.Pi * float64(k) * float64(n) / float64(fbPadded)
				dftCos[k][n] = math.Cos(a)
				dftSin[k][n] = math.Sin(a)
			}
		}
		buildMelBank()
	})
}

// buildMelBank builds Kaldi's triangular filterbank: bands are equal-width in
// the mel domain and overlap at their neighbours' centres, with no area
// normalisation.
func buildMelBank() {
	const binWidth = float64(fbSampleRate) / float64(fbPadded)
	melLow, melHigh := hzToMel(fbLowFreq), hzToMel(fbHighFreq)
	delta := (melHigh - melLow) / float64(fbMels+1)
	for m := 0; m < fbMels; m++ {
		left := melLow + float64(m)*delta
		center := left + delta
		right := center + delta
		// The Nyquist bin keeps a zero weight, matching Kaldi's layout.
		for k := 0; k < fbFreq-1; k++ {
			mel := hzToMel(binWidth * float64(k))
			w := math.Min((mel-left)/(center-left), (right-mel)/(right-center))
			if w < 0 {
				w = 0
			}
			melBank[m][k] = w
		}
	}
}

// Kaldi/HTK mel scale.
func hzToMel(f float64) float64 { return 1127.0 * math.Log(1.0+f/700.0) }

// fbankFeatures turns 16 kHz mono samples into the [frames*fbMels] feature
// vector CAM++ expects, row-major by frame. Returns nil when the audio is
// shorter than one window.
func fbankFeatures(samples []int16) []float32 {
	fbInit()
	if len(samples) < fbWindow {
		return nil
	}
	// Kaldi's snip_edges: only whole windows produce a frame.
	frames := 1 + (len(samples)-fbWindow)/fbShift
	out := make([]float32, frames*fbMels)
	frame := make([]float64, fbWindow)
	power := make([]float64, fbFreq)

	for t := 0; t < frames; t++ {
		base := t * fbShift
		var mean float64
		for n := 0; n < fbWindow; n++ {
			mean += float64(samples[base+n]) / 32768.0
		}
		mean /= float64(fbWindow)
		for n := 0; n < fbWindow; n++ {
			frame[n] = float64(samples[base+n])/32768.0 - mean // remove DC offset
		}
		// Pre-emphasis, with the first sample replicated off the left edge.
		prev := frame[0]
		for n := 0; n < fbWindow; n++ {
			cur := frame[n]
			frame[n] = cur - fbPreemph*prev
			prev = cur
		}
		for n := 0; n < fbWindow; n++ {
			frame[n] *= poveyWin[n]
		}
		for k := 0; k < fbFreq; k++ {
			var re, im float64
			ck, sk := &dftCos[k], &dftSin[k]
			for n := 0; n < fbWindow; n++ {
				re += frame[n] * ck[n]
				im -= frame[n] * sk[n]
			}
			power[k] = re*re + im*im
		}
		for m := 0; m < fbMels; m++ {
			var acc float64
			bm := &melBank[m]
			for k := 0; k < fbFreq; k++ {
				acc += bm[k] * power[k]
			}
			if acc < fbEpsilon {
				acc = fbEpsilon
			}
			out[t*fbMels+m] = float32(math.Log(acc))
		}
	}
	subtractMean(out, frames)
	return out
}

// subtractMean applies per-utterance cepstral mean normalisation: each band's
// mean over the utterance is removed, which is also what makes the features
// independent of the overall gain.
func subtractMean(feat []float32, frames int) {
	for m := 0; m < fbMels; m++ {
		var sum float64
		for t := 0; t < frames; t++ {
			sum += float64(feat[t*fbMels+m])
		}
		mean := float32(sum / float64(frames))
		for t := 0; t < frames; t++ {
			feat[t*fbMels+m] -= mean
		}
	}
}
