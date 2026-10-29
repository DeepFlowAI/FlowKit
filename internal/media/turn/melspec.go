package turn

import (
	"math"
	"sync"
)

// Whisper-tiny log-mel front-end, replicating HuggingFace
// WhisperFeatureExtractor(chunk_length=8, sampling_rate=16000, do_normalize=True)
// exactly enough that its output feeds Smart Turn v3's ONNX "input_features"
// tensor (shape [1,80,800]). Steps, in order (matching smart-turn's inference.py
// + audio_utils.py + transformers' _np_extract_fbank_features):
//
//  1. Keep the last 8 s of audio, else left-pad with zeros to 8 s (128000 samples).
//  2. Zero-mean / unit-variance normalise the whole 128000-sample waveform.
//  3. Reflect-pad by n_fft/2, frame with a periodic Hann window (n_fft=400,
//     hop=160), power spectrum (|FFT|^2), keep the first 800 frames.
//  4. Apply an 80-band Slaney mel filterbank (0..8000 Hz).
//  5. log10, floor at (max-8), then (x+4)/4.
//
// The result is laid out row-major as [mel*nFrames + frame], the layout the ONNX
// tensor expects for shape [1, nMels, nFrames].
const (
	melSampleRate = 16000
	melChunkSec   = 8
	melMaxSamples = melSampleRate * melChunkSec // 128000
	nFFT          = 400
	hopLength     = 160
	nMels         = 80
	nFreq         = nFFT/2 + 1 // 201
	nFrames       = 800        // 8 s @ hop 160, dropping the trailing frame
	melFmax       = 8000.0
)

var (
	melInitOnce sync.Once
	melBank     [nMels][nFreq]float64 // Slaney mel filterbank
	hannWindow  [nFFT]float64         // periodic Hann
	dftCos      [nFreq][nFFT]float64
	dftSin      [nFreq][nFFT]float64
)

func melInit() {
	melInitOnce.Do(func() {
		// Periodic Hann: np.hanning(n_fft+1)[:-1].
		for n := 0; n < nFFT; n++ {
			hannWindow[n] = 0.5 - 0.5*math.Cos(2*math.Pi*float64(n)/float64(nFFT))
		}
		// Real-DFT basis for a 400-point window, first 201 bins.
		for k := 0; k < nFreq; k++ {
			for n := 0; n < nFFT; n++ {
				a := 2 * math.Pi * float64(k) * float64(n) / float64(nFFT)
				dftCos[k][n] = math.Cos(a)
				dftSin[k][n] = math.Sin(a)
			}
		}
		buildMelBank()
	})
}

// buildMelBank builds an 80-band Slaney mel filterbank matching
// librosa.filters.mel(sr=16000, n_fft=400, n_mels=80, fmin=0, fmax=8000,
// htk=False, norm="slaney"), which is what transformers uses for Whisper.
func buildMelBank() {
	// FFT bin centre frequencies (Hz): linspace(0, sr/2, nFreq).
	fftFreqs := make([]float64, nFreq)
	for i := 0; i < nFreq; i++ {
		fftFreqs[i] = float64(i) * (melSampleRate / 2.0) / float64(nFreq-1)
	}
	// nMels+2 mel points spanning [0, fmax], back to Hz.
	melMin := hzToMel(0)
	melMax := hzToMel(melFmax)
	melPts := make([]float64, nMels+2)
	freqs := make([]float64, nMels+2)
	for i := range melPts {
		melPts[i] = melMin + (melMax-melMin)*float64(i)/float64(nMels+1)
		freqs[i] = melToHz(melPts[i])
	}
	for m := 0; m < nMels; m++ {
		lower := freqs[m]
		center := freqs[m+1]
		upper := freqs[m+2]
		enorm := 2.0 / (upper - lower) // Slaney area normalisation
		for k := 0; k < nFreq; k++ {
			f := fftFreqs[k]
			var w float64
			down := (f - lower) / (center - lower)
			up := (upper - f) / (upper - center)
			if w = math.Min(down, up); w < 0 {
				w = 0
			}
			melBank[m][k] = w * enorm
		}
	}
}

// Slaney mel scale (htk=False): linear below 1000 Hz, log above.
func hzToMel(f float64) float64 {
	const fSp = 200.0 / 3.0
	const minLogHz = 1000.0
	minLogMel := minLogHz / fSp
	logStep := math.Log(6.4) / 27.0
	if f < minLogHz {
		return f / fSp
	}
	return minLogMel + math.Log(f/minLogHz)/logStep
}

func melToHz(m float64) float64 {
	const fSp = 200.0 / 3.0
	const minLogHz = 1000.0
	minLogMel := minLogHz / fSp
	logStep := math.Log(6.4) / 27.0
	if m < minLogMel {
		return m * fSp
	}
	return minLogHz * math.Exp(logStep*(m-minLogMel))
}

// logMelFeatures turns 16 kHz mono samples in [-1,1] into the [nMels*nFrames]
// Whisper log-mel feature vector Smart Turn v3 expects.
func logMelFeatures(audio []float32) []float32 {
	melInit()

	// (1) last 8 s, else left-pad with zeros to 8 s.
	wave := make([]float64, melMaxSamples)
	if len(audio) >= melMaxSamples {
		src := audio[len(audio)-melMaxSamples:]
		for i, v := range src {
			wave[i] = float64(v)
		}
	} else {
		off := melMaxSamples - len(audio)
		for i, v := range audio {
			wave[off+i] = float64(v)
		}
	}

	// (2) zero-mean / unit-variance over the whole 128000-sample waveform.
	var mean float64
	for _, v := range wave {
		mean += v
	}
	mean /= float64(len(wave))
	var variance float64
	for _, v := range wave {
		d := v - mean
		variance += d * d
	}
	variance /= float64(len(wave))
	std := math.Sqrt(variance + 1e-7)
	for i := range wave {
		wave[i] = (wave[i] - mean) / std
	}

	// (3) reflect-pad by n_fft/2 so frame t is centred at t*hop.
	pad := nFFT / 2
	padded := make([]float64, len(wave)+2*pad)
	copy(padded[pad:pad+len(wave)], wave)
	for i := 0; i < pad; i++ {
		padded[pad-1-i] = wave[i+1]                 // left reflect (no edge repeat)
		padded[pad+len(wave)+i] = wave[len(wave)-2-i] // right reflect
	}

	// (3)+(4) framed power spectrum → mel, then (5) log scaling.
	out := make([]float32, nMels*nFrames)
	logSpec := make([]float64, nMels*nFrames)
	frame := make([]float64, nFFT)
	power := make([]float64, nFreq)
	maxLog := math.Inf(-1)
	for t := 0; t < nFrames; t++ {
		base := t * hopLength
		for n := 0; n < nFFT; n++ {
			frame[n] = padded[base+n] * hannWindow[n]
		}
		for k := 0; k < nFreq; k++ {
			var re, im float64
			ck := &dftCos[k]
			sk := &dftSin[k]
			for n := 0; n < nFFT; n++ {
				re += frame[n] * ck[n]
				im -= frame[n] * sk[n]
			}
			power[k] = re*re + im*im
		}
		for m := 0; m < nMels; m++ {
			var acc float64
			bm := &melBank[m]
			for k := 0; k < nFreq; k++ {
				acc += bm[k] * power[k]
			}
			if acc < 1e-10 {
				acc = 1e-10
			}
			l := math.Log10(acc)
			logSpec[m*nFrames+t] = l
			if l > maxLog {
				maxLog = l
			}
		}
	}

	floor := maxLog - 8.0
	for i, l := range logSpec {
		if l < floor {
			l = floor
		}
		out[i] = float32((l + 4.0) / 4.0)
	}
	return out
}
