package audio

import (
	"math"
	"sort"

	"github.com/DeepFlowAI/flowkit/internal/media/bus"
)

const (
	playbackFrameSamples = bus.SampleRate / 50
	playbackNoiseRMS     = 32.0
	playbackTargetRMS    = 5500.0
	playbackMinGain      = 0.25
	playbackMaxGain      = 32.0
	playbackPeakCeiling  = 29204.0
	healthyMinRMS        = 1200.0
	healthyMaxRMS        = 9000.0
)

// LevelSpeechForPlayback corrects only a prompt's overall loudness. Prompts
// whose voiced-speech median RMS falls outside the healthy band, or whose
// robust peak exceeds the ceiling, are scaled by a single scalar gain — toward
// playbackTargetRMS, capped so the robust peak stays within
// playbackPeakCeiling. The robust peak is the p95 of per-frame peaks, so a few
// full-scale spikes in a badly mastered prompt do not read as clipping or cap
// the loudness gain; the tradeoff is that those top ~5% spiky frames may clip
// lightly at the ceiling after scaling, which is inaudible since they already
// sat near full scale. The prompt's internal dynamics are preserved (no
// per-frame AGC, which pumped noise and flattened speech in production).
// Healthy prompts pass through byte-for-byte, and the output always keeps the
// original sample count and timing.
func LevelSpeechForPlayback(pcm []int16) []int16 {
	if len(pcm) == 0 {
		return pcm
	}
	median, robustPeak, voiced := playbackStats(pcm)
	if playbackLevelIsHealthy(median, robustPeak, voiced) {
		return pcm
	}

	gain := playbackTargetRMS / median
	gain = math.Max(playbackMinGain, math.Min(playbackMaxGain, gain))
	if robustPeak > 0 {
		gain = math.Min(gain, playbackPeakCeiling/robustPeak)
	}

	out := make([]int16, len(pcm))
	for i, sample := range pcm {
		value := math.Round(float64(sample) * gain)
		value = math.Max(-playbackPeakCeiling, math.Min(playbackPeakCeiling, value))
		out[i] = int16(value)
	}
	return out
}

// playbackStats returns the median RMS across voiced frames (frame RMS >=
// playbackNoiseRMS), the robust peak (p95 of the per-frame peaks across voiced
// frames), and whether any voiced frame was found.
func playbackStats(pcm []int16) (median, robustPeak float64, voiced bool) {
	frameCount := (len(pcm) + playbackFrameSamples - 1) / playbackFrameSamples
	voicedRMS := make([]float64, 0, frameCount)
	voicedPeaks := make([]float64, 0, frameCount)
	for start := 0; start < len(pcm); start += playbackFrameSamples {
		end := min(start+playbackFrameSamples, len(pcm))
		var sum, framePeak float64
		for _, sample := range pcm[start:end] {
			value := math.Abs(float64(sample))
			sum += value * value
			framePeak = math.Max(framePeak, value)
		}
		if rms := math.Sqrt(sum / float64(end-start)); rms >= playbackNoiseRMS {
			voicedRMS = append(voicedRMS, rms)
			voicedPeaks = append(voicedPeaks, framePeak)
		}
	}
	if len(voicedRMS) == 0 {
		return 0, 0, false
	}
	sort.Float64s(voicedRMS)
	sort.Float64s(voicedPeaks)
	return voicedRMS[len(voicedRMS)/2], voicedPeaks[(len(voicedPeaks)-1)*95/100], true
}

func playbackLevelIsHealthy(median, robustPeak float64, voiced bool) bool {
	if !voiced {
		return true
	}
	return robustPeak <= playbackPeakCeiling &&
		median >= healthyMinRMS && median <= healthyMaxRMS
}
