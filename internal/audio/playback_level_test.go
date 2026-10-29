package audio

import (
	"math"
	"testing"

	"github.com/DeepFlowAI/flowkit/internal/media/bus"
)

func sineSegment(amplitude, samples int) []int16 {
	out := make([]int16, samples)
	for i := range out {
		out[i] = int16(float64(amplitude) * math.Sin(2*math.Pi*440*float64(i)/bus.SampleRate))
	}
	return out
}

func sampleRMS(samples []int16) float64 {
	var sum float64
	for _, sample := range samples {
		v := float64(sample)
		sum += v * v
	}
	return math.Sqrt(sum / float64(len(samples)))
}

func TestLevelSpeechForPlaybackKeepsNaturalDynamics(t *testing.T) {
	// Natural speech has a wide per-frame dynamic range (~19x between the
	// quietest and loudest frames here); with a healthy median RMS (~1800)
	// and no clipping, the prompt must pass through byte-for-byte.
	frame := bus.SampleRate / 50
	var input []int16
	input = append(input, sineSegment(254, frame)...)   // rms ~180
	for i := 0; i < 8; i++ {
		input = append(input, sineSegment(2546, frame)...) // rms ~1800
	}
	input = append(input, sineSegment(4800, frame)...) // rms ~3394

	output := LevelSpeechForPlayback(input)
	if len(output) != len(input) {
		t.Fatalf("sample count changed: got %d want %d", len(output), len(input))
	}
	for i := range input {
		if output[i] != input[i] {
			t.Fatalf("healthy-but-dynamic prompt changed at sample %d: got %d want %d", i, output[i], input[i])
		}
	}
}

func TestLevelSpeechForPlaybackScalesQuietPromptUniformly(t *testing.T) {
	input := sineSegment(1131, bus.SampleRate) // rms ~800, below the healthy band

	output := LevelSpeechForPlayback(input)
	if len(output) != len(input) {
		t.Fatalf("sample count changed: got %d want %d", len(output), len(input))
	}
	// One scalar gain for the whole prompt: every sample scales by the same
	// ratio (within int16 rounding), so internal dynamics are preserved.
	ref := 0
	for i, sample := range input {
		if sample > input[ref] {
			ref = i
		}
	}
	gain := float64(output[ref]) / float64(input[ref])
	if want := playbackTargetRMS / 800; gain < want*0.95 || gain > want*1.05 {
		t.Fatalf("gain not toward target rms: got %f want ~%f", gain, want)
	}
	for i, sample := range input {
		if sample == 0 {
			continue
		}
		if diff := math.Abs(float64(output[i]) - float64(sample)*gain); diff > 1 {
			t.Fatalf("non-uniform gain at sample %d: in=%d out=%d gain=%f", i, sample, output[i], gain)
		}
	}
}

// peakyFrame builds one 20 ms frame whose energy sits in a few spikes, so the
// frame RMS stays low while the frame peak is high (crest factor ~5, like a
// badly mastered prompt).
func peakyFrame(amplitude, spikes int) []int16 {
	frame := make([]int16, bus.SampleRate/50)
	for i := 0; i < spikes; i++ {
		frame[i*len(frame)/spikes] = int16(amplitude)
	}
	return frame
}

func TestLevelSpeechForPlaybackPassesFullScaleSpikesThrough(t *testing.T) {
	// Badly mastered but healthy: median RMS ~1800 with a few full-scale
	// spikes. The p95 frame peak (~25000) is under the ceiling, so the stray
	// 32766 sample must not read as clipping — pass through byte-for-byte.
	frame := bus.SampleRate / 50
	var input []int16
	for i := 0; i < 20; i++ {
		input = append(input, sineSegment(2546, frame)...) // rms ~1800
	}
	input[18*frame] = 25000
	input[19*frame] = 32766

	output := LevelSpeechForPlayback(input)
	if len(output) != len(input) {
		t.Fatalf("sample count changed: got %d want %d", len(output), len(input))
	}
	for i := range input {
		if output[i] != input[i] {
			t.Fatalf("spiky-but-healthy prompt changed at sample %d: got %d want %d", i, output[i], input[i])
		}
	}
}

func TestLevelSpeechForPlaybackScalesPeakyQuietPromptCleanly(t *testing.T) {
	// Uniformly quiet (median RMS ~800) with frame peaks ~4000: the peak cap
	// (29204/4000 ~ 7.3) does not bind, so the loudness gain ~5500/800
	// applies cleanly and nothing clips.
	var input []int16
	for i := 0; i < 20; i++ {
		input = append(input, peakyFrame(4000, 13)...) // rms ~806, peak 4000
	}

	output := LevelSpeechForPlayback(input)
	if len(output) != len(input) {
		t.Fatalf("sample count changed: got %d want %d", len(output), len(input))
	}
	gain := float64(output[0]) / float64(input[0]) // input[0] is a 4000 spike
	if want := playbackTargetRMS / 800; gain < want*0.95 || gain > want*1.05 {
		t.Fatalf("gain not toward target rms: got %f want ~%f", gain, want)
	}
	for i, sample := range input {
		if sample == 0 {
			continue
		}
		if diff := math.Abs(float64(output[i]) - float64(sample)*gain); diff > 1 {
			t.Fatalf("non-uniform gain at sample %d: in=%d out=%d gain=%f", i, sample, output[i], gain)
		}
		if output[i] > 29204 || output[i] < -29204 {
			t.Fatalf("clean scale-up must not clip, sample %d: %d", i, output[i])
		}
	}
}

func TestLevelSpeechForPlaybackCapsGainAtRobustPeak(t *testing.T) {
	// Quiet (median RMS ~800) but spiky, with a p95 frame peak ~24000: the
	// gain is capped at 29204/24000 ~ 1.2 instead of 5500/800 ~ 6.9, so only
	// the top ~5% spikiest frames clip at the ceiling.
	frame := bus.SampleRate / 50
	var input []int16
	for i := 0; i < 20; i++ {
		input = append(input, peakyFrame(4000, 13)...)
	}
	input[18*frame] = 24000
	input[19*frame] = 32766

	output := LevelSpeechForPlayback(input)
	if len(output) != len(input) {
		t.Fatalf("sample count changed: got %d want %d", len(output), len(input))
	}
	want := playbackPeakCeiling / 24000
	if gain := sampleRMS(output[:18*frame]) / sampleRMS(input[:18*frame]); gain < want*0.98 || gain > want*1.02 {
		t.Fatalf("gain not capped at the robust peak: got %f want ~%f", gain, want)
	}
	clipped := 0
	for start := 0; start < len(input); start += frame {
		for i := start; i < start+frame; i++ {
			if output[i] > 29204 || output[i] < -29204 {
				t.Fatalf("playback exceeded the -1 dBFS ceiling at sample %d: %d", i, output[i])
			}
			if math.Abs(float64(input[i]))*want > playbackPeakCeiling*1.001 {
				clipped++
				break
			}
		}
	}
	if frames := len(input) / frame; clipped*100 > frames*5 {
		t.Fatalf("too many frames clipped: %d of %d", clipped, frames)
	}
}

func TestLevelSpeechForPlaybackPassesSilenceThrough(t *testing.T) {
	input := make([]int16, 3200)
	output := LevelSpeechForPlayback(input)
	if len(output) != len(input) {
		t.Fatalf("sample count changed: got %d want %d", len(output), len(input))
	}
	for i := range input {
		if output[i] != 0 {
			t.Fatalf("silence changed at sample %d: got %d", i, output[i])
		}
	}
}

func TestLevelSpeechForPlaybackLeavesHealthyPromptUnchanged(t *testing.T) {
	input := sineSegment(5000, bus.SampleRate)
	output := LevelSpeechForPlayback(input)
	if len(output) != len(input) {
		t.Fatalf("sample count changed: got %d want %d", len(output), len(input))
	}
	for i := range input {
		if output[i] != input[i] {
			t.Fatalf("healthy prompt changed at sample %d: got %d want %d", i, output[i], input[i])
		}
	}
}
