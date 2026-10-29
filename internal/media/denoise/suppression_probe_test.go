package denoise

import (
	"math"
	"os"
	"testing"
)

const (
	probeFrame     = 160 // 10ms at 16kHz, what the call pipeline hands over
	probeSpeechRMS = 800.0
	probeEatenRMS  = probeSpeechRMS / 4
)

// TestProbeSuppressionOnSpeech measures how often the enhancer takes a
// speech-level frame down to silence, and what holding back a share of the dry
// signal does about it. Two readings per setting: the naive one the call
// pipeline's counter used to take, and one that waits out the hop of latency
// the enhancer adds — an onset otherwise looks like loss, because the input is
// already loud while the output is still the silence from before.
func TestProbeSuppressionOnSpeech(t *testing.T) {
	lib := os.Getenv("DENOISE_ONNXRUNTIME_LIB")
	if lib == "" {
		t.Skip("DENOISE_ONNXRUNTIME_LIB not set")
	}
	if err := Init(lib, 1); err != nil {
		t.Fatal(err)
	}
	raw := readPCM(t, "testdata/noisy.pcm")

	modelOnly := probeEaten(t, raw, 1)
	mixed := probeEaten(t, raw, defaultMixWet)
	t.Logf("model alone (mix=1.0):    voiced=%d naive=%d (%.1f%%) aligned=%d (%.1f%%)",
		modelOnly.voiced, modelOnly.naive, modelOnly.naivePct(),
		modelOnly.aligned, modelOnly.alignedPct())
	t.Logf("with dry mix (mix=%.2f):  voiced=%d naive=%d (%.1f%%) aligned=%d (%.1f%%)",
		defaultMixWet, mixed.voiced, mixed.naive, mixed.naivePct(),
		mixed.aligned, mixed.alignedPct())

	// The whole point of the mix is that speech stops disappearing.
	if mixed.aligned >= modelOnly.aligned {
		t.Errorf("dry mix did not reduce suppressed speech: %d vs %d",
			mixed.aligned, modelOnly.aligned)
	}
	if mixed.aligned > modelOnly.voiced/20 {
		t.Errorf("still losing %d of %d voiced frames with the dry mix",
			mixed.aligned, mixed.voiced)
	}
}

// TestProbeNoiseAttenuation checks the other side of the trade: the mix must
// still buy enough quiet that ASR does not invent sentences out of the
// background. Steady synthetic noise, because the speech vector has no gap long
// enough to read an attenuation off.
func TestProbeNoiseAttenuation(t *testing.T) {
	lib := os.Getenv("DENOISE_ONNXRUNTIME_LIB")
	if lib == "" {
		t.Skip("DENOISE_ONNXRUNTIME_LIB not set")
	}
	if err := Init(lib, 1); err != nil {
		t.Fatal(err)
	}
	noise := whiteNoise(16000, 400) // 1s at a typical background level

	for _, mix := range []float32{1, defaultMixWet} {
		d := New()
		if d == nil {
			t.Fatal("New returned nil")
		}
		d.mix = mix
		var inPow, outPow float64
		for off := 0; off+probeFrame <= len(noise); off += probeFrame {
			in := noise[off : off+probeFrame]
			out := d.Process(append([]int16(nil), in...))
			if off < 3200 {
				continue // let the recurrent state settle
			}
			inPow += rms(in) * rms(in)
			outPow += rms(out) * rms(out)
		}
		d.Close()
		db := 10 * math.Log10(outPow/inPow)
		t.Logf("mix=%.2f: noise attenuation %.1f dB", mix, db)
		if mix == defaultMixWet && db > -8 {
			t.Errorf("only %.1f dB of attenuation left with the dry mix", db)
		}
	}
}

// whiteNoise returns deterministic noise at roughly the requested RMS.
func whiteNoise(n int, level float64) []int16 {
	out := make([]int16, n)
	var state uint32 = 12345
	for i := range out {
		state = state*1664525 + 1013904223
		// Two draws averaged, so the amplitude distribution is not flat.
		a := float64(int32(state>>8)%2000-1000) / 1000
		state = state*1664525 + 1013904223
		b := float64(int32(state>>8)%2000-1000) / 1000
		out[i] = int16((a + b) / 2 * level * 2.4)
	}
	return out
}

type probeResult struct{ voiced, naive, aligned int }

func (r probeResult) naivePct() float64   { return 100 * float64(r.naive) / float64(r.voiced) }
func (r probeResult) alignedPct() float64 { return 100 * float64(r.aligned) / float64(r.voiced) }

func probeEaten(t *testing.T, raw []int16, mix float32) probeResult {
	t.Helper()
	d := New()
	if d == nil {
		t.Fatal("New returned nil")
	}
	defer d.Close()
	d.mix = mix

	var ins, outs []float64
	for off := 0; off+probeFrame <= len(raw); off += probeFrame {
		in := raw[off : off+probeFrame]
		out := d.Process(append([]int16(nil), in...))
		ins = append(ins, rms(in))
		outs = append(outs, rms(out))
	}

	var res probeResult
	for i := range ins {
		if ins[i] < probeSpeechRMS {
			continue
		}
		res.voiced++
		if outs[i] < probeEatenRMS {
			res.naive++
			// Only a run of speech-level input clears the enhancer's delay.
			if i >= 2 && ins[i-1] >= probeSpeechRMS && ins[i-2] >= probeSpeechRMS {
				res.aligned++
			}
		}
	}
	return res
}

func rms(f []int16) float64 {
	if len(f) == 0 {
		return 0
	}
	var s float64
	for _, v := range f {
		s += float64(v) * float64(v)
	}
	return math.Sqrt(s / float64(len(f)))
}
