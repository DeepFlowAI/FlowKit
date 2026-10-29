package turn

import (
	"encoding/binary"
	"math"
	"os"
	"testing"
)

// refSignal is a deterministic <8 s test waveform (so the left-pad path is
// exercised) shared bit-for-bit with the Python cross-validation script.
func refSignal() []float32 {
	const n = 20000 // 1.25 s @ 16 kHz
	s := make([]float32, n)
	for i := 0; i < n; i++ {
		t := float64(i) / float64(melSampleRate)
		s[i] = float32(0.5*math.Sin(2*math.Pi*220*t) +
			0.3*math.Sin(2*math.Pi*440*t) +
			0.1*math.Sin(2*math.Pi*1500*t))
	}
	return s
}

func TestLogMelShape(t *testing.T) {
	feats := logMelFeatures(refSignal())
	if len(feats) != nMels*nFrames {
		t.Fatalf("feature length = %d, want %d", len(feats), nMels*nFrames)
	}
	for i, v := range feats {
		if math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) {
			t.Fatalf("non-finite feature at %d: %v", i, v)
		}
	}
}

func TestMelBankSanity(t *testing.T) {
	melInit()
	for m := 0; m < nMels; m++ {
		var sum float64
		for k := 0; k < nFreq; k++ {
			if melBank[m][k] < 0 {
				t.Fatalf("negative mel weight at [%d][%d]", m, k)
			}
			sum += melBank[m][k]
		}
		if sum <= 0 {
			t.Fatalf("mel band %d is all zero", m)
		}
	}
}

// TestDumpFeatures writes the reference-signal features as little-endian float32
// to $TURN_DUMP for numerical comparison against the Python WhisperFeatureExtractor.
// It is a no-op unless TURN_DUMP is set.
func TestDumpFeatures(t *testing.T) {
	path := os.Getenv("TURN_DUMP")
	if path == "" {
		t.Skip("set TURN_DUMP to dump features")
	}
	feats := logMelFeatures(refSignal())
	buf := make([]byte, len(feats)*4)
	for i, v := range feats {
		binary.LittleEndian.PutUint32(buf[i*4:], math.Float32bits(v))
	}
	if err := os.WriteFile(path, buf, 0o644); err != nil {
		t.Fatal(err)
	}
}
