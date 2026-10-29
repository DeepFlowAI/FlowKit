package spk

import (
	"encoding/binary"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/dialogpolicy"
)

// Golden vectors come from torchaudio.compliance.kaldi.fbank + the shipped CAM++
// model (onnxruntime 1.26), over three synthesized Mandarin utterances: two by
// the caller's voice at handset level and one by a different, much quieter voice
// standing in for a bystander.
func readPCM(t *testing.T, name string) []int16 {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", name+".pcm"))
	if err != nil {
		t.Fatalf("read pcm: %v", err)
	}
	out := make([]int16, len(raw)/2)
	for i := range out {
		out[i] = int16(binary.LittleEndian.Uint16(raw[2*i:]))
	}
	return out
}

func readF32(t *testing.T, name, ext string) []float32 {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", name+"."+ext))
	if err != nil {
		t.Fatalf("read %s: %v", ext, err)
	}
	out := make([]float32, len(raw)/4)
	for i := range out {
		out[i] = math.Float32frombits(binary.LittleEndian.Uint32(raw[4*i:]))
	}
	return out
}

// The front-end has to match Kaldi's, not merely resemble it: the model was
// trained on those features and drifts silently on anything else.
func TestFbankMatchesKaldi(t *testing.T) {
	for _, name := range []string{"caller1", "caller2", "bystander"} {
		want := readF32(t, name, "fbank")
		got := fbankFeatures(readPCM(t, name))
		if len(got) != len(want) {
			t.Fatalf("%s: %d features, want %d", name, len(got), len(want))
		}
		var maxDiff float64
		for i := range want {
			if d := math.Abs(float64(got[i] - want[i])); d > maxDiff {
				maxDiff = d
			}
		}
		if maxDiff > 1e-3 {
			t.Errorf("%s: max deviation %.5f from Kaldi", name, maxDiff)
		}
	}
}

func requireModel(t *testing.T) {
	t.Helper()
	lib := os.Getenv("SPK_ONNXRUNTIME_LIB")
	if lib == "" {
		t.Skip("SPK_ONNXRUNTIME_LIB not set")
	}
	if err := Init(lib, 1); err != nil {
		t.Fatalf("init: %v", err)
	}
}

func TestEmbedMatchesReference(t *testing.T) {
	requireModel(t)
	for _, name := range []string{"caller1", "caller2", "bystander"} {
		got, err := Embed(readPCM(t, name))
		if err != nil {
			t.Fatalf("%s: embed: %v", name, err)
		}
		// Not bit-exact: this front-end sums a direct DFT where torchaudio runs an
		// FFT, and the quantised kernels amplify that last-digit difference. What
		// matters is that it lands on the same vector — decisions are made across
		// a gap of 0.4, so 1e-3 of drift is immaterial.
		if sim := similarity(got, readF32(t, name, "emb")); sim < 0.999 {
			t.Errorf("%s: embedding differs from reference (cosine %.5f)", name, sim)
		}
	}
}

// The margin this whole layer rests on: the caller's two utterances have to look
// alike, and the bystander has to fall far below the threshold.
func TestSeparatesCallerFromBystander(t *testing.T) {
	requireModel(t)
	caller1, err := Embed(readPCM(t, "caller1"))
	if err != nil {
		t.Fatal(err)
	}
	caller2, err := Embed(readPCM(t, "caller2"))
	if err != nil {
		t.Fatal(err)
	}
	bystander, err := Embed(readPCM(t, "bystander"))
	if err != nil {
		t.Fatal(err)
	}
	same := similarity(caller1, caller2)
	other := similarity(caller1, bystander)
	// The separation the model achieves on these voices is what the layer rests
	// on, so it is asserted in absolute terms — not relative to the threshold,
	// which is an operational knob tuned from production scores and moves without
	// the model changing at all.
	if same < 0.55 {
		t.Errorf("caller's own utterances scored %.3f, separation has degraded", same)
	}
	if other > 0.25 {
		t.Errorf("bystander scored %.3f, separation has degraded", other)
	}
	thr := float32(dialogpolicy.Defaults().SpkThreshold)
	if thr <= other || thr >= same {
		t.Errorf("threshold %.2f is outside the measured gap %.3f~%.3f", thr, other, same)
	}
	t.Logf("caller vs caller %.3f, caller vs bystander %.3f (threshold %.2f)", same, other, thr)
}

// The gate runs on the path between the user finishing a sentence and the bot
// starting to answer, so its cost is added latency on every turn.
func TestEmbedLatency(t *testing.T) {
	if raceEnabled {
		t.Skip("wall-clock budget is meaningless under the race detector")
	}
	requireModel(t)
	pcm := readPCM(t, "bystander") // the longest segment, 2 s
	if _, err := Embed(pcm); err != nil {
		t.Fatal(err)
	}
	res := testing.Benchmark(func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, err := Embed(pcm); err != nil {
				b.Fatal(err)
			}
		}
	})
	perCall := res.T / time.Duration(max(1, res.N))
	t.Logf("%v per 2 s utterance", perCall)
	if perCall.Milliseconds() > 150 {
		t.Errorf("embedding a turn costs %v, too much to sit in front of a reply", perCall)
	}
}

func TestGateEnrollsThenRejectsAnotherVoice(t *testing.T) {
	requireModel(t)
	g := NewGate()
	if g == nil {
		t.Fatal("gate unavailable with the model loaded")
	}
	if res := g.Check(readPCM(t, "caller1")); !res.OK || !res.Enrolled {
		t.Fatalf("first solid utterance should enroll: %+v", res)
	}
	if res := g.Check(readPCM(t, "caller2")); !res.OK || !res.Compared {
		t.Fatalf("caller's next utterance should pass: %+v", res)
	}
	res := g.Check(readPCM(t, "bystander"))
	if res.OK {
		t.Fatalf("bystander should be rejected: %+v", res)
	}
}

// A gate that enrolled the wrong voice must let go of it rather than lock the
// caller out for the rest of the call. In these fixtures the real caller takes
// the call back on their second utterance: they are on the handset, so they
// clear the level condition, and two turns is what the handover asks for.
func TestGateHandsTheCallToTheRealCaller(t *testing.T) {
	requireModel(t)
	g := NewGate()
	if res := g.Check(readPCM(t, "bystander")); !res.Enrolled {
		t.Fatalf("expected enrollment: %+v", res)
	}
	caller := readPCM(t, "caller1")
	if res := g.Check(caller); res.OK {
		t.Fatalf("one utterance must not be enough to take the call: %+v", res)
	}
	res := g.Check(readPCM(t, "caller2"))
	if !res.Switched || res.SwitchReason != switchHandover {
		t.Fatalf("the caller's second utterance should win the call back: %+v", res)
	}
	if !res.OK {
		t.Error("the new caller should be accepted once the call has changed hands")
	}
	// The margin this rests on, made visible: the handset is far louder than the
	// room, which is what stops a bystander from doing the same thing.
	t.Logf("caller %.0f RMS vs bystander %.0f RMS",
		speechRMS(caller), speechRMS(readPCM(t, "bystander")))
	if res := g.Check(caller); !res.OK {
		t.Errorf("the caller should keep matching after the handover: %+v", res)
	}
}

// Short audio has nothing to enroll from and must not be rejected either.
func TestGateAcceptsShortAudioBeforeEnrollment(t *testing.T) {
	requireModel(t)
	g := NewGate()
	if res := g.Check(readPCM(t, "caller1")[:1600]); !res.OK || res.Enrolled {
		t.Fatalf("100ms should be accepted without enrolling: %+v", res)
	}
}
