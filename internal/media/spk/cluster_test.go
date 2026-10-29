package spk

import (
	"math"
	"testing"

	"github.com/DeepFlowAI/flowkit/internal/dialogpolicy"
)

// These tests drive the tracker with synthetic embeddings instead of the model,
// so the decision logic is covered on every CI run — the model-backed tests all
// skip without onnxruntime, which is exactly where a threshold or state-machine
// mistake would hide.
//
// voiceAt builds a unit vector at a given angle in the first two dimensions.
// Cosine similarity between two of them is cos(difference), so an angle gap maps
// straight onto a score: 0.42 rad ≈ 0.91, 1.2 rad ≈ 0.36 (a bystander).
func voiceAt(angle float64) []float32 {
	v := make([]float32, EmbeddingDim)
	v[0] = float32(math.Cos(angle))
	v[1] = float32(math.Sin(angle))
	return v
}

// Levels in int16 RMS, matching what the street recording showed: the caller
// arrives an order of magnitude above a bystander a few metres away.
const (
	handsetLevel  = 6000.0
	farFieldLevel = 150.0
	// Under the handover bar but above the recovery floor: a caller who dropped
	// their voice, or turned away from the handset.
	softCaller = 1800.0
)

func testTracker() *tracker {
	d := dialogpolicy.Defaults()
	return newTracker(d.SpkThreshold, d.SpkMaxSpeakers, d.SpkHandoverTurns,
		d.SpkHandoverLevelRatio, d.SpkReenrollAfter)
}

func TestFirstVoiceEnrolls(t *testing.T) {
	tr := testTracker()
	res := tr.observe(voiceAt(0), handsetLevel)
	if !res.OK || !res.Enrolled {
		t.Fatalf("first utterance should enroll: %+v", res)
	}
	if res.SpeakerID != 1 || res.Speakers != 1 {
		t.Fatalf("expected speaker 1 of 1, got %d of %d", res.SpeakerID, res.Speakers)
	}
}

func TestCallerKeepsMatchingAcrossSlightDrift(t *testing.T) {
	tr := testTracker()
	tr.observe(voiceAt(0), handsetLevel)
	// Same person, a little different each time — the score stays well above the
	// 0.45 bar.
	for i, angle := range []float64{0.1, 0.2, 0.3, 0.4} {
		res := tr.observe(voiceAt(angle), handsetLevel)
		if !res.OK {
			t.Fatalf("turn %d (angle %.1f) rejected the caller: %+v", i, angle, res)
		}
		if res.SpeakerID != 1 {
			t.Fatalf("turn %d attributed to speaker %d, want 1", i, res.SpeakerID)
		}
	}
	if tr.speakers[0].turns != 5 {
		t.Errorf("caller cluster absorbed %d utterances, want 5", tr.speakers[0].turns)
	}
}

// The centroid has to follow the caller, because a fixed first-utterance
// reference slowly stops matching the person it came from. Drifting one step at
// a time must remain matched even past the point where the last observation no
// longer resembles the original enrollment at all.
func TestCentroidFollowsTheCaller(t *testing.T) {
	tr := testTracker()
	tr.observe(voiceAt(0), handsetLevel)
	for angle := 0.3; angle <= 2.4; angle += 0.3 {
		if res := tr.observe(voiceAt(angle), handsetLevel); !res.OK {
			t.Fatalf("gradual drift to %.1f rad lost the caller: %+v", angle, res)
		}
	}
	if sim := similarity(tr.speakers[0].centroid, voiceAt(0)); sim > 0.9 {
		t.Errorf("centroid never moved (%.3f against the enrollment)", sim)
	}
	if len(tr.speakers) != 1 {
		t.Errorf("drift opened %d clusters, want 1", len(tr.speakers))
	}
}

func TestBystanderIsTrackedSeparatelyAndRejected(t *testing.T) {
	tr := testTracker()
	tr.observe(voiceAt(0), handsetLevel)
	res := tr.observe(voiceAt(1.2), farFieldLevel)
	if res.OK {
		t.Fatalf("far-field bystander should not pass: %+v", res)
	}
	if res.SpeakerID != 2 || res.Speakers != 2 {
		t.Fatalf("expected a second voice, got speaker %d of %d", res.SpeakerID, res.Speakers)
	}
	if res.Switched {
		t.Error("a single bystander utterance must not change the caller")
	}
}

// The case that motivates level as a condition: someone in the background talks
// throughout the call. They must never inherit it, however much they say.
func TestTalkativeBystanderNeverInheritsTheCall(t *testing.T) {
	tr := testTracker()
	tr.observe(voiceAt(0), handsetLevel)
	for i := 0; i < 8; i++ {
		res := tr.observe(voiceAt(1.2), farFieldLevel)
		if res.OK {
			t.Fatalf("utterance %d: bystander was accepted as the caller: %+v", i, res)
		}
		if res.SwitchReason == switchHandover {
			t.Fatalf("utterance %d: bystander triggered a handover: %+v", i, res)
		}
	}
}

// The handset changes hands: the first caller goes quiet and someone else starts
// talking into it, so they arrive at the same level.
func TestHandsetHandoverSwitchesTheCaller(t *testing.T) {
	tr := testTracker()
	tr.observe(voiceAt(0), handsetLevel)
	tr.observe(voiceAt(0.1), handsetLevel)

	first := tr.observe(voiceAt(1.2), handsetLevel)
	if first.OK {
		t.Fatalf("one utterance is not enough to take over: %+v", first)
	}
	second := tr.observe(voiceAt(1.25), handsetLevel)
	if !second.Switched || second.SwitchReason != switchHandover {
		t.Fatalf("second utterance at handset level should hand the call over: %+v", second)
	}
	if !second.OK {
		t.Error("the new caller's utterance should be accepted")
	}
	if second.SpeakerID != 2 {
		t.Errorf("call handed to speaker %d, want 2", second.SpeakerID)
	}
	// And the original caller is now the one who does not match.
	if res := tr.observe(voiceAt(0), handsetLevel); res.OK {
		t.Errorf("the previous caller should no longer be the primary: %+v", res)
	}
}

// Two people both talking to the bot, alternating. Nobody has left, so the
// enrolled caller keeps the call and the second voice stays annotated — the
// agent decides what to do with it, which is not this layer's call to make.
func TestAlternatingSpeakersDoNotTriggerHandover(t *testing.T) {
	tr := testTracker()
	tr.observe(voiceAt(0), handsetLevel)
	for i := 0; i < 4; i++ {
		if res := tr.observe(voiceAt(1.2), handsetLevel); res.Switched {
			t.Fatalf("round %d: alternating speech must not change the caller: %+v", i, res)
		}
		if res := tr.observe(voiceAt(0.05), handsetLevel); !res.OK {
			t.Fatalf("round %d: the enrolled caller should still match: %+v", i, res)
		}
	}
	if got := len(tr.speakers); got != 2 {
		t.Errorf("tracked %d voices, want 2", got)
	}
}

// Enrolling the wrong voice must not lock the real caller out for the whole
// call. This one is too soft for a handover but still clearly on the handset, so
// the reject streak is what gets them back.
func TestSoftCallerRecoversFromABadEnrollment(t *testing.T) {
	tr := testTracker()
	tr.observe(voiceAt(1.2), handsetLevel) // bystander enrolled first
	streak := dialogpolicy.Defaults().SpkReenrollAfter
	var last Result
	for i := 0; i < streak; i++ {
		last = tr.observe(voiceAt(0), softCaller)
		if last.OK {
			t.Fatalf("turn %d passed against the wrong reference: %+v", i, last)
		}
	}
	if !last.Switched || last.SwitchReason != switchRecovery {
		t.Fatalf("after %d rejections the call should be recovered: %+v", streak, last)
	}
	if res := tr.observe(voiceAt(0.05), softCaller); !res.OK {
		t.Errorf("the caller should match after recovery: %+v", res)
	}
}

// The other side of that floor, and the reason it exists: a far-field voice
// never collects the call, no matter how long it keeps talking. The caller keeps
// their commands; a passer-by never gets them.
func TestFarFieldVoiceNeverRecoversTheCall(t *testing.T) {
	tr := testTracker()
	tr.observe(voiceAt(0), handsetLevel)
	for i := 0; i < 3*dialogpolicy.Defaults().SpkReenrollAfter; i++ {
		res := tr.observe(voiceAt(1.2), farFieldLevel)
		if res.OK || res.Switched {
			t.Fatalf("utterance %d handed the call to a far-field voice: %+v", i, res)
		}
	}
}

// A matching utterance clears the streak, so scattered rejections over a long
// call never add up to an unwanted recovery.
func TestMatchClearsTheRejectStreak(t *testing.T) {
	tr := testTracker()
	tr.observe(voiceAt(0), handsetLevel)
	for i := 0; i < 6; i++ {
		if res := tr.observe(voiceAt(1.2), farFieldLevel); res.Switched {
			t.Fatalf("interleaved rejection %d caused a switch: %+v", i, res)
		}
		tr.observe(voiceAt(0.05), handsetLevel)
	}
}

// A crowded street must not push the caller out of the table.
func TestEvictionNeverDropsTheCaller(t *testing.T) {
	tr := testTracker()
	tr.observe(voiceAt(0), handsetLevel)
	// Far more distinct voices than there are slots.
	for i := 0; i < 12; i++ {
		angle := 0.6 + float64(i)*0.45
		tr.observe(voiceAt(angle), farFieldLevel)
	}
	if got := len(tr.speakers); got > dialogpolicy.Defaults().SpkMaxSpeakers {
		t.Fatalf("tracked %d voices, over the cap", got)
	}
	if res := tr.observe(voiceAt(0), handsetLevel); !res.OK {
		t.Errorf("the caller was evicted by background voices: %+v", res)
	}
}

func TestSpeechRMS(t *testing.T) {
	if got := speechRMS(nil); got != 0 {
		t.Errorf("empty audio: got %v, want 0", got)
	}
	samples := make([]int16, 320)
	for i := range samples {
		samples[i] = 1000
	}
	if got := speechRMS(samples); math.Abs(got-1000) > 1e-6 {
		t.Errorf("constant 1000: got %v", got)
	}
}
