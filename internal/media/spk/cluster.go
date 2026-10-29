package spk

// A call can pick up more than one voice, and the three ways that happens need
// three different answers:
//
//   - a bystander talking near the caller — answer them and nothing more;
//   - the caller themselves, quieter or hoarser than when they enrolled — must
//     keep working, including the commands they are allowed to issue;
//   - a second person taking the handset over mid-call — from then on they are
//     the caller.
//
// A single enrolled reference cannot tell these apart, because all three score
// low against it. Tracking every voice heard on the call as its own cluster can:
// the bystander stays a separate cluster that never gains ground, while a
// handover shows up as one cluster going quiet exactly as another starts
// carrying the conversation at the same loudness.
//
// Loudness is what makes this work on a phone call specifically. The handset
// sits at the talker's mouth, so whoever holds it arrives 30 dB above anyone in
// the room; measured on a street call, rejected far-field speech peaked at
// 94~197 RMS against the caller's own 5557~7678. A conference microphone has no
// such prior, which is why this trick is worth taking here and not in general.
//
// The tracker deliberately knows nothing about the model: it consumes
// embeddings and speech levels, so all of its behaviour is testable without
// onnxruntime.

import "math"

const (
	// How much of a new observation folds into the cluster it joined. The
	// centroid has to drift, because the caller's own voice drifts: uplink level
	// swings ~17 dB within a single call, and a fixed first-utterance reference
	// slowly stops matching the person it was taken from. Slow enough that one
	// stray assignment cannot walk a cluster onto somebody else.
	centroidWeight = 0.3
	levelWeight    = 0.3
	// How far below the caller a voice may be and still inherit the call as a
	// last resort. Without a floor here a bystander who keeps talking is handed
	// the call by the very mechanism meant to rescue the caller from them.
	//
	// The two distributions this tries to separate overlap: the caller's own
	// level swings ~17 dB within one call (1086 vs 7678 RMS measured), and a
	// bystander sits 17~38 dB down (182 vs the caller's 1386 on the test
	// recording, 94~197 vs 5557~7678 on a street call). So no value splits them
	// in every case, and the choice is which way to fail.
	//
	// It fails safe, at -12 dB: a caller who has gone quiet keeps talking to the
	// bot either way — a mismatch only annotates the turn, it does not drop it —
	// and loses just the irreversible commands until they speak up. Handing the
	// call to a bystander would grant those same commands to a passer-by, which
	// is the worse of the two.
	recoveryLevelFloor = 0.25
)

// speaker is one voice heard on this call.
type speaker struct {
	// centroid is the running mean of the embeddings assigned here, unit length
	// so a dot product stays the cosine similarity.
	centroid []float32
	// level is the smoothed speech RMS of this voice, in int16 scale.
	level float64
	turns int
	// lastTurn is the utterance counter when this voice was last heard, which is
	// how "the caller went quiet" is measured.
	lastTurn int
}

// tracker holds the voices heard on one call and which of them is the caller.
// It is not safe for concurrent use; Gate serialises access.
type tracker struct {
	threshold     float64
	maxSpeakers   int
	handoverTurns int
	levelRatio    float64

	speakers []*speaker
	// primary indexes the caller in speakers, or -1 before enrollment.
	primary int
	turn    int
	// rejectStreak counts consecutive utterances attributed to someone other
	// than the caller, the last-resort escape from having enrolled the wrong
	// voice.
	rejectStreak  int
	reenrollAfter int
}

func newTracker(threshold float64, maxSpeakers, handoverTurns int, levelRatio float64, reenrollAfter int) *tracker {
	return &tracker{
		threshold:     threshold,
		maxSpeakers:   maxSpeakers,
		handoverTurns: handoverTurns,
		levelRatio:    levelRatio,
		primary:       -1,
		reenrollAfter: reenrollAfter,
	}
}

// observe attributes one utterance to a voice and reports whether that voice is
// the caller. level is the RMS of the utterance's voiced audio.
func (t *tracker) observe(emb []float32, level float64) Result {
	t.turn++
	idx, sim, joined := t.assign(emb, level)
	s := t.speakers[idx]

	res := Result{
		SpeakerID: idx + 1,
		Speakers:  len(t.speakers),
		Threshold: float32(t.threshold),
		// Report the score even when it lost — how close the nearest known voice
		// came is what says whether the bar is in the right place, and tuning it
		// needs the rejected side of the distribution as much as the accepted
		// one. Compared says whether it cleared the bar and joined.
		Similarity: float32(sim),
		Compared:   joined,
	}

	// Nobody enrolled yet: whoever spoke first is the caller. Being wrong here
	// is recoverable — that is what handover and the reject streak are for.
	if t.primary < 0 {
		t.primary = idx
		res.OK, res.Enrolled = true, true
		return res
	}
	if idx == t.primary {
		t.rejectStreak = 0
		res.OK = true
		return res
	}

	// Another voice. Either it is taking over the call, or it is someone in the
	// background who should be answered but not obeyed.
	if reason := t.handover(s); reason != "" {
		t.primary = idx
		t.rejectStreak = 0
		res.OK, res.Switched, res.SwitchReason = true, true, reason
		return res
	}
	t.rejectStreak++
	// Last resort: the caller may have been locked out of their own call for a
	// run of turns, either because the wrong voice enrolled or because their own
	// went hoarse or quiet enough to stop matching. Hand the call over rather
	// than keep rejecting them — but only to a voice loud enough to be holding
	// the handset, or a persistent bystander would collect the call here. This
	// utterance stays marked; the next one matches.
	if t.rejectStreak >= t.reenrollAfter && s.level >= t.speakers[t.primary].level*recoveryLevelFloor {
		t.primary = idx
		t.rejectStreak = 0
		res.Switched, res.SwitchReason = true, switchRecovery
	}
	return res
}

// Reasons a call changes hands, reported so the two can be told apart in logs:
// one is the feature working, the other is a bad enrollment being undone.
const (
	switchHandover = "handover"
	switchRecovery = "recovery"
)

// handover reports why s should become the caller, or "" if it should not.
func (t *tracker) handover(s *speaker) string {
	p := t.speakers[t.primary]
	// Has it earned it? One loud sentence from a passer-by is not a handover;
	// carrying the conversation for a couple of turns is.
	if s.turns < t.handoverTurns {
		return ""
	}
	// Has the caller stopped? While they are still talking every other turn,
	// there is no handover — there are two people in the room and the one who
	// enrolled is still the one holding the phone.
	if t.turn-p.lastTurn < t.handoverTurns {
		return ""
	}
	// Is it on the handset? This is the condition that keeps a talkative
	// bystander from inheriting the call: they are far-field, tens of dB down,
	// and never come close to the level the caller arrives at.
	if s.level < p.level*t.levelRatio {
		return ""
	}
	return switchHandover
}

// assign puts an utterance in the closest cluster, or opens a new one when it
// resembles none of them. joined is false when a cluster was created, in which
// case sim describes no comparison.
func (t *tracker) assign(emb []float32, level float64) (idx int, sim float64, joined bool) {
	best, bestSim := -1, 0.0
	for i, s := range t.speakers {
		if v := float64(similarity(s.centroid, emb)); best < 0 || v > bestSim {
			best, bestSim = i, v
		}
	}
	if best >= 0 && bestSim >= t.threshold {
		t.speakers[best].absorb(emb, level, t.turn)
		return best, bestSim, true
	}

	s := &speaker{
		centroid: append(make([]float32, 0, len(emb)), emb...),
		level:    level,
		turns:    1,
		lastTurn: t.turn,
	}
	if len(t.speakers) < t.maxSpeakers {
		t.speakers = append(t.speakers, s)
		return len(t.speakers) - 1, bestSim, false
	}
	// Full. Drop the voice heard longest ago, never the caller: a crowded street
	// would otherwise evict the person we are talking to.
	victim := t.evict()
	t.speakers[victim] = s
	return victim, bestSim, false
}

// absorb folds an utterance into this voice.
func (s *speaker) absorb(emb []float32, level float64, turn int) {
	for i := range s.centroid {
		s.centroid[i] += float32(centroidWeight) * (emb[i] - s.centroid[i])
	}
	normalize(s.centroid)
	if s.level == 0 {
		s.level = level
	} else {
		s.level += levelWeight * (level - s.level)
	}
	s.turns++
	s.lastTurn = turn
}

// evict picks the cluster to recycle: least recently heard, excluding the
// caller.
func (t *tracker) evict() int {
	victim, oldest := -1, 0
	for i, s := range t.speakers {
		if i == t.primary {
			continue
		}
		if victim < 0 || s.lastTurn < oldest {
			victim, oldest = i, s.lastTurn
		}
	}
	if victim < 0 {
		// Only the caller exists, which needs maxSpeakers < 2; policy validation
		// rules that out. Overwrite a non-caller slot rather than the caller.
		return len(t.speakers) - 1
	}
	return victim
}

// speechRMS is the level of an utterance's voiced audio, the loudness half of
// the handover decision.
func speechRMS(samples []int16) float64 {
	if len(samples) == 0 {
		return 0
	}
	var sum float64
	for _, s := range samples {
		f := float64(s)
		sum += f * f
	}
	return math.Sqrt(sum / float64(len(samples)))
}
