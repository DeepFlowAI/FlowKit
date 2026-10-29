package calls

import (
	"math"
	"strings"
	"time"
	"unicode"

	"github.com/DeepFlowAI/flowkit/internal/dialogpolicy"
)

const (
	// How long after a question the caller is still expected to be answering it.
	shortReplyWindow = 6 * time.Second
)

// relaxedMinVoiced returns the voiced-time bar to apply to the utterance being
// finalised, or 0 to keep the configured default. A question invites a one-word
// answer, so for a short window afterwards the bar drops far enough for "对" to
// pass; the rest of the call keeps the strict bar that rejects noise.
func relaxedMinVoiced(cs *callState) time.Duration {
	if cs == nil {
		return 0
	}
	cs.sayMu.Lock()
	text := cs.lastSayText
	at := cs.lastSayAt
	cs.sayMu.Unlock()
	if at.IsZero() || time.Since(at) > shortReplyWindow || !asksQuestion(text) {
		return 0
	}
	ms := dialogpolicy.Get().ASRMinVoicedShortMs
	return time.Duration(ms) * time.Millisecond
}

// listenMinVoiced keeps normal listening strict, but uses the shared short-
// utterance threshold while an explicitly adaptive listener is competing with
// active playback. Short stop phrases must reach the same dialog controller
// that already decides whether they are safe to interrupt on.
func listenMinVoiced(cs *callState) time.Duration {
	if cs != nil && cs.adaptiveListen.Load() && cs.playing.Load() {
		ms := dialogpolicy.Get().ASRMinVoicedShortMs
		return time.Duration(ms) * time.Millisecond
	}
	return relaxedMinVoiced(cs)
}

// shortReplyVoicedPerRune is the least speech one transcribed character can be
// backed by. Spoken Chinese runs at roughly 4-5 characters a second, so a real
// utterance carries well over this per character; a long transcript behind a
// fraction of a second of speech is ASR inventing words from noise or a voice
// far from the handset.
const shortReplyVoicedPerRune = 80 * time.Millisecond

// finalMinVoiced is the voiced-time bar for a finalised transcript. A relaxed
// bar (base > 0) exists so a one- or two-character reply ("对" / "等等") gets
// through; it scales with the transcript's length so a five-character sentence
// cannot ride on it, and never rises above the normal bar. base == 0 keeps the
// configured default untouched.
func finalMinVoiced(base time.Duration, text string) time.Duration {
	if base <= 0 {
		return 0
	}
	def := time.Duration(dialogpolicy.Get().ASRMinVoicedMs) * time.Millisecond
	need := time.Duration(bargeContentRunes(text)) * shortReplyVoicedPerRune
	return max(base, min(need, def))
}

// commandMinVoicedMs is the voiced time an utterance needs before an
// irreversible command derived from it (transfer, hang up) is executed.
func commandMinVoicedMs() int {
	return dialogpolicy.Get().CommandMinVoicedMs
}

// asksQuestion reports whether text ends in a question mark, ignoring trailing
// quotes and brackets.
func asksQuestion(text string) bool {
	runes := []rune(strings.TrimSpace(text))
	for len(runes) > 0 {
		last := runes[len(runes)-1]
		if last == '？' || last == '?' {
			return true
		}
		if !unicode.IsPunct(last) && !unicode.IsSpace(last) {
			return false
		}
		runes = runes[:len(runes)-1]
	}
	return false
}

// roundedSim trims a similarity score to three decimals, which is all a log line
// needs to be read against a threshold.
func roundedSim(v float32) float64 {
	return math.Round(float64(v)*1000) / 1000
}

// normalizeASRCompare strips punctuation for echo matching.
func normalizeASRCompare(s string) string {
	var b strings.Builder
	for _, r := range s {
		if unicode.Is(unicode.Han, r) {
			b.WriteRune(r)
		} else if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(unicode.ToLower(r))
		}
	}
	return b.String()
}

// likelyEchoASR reports whether user text is probably playback leaking into the mic.
func likelyEchoASR(user, lastAssistant string) bool {
	u := normalizeASRCompare(user)
	a := normalizeASRCompare(lastAssistant)
	if u == "" || a == "" {
		return false
	}
	if u == a {
		return true
	}
	// Short utterance fully contained in the last TTS script (common echo).
	if len([]rune(u)) <= 8 && strings.Contains(a, u) {
		return true
	}
	return false
}

const (
	// Shorter transcripts carry too little text to tell an echo from a caller
	// quoting the bot back ("九十块钱吗" over "……九十块钱……").
	echoOverlapMinRunes = 6
	// Share of the transcript, in order, that must also appear in what the bot
	// just said. Real callers' turns on recorded calls stay at or below about
	// half; leaked playback that ASR garbled a little stays well above.
	echoOverlapRatio = 0.75
)

// likelyEchoOverPlayback is the looser echo test for an utterance finalised
// while the bot is still talking: the caller's handset echo canceller lets
// the bot's words back up the line, and ASR returns them with a few
// characters dropped or misheard, so an exact match is too strict. recent is
// what the bot said most recently (the echo can trail the sentence change).
func likelyEchoOverPlayback(user string, recent ...string) bool {
	u := []rune(normalizeASRCompare(user))
	if len(u) < echoOverlapMinRunes {
		return false
	}
	var b strings.Builder
	for _, s := range recent {
		b.WriteString(normalizeASRCompare(s))
	}
	a := []rune(b.String())
	if len(a) == 0 {
		return false
	}
	return float64(lcsLen(u, a)) >= echoOverlapRatio*float64(len(u))
}

// lcsLen is the length of the longest common subsequence of a and b.
func lcsLen(a, b []rune) int {
	prev := make([]int, len(b)+1)
	cur := make([]int, len(b)+1)
	for _, ra := range a {
		for j, rb := range b {
			if ra == rb {
				cur[j+1] = prev[j] + 1
			} else {
				cur[j+1] = max(prev[j+1], cur[j])
			}
		}
		prev, cur = cur, prev
	}
	return prev[len(b)]
}
