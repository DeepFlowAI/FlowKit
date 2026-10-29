package calls

import "strings"

// What the transcript so far says about whether the caller has finished. The
// same 300 ms pause means opposite things after "好的" and after "我想", so the
// two get different bars before the turn is released. This is the cheap, robust
// half of endpointing: no model, no per-language training, and it is what keeps
// an ordinary short exchange feeling immediate.
type turnShape int

const (
	// Nothing in the text either way — the end-of-turn model decides.
	shapeNeutral turnShape = iota
	// A whole short answer, or a sentence that closed itself.
	shapeFinished
	// Ends on a word nobody stops on: the caller is still collecting their words.
	shapeUnfinished
)

func (s turnShape) String() string {
	switch s {
	case shapeFinished:
		return "finished"
	case shapeUnfinished:
		return "unfinished"
	default:
		return "neutral"
	}
}

const (
	// Trailing silence a turn needs before it may be released, by shape.
	defaultFinishedSilenceMs = 300
	defaultNeutralSilenceMs  = 450
	// Digits in a row at the end read as a number being dictated.
	dictatedDigitRunes = 2
)

// Turns that are an answer in their entirety: the caller said their whole piece
// and stopped. Matched whole, never as a prefix or suffix. ASR partials run
// several characters behind the caller and arrive in fragments, so the
// transcript of a long sentence is briefly short too — treating any short
// transcript as a short answer released turns on the short bar while the caller
// was still talking, and they heard themselves being cut off.
var wholeAnswers = map[string]bool{
	"对": true, "对的": true, "对对": true, "是": true, "是的": true,
	"好": true, "好的": true, "行": true, "有": true, "没": true, "没有": true,
	"不是": true, "不用": true, "不要": true, "没了": true, "就这些": true,
	"知道了": true, "明白了": true, "谢谢": true, "再见": true,
}

// Words that carry a sentence on. A pause right after one of them is the caller
// thinking, not the caller finishing.
var carryOnSuffixes = []string{
	"然后", "接着", "因为", "所以", "但是", "可是", "而且", "还有", "就是", "如果",
	"要是", "比如", "或者", "我想", "我要", "想问", "请问", "帮我", "可以", "那个",
	"这个", "嗯", "呃",
}

// Particles that end a spoken Chinese sentence with no punctuation at all.
var closingParticles = []string{"吗", "呢", "吧"}

// Punctuation that closes a sentence, and the soft kind that means more is coming.
const (
	closingPunct = "。！？.!?；;"
	softPunct    = "，,、"
)

// Chinese numerals plus ASCII digits, for spotting a phone number or ID being
// read out one piece at a time.
const digitRunes = "0123456789零一二三四五六七八九十百千万点"

// classifyTurnShape reads the transcript accumulated so far for this turn.
func classifyTurnShape(text string) turnShape {
	runes := []rune(strings.TrimSpace(text))
	// Drop the quotes and brackets FunASR sometimes trails, but keep punctuation
	// that carries meaning about the ending.
	for len(runes) > 0 && strings.ContainsRune("\"'”’)）】]」』", runes[len(runes)-1]) {
		runes = runes[:len(runes)-1]
	}
	if len(runes) == 0 {
		return shapeNeutral
	}
	last := runes[len(runes)-1]
	trimmed := string(runes)

	if strings.ContainsRune(softPunct, last) {
		return shapeUnfinished // a comma mid-sentence: the thought continues
	}
	if strings.ContainsRune(closingPunct, last) {
		return shapeFinished
	}
	for _, s := range carryOnSuffixes {
		if strings.HasSuffix(trimmed, s) {
			return shapeUnfinished
		}
	}
	if endsInDigits(runes) {
		return shapeUnfinished
	}
	for _, p := range closingParticles {
		if strings.HasSuffix(trimmed, p) {
			return shapeFinished
		}
	}
	if wholeAnswers[strings.Trim(trimmed, softPunct+closingPunct)] {
		return shapeFinished
	}
	return shapeNeutral
}

// endsInDigits reports whether the turn trails off inside a number.
func endsInDigits(runes []rune) bool {
	if len(runes) < dictatedDigitRunes {
		return false
	}
	for _, r := range runes[len(runes)-dictatedDigitRunes:] {
		if !strings.ContainsRune(digitRunes, r) {
			return false
		}
	}
	return true
}

// pauseTargetMs is the trailing silence this shape has to reach before the turn
// may be released early, or 0 when the text says to leave the decision to the
// ASR's own long fallback.
func pauseTargetMs(s turnShape) int {
	switch s {
	case shapeFinished:
		if ms := envPosInt("TURN_FINISHED_SILENCE_MS"); ms > 0 {
			return ms
		}
		return defaultFinishedSilenceMs
	case shapeNeutral:
		if ms := envPosInt("TURN_NEUTRAL_SILENCE_MS"); ms > 0 {
			return ms
		}
		return defaultNeutralSilenceMs
	default:
		return 0
	}
}
