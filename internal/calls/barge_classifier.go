package calls

import (
	"strings"
	"unicode"

	"github.com/DeepFlowAI/flowkit/internal/asrgate"
)

// bargeStopWords are explicit "stop talking" cues. When the user's transcript
// contains any of them, the interruption is confirmed immediately regardless of
// length — these are unambiguous commands to the bot.
// Mined from 264k real customer-service user turns (searchmodeltrain corpus):
// "稍等" is by far the most common explicit hold cue and was previously missing.
// Matched as substrings, so "嗯稍等一下啊" / "你稍等" / "是人工吗" all hit.
var bargeStopWords = []string{
	"等等", "等一下", "等一等", "稍等", "等会", "先等", "停一下", "停停",
	"别说", "别念", "打住", "闭嘴", "不对", "不是这", "不是这个", "错了",
	"人工", "转接",
}

// bargeBackchannels holds multi-character acknowledgements that are NOT pure
// particle strings (those are covered by isBackchannelComposed). Mined from real
// customer turns.
var bargeBackchannels = map[string]struct{}{
	"嗯哼": {}, "可以": {}, "知道": {}, "知道了": {}, "明白": {}, "明白了": {},
	"了解": {}, "收到": {}, "清楚": {}, "没关系": {}, "没问题": {},
	"ok": {}, "okok": {},
	// Observed FunASR 2-pass correction for a spoken "对对对".
	"对会告的": {},
}

// bargeBackchannelRunes are the particle / acknowledgement characters that, in
// the 264k-turn corpus, compose the overwhelming majority of backchannels
// (嗯 / 嗯对 / 嗯好的 / 对对对对 / 嗯嗯好的 …, 804 distinct variants). Rather than
// enumerate every combination, any utterance made ENTIRELY of these counts as an
// acknowledgement. Validated to not misclassify content ("没有" / "什么" / "我看
// 一下") or greetings ("喂你好" / "你好"), which all contain out-of-set characters.
var bargeBackchannelRunes = func() map[rune]struct{} {
	m := make(map[rune]struct{})
	for _, r := range "嗯恩呃呢啊哦噢喔唔哎唉诶对是好行的呀吧呐咯啦哈" {
		m[r] = struct{}{}
	}
	return m
}()

// isBackchannelComposed reports whether the (already normalized) text is made up
// only of acknowledgement particles — a backchannel regardless of length.
func isBackchannelComposed(norm string) bool {
	if norm == "" {
		return false
	}
	for _, r := range norm {
		if _, ok := bargeBackchannelRunes[r]; !ok {
			return false
		}
	}
	return true
}

// containsStopWord reports whether text carries an explicit stop cue.
func containsStopWord(text string) bool {
	t := normalizeBargeText(text)
	for _, w := range bargeStopWords {
		if strings.Contains(t, w) {
			return true
		}
	}
	return false
}

// isBackchannelOnly reports whether the whole utterance is just acknowledgement
// or filler (so it must not interrupt the bot).
func isBackchannelOnly(text string) bool {
	t := normalizeBargeText(text)
	if t == "" {
		return false
	}
	if _, ok := bargeBackchannels[t]; ok {
		return true
	}
	if isBackchannelComposed(t) {
		return true
	}
	// Anything the shared gate already rejects as implausible speech (single
	// filler chars, punctuation, noise) also counts as non-interrupting.
	return !asrgate.PlausibleUserSpeech(text)
}

// bargeContentRunes counts the meaningful runes in text (after dropping leading
// ASR punctuation and surrounding space). Used as the "how much real speech so
// far" signal for the candidate → confirm escalation.
func bargeContentRunes(text string) int {
	return len([]rune(normalizeBargeText(text)))
}

func normalizeBargeText(text string) string {
	t := asrgate.TrimLeadingPunct(strings.TrimSpace(text))
	t = strings.Map(func(r rune) rune {
		if unicode.IsSpace(r) || unicode.IsPunct(r) || unicode.IsSymbol(r) {
			return -1
		}
		return r
	}, t)
	return strings.ToLower(t)
}
