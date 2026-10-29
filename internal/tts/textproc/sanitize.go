// Package textproc prepares text for any TTS backend and guards the audio it
// returns. Nothing here depends on a particular model: the same symbol
// cleanup, digit reading, lexicon and audio checks apply whichever
// tts.Provider sits underneath (see Wrap).
package textproc

import "strings"

// preReplacer first folds whitespace, then converts meaningful symbols to
// their spoken Chinese reading (so "≥100" is read "大于等于100", not silently
// dropped) before the markdown noise is removed.
var preReplacer = strings.NewReplacer(
	"\n", " ", "\r", " ", "\t", " ",
	"≥", "大于等于", "≤", "小于等于", "≈", "约等于", "≠", "不等于", "±", "正负",
	"/", "或", "&", "和",
)

// dropChars are markdown / ASCII symbols that carry no spoken meaning but make
// a model's text normalizer switch synthesis mode mid-utterance (e.g. treat a
// clause as English), which shifts the timbre for that sentence. Dropping them
// keeps the utterance pure Chinese so the voice stays consistent.
const dropChars = "#*`_~|^=<>+\\•▪【】（）()《》「」『』〈〉"

// Sanitize converts meaningful symbols to Chinese, strips markdown noise, and
// folds whitespace, leaving clean text for stable synthesis. CJK punctuation
// and digits are kept.
func Sanitize(text string) string {
	text = preReplacer.Replace(text)
	var b strings.Builder
	b.Grow(len(text))
	for _, r := range text {
		if strings.ContainsRune(dropChars, r) {
			continue
		}
		b.WriteRune(r)
	}
	return strings.TrimSpace(strings.Join(strings.Fields(b.String()), " "))
}
