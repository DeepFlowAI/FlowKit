package asrgate

import (
	"strings"
	"unicode"
)

// TrimLeadingPunct removes leading whitespace/punctuation/symbols that FunASR's
// punctuation model prefixes to a segment that continues a prior utterance
// (e.g. "，你好" → "你好"). Trailing punctuation is kept — it aids readability.
func TrimLeadingPunct(text string) string {
	return strings.TrimLeftFunc(text, func(r rune) bool {
		return unicode.IsSpace(r) || unicode.IsPunct(r) || unicode.IsSymbol(r)
	})
}

// PlausibleUserSpeech returns false for obvious FunASR noise/echo garbage on zh
// audio (pure punctuation, lone ASCII hallucinations, fillers).
func PlausibleUserSpeech(text string) bool {
	text = strings.TrimSpace(text)
	if text == "" {
		return false
	}
	runes := []rune(text)
	if len(runes) < 2 {
		return false
	}

	var han, latin, punct, other, latinRuns int
	inLatinRun := false
	for _, r := range runes {
		switch {
		case unicode.Is(unicode.Han, r):
			han++
			inLatinRun = false
		case unicode.IsLetter(r) && r < 128:
			latin++
			if !inLatinRun {
				latinRuns++
				inLatinRun = true
			}
		case unicode.IsPunct(r) || unicode.IsSymbol(r):
			punct++
			inLatinRun = false
		case unicode.IsLetter(r) || unicode.IsDigit(r):
			other++
			inLatinRun = false
		default:
			other++
			inLatinRun = false
		}
	}

	meaningful := han + latin + other
	if meaningful < 2 {
		return false
	}
	if punct > 0 && punct*2 >= len(runes) {
		return false
	}
	// Silence hallucinations on zh lines are often pure ASCII ("goodo").
	if han == 0 && latin >= 2 {
		return false
	}
	// FunASR silence hallucinations sometimes alternate short ASCII and Han
	// fragments ("gy，一YY一的YY"). A legitimate mixed term such as
	// "iPhone怎么用" has one contiguous ASCII run; two or more runs are noise.
	if han > 0 && latinRuns >= 2 {
		return false
	}
	return !isFillerOnly(text)
}

func isFillerOnly(text string) bool {
	var b strings.Builder
	for _, r := range text {
		if unicode.Is(unicode.Han, r) || (unicode.IsLetter(r) && r < 128) {
			b.WriteRune(r)
		}
	}
	switch strings.TrimSpace(b.String()) {
	case "", "嗯", "啊", "呃", "噢", "额", "恩", "唔", "嘿", "喂":
		return true
	default:
		return false
	}
}
