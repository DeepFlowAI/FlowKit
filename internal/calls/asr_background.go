package calls

import (
	"unicode"
)

// A nearby voice (a TV, a livestream, someone across the room) that keeps
// talking runs straight into the caller's own words, and the recogniser closes
// both as one utterance: "你会干什么？教你鸡肉更好吃百分百". The utterance passes
// every acoustic gate on the strength of the caller's part, and the agent then
// answers the background.
//
// The streaming partials say which part was which. Each partial carries the
// voiced time accumulated so far; a partial that added none was recognised
// from audio with no speech at the caller's level. Aligning the partials to the
// final transcript carries that tag over, and leading or trailing runs of
// background are cut. Background in the middle of the utterance is kept: the
// caller was talking on both sides of it, and cutting there risks their words.

// partialSpan is one streaming partial and whether caller speech backed it.
type partialSpan struct {
	text   string
	speech bool
}

// partialSpeechMinGrowthMs is the voiced time a partial has to add to count as
// the caller's. Below it the partial was recognised from audio the voice
// detector did not hear as speech.
const partialSpeechMinGrowthMs = 20

// trimBackgroundSpeech removes leading and trailing transcript recognised from
// background audio. Returns final unchanged when the partials carry no usable
// evidence (none, or none backed by speech).
//
// An edge is only cut when at least two background partials sit there. The
// recogniser often emits the caller's last syllable after the voice detector
// has already closed ("会干什" → "么"), and that lone partial is not background.
func trimBackgroundSpeech(final string, parts []partialSpan) string {
	leadBG, trailBG := edgeBackgroundPartials(parts)
	var pRunes []rune
	var pSpeech []bool
	anySpeech := false
	for _, p := range parts {
		for _, r := range p.text {
			if isTranscriptContent(r) {
				pRunes = append(pRunes, r)
				pSpeech = append(pSpeech, p.speech)
			}
		}
		anySpeech = anySpeech || p.speech
	}
	if !anySpeech || len(pRunes) == 0 {
		return final
	}

	fAll := []rune(final)
	var fIdx []int // positions of content runes in fAll
	var fRunes []rune
	for i, r := range fAll {
		if isTranscriptContent(r) {
			fIdx = append(fIdx, i)
			fRunes = append(fRunes, r)
		}
	}
	if len(fRunes) == 0 {
		return final
	}

	// The final is a second, offline pass over the same audio, so it mostly
	// repeats the partials with corrections. Characters it changed or added
	// take the tag of the nearest aligned neighbour.
	match := alignRunes(fRunes, pRunes)
	tags := make([]int8, len(fRunes)) // 1 speech, -1 background, 0 unknown
	for i, j := range match {
		if j >= 0 {
			if pSpeech[j] {
				tags[i] = 1
			} else {
				tags[i] = -1
			}
		}
	}
	fillUnknownTags(tags)

	first, last := -1, -1
	for i, t := range tags {
		if t > 0 {
			if first < 0 {
				first = i
			}
			last = i
		}
	}
	if first < 0 {
		return final
	}
	start, end := 0, len(fAll)
	if leadBG >= minEdgeBackgroundPartials {
		start = fIdx[first]
	}
	if trailBG >= minEdgeBackgroundPartials {
		end = fIdx[last] + 1
	}
	return string(fAll[start:end])
}

// minEdgeBackgroundPartials is how many background partials an edge needs
// before it is cut.
const minEdgeBackgroundPartials = 2

// edgeBackgroundPartials counts the partials with text before the first and
// after the last speech-backed partial.
func edgeBackgroundPartials(parts []partialSpan) (lead, trail int) {
	first, last := -1, -1
	for i, p := range parts {
		if p.speech {
			if first < 0 {
				first = i
			}
			last = i
		}
	}
	if first < 0 {
		return 0, 0
	}
	for i, p := range parts {
		if !p.speech && hasTranscriptContent(p.text) {
			if i < first {
				lead++
			} else if i > last {
				trail++
			}
		}
	}
	return lead, trail
}

func hasTranscriptContent(s string) bool {
	for _, r := range s {
		if isTranscriptContent(r) {
			return true
		}
	}
	return false
}

// fillUnknownTags gives unaligned characters the tag of the previous aligned
// one, or of the next one at the start.
func fillUnknownTags(tags []int8) {
	var prev int8
	for i, t := range tags {
		if t != 0 {
			prev = t
		} else {
			tags[i] = prev
		}
	}
	var next int8
	for i := len(tags) - 1; i >= 0; i-- {
		if tags[i] != 0 {
			next = tags[i]
		} else {
			tags[i] = next
		}
	}
}

// alignRunes returns, for each rune of a, the index of the rune of b it is
// paired with in a longest common subsequence, or -1. Transcripts are short,
// so the quadratic table is fine.
func alignRunes(a, b []rune) []int {
	n, m := len(a), len(b)
	dp := make([][]int, n+1)
	for i := range dp {
		dp[i] = make([]int, m+1)
	}
	for i := n - 1; i >= 0; i-- {
		for j := m - 1; j >= 0; j-- {
			if a[i] == b[j] {
				dp[i][j] = dp[i+1][j+1] + 1
			} else {
				dp[i][j] = max(dp[i+1][j], dp[i][j+1])
			}
		}
	}
	out := make([]int, n)
	for i := range out {
		out[i] = -1
	}
	i, j := 0, 0
	for i < n && j < m {
		switch {
		case a[i] == b[j]:
			out[i] = j
			i++
			j++
		case dp[i+1][j] >= dp[i][j+1]:
			i++
		default:
			j++
		}
	}
	return out
}

func isTranscriptContent(r rune) bool {
	return !unicode.IsSpace(r) && !unicode.IsPunct(r) && !unicode.IsSymbol(r)
}
