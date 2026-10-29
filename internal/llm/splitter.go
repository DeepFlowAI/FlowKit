package llm

import "strings"

// SentenceSplitter accumulates streaming tokens into speakable sentences.
type SentenceSplitter struct {
	buf         strings.Builder
	maxLen      int
	firstMaxLen int
	emitted     bool
	onSentence  func(string)
}

// NewSentenceSplitter creates a splitter; maxLen defaults to 80 when <= 0.
func NewSentenceSplitter(maxLen int, onSentence func(string)) *SentenceSplitter {
	if maxLen <= 0 {
		maxLen = 80
	}
	return &SentenceSplitter{maxLen: maxLen, onSentence: onSentence}
}

// NewSentenceSplitterFastFirst creates a splitter whose first chunk may end at
// the first punctuation past firstMaxLen runes, while later chunks use maxLen.
// A short opening chunk ("您好，这里是中国移动，") reaches TTS while the model is
// still writing the rest of the reply, so the caller hears speech sooner; the
// remaining chunks stay long enough to keep prosody natural.
func NewSentenceSplitterFastFirst(maxLen, firstMaxLen int, onSentence func(string)) *SentenceSplitter {
	s := NewSentenceSplitter(maxLen, onSentence)
	if firstMaxLen > 0 && firstMaxLen < s.maxLen {
		s.firstMaxLen = firstMaxLen
	}
	return s
}

// Feed appends a token delta; may invoke onSentence zero or more times.
func (s *SentenceSplitter) Feed(text string) {
	s.buf.WriteString(text)
	for {
		cur := s.buf.String()
		idx := findSentenceBoundary(cur, s.boundaryMaxLen(), softSplitMinTail)
		if idx < 0 {
			return
		}
		sentence := strings.TrimSpace(cur[:idx])
		if sentence != "" {
			s.emitted = true
			s.onSentence(sentence)
		}
		s.buf.Reset()
		if idx < len(cur) {
			s.buf.WriteString(cur[idx:])
		}
	}
}

// Flush emits any trailing buffer as the final sentence.
func (s *SentenceSplitter) Flush() {
	rest := strings.TrimSpace(s.buf.String())
	if rest != "" {
		s.emitted = true
		s.onSentence(rest)
	}
	s.buf.Reset()
}

func (s *SentenceSplitter) boundaryMaxLen() int {
	if !s.emitted && s.firstMaxLen > 0 {
		return s.firstMaxLen
	}
	return s.maxLen
}

// softSplitMinTail is how many runes must follow a soft (comma) boundary before
// the splitter commits to it. A sentence end inside that span pulls the short
// tail into the same chunk: CosyVoice2 often stretches one character of a
// tiny standalone chunk ("您继续使用。") for a second or more.
const softSplitMinTail = 8

const (
	hardEnders = "。！？.!?；;\n"
	softEnders = "，,、 "
)

func isHardEnd(runes []rune, i int) bool {
	if !strings.ContainsRune(hardEnders, runes[i]) {
		return false
	}
	// An ASCII '.' right after a digit is a decimal point or list marker
	// ("3.14", "1.通过…"), not a sentence end. Splitting there isolates a
	// bare "1." segment, which CosyVoice's TN then reads as English "one".
	return !(runes[i] == '.' && i > 0 && runes[i-1] >= '0' && runes[i-1] <= '9')
}

func findSentenceBoundary(s string, maxLen, minTail int) int {
	runes := []rune(s)
	for i, r := range runes {
		if isHardEnd(runes, i) {
			return runeIndexToByteIndex(s, i+1)
		}
		if i+1 >= maxLen && strings.ContainsRune(softEnders, r) {
			for j := i + 1; j < len(runes) && j <= i+minTail; j++ {
				if isHardEnd(runes, j) {
					return runeIndexToByteIndex(s, j+1)
				}
			}
			if len(runes)-(i+1) < minTail {
				return -1 // tail still short — wait for more text (Flush emits it)
			}
			return runeIndexToByteIndex(s, i+1)
		}
	}
	return -1
}

func runeIndexToByteIndex(s string, runeIdx int) int {
	count := 0
	for i := range s {
		if count == runeIdx {
			return i
		}
		count++
	}
	return len(s)
}
