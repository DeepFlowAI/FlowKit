package llm

import (
	"strings"
	"testing"
)

func collectSentences(input string, maxLen int) []string {
	var out []string
	s := NewSentenceSplitter(maxLen, func(sent string) { out = append(out, sent) })
	s.Feed(input)
	s.Flush()
	return out
}

// feedFastFirst streams input one rune at a time and reports the sentences plus
// how many runes had been fed when the first one was emitted.
func feedFastFirst(input string, maxLen, firstMaxLen int) (sentences []string, runesAtFirst int) {
	fed := 0
	s := NewSentenceSplitterFastFirst(maxLen, firstMaxLen, func(sent string) {
		if len(sentences) == 0 {
			runesAtFirst = fed
		}
		sentences = append(sentences, sent)
	})
	for _, r := range input {
		fed++
		s.Feed(string(r))
	}
	s.Flush()
	return sentences, runesAtFirst
}

func TestSplitter_FastFirstStartsAtFirstComma(t *testing.T) {
	// The opening clause runs past maxLen before its next comma, so without the
	// fast first chunk TTS could not start until rune 26.
	text := "您好，我是示例医院的客服，可以为您解答医院相关的各种问题，有什么可以帮您。"
	got, runesAtFirst := feedFastFirst(text, streamSplitMaxLen, streamFirstSplitMaxLen)
	if len(got) == 0 || got[0] != "您好，我是示例医院的客服，" {
		t.Fatalf("first chunk should be the opening clause, got %v", got)
	}
	if runesAtFirst != 21 {
		t.Fatalf("first chunk emitted after %d runes, want 21", runesAtFirst)
	}
}

func TestSplitter_FastFirstNotBareGreeting(t *testing.T) {
	// Call bc82d5d9: "您好，" synthesized alone came back with "好" stretched
	// to ~1s, so the first chunk must run on to the next clause.
	got, _ := feedFastFirst("您好，这里是中国移动，看到您现在的套餐优惠快到期了，这边有个优惠套餐想跟您确认一下。",
		streamSplitMaxLen, streamFirstSplitMaxLen)
	want := []string{"您好，这里是中国移动，", "看到您现在的套餐优惠快到期了，这边有个优惠套餐想跟您确认一下。"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestSplitter_FastFirstOnlyAppliesToFirstChunk(t *testing.T) {
	// "我是客服，" ends before maxLen, so it must stay glued to the rest instead of
	// becoming its own short chunk.
	got, _ := feedFastFirst("您好，这里是医院，我是客服，可以为您解答医院相关的各种问题哦。",
		streamSplitMaxLen, streamFirstSplitMaxLen)
	want := []string{"您好，这里是医院，", "我是客服，可以为您解答医院相关的各种问题哦。"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestSplitter_ShortTailMergedIntoSoftSplitChunk(t *testing.T) {
	// Call 06a2de82: the soft split at "上，" left "您继续使用。" as its own
	// 6-rune chunk, and CosyVoice2 stretched "继" for ~1.3s. The fast first
	// chunk gets the same protection ("打扰您了。").
	text := "您好，这边是中国移动的，打扰您了。给您来电是因为您之前办理的套餐优惠快到期了，我们可以帮您把优惠再延续上，您继续使用。"
	got, _ := feedFastFirst(text, streamSplitMaxLen, streamFirstSplitMaxLen)
	want := []string{
		"您好，这边是中国移动的，打扰您了。",
		"给您来电是因为您之前办理的套餐优惠快到期了，我们可以帮您把优惠再延续上，您继续使用。",
	}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestSplitter_SoftSplitKeptWhenTailIsLong(t *testing.T) {
	text := "给您来电是因为您之前办理的套餐优惠快到期了，我们可以帮您把优惠再延续上，您后续继续使用原来的套餐资费都不会变。"
	got, _ := feedFastFirst(text, streamSplitMaxLen, streamFirstSplitMaxLen)
	want := []string{
		"给您来电是因为您之前办理的套餐优惠快到期了，",
		"我们可以帮您把优惠再延续上，您后续继续使用原来的套餐资费都不会变。",
	}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestSplitter_ShortTailWithoutEnderFlushed(t *testing.T) {
	got, _ := feedFastFirst("您好，我们可以帮您把之前办理的那个套餐优惠活动再延续上，您继续用",
		streamSplitMaxLen, streamFirstSplitMaxLen)
	want := []string{"您好，我们可以帮您把之前办理的那个套餐优惠活动再延续上，您继续用"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestSplitter_NumberedListNotIsolated(t *testing.T) {
	got := collectSentences("您可以这样：1.通过官网查询；2.关注公众号；3.用App查询。", 80)
	for _, s := range got {
		if s == "1." || s == "2." || s == "3." {
			t.Fatalf("isolated list marker %q in %v", s, got)
		}
	}
	if !strings.Contains(strings.Join(got, "|"), "1.通过官网查询") {
		t.Fatalf("list item split wrong: %v", got)
	}
}

func TestSplitter_DecimalNotSplit(t *testing.T) {
	got := collectSentences("水价是3.14元。", 80)
	if len(got) != 1 || got[0] != "水价是3.14元。" {
		t.Fatalf("decimal split: %v", got)
	}
}

func TestSplitter_NormalSentencesStillSplit(t *testing.T) {
	got := collectSentences("你好。今天天气不错！", 80)
	if len(got) != 2 {
		t.Fatalf("expected 2 sentences, got %v", got)
	}
}

func TestSplitter_SoftSplitEarlierForLongClause(t *testing.T) {
	// Converse uses maxLen=24 so a long first clause can soft-split on "，"
	// before the hard "。" — first TTS can start sooner.
	text := "您好，我是某某理工大学某某医学院附属第一医院的客服，可以为您解答医院相关的问题。"
	got := collectSentences(text, 24)
	if len(got) < 2 {
		t.Fatalf("expected soft-split into multiple chunks, got %v", got)
	}
	if !strings.HasSuffix(got[0], "，") {
		t.Fatalf("first chunk should soft-split on comma, got %q", got[0])
	}
}
