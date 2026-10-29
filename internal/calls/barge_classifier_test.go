package calls

import "testing"

func TestContainsStopWord(t *testing.T) {
	yes := []string{"等等", "你等一下", "先等一下我", "停一下", "不对不对", "转人工", "我要找人工客服",
		// Corpus-mined additions.
		"稍等", "稍等一下啊", "你稍等", "嗯稍等一下", "等会啊", "是人工吗", "人工服务", "帮我转接一下"}
	for _, s := range yes {
		if !containsStopWord(s) {
			t.Errorf("containsStopWord(%q) = false, want true", s)
		}
	}
	// "稍微"/"等下次" contain 稍/等 but not a stop-word substring → must not fire.
	no := []string{"我想查一下我的水费", "你好", "帮我查询余额", "稍微便宜点", "我等下次再打", ""}
	for _, s := range no {
		if containsStopWord(s) {
			t.Errorf("containsStopWord(%q) = true, want false", s)
		}
	}
}

func TestIsBackchannelOnly(t *testing.T) {
	yes := []string{"嗯", "嗯嗯", "对", "对对对", "对，会告的", "好的", "知道了", "，嗯", "啊",
		// Composed of acknowledgement particles (corpus-mined, previously missed).
		"嗯对", "嗯好的", "对对对对对", "嗯嗯好的", "啊对", "对呀", "嗯行", "没关系"}
	for _, s := range yes {
		if !isBackchannelOnly(s) {
			t.Errorf("isBackchannelOnly(%q) = false, want true", s)
		}
	}
	// Content and greetings contain out-of-set characters → must stay real speech.
	no := []string{"我想查一下", "嗯我想问一下", "好的那你帮我查", "转人工",
		"没有", "什么", "我看一下", "喂你好", "你好"}
	for _, s := range no {
		if isBackchannelOnly(s) {
			t.Errorf("isBackchannelOnly(%q) = true, want false", s)
		}
	}
}

func TestBargeContentRunes(t *testing.T) {
	cases := map[string]int{
		"，我想查一下。": 5,
		"你好":      2,
		"":        0,
		"，。？":     0,
	}
	for in, want := range cases {
		if got := bargeContentRunes(in); got != want {
			t.Errorf("bargeContentRunes(%q) = %d, want %d", in, got, want)
		}
	}
}
