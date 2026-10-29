package calls

import "testing"

// spans builds partials from (text, cumulative voiced ms) pairs as the
// listener records them.
func spans(pairs ...any) []partialSpan {
	var out []partialSpan
	prev := 0
	for i := 0; i < len(pairs); i += 2 {
		v := pairs[i+1].(int)
		out = append(out, partialSpan{text: pairs[i].(string), speech: v-prev >= partialSpeechMinGrowthMs})
		prev = v
	}
	return out
}

func TestTrimBackgroundSpeech(t *testing.T) {
	cases := []struct {
		name  string
		final string
		parts []partialSpan
		want  string
	}{
		{
			name:  "background after the caller",
			final: "你会干什么？教你鸡肉更好吃百分百里鲜肉二十",
			parts: spans("你会干", 1216, "什么", 1248, "教你接肉", 1248, "更好吃百", 1248),
			want:  "你会干什么",
		},
		{
			name:  "background before the caller",
			final: "君母爱同源嗯，有什么套餐",
			parts: spans("母母", 0, "爱同", 0, "源", 480, "嗯有什", 1764, "么套", 1924),
			want:  "源嗯，有什么套餐",
		},
		{
			name:  "offline pass rewrote the background",
			final: "的搭档，继自自由的。嗯，好了好了，我再想想",
			parts: spans("这个", 0, "搭档", 0, "继承", 0, "自", 0, "己", 0, "的", 680, "嗯好了", 1112, "好", 1752, "了我再", 2048),
			want:  "的。嗯，好了好了，我再想想",
		},
		{
			name:  "all caller speech",
			final: "呃，之前是什么套餐",
			parts: spans("五", 492, "呃", 1268, "之前是", 1920, "什么套", 2112),
			want:  "呃，之前是什么套餐",
		},
		{
			name:  "background in the middle is kept",
			final: "我想问一下那个套餐多少钱",
			parts: spans("我想问", 600, "一下那", 600, "个套餐多少", 1400),
			want:  "我想问一下那个套餐多少钱",
		},
		{
			name:  "lagging last syllable is kept",
			final: "你会干什么",
			parts: spans("会干什", 1076, "么么", 1076),
			want:  "你会干什么",
		},
		{
			name:  "lone leading partial is kept",
			final: "嗯什么套餐",
			parts: spans("嗯", 0, "什么套", 792, "餐", 824),
			want:  "嗯什么套餐",
		},
		{
			name:  "no partials",
			final: "好的",
			parts: nil,
			want:  "好的",
		},
		{
			name:  "no speech-backed partial",
			final: "冷鲜肉牛奶",
			parts: spans("肉", 0, "肉牛奶", 0),
			want:  "冷鲜肉牛奶",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := trimBackgroundSpeech(tc.final, tc.parts); got != tc.want {
				t.Errorf("trimBackgroundSpeech(%q) = %q, want %q", tc.final, got, tc.want)
			}
		})
	}
}
