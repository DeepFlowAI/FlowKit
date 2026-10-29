package asrgate

import "testing"

func TestTrimLeadingPunct(t *testing.T) {
	cases := map[string]string{
		"，你好":       "你好",
		"，我在看":      "我在看",
		"。 你在干什么":   "你在干什么",
		"你好啊":       "你好啊",   // nothing to trim
		"你好。":       "你好。",   // trailing kept
		" ,， hello": "hello", // mixed leading space + ascii/cjk punct
		"":          "",
		"，。！":       "", // all punctuation
	}
	for in, want := range cases {
		if got := TrimLeadingPunct(in); got != want {
			t.Errorf("TrimLeadingPunct(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestPlausibleUserSpeech(t *testing.T) {
	cases := []struct {
		text string
		want bool
	}{
		{"停水了怎么办", true},
		{"iPhone怎么用", true},
		{"gy，一YY一的YY", false},
		{"good", false},
		{"嗯", false},
		{"，。！", false},
	}
	for _, tc := range cases {
		if got := PlausibleUserSpeech(tc.text); got != tc.want {
			t.Errorf("PlausibleUserSpeech(%q) = %v, want %v", tc.text, got, tc.want)
		}
	}
}
