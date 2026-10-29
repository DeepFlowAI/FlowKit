package calls

import "testing"

func TestClassifyTurnShape(t *testing.T) {
	cases := []struct {
		text string
		want turnShape
	}{
		// Whole short answers and self-closed sentences: the caller is done.
		{"好的", shapeFinished},
		{"对", shapeFinished},
		{"没有", shapeFinished},
		{"谢谢。", shapeFinished},
		{"我要挂号。", shapeFinished},
		{"你那里能停车吗", shapeFinished},
		{"需要转接人工吧", shapeFinished},
		{"多少钱呢」", shapeFinished},
		// Mid-thought: a pause here is the caller collecting their words.
		{"我想", shapeUnfinished},
		{"然后", shapeUnfinished},
		{"我要挂号，", shapeUnfinished},
		{"我的手机号是一三八", shapeUnfinished},
		{"验证码是9527", shapeUnfinished},
		{"帮我", shapeUnfinished},
		{"嗯", shapeUnfinished},
		// A short transcript is not an answer by itself: ASR partials lag, so
		// these are what a long sentence looks like a moment before it is done.
		{"如何挂号", shapeNeutral},
		{"你会干什", shapeNeutral},
		{"待会可", shapeNeutral},
		{"头腐", shapeNeutral},
		{"好的呀", shapeNeutral},
		{"是不是", shapeNeutral},
		// Neither: the end-of-turn model gets to decide.
		{"我昨天在门诊做了检查", shapeNeutral},
		{"", shapeNeutral},
	}
	for _, c := range cases {
		if got := classifyTurnShape(c.text); got != c.want {
			t.Errorf("classifyTurnShape(%q)=%v want %v", c.text, got, c.want)
		}
	}
}

func TestPauseTargetMs(t *testing.T) {
	if got := pauseTargetMs(shapeFinished); got != defaultFinishedSilenceMs {
		t.Errorf("finished target=%d want %d", got, defaultFinishedSilenceMs)
	}
	if got := pauseTargetMs(shapeNeutral); got != defaultNeutralSilenceMs {
		t.Errorf("neutral target=%d want %d", got, defaultNeutralSilenceMs)
	}
	// An unfinished turn has no early-release bar at all: the ASR's own long
	// fallback is what ends it.
	if got := pauseTargetMs(shapeUnfinished); got != 0 {
		t.Errorf("unfinished target=%d want 0", got)
	}
}
