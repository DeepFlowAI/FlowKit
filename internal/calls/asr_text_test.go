package calls

import (
	"testing"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/asrgate"
	"github.com/DeepFlowAI/flowkit/internal/dialogpolicy"
)

func TestAsksQuestion(t *testing.T) {
	cases := []struct {
		text string
		want bool
	}{
		{"请问您要挂哪个科？", true},
		{"Do you want a human?", true},
		{"需要转接人工吗？」", true},
		{"预约挂号可以通过掌上医院APP进行。", false},
		{"您好，", false},
		{"", false},
	}
	for _, c := range cases {
		if got := asksQuestion(c.text); got != c.want {
			t.Errorf("asksQuestion(%q)=%v want %v", c.text, got, c.want)
		}
	}
}

func TestRelaxedMinVoiced(t *testing.T) {
	if got := relaxedMinVoiced(nil); got != 0 {
		t.Fatalf("no call state should keep the default bar, got %v", got)
	}

	asked := &callState{lastSayText: "您是本人来就诊吗？", lastSayAt: time.Now()}
	wantShort := time.Duration(dialogpolicy.Defaults().ASRMinVoicedShortMs) * time.Millisecond
	if got := relaxedMinVoiced(asked); got != wantShort {
		t.Fatalf("right after a question the bar should relax, got %v", got)
	}

	stale := &callState{lastSayText: "您是本人来就诊吗？", lastSayAt: time.Now().Add(-2 * shortReplyWindow)}
	if got := relaxedMinVoiced(stale); got != 0 {
		t.Fatalf("a stale question should not relax the bar, got %v", got)
	}

	statement := &callState{lastSayText: "预约挂号可以通过掌上医院APP进行。", lastSayAt: time.Now()}
	if got := relaxedMinVoiced(statement); got != 0 {
		t.Fatalf("a statement should not relax the bar, got %v", got)
	}
}

// Ducking the playback must cost exactly what being accepted costs, in both the
// strict and the relaxed-after-a-question case. A cheaper bar for interrupting
// lets noise dip the bot's voice and then get thrown out at the final.
func TestBargeBarMatchesAcceptBar(t *testing.T) {
	t.Setenv("SIP_LLM_ASR_MIN_VOICED_MS", "450")
	t.Setenv("SIP_LLM_ASR_MIN_VOICED_SHORT_MS", "150")
	t.Setenv("SIP_LLM_COMMAND_MIN_VOICED_MS", "500")
	p, err := dialogpolicy.LoadFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	dialogpolicy.Set(p)
	t.Cleanup(dialogpolicy.ResetForTest)
	m := asrgate.New()

	if got := m.MinVoicedMs(relaxedMinVoiced(nil)); got != 450 {
		t.Fatalf("strict barge bar=%d want the accept bar 450", got)
	}
	asked := &callState{lastSayText: "您是本人来就诊吗？", lastSayAt: time.Now()}
	if got := m.MinVoicedMs(relaxedMinVoiced(asked)); got != 150 {
		t.Fatalf("barge bar after a question=%d want the relaxed accept bar 150", got)
	}
}

func TestAdaptiveListenUsesShortSpeechBarOnlyDuringPlayback(t *testing.T) {
	t.Setenv("SIP_LLM_ASR_MIN_VOICED_MS", "450")
	t.Setenv("SIP_LLM_ASR_MIN_VOICED_SHORT_MS", "150")
	p, err := dialogpolicy.LoadFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	dialogpolicy.Set(p)
	t.Cleanup(dialogpolicy.ResetForTest)
	m := asrgate.New()

	cs := &callState{}
	cs.adaptiveListen.Store(true)
	cs.playing.Store(true)
	if got := m.MinVoicedMs(listenMinVoiced(cs)); got != 150 {
		t.Fatalf("adaptive listen while playing should accept short speech, got %dms", got)
	}

	cs.playing.Store(false)
	if got := m.MinVoicedMs(listenMinVoiced(cs)); got != 450 {
		t.Fatalf("adaptive listen outside playback must keep the normal bar, got %dms", got)
	}

	cs.adaptiveListen.Store(false)
	cs.playing.Store(true)
	if got := m.MinVoicedMs(listenMinVoiced(cs)); got != 450 {
		t.Fatalf("ordinary listen must keep the normal bar during playback, got %dms", got)
	}
}

// Transcripts from a real browser call that interrupted the bot on a fraction
// of a second of residual noise, next to genuine short replies on the same bar.
func TestFinalMinVoicedScalesRelaxedBarWithLength(t *testing.T) {
	t.Setenv("SIP_LLM_ASR_MIN_VOICED_MS", "450")
	t.Setenv("SIP_LLM_ASR_MIN_VOICED_SHORT_MS", "150")
	p, err := dialogpolicy.LoadFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	dialogpolicy.Set(p)
	t.Cleanup(dialogpolicy.ResetForTest)
	short := 150 * time.Millisecond

	cases := []struct {
		text     string
		voicedMs int
		accept   bool
	}{
		{"？你是个是头", 180, false},
		{"，我吃了你", 260, false},
		{"对", 160, true},
		{"等等", 170, true},
		{"多少钱", 672, true},
		{"什么套牌啊", 776, true},
		{"厂家来看过游戏机要是多少钱", 520, true},
	}
	for _, c := range cases {
		bar := finalMinVoiced(short, c.text)
		got := time.Duration(c.voicedMs)*time.Millisecond >= bar
		if got != c.accept {
			t.Errorf("%q voiced=%dms bar=%v accepted=%v want %v", c.text, c.voicedMs, bar, got, c.accept)
		}
	}
	if got := finalMinVoiced(0, "你是个是头"); got != 0 {
		t.Fatalf("default bar must stay untouched, got %v", got)
	}
}
