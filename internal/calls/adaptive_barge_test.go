package calls

import (
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"
)

// bargeProbe records the audible callbacks the state machine fires, so tests
// can assert exactly which actions each evidence level triggers.
type bargeProbe struct {
	mu       sync.Mutex
	ducks    int
	restores int
	pauses   int
	resumes  int
	confirms int
	duckOK   bool
	pauseOK  bool
}

func newBargeProbe() *bargeProbe { return &bargeProbe{duckOK: true, pauseOK: true} }

func (p *bargeProbe) build(cfg bargeCfg) *adaptiveBarge {
	return newAdaptiveBarge("call", cfg, slog.New(slog.NewTextHandler(io.Discard, nil)),
		func() bool {
			p.mu.Lock()
			defer p.mu.Unlock()
			if !p.duckOK {
				return false
			}
			p.ducks++
			return true
		},
		func() { p.mu.Lock(); p.restores++; p.mu.Unlock() },
		func() bool {
			p.mu.Lock()
			defer p.mu.Unlock()
			if !p.pauseOK {
				return false
			}
			p.pauses++
			return true
		},
		func() { p.mu.Lock(); p.resumes++; p.mu.Unlock() },
		func() { p.mu.Lock(); p.confirms++; p.mu.Unlock() },
	)
}

func (p *bargeProbe) counts() (ducks, restores, pauses, resumes, confirms int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.ducks, p.restores, p.pauses, p.resumes, p.confirms
}

func (p *bargeProbe) assert(t *testing.T, ducks, restores, pauses, resumes, confirms int) {
	t.Helper()
	d, r, pa, re, c := p.counts()
	if d != ducks || r != restores || pa != pauses || re != resumes || c != confirms {
		t.Fatalf("duck/restore/pause/resume/confirm = %d/%d/%d/%d/%d, want %d/%d/%d/%d/%d",
			d, r, pa, re, c, ducks, restores, pauses, resumes, confirms)
	}
}

func waitBarge(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met before deadline")
}

func (b *adaptiveBarge) stateNow() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

func testBargeCfg() bargeCfg {
	return bargeCfg{
		candidateChars: 2,
		confirmChars:   4,
		maxPause:       time.Second,
		onsetGrace:     time.Second,
		falseInterrupt: time.Second,
		onsetAction:    "none",
		resumeOnFalse:  true,
	}
}

func TestBargeOnsetAloneIsSilent(t *testing.T) {
	p := newBargeProbe()
	cfg := testBargeCfg()
	cfg.onsetGrace = 20 * time.Millisecond
	b := p.build(cfg)

	b.onSpeechStart()
	p.assert(t, 0, 0, 0, 0, 0)

	b.mu.Lock()
	gen := b.timerGen
	b.mu.Unlock()
	b.onWatchTimeout(gen)
	if b.stateNow() != bargeWatching {
		t.Fatalf("state = %d, want watching until ASR resolves the utterance", b.stateNow())
	}
	p.assert(t, 0, 0, 0, 0, 0)

	b.onSpeechDiscard()
	if b.stateNow() != bargeIdle {
		t.Fatalf("state = %d, want idle after discard", b.stateNow())
	}
}

func TestBargeLateFinalAfterOnsetGraceStillConfirms(t *testing.T) {
	p := newBargeProbe()
	cfg := testBargeCfg()
	cfg.onsetGrace = 20 * time.Millisecond
	b := p.build(cfg)

	b.onSpeechStart()
	b.mu.Lock()
	gen := b.timerGen
	b.mu.Unlock()
	b.onWatchTimeout(gen)

	if !b.onSpeechEnd("等一下") {
		t.Fatal("real final should start a turn after delayed ASR")
	}
	p.assert(t, 0, 0, 0, 0, 1)
}

func TestBargeBackchannelNeverActs(t *testing.T) {
	p := newBargeProbe()
	b := p.build(testBargeCfg())

	b.onSpeechStart()
	b.onPartial("嗯")
	b.onPartial("对对")
	if b.onSpeechEnd("嗯对对") {
		t.Fatal("backchannel final should not start a turn")
	}
	p.assert(t, 0, 0, 0, 0, 0)
}

// With nothing playing the utterance is an answer, not an interruption: "可以"
// after "您看愿意办理吗？" must reach the agent even though it would only be a
// backchannel had the bot still been talking.
func TestBargeAcknowledgementWhileSilentStartsTurn(t *testing.T) {
	p := newBargeProbe()
	b := p.build(testBargeCfg())

	for _, final := range []string{"可以", "好的", "对的", "嗯对对"} {
		if !b.onSpeechEnd(final) {
			t.Fatalf("%q with nothing playing should start a turn", final)
		}
	}
	if b.onSpeechEnd("。") {
		t.Fatal("implausible speech should still not start a turn")
	}
	p.assert(t, 0, 0, 0, 0, 0)
}

func TestBargeAcknowledgementWhilePlayingKeepsReply(t *testing.T) {
	p := newBargeProbe()
	b := p.build(testBargeCfg())

	b.onSpeechStart()
	b.onPartial("可以")
	if b.onSpeechEnd("可以") {
		t.Fatal("an acknowledgement while the bot is talking should not start a turn")
	}
	p.assert(t, 0, 0, 0, 0, 0)
}

func TestBargeDiscardWhileWatchingIsSilent(t *testing.T) {
	p := newBargeProbe()
	b := p.build(testBargeCfg())

	b.onSpeechStart()
	b.onSpeechDiscard()
	p.assert(t, 0, 0, 0, 0, 0)
	if b.stateNow() != bargeIdle {
		t.Fatalf("state = %d, want idle", b.stateNow())
	}

	// The reset must not leak into the next utterance: it still escalates.
	b.onSpeechStart()
	b.onPartial("停水")
	p.assert(t, 1, 0, 0, 0, 0)
}

func TestBargeEscalatesDuckThenPauseThenConfirm(t *testing.T) {
	p := newBargeProbe()
	b := p.build(testBargeCfg())

	b.onSpeechStart()
	b.onPartial("停水") // 2 content runes → duck
	p.assert(t, 1, 0, 0, 0, 0)

	b.onPartial("了怎么办") // 6 total ≥ confirmChars → lossless pause, reply kept
	p.assert(t, 1, 0, 1, 0, 0)

	if !b.onSpeechEnd("停水了怎么办") {
		t.Fatal("real final should start a turn")
	}
	p.assert(t, 1, 0, 1, 0, 1)
}

func TestBargeStopWordConfirmsFromWatching(t *testing.T) {
	p := newBargeProbe()
	b := p.build(testBargeCfg())

	b.onSpeechStart()
	b.onPartial("等等")
	p.assert(t, 0, 0, 0, 0, 1)
}

func TestBargeStopWordConfirmsFromDucked(t *testing.T) {
	p := newBargeProbe()
	b := p.build(testBargeCfg())

	b.onSpeechStart()
	b.onPartial("停水")
	p.assert(t, 1, 0, 0, 0, 0)
	b.onPartial("别说了")
	p.assert(t, 1, 0, 0, 0, 1)
}

func TestBargeFalseInterruptResumes(t *testing.T) {
	p := newBargeProbe()
	cfg := testBargeCfg()
	cfg.falseInterrupt = 20 * time.Millisecond
	b := p.build(cfg)

	b.onSpeechStart()
	b.onPartial("停水了怎么办") // straight past confirmChars → yield
	p.assert(t, 0, 0, 1, 0, 0)

	waitBarge(t, func() bool { _, _, _, re, _ := p.counts(); return re == 1 })
	p.assert(t, 0, 0, 1, 1, 0)
	if b.stateNow() != bargeIdle {
		t.Fatalf("state = %d, want idle after resume", b.stateNow())
	}
}

func TestBargeFalseInterruptWithoutResumeConfirms(t *testing.T) {
	p := newBargeProbe()
	cfg := testBargeCfg()
	cfg.falseInterrupt = 20 * time.Millisecond
	cfg.resumeOnFalse = false
	b := p.build(cfg)

	b.onSpeechStart()
	b.onPartial("停水了怎么办")
	p.assert(t, 0, 0, 1, 0, 0)

	waitBarge(t, func() bool { _, _, _, _, c := p.counts(); return c == 1 })
	p.assert(t, 0, 0, 1, 0, 1)
}

func TestBargeDiscardWhileDuckedRestores(t *testing.T) {
	p := newBargeProbe()
	b := p.build(testBargeCfg())

	b.onSpeechStart()
	b.onPartial("停水")
	p.assert(t, 1, 0, 0, 0, 0)

	b.onSpeechDiscard()
	p.assert(t, 1, 1, 0, 0, 0)
}

func TestBargeDiscardWhileYieldedResumes(t *testing.T) {
	p := newBargeProbe()
	b := p.build(testBargeCfg())

	b.onSpeechStart()
	b.onPartial("停水了怎么办")
	p.assert(t, 0, 0, 1, 0, 0)

	b.onSpeechDiscard()
	p.assert(t, 0, 0, 1, 1, 0)
}

func TestBargeYieldFallsBackToDuckWhenPauseUnavailable(t *testing.T) {
	p := newBargeProbe()
	p.pauseOK = false
	b := p.build(testBargeCfg())

	b.onSpeechStart()
	b.onPartial("停水了怎么办")
	p.assert(t, 1, 0, 0, 0, 0)

	b.onSpeechDiscard()
	p.assert(t, 1, 1, 0, 0, 0)
}

func TestBargeYieldConfirmsWhenNothingPlaying(t *testing.T) {
	p := newBargeProbe()
	p.pauseOK = false
	p.duckOK = false
	b := p.build(testBargeCfg())

	b.onSpeechStart()
	b.onPartial("停水了怎么办")
	p.assert(t, 0, 0, 0, 0, 1)
}

func TestBargeDoesNotReduckSameUtteranceAfterTimeout(t *testing.T) {
	p := newBargeProbe()
	b := p.build(testBargeCfg())

	b.onSpeechStart()
	b.onPartial("停水")
	p.assert(t, 1, 0, 0, 0, 0)

	// Fire the duck safety timeout deterministically.
	b.mu.Lock()
	gen := b.timerGen
	b.mu.Unlock()
	b.onDuckTimeout(gen)
	p.assert(t, 1, 1, 0, 0, 0)

	b.onSpeechStart() // same utterance, choppy VAD rising edge
	b.onPartial("了")  // still ≥ candidateChars, but the utterance was observed
	p.assert(t, 1, 1, 0, 0, 0)

	// A real final can still confirm the interruption.
	if !b.onSpeechEnd("停水了没人修") {
		t.Fatal("real final should start a turn")
	}
	p.assert(t, 1, 1, 0, 0, 1)
}

func TestBargeOnsetActionDuckCompat(t *testing.T) {
	p := newBargeProbe()
	cfg := testBargeCfg()
	cfg.onsetAction = "duck"
	cfg.onsetGrace = 20 * time.Millisecond
	b := p.build(cfg)

	b.onSpeechStart()
	p.assert(t, 1, 0, 0, 0, 0)

	waitBarge(t, func() bool { _, r, _, _, _ := p.counts(); return r == 1 })
	p.assert(t, 1, 1, 0, 0, 0)
}

func TestBargeOnsetActionPauseCompat(t *testing.T) {
	p := newBargeProbe()
	cfg := testBargeCfg()
	cfg.onsetAction = "pause"
	cfg.onsetGrace = 20 * time.Millisecond
	b := p.build(cfg)

	b.onSpeechStart()
	p.assert(t, 0, 0, 1, 0, 0)

	waitBarge(t, func() bool { _, _, _, re, _ := p.counts(); return re == 1 })
	p.assert(t, 0, 0, 1, 1, 0)
}

func TestBargeCloseUnwindsYield(t *testing.T) {
	p := newBargeProbe()
	b := p.build(testBargeCfg())

	b.onSpeechStart()
	b.onPartial("停水了怎么办")
	p.assert(t, 0, 0, 1, 0, 0)

	b.close()
	p.assert(t, 0, 0, 1, 1, 0)
}
