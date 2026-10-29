package calls

import (
	"io"
	"log/slog"
	"sync"
	"testing"

	"github.com/DeepFlowAI/flowkit/internal/session"
	"github.com/DeepFlowAI/flowkit/internal/transport/fakeio"
	"github.com/stretchr/testify/require"
)

// playbackOpsIO records the playback controls the barge controller applies.
type playbackOpsIO struct {
	*fakeio.FakeIO
	mu  sync.Mutex
	ops []string
}

func (p *playbackOpsIO) record(op string) {
	p.mu.Lock()
	p.ops = append(p.ops, op)
	p.mu.Unlock()
}

func (p *playbackOpsIO) PauseOutgoing()       { p.record("pause") }
func (p *playbackOpsIO) ResumeOutgoing()      { p.record("resume") }
func (p *playbackOpsIO) DuckOutgoing(float64) { p.record("duck") }
func (p *playbackOpsIO) RestoreOutgoing()     { p.record("restore") }
func (p *playbackOpsIO) recorded() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.ops...)
}

func TestMergeDialogConfig_BargeDuckOverride(t *testing.T) {
	base := dialogConfig{Duck: true}
	require.True(t, mergeDialogConfig(base, &PerformanceOverrides{}).Duck, "unset keeps the env default")
	off := false
	require.False(t, mergeDialogConfig(base, &PerformanceOverrides{BargeDuck: &off}).Duck)
	on := true
	require.True(t, mergeDialogConfig(dialogConfig{}, &PerformanceOverrides{BargeDuck: &on}).Duck)
}

func TestBargeDuckSwitch(t *testing.T) {
	for _, tc := range []struct {
		name string
		duck bool
		want []string
	}{
		{"duck lowers the volume", true, []string{"duck", "restore"}},
		{"no duck goes silent and resumes", false, []string{"pause", "resume", "restore"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, mgr, h := newTestStack(t)
			call := mgr.Create(session.DirectionInbound)
			mio := &playbackOpsIO{FakeIO: fakeio.New(8)}
			require.NoError(t, call.BindMediaIO(mio))
			t.Cleanup(func() { _ = call.Hangup("done") })
			h.stateFor(call.ID(), nil).playing.Store(true)

			cfg := loadDialogConfig()
			cfg.Adaptive, cfg.Duck, cfg.FullPause = true, tc.duck, true
			cfg.CandidateChars, cfg.ConfirmChars = 2, 6
			dc := h.newDialogControl(call.ID(), cfg, func() {}, slog.New(slog.NewTextHandler(io.Discard, nil)))
			require.NotNil(t, dc)
			t.Cleanup(dc.close)

			dc.barge.onPartial("多少钱")
			dc.barge.onSpeechDiscard() // false trigger: playback comes back
			require.Equal(t, tc.want, mio.recorded())
		})
	}
}
