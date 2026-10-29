package session

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/media/bus"
	"github.com/DeepFlowAI/flowkit/internal/observability"
	"github.com/DeepFlowAI/flowkit/internal/transport"
	"github.com/DeepFlowAI/flowkit/internal/transport/fakeio"
	"github.com/DeepFlowAI/flowkit/internal/transport/pacer"
	"github.com/stretchr/testify/require"
)

// slogRecorder captures Info-level records for call.trace assertions.
type slogRecorder struct {
	mu      sync.Mutex
	records []map[string]any
}

func (r *slogRecorder) Enabled(context.Context, slog.Level) bool { return true }

func (r *slogRecorder) Handle(_ context.Context, rec slog.Record) error {
	m := map[string]any{"msg": rec.Message}
	rec.Attrs(func(a slog.Attr) bool {
		m[a.Key] = a.Value.Any()
		return true
	})
	r.mu.Lock()
	r.records = append(r.records, m)
	r.mu.Unlock()
	return nil
}

func (r *slogRecorder) WithAttrs(attrs []slog.Attr) slog.Handler {
	return r
}

func (r *slogRecorder) WithGroup(string) slog.Handler { return r }

func (r *slogRecorder) findEvent(event string) map[string]any {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, rec := range r.records {
		if rec["msg"] == observability.CallTraceMessage && rec["event"] == event {
			return rec
		}
	}
	return nil
}

func newRecordingCall(t *testing.T) (*Call, *slogRecorder) {
	t.Helper()
	rec := &slogRecorder{}
	c := NewCall("obs-"+t.Name(), DirectionInbound, slog.New(rec))
	return c, rec
}

func TestProbeMediaIOCaps_FakeIOMissingOnlyDTMF(t *testing.T) {
	have, missing := probeMediaIOCaps(fakeio.New(1))
	require.Equal(t, "none", have)
	require.Equal(t, "DTMFReceiver,DTMFSender", missing)
}

type dtmfSenderIO struct {
	*fakeio.FakeIO
}

func (d *dtmfSenderIO) SendDTMF(string, int) error { return nil }

var _ transport.MediaIO = (*dtmfSenderIO)(nil)
var _ transport.DTMFSender = (*dtmfSenderIO)(nil)

func TestProbeMediaIOCaps_ReportsPartialHave(t *testing.T) {
	have, missing := probeMediaIOCaps(&dtmfSenderIO{FakeIO: fakeio.New(1)})
	require.Equal(t, "DTMFSender", have)
	require.Equal(t, "DTMFReceiver", missing)
}

func TestCall_BindMediaIO_LogsCapabilityMatrix(t *testing.T) {
	c, rec := newRecordingCall(t)
	defer c.Hangup("end")

	require.NoError(t, c.BindMediaIO(fakeio.New(4)))
	require.Eventually(t, func() bool { return rec.findEvent("media.bind") != nil }, time.Second, 5*time.Millisecond)

	ev := rec.findEvent("media.bind")
	require.NotNil(t, ev)
	require.Equal(t, string(transport.KindFake), ev["transport"])
	missing, ok := ev["missing"].(string)
	require.True(t, ok, "missing=%T %#v", ev["missing"], ev["missing"])
	require.Equal(t, "DTMFReceiver,DTMFSender", missing)
	have, ok := ev["have"].(string)
	require.True(t, ok)
	require.Equal(t, "none", have)
}

func TestCall_MediaSummary_HeldFramesAndLostMS(t *testing.T) {
	c, rec := newRecordingCall(t)
	require.NoError(t, c.SetState(StateAnswered))

	io := fakeio.New(0)
	require.NoError(t, c.BindMediaIO(io))
	sub := c.AudioBus().Subscribe()
	require.NotNil(t, sub)

	frame := make([]int16, bus.SamplesPerFrame)
	pushIncoming(t, io, frame)
	select {
	case <-sub.Channel():
	case <-time.After(time.Second):
		t.Fatal("missing published frame")
	}

	require.NoError(t, c.SetHeld(true))
	const holdGap = 80 * time.Millisecond
	// Gap while held advances wall_ms (last frame updates) but not media_ms.
	time.Sleep(holdGap)
	pushIncoming(t, io, frame)
	pushIncoming(t, io, frame)
	require.Never(t, func() bool { return c.MediaTime() != bus.FrameDuration }, 50*time.Millisecond, 5*time.Millisecond)

	require.NoError(t, c.Hangup("test"))
	require.Eventually(t, func() bool { return rec.findEvent("media.summary") != nil }, time.Second, 5*time.Millisecond)

	ev := rec.findEvent("media.summary")
	require.NotNil(t, ev)
	require.Equal(t, string(transport.KindFake), ev["transport"])
	require.Equal(t, uint64(2), ev["held_frames"])
	lost := ev["lost_ms"].(int64)
	// With aligned zeros, lost_ms ≈ hold gap (held audio never entered media_ms).
	require.InDelta(t, holdGap.Milliseconds(), lost, 30)
}

// playbackStatsIO wraps FakeIO with a static pacer.Stats source so
// media.summary pacer fields can be asserted.
type playbackStatsIO struct {
	*fakeio.FakeIO
	stats pacer.Stats
}

func (p *playbackStatsIO) PlaybackStats() pacer.Stats { return p.stats }

func TestCall_MediaSummary_PacerStats(t *testing.T) {
	c, rec := newRecordingCall(t)

	io := &playbackStatsIO{
		FakeIO: fakeio.New(0),
		stats: pacer.Stats{
			FramesSent:      100,
			SilenceFrames:   3,
			KeepaliveFrames: 7,
			FramesDropped:   2,
			PaceResets:      1,
			DuckEvents:      2,
			DuckedFrames:    40,
			PauseEvents:     1,
			PausedMs:        250,
			SendGapOverruns: 3,
			MaxSendGapMs:    61,
		},
	}
	require.NoError(t, c.BindMediaIO(io))
	require.NoError(t, c.Hangup("test"))
	require.Eventually(t, func() bool { return rec.findEvent("media.summary") != nil }, time.Second, 5*time.Millisecond)

	ev := rec.findEvent("media.summary")
	require.NotNil(t, ev)
	require.Equal(t, uint64(100), ev["pacer_frames_sent"])
	require.Equal(t, uint64(7), ev["pacer_keepalive_frames"])
	require.Equal(t, uint64(2), ev["pacer_duck_events"])
	require.Equal(t, uint64(40), ev["pacer_ducked_frames"])
	require.Equal(t, uint64(1), ev["pacer_pause_events"])
	require.Equal(t, uint64(250), ev["pacer_paused_ms"])
	require.Equal(t, uint64(3), ev["pacer_send_gap_overruns"])
	require.Equal(t, uint64(61), ev["pacer_max_send_gap_ms"])
}

func TestCall_MediaSummary_NoLossNearZeroLostMS(t *testing.T) {
	c, rec := newRecordingCall(t)

	io := fakeio.New(0)
	require.NoError(t, c.BindMediaIO(io))
	sub := c.AudioBus().Subscribe()
	require.NotNil(t, sub)

	frame := make([]int16, bus.SamplesPerFrame)
	for i := 0; i < 5; i++ {
		pushIncoming(t, io, frame)
		select {
		case <-sub.Channel():
		case <-time.After(time.Second):
			t.Fatalf("missing frame %d", i)
		}
		if i < 4 {
			time.Sleep(bus.FrameDuration)
		}
	}

	// Stop inbound audio; this gap must land in tail_idle_ms, not lost_ms.
	time.Sleep(100 * time.Millisecond)
	require.NoError(t, c.Hangup("test"))
	require.Eventually(t, func() bool { return rec.findEvent("media.summary") != nil }, time.Second, 5*time.Millisecond)

	ev := rec.findEvent("media.summary")
	require.NotNil(t, ev)
	require.Equal(t, string(transport.KindFake), ev["transport"])
	tailIdle := ev["tail_idle_ms"].(int64)
	lost := ev["lost_ms"].(int64)
	require.GreaterOrEqual(t, tailIdle, int64(80), "tail_idle_ms should cover the post-stream silence")
	require.InDelta(t, 0, lost, 10, "lossless stream: |lost_ms| should be near zero after zero alignment")
}
