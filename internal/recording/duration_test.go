package recording

import (
	"context"
	"testing"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/session"
)

// The reported duration has to describe the audio that was stored. A recorder
// can stay open far longer than the media it receives — a leg that stops
// sending, a stream that is cut short by a takeover — and consumers render a
// playback timeline from this value, so reporting how long the recorder was
// open would leave them seeking past the end of the file.
func TestCompletedDurationDescribesStoredAudio(t *testing.T) {
	cfg := takeoverTestConfig(t.TempDir())
	factory, err := NewSinkFactory(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	user := session.NewCall("u1", session.DirectionInbound, nil)
	agent := session.NewCall("a1", session.DirectionOutbound, nil)

	done := make(chan RecordingCompleted, 1)

	br := NewBridgeRecorder(cfg, factory, user, agent, nil)
	br.hooks = &Hooks{OnCompleted: func(c RecordingCompleted) { done <- c }}
	br.Begin(context.Background())

	for i := 0; i < 5; i++ {
		publishFrame(user, 1000)
	}

	// Keep the recorder open well past the audio it was given.
	const idle = 600 * time.Millisecond
	time.Sleep(idle)
	br.Stop()

	var completed RecordingCompleted
	select {
	case completed = <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("recording never reported completion")
	}

	t.Logf("duration=%dms while the recorder was open for more than %s",
		completed.DurationMS, idle)

	if completed.DurationMS == 0 {
		t.Fatal("DurationMS=0, want the length of the audio that was stored")
	}
	if completed.DurationMS >= idle.Milliseconds() {
		t.Fatalf("DurationMS=%d reads like how long the recorder was open, not the audio it stored",
			completed.DurationMS)
	}
}

func TestDurationMSForLayout(t *testing.T) {
	// 16 kHz, 16-bit: 32 bytes per millisecond per channel.
	for _, tc := range []struct {
		name     string
		layout   Layout
		dataSize uint32
		wantMS   int64
	}{
		{"stereo one second", LayoutStereo, 16000 * 2 * 2, 1000},
		{"stereo empty", LayoutStereo, 0, 0},
		{"stereo half second", LayoutStereo, 16000 * 2, 500},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := durationMSForLayout(tc.layout, tc.dataSize); got != tc.wantMS {
				t.Fatalf("durationMSForLayout(%v, %d)=%d want %d",
					tc.layout, tc.dataSize, got, tc.wantMS)
			}
		})
	}
}
