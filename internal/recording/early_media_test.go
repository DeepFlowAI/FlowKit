package recording

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/media/bus"
	"github.com/DeepFlowAI/flowkit/internal/session"
)

func earlyMediaTestConfig(dir string) Config {
	cfg := takeoverTestConfig(dir)
	cfg.CaptureEarlyMedia = true
	return cfg
}

func TestRecorder_CaptureEarlyMediaLazyOpensOnFirstFrame(t *testing.T) {
	dir := t.TempDir()
	cfg := earlyMediaTestConfig(dir)
	factory, err := NewSinkFactory(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	call := session.NewCall("early-with-audio", session.DirectionOutbound, nil)
	if err := call.SetState(session.StateRinging); err != nil {
		t.Fatal(err)
	}

	started := make(chan RecordingStarted, 1)
	completed := make(chan RecordingCompleted, 1)
	rec := NewRecorder(cfg, factory, call, nil)
	rec.hooks = &Hooks{
		OnStarted:   func(event RecordingStarted) { started <- event },
		OnCompleted: func(event RecordingCompleted) { completed <- event },
	}
	attachDone := make(chan struct{})
	go func() {
		rec.Attach(context.Background())
		close(attachDone)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for call.AudioBus().SubscriberCount() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if call.AudioBus().SubscriberCount() == 0 {
		t.Fatal("early-media recorder did not subscribe while ringing")
	}
	if got := countNonDirFiles(t, dir); got != 0 {
		t.Fatalf("opened %d files before the first RTP frame", got)
	}

	call.AudioBus().Publish(bus.Frame{
		Samples:   make([]int16, bus.SamplesPerFrame),
		Timestamp: 0,
		Source:    "transport",
	})
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("first early-media frame did not open the recording")
	}
	if err := call.Hangup("subscriber_absent"); err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-completed:
		if event.DurationMS <= 0 {
			t.Fatalf("duration_ms=%d, want non-zero", event.DurationMS)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("non-empty early-media recording did not complete")
	}
	select {
	case <-attachDone:
	case <-time.After(time.Second):
		t.Fatal("recorder goroutine did not exit")
	}
	if got := countNonDirFiles(t, dir); got != 1 {
		t.Fatalf("recording files=%d, want 1", got)
	}
}

func TestRecorder_CaptureEarlyMediaWithoutFramesCreatesNoArtifact(t *testing.T) {
	dir := t.TempDir()
	cfg := earlyMediaTestConfig(dir)
	factory, err := NewSinkFactory(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	call := session.NewCall("early-empty", session.DirectionOutbound, nil)
	if err := call.SetState(session.StateRinging); err != nil {
		t.Fatal(err)
	}

	started := make(chan RecordingStarted, 1)
	completed := make(chan RecordingCompleted, 1)
	rec := NewRecorder(cfg, factory, call, nil)
	rec.hooks = &Hooks{
		OnStarted:   func(event RecordingStarted) { started <- event },
		OnCompleted: func(event RecordingCompleted) { completed <- event },
	}
	attachDone := make(chan struct{})
	go func() {
		rec.Attach(context.Background())
		close(attachDone)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for call.AudioBus().SubscriberCount() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if call.AudioBus().SubscriberCount() == 0 {
		t.Fatal("early-media recorder did not subscribe while ringing")
	}
	if err := call.Hangup("busy"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-attachDone:
	case <-time.After(time.Second):
		t.Fatal("recorder goroutine did not exit")
	}
	select {
	case <-started:
		t.Fatal("empty early-media attempt emitted recording.started")
	default:
	}
	select {
	case <-completed:
		t.Fatal("empty early-media attempt emitted recording.completed")
	default:
	}
	if got := countNonDirFiles(t, dir); got != 0 {
		t.Fatalf("recording files=%d, want 0", got)
	}
}

func TestRecorder_CaptureEarlyMediaKeepsRemoteAudioAfterRTPGap(t *testing.T) {
	dir := t.TempDir()
	cfg := earlyMediaTestConfig(dir)
	factory, err := NewSinkFactory(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	call := session.NewCall("early-rtp-gap", session.DirectionOutbound, nil)
	if err := call.SetState(session.StateRinging); err != nil {
		t.Fatal(err)
	}
	rec := NewRecorder(cfg, factory, call, nil)
	if err := rec.Begin(context.Background()); err != nil {
		t.Fatal(err)
	}

	firstRemote := constantSamples(1000)
	call.AudioBus().Publish(bus.Frame{Samples: firstRemote, Timestamp: 0, Source: "transport"})
	waitForRecorderSlot(t, rec, func(slot *slotBuf) bool { return slot.hasL })

	// Simulate a carrier that sends no RTP during two seconds of silence. The
	// local render clock keeps advancing, so the following remote prompt must
	// land near the current wall-clock slot instead of its stale RTP sample slot.
	rec.clockStart = rec.clockStart.Add(-2 * time.Second)
	rec.tapOutgoing(constantSamples(2000))
	waitForRecorderSlot(t, rec, func(slot *slotBuf) bool { return slot.hasR })
	call.AudioBus().Publish(bus.Frame{
		Samples:   constantSamples(3000),
		Timestamp: bus.FrameDuration,
		Source:    "transport",
	})
	waitForRemoteTimeline(t, rec, time.Second)
	rec.Stop()

	data, err := os.ReadFile(filepath.Join(dir, "test", "early-rtp-gap.wav"))
	if err != nil {
		t.Fatal(err)
	}
	if !wavChannelContains(data, 0, 3000) {
		t.Fatal("remote audio after RTP silence was dropped from the left channel")
	}
	if !wavChannelContains(data, 1, 2000) {
		t.Fatal("local audio was not recorded in the right channel")
	}
	if duration := durationMSForLayout(LayoutStereo, uint32(len(data)-wavHeaderSize)); duration < 1900 {
		t.Fatalf("recording duration_ms=%d, want the RTP silence preserved", duration)
	}
	if rec.partial.Load() || rec.encoder.HadIssues() {
		t.Fatal("aligned audio after RTP silence was incorrectly marked partial")
	}
}

func constantSamples(level int16) []int16 {
	samples := make([]int16, bus.SamplesPerFrame)
	for i := range samples {
		samples[i] = level
	}
	return samples
}

func waitForRecorderSlot(t *testing.T, rec *Recorder, ready func(*slotBuf) bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		rec.encoder.mu.Lock()
		for _, slot := range rec.encoder.slots {
			if ready(slot) {
				rec.encoder.mu.Unlock()
				return
			}
		}
		rec.encoder.mu.Unlock()
		time.Sleep(time.Millisecond)
	}
	t.Fatal("recorder did not ingest the expected frame")
}

func waitForRemoteTimeline(t *testing.T, rec *Recorder, after time.Duration) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		rec.remoteClock.mu.Lock()
		next := rec.remoteClock.next
		rec.remoteClock.mu.Unlock()
		if next > after {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("recorder did not advance the remote timeline")
}

func wavChannelContains(data []byte, channel int, level int16) bool {
	for offset := wavHeaderSize + channel*2; offset+1 < len(data); offset += wavChannels * 2 {
		if int16(binary.LittleEndian.Uint16(data[offset:])) == level {
			return true
		}
	}
	return false
}
