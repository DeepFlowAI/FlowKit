package recording

import (
	"testing"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/session"
)

type capturedRecordingEvent struct {
	method string
	callID string
	params map[string]any
}

type recordingEventCapture struct {
	events []capturedRecordingEvent
}

func (c *recordingEventCapture) Notify(method, callID string, params map[string]any) {
	cloned := make(map[string]any, len(params))
	for key, value := range params {
		cloned[key] = value
	}
	c.events = append(c.events, capturedRecordingEvent{method: method, callID: callID, params: cloned})
}

func TestRecordingCompletedEventHasStableArtifactContract(t *testing.T) {
	call := session.NewCall("call-1", session.DirectionInbound, nil)
	capture := &recordingEventCapture{}
	manager := &Manager{}
	manager.SetNotifier(capture)
	hooks := manager.hooksForCall(call, "ai")
	startedAt := time.Date(2026, 8, 9, 10, 0, 0, 0, time.UTC)
	completed := RecordingCompleted{
		URL:           "https://example.invalid/recordings/call-1.wav",
		StorageKey:    "recordings/2026-08-09/call-1.wav",
		StartedAt:     startedAt,
		EndedAt:       startedAt.Add(42 * time.Second),
		DurationMS:    42_000,
		HeaderPatched: true,
	}

	hooks.OnCompleted(completed)
	hooks.OnCompleted(completed)

	if len(capture.events) != 2 {
		t.Fatalf("events=%d want=2", len(capture.events))
	}
	first := capture.events[0]
	second := capture.events[1]
	if first.method != "call.recording.completed" || first.callID != call.ID() {
		t.Fatalf("unexpected notification: %+v", first)
	}
	if first.params["recording_id"] == "" || first.params["event_id"] == "" {
		t.Fatalf("missing stable identifiers: %+v", first.params)
	}
	if first.params["recording_id"] != second.params["recording_id"] ||
		first.params["event_id"] != second.params["event_id"] {
		t.Fatalf("replayed event identifiers changed: first=%+v second=%+v", first.params, second.params)
	}
	if first.params["storage_key"] != completed.StorageKey ||
		first.params["call_leg_id"] != call.ID() ||
		first.params["duration_ms"] != int64(42_000) {
		t.Fatalf("unexpected artifact metadata: %+v", first.params)
	}
	if first.params["started_at"] != startedAt.Format(time.RFC3339Nano) ||
		first.params["ended_at"] != completed.EndedAt.Format(time.RFC3339Nano) {
		t.Fatalf("unexpected timestamps: %+v", first.params)
	}
	if first.params["oss_url"] != completed.URL {
		t.Fatalf("legacy oss_url compatibility was removed: %+v", first.params)
	}
}

func TestBridgeRecordingUsesOneArtifactAndPerLegEventIDs(t *testing.T) {
	user := session.NewCall("user-leg", session.DirectionInbound, nil)
	agent := session.NewCall("agent-leg", session.DirectionOutbound, nil)
	session.LinkConversation(user, agent)
	capture := &recordingEventCapture{}
	manager := &Manager{}
	manager.SetNotifier(capture)
	hooks := manager.hooksForBridge(user, agent)
	startedAt := time.Date(2026, 8, 9, 10, 0, 0, 0, time.UTC)

	hooks.OnCompleted(RecordingCompleted{
		URL:        "https://example.invalid/recordings/bridge.wav",
		StorageKey: "recordings/2026-08-09/bridge.wav",
		StartedAt:  startedAt,
		EndedAt:    startedAt.Add(time.Minute),
		DurationMS: 60_000,
	})

	if len(capture.events) != 2 {
		t.Fatalf("events=%d want=2", len(capture.events))
	}
	if capture.events[0].params["recording_id"] != capture.events[1].params["recording_id"] {
		t.Fatalf("bridge legs must share recording_id: %+v", capture.events)
	}
	if capture.events[0].params["event_id"] == capture.events[1].params["event_id"] {
		t.Fatalf("bridge legs must have distinct event_id: %+v", capture.events)
	}
	for _, event := range capture.events {
		if event.params["call_leg_id"] != event.callID {
			t.Fatalf("call_leg_id mismatch: %+v", event)
		}
	}
}

func TestUnansweredOutboundRecordingUsesEarlyMediaPhase(t *testing.T) {
	call := session.NewCall("early-leg", session.DirectionOutbound, nil)
	if err := call.SetState(session.StateRinging); err != nil {
		t.Fatal(err)
	}
	capture := &recordingEventCapture{}
	manager := &Manager{cfg: Config{CaptureEarlyMedia: true}}
	manager.SetNotifier(capture)
	hooks := manager.hooksForCall(call, "ai")
	startedAt := time.Now().UTC()
	hooks.OnCompleted(RecordingCompleted{
		URL:        "https://example.invalid/recordings/early.wav",
		StorageKey: "recordings/early.wav",
		StartedAt:  startedAt,
		EndedAt:    startedAt.Add(time.Second),
		DurationMS: 1000,
	})

	if len(capture.events) != 1 {
		t.Fatalf("events=%d want=1", len(capture.events))
	}
	if got := capture.events[0].params["phase"]; got != "early_media" {
		t.Fatalf("phase=%v want=early_media", got)
	}
}

func TestEarlyMediaRecordingThatAnswersUsesAIPhase(t *testing.T) {
	call := session.NewCall("answered-leg", session.DirectionOutbound, nil)
	if err := call.SetState(session.StateRinging); err != nil {
		t.Fatal(err)
	}
	if err := call.SetState(session.StateAnswered); err != nil {
		t.Fatal(err)
	}
	if err := call.Hangup("remote_bye"); err != nil {
		t.Fatal(err)
	}
	capture := &recordingEventCapture{}
	manager := &Manager{cfg: Config{CaptureEarlyMedia: true}}
	manager.SetNotifier(capture)
	hooks := manager.hooksForCall(call, "ai")
	startedAt := time.Now().UTC()
	hooks.OnCompleted(RecordingCompleted{
		URL:        "https://example.invalid/recordings/answered.wav",
		StorageKey: "recordings/answered.wav",
		StartedAt:  startedAt,
		EndedAt:    startedAt.Add(time.Second),
		DurationMS: 1000,
	})

	if len(capture.events) != 1 {
		t.Fatalf("events=%d want=1", len(capture.events))
	}
	if got := capture.events[0].params["phase"]; got != "ai" {
		t.Fatalf("phase=%v want=ai", got)
	}
}
