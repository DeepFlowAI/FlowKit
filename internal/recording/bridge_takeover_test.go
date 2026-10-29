package recording

import (
	"context"
	"testing"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/media/bus"
	"github.com/DeepFlowAI/flowkit/internal/session"
)

func takeoverTestConfig(dir string) Config {
	return Config{
		Enabled:       true,
		StartOn:       "answered",
		Encoding:      "pcm16",
		Layout:        LayoutStereo,
		FlushInterval: 10 * time.Millisecond,
		Storage: StorageConfig{
			Backend:   "local",
			Local:     LocalConfig{Dir: dir},
			AliyunOSS: AliyunOSSConfig{KeyTemplate: "test/{call_id}.wav"},
		},
	}
}

func publishFrame(call *session.Call, level int16) {
	samples := make([]int16, bus.SamplesPerFrame)
	for i := range samples {
		samples[i] = level
	}
	call.AudioBus().Publish(bus.Frame{Samples: samples, Timestamp: 0})
}

// waitForBytes polls until the recorder has queued more PCM than before.
func waitForBytes(t *testing.T, br *BridgeRecorder, above int64) int64 {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if n := br.pcmBytes.Load(); n > above {
			return n
		}
		time.Sleep(10 * time.Millisecond)
	}
	return br.pcmBytes.Load()
}

// A takeover must not end the bridge recording: the recorder follows the leg
// that replaced the agent, so the local channel keeps carrying audio and the
// manager no longer finds this recorder by the departing leg's id.
func TestBridgeRecorder_FollowsAgentLegOnTakeover(t *testing.T) {
	cfg := takeoverTestConfig(t.TempDir())
	factory, err := NewSinkFactory(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	user := session.NewCall("u1", session.DirectionInbound, nil)
	agent := session.NewCall("a1", session.DirectionOutbound, nil)
	supervisor := session.NewCall("s1", session.DirectionOutbound, nil)

	br := NewBridgeRecorder(cfg, factory, user, agent, nil)
	br.Begin(context.Background())
	defer br.Stop()

	br.ReplaceAgentLeg(supervisor)
	if got := br.AgentLeg().ID(); got != "s1" {
		t.Fatalf("agent leg after takeover = %s, want s1", got)
	}

	// The supervisor's audio reaches the recording (both channels are fed, since
	// the encoder only emits once it has a frame from each side).
	before := br.pcmBytes.Load()
	publishFrame(user, 100)
	publishFrame(supervisor, 200)
	if after := waitForBytes(t, br, before); after <= before {
		t.Fatalf("supervisor audio not recorded: pcmBytes stayed at %d", after)
	}

	// The replaced leg is no longer captured, so its late frames cannot bleed
	// into the customer channel pairing.
	quiet := br.pcmBytes.Load()
	publishFrame(agent, 300)
	time.Sleep(100 * time.Millisecond)
	if got := br.pcmBytes.Load(); got != quiet {
		t.Fatalf("replaced leg still recorded: pcmBytes %d → %d", quiet, got)
	}
}

// The departing leg's hangup arrives as StopCall for its id. Since the recorder
// now belongs to the taking-over leg, that must leave the recording running.
func TestManager_TakeoverKeepsBridgeRecordingThroughOldLegHangup(t *testing.T) {
	cfg := takeoverTestConfig(t.TempDir())
	m, err := NewManager(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	user := session.NewCall("u1", session.DirectionInbound, nil)
	agent := session.NewCall("a1", session.DirectionOutbound, nil)
	supervisor := session.NewCall("s1", session.DirectionOutbound, nil)

	br := NewBridgeRecorder(cfg, m.factory, user, agent, nil)
	br.Begin(context.Background())
	defer br.Stop()
	m.mu.Lock()
	m.bridgeRec[user.ConversationID()] = br
	m.mu.Unlock()

	m.OnLegReplaced("a1", supervisor)

	if _, err := m.StopCall("a1"); err == nil {
		t.Fatal("StopCall on the replaced leg stopped the bridge recording")
	}
	if br.stopped.Load() {
		t.Fatal("bridge recording stopped by the replaced leg's hangup")
	}
	if _, err := m.StopCall("s1"); err != nil {
		t.Fatalf("StopCall on the taking-over leg should stop the recording: %v", err)
	}
}
