package recording

import (
	"testing"

	"github.com/DeepFlowAI/flowkit/internal/session"
)

func TestRecordingNotifyIDs_BridgeOutboundAndWebRTC(t *testing.T) {
	user := session.NewCall("webrtc-leg", session.DirectionInbound, nil)
	agent := session.NewCall("sip-out-leg", session.DirectionOutbound, nil)

	ids := recordingNotifyIDs(user, agent)
	want := map[string]bool{
		user.ID(): true,
		agent.ID(): true,
		user.RootCallID(): true,
		agent.RootCallID(): true,
	}
	if len(ids) != len(want) {
		t.Fatalf("ids=%v want %d unique", ids, len(want))
	}
	for _, id := range ids {
		if !want[id] {
			t.Fatalf("unexpected id %s in %v", id, ids)
		}
	}
}
