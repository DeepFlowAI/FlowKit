package recording

import (
	"testing"

	"github.com/DeepFlowAI/flowkit/internal/session"
)

func TestPickBridgeLegs_InboundUser(t *testing.T) {
	user := session.NewCall("u1", session.DirectionInbound, nil)
	agent := session.NewCall("a1", session.DirectionOutbound, nil)
	u, a := PickBridgeLegs(user, agent)
	if u.ID() != "u1" || a.ID() != "a1" {
		t.Fatalf("got %s / %s", u.ID(), a.ID())
	}
}

func TestPickBridgeLegs_Swap(t *testing.T) {
	a := session.NewCall("a1", session.DirectionOutbound, nil)
	b := session.NewCall("b1", session.DirectionInbound, nil)
	u, ag := PickBridgeLegs(a, b)
	if u.ID() != "b1" || ag.ID() != "a1" {
		t.Fatalf("got %s / %s", u.ID(), ag.ID())
	}
}
