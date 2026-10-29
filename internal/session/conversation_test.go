package session

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewConversationID_Format(t *testing.T) {
	id := NewConversationID()
	require.True(t, strings.HasPrefix(id, "CIG-"))
	parts := strings.Split(id, "-")
	require.Len(t, parts, 3)
	require.Len(t, parts[2], 8)
}

func TestLinkConversation_UnifiesIDs(t *testing.T) {
	sipLeg := NewCall("sip-leg", DirectionInbound, quietLogger())
	agentLeg := NewCall("webrtc-leg", DirectionOutbound, quietLogger())

	sipConv := sipLeg.ConversationID()
	LinkConversation(sipLeg, agentLeg)

	require.Equal(t, sipConv, sipLeg.ConversationID())
	require.Equal(t, sipConv, agentLeg.ConversationID())
	require.Equal(t, sipLeg.ID(), sipLeg.RootCallID())
	require.Equal(t, sipLeg.ID(), agentLeg.RootCallID())
	require.Equal(t, PhaseAgent, sipLeg.Phase())
	require.Equal(t, PhaseAgent, agentLeg.Phase())
}

func TestNewCall_HasConversationFields(t *testing.T) {
	c := NewCall("abc", DirectionInbound, quietLogger())
	require.NotEmpty(t, c.ConversationID())
	require.Equal(t, "abc", c.RootCallID())
	require.Equal(t, PhaseLLM, c.Phase())
}
