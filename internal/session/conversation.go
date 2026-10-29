package session

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

const (
	// PhaseLLM is the default phase: kernel ASR/LLM handles the caller.
	PhaseLLM = "llm"
	// PhaseAgent is set while two legs are bridged (human agent on the line).
	PhaseAgent = "agent"
)

// NewConversationID returns a human-friendly session id (scheme B).
// Format: CIG-YYYYMMDD-<8 hex chars>.
func NewConversationID() string {
	date := time.Now().UTC().Format("20060102")
	short := uuid.NewString()[:8]
	return fmt.Sprintf("CIG-%s-%s", date, short)
}

// LinkConversation unifies conversation_id and root_call_id across bridged legs.
// The inbound leg (typically SIP user) becomes root_call_id when present.
func LinkConversation(a, b *Call) {
	if a == nil || b == nil || a.ID() == b.ID() {
		return
	}

	cid := a.ConversationID()
	if b.CreatedAt().Before(a.CreatedAt()) {
		cid = b.ConversationID()
	}

	root := pickRootCallID(a, b)
	if a.Direction() == DirectionInbound && b.Direction() != DirectionInbound {
		cid = a.ConversationID()
		root = a.ID()
	} else if b.Direction() == DirectionInbound && a.Direction() != DirectionInbound {
		cid = b.ConversationID()
		root = b.ID()
	}

	a.applyConversation(cid, root, PhaseAgent)
	b.applyConversation(cid, root, PhaseAgent)
}

func pickRootCallID(a, b *Call) string {
	if a.Direction() == DirectionInbound {
		return a.ID()
	}
	if b.Direction() == DirectionInbound {
		return b.ID()
	}
	if a.CreatedAt().Before(b.CreatedAt()) {
		return a.ID()
	}
	return b.ID()
}
