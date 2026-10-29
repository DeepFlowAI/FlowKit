package recording

import "github.com/DeepFlowAI/flowkit/internal/session"

// recordingNotifyIDs returns call IDs that should receive recording WS events
// for a bridged conversation (user + agent legs and their roots).
func recordingNotifyIDs(userLeg, agentLeg *session.Call) []string {
	seen := make(map[string]struct{}, 4)
	var ids []string
	for _, id := range []string{
		userLeg.ID(),
		agentLeg.ID(),
		userLeg.RootCallID(),
		agentLeg.RootCallID(),
	} {
		if id == "" {
			continue
		}
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	return ids
}
