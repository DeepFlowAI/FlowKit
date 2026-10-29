package calls

// Transcription across the move into a mixing room.
//
// Live transcription is normally owned by a two-leg bridge. Joining a room
// tears that bridge down, which would end the agent's live transcript the
// moment a third party is added — the call carries on, the text stops. So a
// leg that was being transcribed keeps being transcribed while it is mixed,
// under the same speaker and the same anchor call id.
//
// Only legs that already had a transcript carry one in. A conference
// participant or a monitoring supervisor was never on the bridge, so they add
// no lines, and the transcript keeps the two-speaker shape its readers expect.

import (
	"sync"

	"github.com/DeepFlowAI/flowkit/internal/session"
)

// roomTranscript is one leg's transcription for as long as it is in a room.
type roomTranscript struct {
	session *bridgeASRSession
	wg      sync.WaitGroup
}

// startRoomTranscript opens a transcript for a leg entering a room, under the
// role it carried out of the bridge it just left. An inactive role means the
// leg was not being transcribed and nothing is started.
func (h *Handlers) startRoomTranscript(call *session.Call, role transcriptRole) {
	if !role.active() || !h.bridgeASREnabled {
		return
	}
	rt := &roomTranscript{}
	s := h.openLegTranscript(call, role.speaker, role.anchorID, &rt.wg)
	if s == nil {
		return
	}
	rt.session = s

	callID := call.ID()
	h.roomASRMu.Lock()
	if h.roomASR == nil {
		h.roomASR = make(map[string]*roomTranscript)
	}
	previous := h.roomASR[callID]
	h.roomASR[callID] = rt
	h.roomASRMu.Unlock()

	// A leg belongs to one room at a time; drop any stale stream rather than
	// leaving two providers transcribing the same audio into the same anchor.
	if previous != nil {
		closeLegTranscript(previous.session)
		previous.wg.Wait()
	}
	h.log.Debug("room transcript started",
		"leg", callID, "speaker", role.speaker, "anchor_call_id", role.anchorID)
}

// roomTranscriptRoleOf reports how a leg currently in a room is being
// transcribed, so a leg replacing it can inherit that identity.
func (h *Handlers) roomTranscriptRoleOf(callID string) transcriptRole {
	h.roomASRMu.Lock()
	defer h.roomASRMu.Unlock()
	rt := h.roomASR[callID]
	if rt == nil || rt.session == nil {
		return transcriptRole{}
	}
	return transcriptRole{speaker: rt.session.speaker, anchorID: rt.session.anchorID}
}

// stopRoomTranscript ends a leg's room transcript. Idempotent, and safe for a
// leg that never had one — legs leave rooms for many reasons (explicit
// removal, the room auto-destroying, the call simply ending) and every one of
// them lands here.
func (h *Handlers) stopRoomTranscript(callID string) {
	h.roomASRMu.Lock()
	rt := h.roomASR[callID]
	delete(h.roomASR, callID)
	h.roomASRMu.Unlock()
	if rt == nil {
		return
	}
	closeLegTranscript(rt.session)
	rt.wg.Wait()
	h.log.Debug("room transcript stopped", "leg", callID)
}
