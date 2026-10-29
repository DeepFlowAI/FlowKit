package calls

import (
	"context"
	"encoding/json"

	"github.com/DeepFlowAI/flowkit/internal/observability"
	"github.com/DeepFlowAI/flowkit/internal/room"
	"github.com/DeepFlowAI/flowkit/internal/rpc"
	"github.com/DeepFlowAI/flowkit/internal/session"
)

// RoomController is the subset of room.Manager the room.* RPC methods use. It
// is an interface so the calls package does not force a room manager to exist
// (the feature is gated off by default).
type RoomController interface {
	Create() *room.Room
	Get(id string) (*room.Room, error)
	AddLeg(roomID string, call *session.Call) error
	RemoveLeg(roomID, callID string) error
	ReplaceLeg(roomID, oldID string, newCall *session.Call) error
	SetMixGain(roomID, listenerID, speakerID string, gain float64) error
	Close(roomID, reason string) error
	OnLegDetached(func(callID string))
	OnLegCallEnded(func(callID string))
}

// EnableRooms wires the room.* JSON-RPC methods. Off by default; call this
// only when multi-party mixing is enabled in config.
func (h *Handlers) EnableRooms(rooms RoomController, reg *rpc.Registry) {
	h.rooms = rooms
	// Legs also leave a room without an RPC — the call ends, or the room
	// auto-destroys once it is down to one leg — so release the transcript
	// from the one place every exit passes through.
	rooms.OnLegDetached(h.stopRoomTranscript)
	// A leg that dies on its own inside a room (browser closed, network
	// loss) is invisible to the call-control backend unless someone tells
	// it: two-leg bridges broadcast call.hangup on peer teardown, room legs
	// did not. Mirror the bridge behavior so the backend can release
	// per-leg state (e.g. a supervisor monitor lock) instead of waiting out
	// a TTL. RPC-driven exits (remove_leg / replace_leg / close) already
	// have their initiator's bookkeeping, so the watcher does not fire for
	// them.
	rooms.OnLegCallEnded(func(callID string) {
		h.broadcastCallHangup(callID, "room_leg_ended")
	})
	reg.Register("room.create", h.handleRoomCreate)
	reg.Register("room.add_leg", h.handleRoomAddLeg)
	reg.Register("room.remove_leg", h.handleRoomRemoveLeg)
	reg.Register("room.replace_leg", h.handleRoomReplaceLeg)
	reg.Register("room.set_mix_gain", h.handleRoomSetMixGain)
	reg.Register("room.close", h.handleRoomClose)
}

func (h *Handlers) roomsReady() *rpc.Error {
	if h.rooms == nil {
		return rpc.ErrMethodNotFound("room.*")
	}
	return nil
}

// prepareRoomLeg validates a call is fit to join a room and detaches any
// existing media (TTS/ASR playback and an active two-leg bridge) so it is not
// mixed by two owners at once — mirroring call.bridge's pre-bridge cleanup.
//
// Callers that want the leg's transcript to continue must read its role with
// transcriptRoleOf BEFORE calling this, for every leg they intend to move:
// unbridging one leg ends its peer's transcript too, so a role read afterwards
// comes back empty for anyone who shared that bridge.
func (h *Handlers) prepareRoomLeg(callID, method string, conn *rpc.Conn) (*session.Call, *rpc.Error) {
	call, err := h.mgr.Get(callID)
	if err != nil {
		return nil, rpc.ErrCallNotFound(callID)
	}
	if rpcErr := h.kernelExclusiveReject(conn, call, method); rpcErr != nil {
		return nil, rpcErr
	}
	if call.IsEnded() {
		return nil, rpc.ErrCallStateInvalid("call ended")
	}
	if call.MediaIO() == nil {
		return nil, rpc.ErrCallStateInvalid("media not ready")
	}
	if !bridgeableState(call.State()) {
		return nil, rpc.ErrCallStateInvalid("invalid state for room")
	}
	// Detach from any active two-leg bridge and stop TTS/ASR before mixing.
	h.unbridgeCall(callID)
	h.stopCallMedia(callID, "all")
	// A room leg is no longer in a two-leg bridge. Leaving it in
	// StateBridged makes any later call.bridge (transfer, re-bridge after
	// the room collapses) fail with "bridged → bridged".
	if call.State() == session.StateBridged {
		_ = call.SetState(session.StateActive)
	}
	return call, nil
}

type roomCreateParams struct {
	LegCallIDs []string `json:"leg_call_ids,omitempty"`
}

func (h *Handlers) handleRoomCreate(ctx context.Context, conn *rpc.Conn, params json.RawMessage) (any, *rpc.Error) {
	if rpcErr := h.roomsReady(); rpcErr != nil {
		return nil, rpcErr
	}
	var p roomCreateParams
	if len(params) > 0 {
		if rpcErr := rpc.DecodeParams(params, &p); rpcErr != nil {
			return nil, rpcErr
		}
	}
	// Validate every requested leg BEFORE creating the room, so a bad id does
	// not leave an orphan room behind.
	// Roles first, for every leg, while all the bridges are still standing.
	roles := make([]transcriptRole, len(p.LegCallIDs))
	for i, id := range p.LegCallIDs {
		roles[i] = h.transcriptRoleOf(id)
	}
	calls := make([]*session.Call, 0, len(p.LegCallIDs))
	for _, id := range p.LegCallIDs {
		call, rpcErr := h.prepareRoomLeg(id, "room.create", conn)
		if rpcErr != nil {
			return nil, rpcErr
		}
		calls = append(calls, call)
	}

	r := h.rooms.Create()
	for i, call := range calls {
		if err := h.rooms.AddLeg(r.ID(), call); err != nil {
			_ = h.rooms.Close(r.ID(), "room_create_failed")
			return nil, rpc.NewErrorf(rpc.CodeInternalError, "%v", err)
		}
		h.startRoomTranscript(call, roles[i])
		h.stateFor(call.ID(), conn)
	}
	observability.CallTrace(ctx, h.log, "rpc.room.create", r.ID(), "legs", len(calls))
	return map[string]any{"room_id": r.ID(), "legs": r.Legs()}, nil
}

type roomLegParams struct {
	RoomID string `json:"room_id"`
	CallID string `json:"call_id"`
}

func (h *Handlers) handleRoomAddLeg(ctx context.Context, conn *rpc.Conn, params json.RawMessage) (any, *rpc.Error) {
	if rpcErr := h.roomsReady(); rpcErr != nil {
		return nil, rpcErr
	}
	var p roomLegParams
	if rpcErr := rpc.DecodeParams(params, &p); rpcErr != nil {
		return nil, rpcErr
	}
	if p.RoomID == "" || p.CallID == "" {
		return nil, rpc.ErrInvalidParams("room_id and call_id required")
	}
	if _, err := h.rooms.Get(p.RoomID); err != nil {
		return nil, roomErrToRPC(err)
	}
	role := h.transcriptRoleOf(p.CallID)
	call, rpcErr := h.prepareRoomLeg(p.CallID, "room.add_leg", conn)
	if rpcErr != nil {
		return nil, rpcErr
	}
	if err := h.rooms.AddLeg(p.RoomID, call); err != nil {
		return nil, roomErrToRPC(err)
	}
	h.startRoomTranscript(call, role)
	h.stateFor(p.CallID, conn)
	observability.CallTraceCall(ctx, h.log, "rpc.room.add_leg", call, "room_id", p.RoomID)
	return map[string]any{"ok": true, "room_id": p.RoomID, "call_id": p.CallID}, nil
}

func (h *Handlers) handleRoomRemoveLeg(ctx context.Context, conn *rpc.Conn, params json.RawMessage) (any, *rpc.Error) {
	if rpcErr := h.roomsReady(); rpcErr != nil {
		return nil, rpcErr
	}
	var p roomLegParams
	if rpcErr := rpc.DecodeParams(params, &p); rpcErr != nil {
		return nil, rpcErr
	}
	if p.RoomID == "" || p.CallID == "" {
		return nil, rpc.ErrInvalidParams("room_id and call_id required")
	}
	if err := h.rooms.RemoveLeg(p.RoomID, p.CallID); err != nil {
		return nil, roomErrToRPC(err)
	}
	observability.CallTrace(ctx, h.log, "rpc.room.remove_leg", p.CallID, "room_id", p.RoomID)
	return map[string]any{"ok": true, "room_id": p.RoomID, "call_id": p.CallID}, nil
}

type roomReplaceParams struct {
	RoomID    string `json:"room_id"`
	OldCallID string `json:"old_call_id"`
	NewCallID string `json:"new_call_id"`
}

func (h *Handlers) handleRoomReplaceLeg(ctx context.Context, conn *rpc.Conn, params json.RawMessage) (any, *rpc.Error) {
	if rpcErr := h.roomsReady(); rpcErr != nil {
		return nil, rpcErr
	}
	var p roomReplaceParams
	if rpcErr := rpc.DecodeParams(params, &p); rpcErr != nil {
		return nil, rpcErr
	}
	if p.RoomID == "" || p.OldCallID == "" || p.NewCallID == "" {
		return nil, rpc.ErrInvalidParams("room_id, old_call_id and new_call_id required")
	}
	if _, err := h.rooms.Get(p.RoomID); err != nil {
		return nil, roomErrToRPC(err)
	}
	// The replacement takes over the outgoing leg's place in the conversation,
	// so it inherits its transcript identity: a supervisor stepping in for the
	// agent is still the agent side of the transcript.
	role := h.roomTranscriptRoleOf(p.OldCallID)
	if !role.active() {
		role = h.transcriptRoleOf(p.NewCallID)
	}
	newCall, rpcErr := h.prepareRoomLeg(p.NewCallID, "room.replace_leg", conn)
	if rpcErr != nil {
		return nil, rpcErr
	}
	if err := h.rooms.ReplaceLeg(p.RoomID, p.OldCallID, newCall); err != nil {
		return nil, roomErrToRPC(err)
	}
	if h.bridgeObs != nil {
		h.bridgeObs.OnLegReplaced(p.OldCallID, newCall)
	}
	h.startRoomTranscript(newCall, role)
	h.stateFor(p.NewCallID, conn)
	observability.CallTraceCall(ctx, h.log, "rpc.room.replace_leg", newCall, "room_id", p.RoomID, "old_call_id", p.OldCallID)
	return map[string]any{"ok": true, "room_id": p.RoomID, "old_call_id": p.OldCallID, "new_call_id": p.NewCallID}, nil
}

type roomGainParams struct {
	RoomID         string  `json:"room_id"`
	ListenerCallID string  `json:"listener_call_id"`
	SpeakerCallID  string  `json:"speaker_call_id"`
	Gain           float64 `json:"gain"`
}

func (h *Handlers) handleRoomSetMixGain(ctx context.Context, conn *rpc.Conn, params json.RawMessage) (any, *rpc.Error) {
	if rpcErr := h.roomsReady(); rpcErr != nil {
		return nil, rpcErr
	}
	var p roomGainParams
	if rpcErr := rpc.DecodeParams(params, &p); rpcErr != nil {
		return nil, rpcErr
	}
	if p.RoomID == "" || p.ListenerCallID == "" || p.SpeakerCallID == "" {
		return nil, rpc.ErrInvalidParams("room_id, listener_call_id and speaker_call_id required")
	}
	if p.Gain < 0 || p.Gain > 1 {
		return nil, rpc.ErrInvalidParams("gain must be in [0,1]")
	}
	if err := h.rooms.SetMixGain(p.RoomID, p.ListenerCallID, p.SpeakerCallID, p.Gain); err != nil {
		return nil, roomErrToRPC(err)
	}
	observability.CallTrace(ctx, h.log, "rpc.room.set_mix_gain", p.ListenerCallID,
		"room_id", p.RoomID, "speaker_call_id", p.SpeakerCallID, "gain", p.Gain)
	return map[string]any{"ok": true}, nil
}

type roomCloseParams struct {
	RoomID string `json:"room_id"`
	Reason string `json:"reason,omitempty"`
}

func (h *Handlers) handleRoomClose(ctx context.Context, conn *rpc.Conn, params json.RawMessage) (any, *rpc.Error) {
	if rpcErr := h.roomsReady(); rpcErr != nil {
		return nil, rpcErr
	}
	var p roomCloseParams
	if rpcErr := rpc.DecodeParams(params, &p); rpcErr != nil {
		return nil, rpcErr
	}
	if p.RoomID == "" {
		return nil, rpc.ErrInvalidParams("room_id required")
	}
	reason := p.Reason
	if reason == "" {
		reason = "room_close"
	}
	if err := h.rooms.Close(p.RoomID, reason); err != nil {
		return nil, roomErrToRPC(err)
	}
	observability.CallTrace(ctx, h.log, "rpc.room.close", p.RoomID, "reason", reason)
	return map[string]any{"ok": true, "room_id": p.RoomID}, nil
}

func roomErrToRPC(err error) *rpc.Error {
	switch err {
	case room.ErrRoomNotFound:
		return rpc.NewErrorf(rpc.CodeCallNotFound, "room not found")
	case room.ErrLegNotFound:
		return rpc.NewErrorf(rpc.CodeCallNotFound, "leg not found")
	case room.ErrLegExists:
		return rpc.ErrCallStateInvalid("leg already in room")
	case room.ErrClosed:
		return rpc.ErrCallStateInvalid("room closed")
	case room.ErrGainRange:
		return rpc.ErrInvalidParams("gain out of range")
	}
	return rpc.NewErrorf(rpc.CodeInternalError, "%v", err)
}
