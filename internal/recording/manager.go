package recording

import (
	"context"
	"crypto/sha256"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/session"
)

// Manager attaches recorders to new calls when recording is enabled.
type Manager struct {
	cfg     Config
	factory SinkFactory
	log     *slog.Logger
	notify  atomic.Pointer[Notifier]

	mu        sync.Mutex
	recorders map[string]*Recorder
	bridgeRec map[string]*BridgeRecorder // conversation_id → recorder
}

// NewManager builds a Manager. Returns (nil, nil) when cfg.Enabled is false.
func NewManager(cfg Config, log *slog.Logger) (*Manager, error) {
	if !cfg.Enabled {
		return nil, nil
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	factory, err := NewSinkFactory(cfg, log)
	if err != nil {
		return nil, err
	}
	if log == nil {
		log = slog.Default()
	}
	return &Manager{
		cfg:       cfg,
		factory:   factory,
		log:       log,
		recorders: make(map[string]*Recorder),
		bridgeRec: make(map[string]*BridgeRecorder),
	}, nil
}

// SetNotifier sets the optional event broadcaster.
func (m *Manager) SetNotifier(n Notifier) {
	if n == nil {
		m.notify.Store(nil)
		return
	}
	m.notify.Store(&n)
}

// RegisterHooks installs a session.Manager create hook to auto-attach recorders.
func (m *Manager) RegisterHooks(mgr *session.Manager) {
	mgr.AddCreateHook(m.onCallCreated)
}

func (m *Manager) onCallCreated(call *session.Call) {
	m.mu.Lock()
	if _, dup := m.recorders[call.ID()]; dup {
		m.mu.Unlock()
		return
	}
	rec := NewRecorder(m.cfg, m.factory, call, m.log)
	rec.hooks = m.hooksForCall(call, "ai")
	m.recorders[call.ID()] = rec
	m.mu.Unlock()

	go func() {
		rec.Attach(context.Background()) // blocks until call ends
		m.mu.Lock()
		delete(m.recorders, call.ID())
		m.mu.Unlock()
	}()
}

func (m *Manager) hooksForCall(call *session.Call, phase string) *Hooks {
	return m.hooksFor(call.ID(), call.ConversationID(), func() string {
		if m.cfg.CaptureEarlyMedia &&
			call.Direction() == session.DirectionOutbound &&
			!call.EverAnswered() {
			return "early_media"
		}
		return phase
	}, nil, nil)
}

func (m *Manager) hooksForBridge(userLeg, agentLeg *session.Call) *Hooks {
	ids := recordingNotifyIDs(userLeg, agentLeg)
	return m.hooksFor(ids[0], userLeg.ConversationID(), func() string { return "bridged" }, userLeg, agentLeg)
}

func (m *Manager) hooksFor(
	callID, conversationID string, phase func() string,
	userLeg, agentLeg *session.Call,
) *Hooks {
	notifyIDs := []string{callID}
	if userLeg != nil && agentLeg != nil {
		notifyIDs = recordingNotifyIDs(userLeg, agentLeg)
	}
	return &Hooks{
		OnStarted: func(started RecordingStarted) {
			recordingID := stableRecordingID(started.StorageKey)
			params := map[string]any{
				"phase":           phase(),
				"conversation_id": conversationID,
				"recording_id":    recordingID,
				"storage_key":     started.StorageKey,
				"started_at":      started.StartedAt.UTC().Format(time.RFC3339Nano),
			}
			if userLeg != nil && agentLeg != nil {
				params["user_leg_id"] = userLeg.ID()
				params["agent_leg_id"] = agentLeg.ID()
			}
			for _, id := range notifyIDs {
				m.notifyRecordingEvent("call.recording.started", id, recordingID, params)
			}
		},
		OnCompleted: func(completed RecordingCompleted) {
			if completed.URL == "" {
				return
			}
			recordingID := stableRecordingID(completed.StorageKey)
			params := map[string]any{
				"oss_url":         completed.URL,
				"partial":         completed.Partial,
				"phase":           phase(),
				"header_patched":  completed.HeaderPatched,
				"conversation_id": conversationID,
				"recording_id":    recordingID,
				"storage_key":     completed.StorageKey,
				"started_at":      completed.StartedAt.UTC().Format(time.RFC3339Nano),
				"ended_at":        completed.EndedAt.UTC().Format(time.RFC3339Nano),
				"duration_ms":     completed.DurationMS,
			}
			if userLeg != nil && agentLeg != nil {
				params["user_leg_id"] = userLeg.ID()
				params["agent_leg_id"] = agentLeg.ID()
			}
			for _, id := range notifyIDs {
				m.notifyRecordingEvent("call.recording.completed", id, recordingID, params)
			}
		},
	}
}

func stableRecordingID(storageKey string) string {
	sum := sha256.Sum256([]byte(storageKey))
	return fmt.Sprintf("flowkit:recording:%x", sum[:16])
}

func stableRecordingEventID(method, recordingID, callID string) string {
	sum := sha256.Sum256([]byte(method + "\x00" + recordingID + "\x00" + callID))
	return fmt.Sprintf("flowkit:recording-event:%x", sum[:16])
}

func (m *Manager) notifyRecordingEvent(
	method, callID, recordingID string,
	params map[string]any,
) {
	out := make(map[string]any, len(params)+2)
	for key, value := range params {
		out[key] = value
	}
	out["event_id"] = stableRecordingEventID(method, recordingID, callID)
	out["call_leg_id"] = callID
	m.notifyEvent(method, callID, out)
}

func (m *Manager) notifyEvent(method, callID string, params map[string]any) {
	if n := m.notify.Load(); n != nil {
		(*n).Notify(method, callID, params)
	}
}

// OnBridged keeps the caller-leg recorder running and starts a conversation
// bridge recording.
//
// The caller (user) leg recorder is deliberately NOT stopped on bridge: letting
// it run to call end makes its single "ai" recording span the WHOLE call —
// voice-agent phase + human phase. The caller's mic stays on the remote channel
// and the human agent's voice rides the caller leg's outgoing (local) channel
// (bridge WriteOutgoing → post-pacing SentTap), so the one file is the complete,
// stereo-separated call. The agent leg's own per-leg recorder is still stopped,
// and the bridge recorder is kept for non-inbound topologies / as a fallback.
func (m *Manager) OnBridged(userLeg, agentLeg *session.Call) {
	if userLeg == nil || agentLeg == nil {
		return
	}
	go m.detachCallAsync(agentLeg.ID())

	convID := userLeg.ConversationID()
	m.mu.Lock()
	if _, exists := m.bridgeRec[convID]; exists {
		m.mu.Unlock()
		return
	}
	br := NewBridgeRecorder(m.cfg, m.factory, userLeg, agentLeg, m.log)
	br.hooks = m.hooksForBridge(userLeg, agentLeg)
	m.bridgeRec[convID] = br
	m.mu.Unlock()

	go func() {
		br.Run(context.Background())
		m.mu.Lock()
		delete(m.bridgeRec, convID)
		m.mu.Unlock()
	}()
}

// OnLegReplaced follows a takeover: the leg that replaced oldCallID keeps the
// bridge recording going instead of ending it. Also stops the new leg's own
// per-leg recorder, the same handoff OnBridged does, so the leg's audio is not
// captured twice.
func (m *Manager) OnLegReplaced(oldCallID string, newCall *session.Call) {
	if newCall == nil {
		return
	}
	m.mu.Lock()
	var br *BridgeRecorder
	for _, b := range m.bridgeRec {
		if b.AgentLeg().ID() == oldCallID {
			br = b
			break
		}
	}
	m.mu.Unlock()
	if br == nil {
		return
	}
	go m.detachCallAsync(newCall.ID())
	br.ReplaceAgentLeg(newCall)
}

func (m *Manager) detachCallAsync(callID string) {
	m.mu.Lock()
	rec, ok := m.recorders[callID]
	if ok {
		delete(m.recorders, callID)
	}
	m.mu.Unlock()
	if ok {
		rec.StopForBridgeHandoff()
	}
}

// StartCall forces recording to begin on call (idempotent).
func (m *Manager) StartCall(call *session.Call) error {
	m.mu.Lock()
	rec, ok := m.recorders[call.ID()]
	if !ok {
		rec = NewRecorder(m.cfg, m.factory, call, m.log)
		rec.hooks = m.hooksForCall(call, "ai")
		m.recorders[call.ID()] = rec
		go func() {
			rec.Attach(context.Background())
			m.mu.Lock()
			delete(m.recorders, call.ID())
			m.mu.Unlock()
		}()
	}
	m.mu.Unlock()
	return rec.Begin(context.Background())
}

// StopCall stops recording for callID and returns the URL if any.
func (m *Manager) StopCall(callID string) (string, error) {
	var url string
	var rec *Recorder
	var br *BridgeRecorder
	var convID string

	m.mu.Lock()
	rec = m.recorders[callID]
	if rec != nil {
		delete(m.recorders, callID)
	}
	for cid, b := range m.bridgeRec {
		if b.userLeg.ID() == callID || b.AgentLeg().ID() == callID {
			br = b
			convID = cid
			delete(m.bridgeRec, cid)
			break
		}
	}
	m.mu.Unlock()

	if rec != nil {
		rec.Stop()
		if u, ok := rec.url.Load().(string); ok {
			url = u
		}
	}
	if br != nil {
		br.Stop()
		if u := br.URL(); u != "" {
			url = u
		}
		_ = convID
	}
	if url != "" {
		return url, nil
	}
	return "", errRecordingNotActive
}

// Status reports recording state for call_id.
func (m *Manager) Status(callID string) map[string]any {
	m.mu.Lock()
	defer m.mu.Unlock()
	if rec, ok := m.recorders[callID]; ok {
		return rec.Status()
	}
	for _, br := range m.bridgeRec {
		if br.userLeg.ID() == callID || br.AgentLeg().ID() == callID {
			return br.Status()
		}
	}
	return map[string]any{
		"active":  false,
		"url":     "",
		"partial": false,
		"mode":    "none",
	}
}

// ActiveCount returns in-flight per-call + bridge recorders.
func (m *Manager) ActiveCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.recorders) + len(m.bridgeRec)
}

// PickBridgeLegs returns user (inbound) and agent legs for bridge recording.
func PickBridgeLegs(a, b *session.Call) (user, agent *session.Call) {
	if a.Direction() == session.DirectionInbound {
		return a, b
	}
	if b.Direction() == session.DirectionInbound {
		return b, a
	}
	return a, b
}
