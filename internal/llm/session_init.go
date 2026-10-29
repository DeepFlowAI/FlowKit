package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

// SessionInitializer fires a one-shot "session start" callback when a call comes
// in, before any LLM turn. Some voice backends (e.g. the DeepFlow OpenAPI aliyun
// robot) use it to pre-initialise the session so the later /chat/completions call
// (carrying the same instanceId via extendData) can hit a pre-generation cache.
//
// It is OPTIONAL — enabled only when LLM_SESSION_INIT_URL is set. The completion
// request works without it (the backend falls back to real-time generation).
type SessionInitializer struct {
	url     string
	agentID string // = the bearer token / agent_id; reuses LLM_API_KEY
	event   string // session_start event name
	client  *http.Client
}

// NewSessionInitializerFromEnv builds an initializer from env, or nil if
// LLM_SESSION_INIT_URL is unset (feature off).
//
//	LLM_SESSION_INIT_URL   POST target for the session_start callback
//	LLM_SESSION_INIT_AGENT agent/aiAgentId (default: LLM_API_KEY)
//	LLM_SESSION_INIT_EVENT event name (default: "session_start")
func NewSessionInitializerFromEnv() *SessionInitializer {
	url := strings.TrimSpace(os.Getenv("LLM_SESSION_INIT_URL"))
	if url == "" {
		return nil
	}
	agent := strings.TrimSpace(os.Getenv("LLM_SESSION_INIT_AGENT"))
	if agent == "" {
		agent = strings.TrimSpace(os.Getenv("LLM_API_KEY"))
	}
	event := strings.TrimSpace(os.Getenv("LLM_SESSION_INIT_EVENT"))
	if event == "" {
		event = "session_start"
	}
	return &SessionInitializer{
		url:     url,
		agentID: agent,
		event:   event,
		client:  &http.Client{Timeout: 5 * time.Second},
	}
}

// Init posts the session_start callback. instanceID must equal the per-call sid
// later sent as extendData.instanceId (flowkit uses the SIP call id for both).
// Best-effort: it blocks on the HTTP round-trip, so callers should run it in a
// goroutine. Failures are logged, never fatal.
func (s *SessionInitializer) Init(ctx context.Context, instanceID, userPhone, trunkNumber string) {
	if s == nil || s.url == "" || instanceID == "" {
		return
	}
	userData, _ := json.Marshal(map[string]string{
		"callid":       instanceID,
		"user_phone":   userPhone,
		"trunk_number": trunkNumber,
	})
	payload := map[string]any{
		"aiAgentId":    s.agentID,
		"instanceId":   instanceID,
		"event":        s.event,
		"code":         0,
		"message":      "",
		"timestamp":    strconv.FormatInt(time.Now().UnixMilli(), 10),
		"callbackType": "agent_status",
		"userData":     string(userData),
	}
	body, _ := json.Marshal(payload)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.url, bytes.NewReader(body))
	if err != nil {
		slog.Default().Warn("llm.session_init build", "instance_id", instanceID, "err", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	if s.agentID != "" {
		req.Header.Set("Authorization", "Bearer "+s.agentID)
	}
	start := time.Now()
	resp, err := s.client.Do(req)
	if err != nil {
		slog.Default().Warn("llm.session_init failed", "instance_id", instanceID, "err", err)
		return
	}
	defer func() { _ = resp.Body.Close() }()
	slog.Default().Info("llm.session_init",
		"instance_id", instanceID, "status", resp.StatusCode, "ms", time.Since(start).Milliseconds())
}
