package llm

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/observability"
	"github.com/DeepFlowAI/flowkit/internal/rpc"
	"github.com/DeepFlowAI/flowkit/internal/session"
	"github.com/google/uuid"
)

const maxHistoryMessages = 40

// ChatHandlers serves llm.* JSON-RPC methods using the kernel LLM provider.
type ChatHandlers struct {
	client    Provider
	sysPrompt string
	log       *slog.Logger
	mgr       *session.Manager
	// emptyFallback is spoken when the LLM yields no content (e.g. a backend
	// whose knowledge base returns an empty reply for unmatched queries), so the
	// call never goes silent. From LLM_EMPTY_FALLBACK; "" disables it.
	emptyFallback string

	mu       sync.Mutex
	sessions map[string]*chatSession
}

type chatSession struct {
	hist   []Message
	cancel context.CancelFunc
}

// RegisterChat wires llm.chat, llm.reset, and llm.status into reg.
// mgr is optional; when set, LLM requests include call/conversation metadata.
func RegisterChat(reg *rpc.Registry, client Provider, sysPrompt string, log *slog.Logger, mgr *session.Manager) *ChatHandlers {
	if log == nil {
		log = slog.Default()
	}
	// LLM_NO_SYSTEM_PROMPT=true → send no system message at all (backend manages
	// the prompt/knowledge server-side, e.g. DeepFlow OpenAPI agent). Otherwise
	// fall back to the flowkit default when none configured.
	if envTrue("LLM_NO_SYSTEM_PROMPT") {
		sysPrompt = ""
	} else if sysPrompt == "" {
		sysPrompt = defaultSystemPrompt
	}
	emptyFallback := os.Getenv("LLM_EMPTY_FALLBACK")
	if emptyFallback == "" && !envTrue("LLM_NO_EMPTY_FALLBACK") {
		emptyFallback = "不好意思，没听清，麻烦您再说一遍。"
	}
	h := &ChatHandlers{
		client:        client,
		sysPrompt:     sysPrompt,
		log:           log,
		mgr:           mgr,
		emptyFallback: emptyFallback,
		sessions:      make(map[string]*chatSession),
	}
	reg.Register("llm.chat", h.handleChat)
	reg.Register("llm.reset", h.handleReset)
	reg.Register("llm.status", h.handleStatus)
	return h
}

// withSystem prepends the configured system message, or returns hist unchanged
// when no system prompt is set (LLM_NO_SYSTEM_PROMPT — the backend manages the
// prompt/knowledge server-side).
func (h *ChatHandlers) withSystem(hist []Message) []Message {
	if h.sysPrompt == "" {
		return hist
	}
	return append([]Message{{Role: "system", Content: h.sysPrompt}}, hist...)
}

func (h *ChatHandlers) handleStatus(_ context.Context, _ *rpc.Conn, _ json.RawMessage) (any, *rpc.Error) {
	if h.client == nil {
		return map[string]any{
			"enabled": false,
		}, nil
	}
	return map[string]any{
		"enabled":  true,
		"provider": h.client.Name(),
		"model":    h.client.Model(),
		"system":   h.sysPrompt,
	}, nil
}

type chatParams struct {
	Text   string `json:"text"`
	CallID string `json:"call_id,omitempty"`
}

func (h *ChatHandlers) handleChat(ctx context.Context, conn *rpc.Conn, params json.RawMessage) (any, *rpc.Error) {
	if h.client == nil {
		return nil, rpc.NewErrorf(rpc.CodeLLMUnavailable, "LLM provider not configured on server")
	}
	var p chatParams
	if rpcErr := rpc.DecodeParams(params, &p); rpcErr != nil {
		return nil, rpcErr
	}
	if strings.TrimSpace(p.Text) == "" {
		return nil, rpc.ErrInvalidParams("missing text")
	}

	key := historyKey(conn, p.CallID)
	turnID := shortID()
	meta := h.requestMeta(p.CallID)

	turnCtx, cancel := context.WithCancel(ctx)
	h.mu.Lock()
	sess := h.sessionLocked(key)
	if sess.cancel != nil {
		sess.cancel()
	}
	sess.cancel = cancel
	sess.hist = append(sess.hist, Message{Role: "user", Content: p.Text})
	history := h.withSystem(sess.hist)
	h.mu.Unlock()

	observability.DialogTurn(ctx, h.log, meta.ConversationID, meta.CallID, "user", p.Text,
		"root_call_id", meta.RootCallID, "phase", meta.Phase)

	go h.runTurn(turnCtx, conn, key, turnID, history, meta)

	return map[string]string{"turn_id": turnID}, nil
}

func (h *ChatHandlers) runTurn(ctx context.Context, conn *rpc.Conn, key, turnID string, history []Message, meta RequestMeta) {
	defer func() {
		h.mu.Lock()
		if sess := h.sessions[key]; sess != nil && sess.cancel != nil {
			sess.cancel = nil
		}
		h.mu.Unlock()
	}()

	tokenCh := make(chan string, 32)
	errCh := make(chan error, 1)
	go func() {
		errCh <- h.client.StreamChat(ctx, meta, history, tokenCh)
	}()

	var full strings.Builder
	for delta := range tokenCh {
		full.WriteString(delta)
		h.notify(conn, "llm.token", map[string]any{
			"turn_id": turnID,
			"delta":   delta,
			"text":    full.String(),
		})
	}

	err := <-errCh
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		h.log.Debug("llm stream failed", "turn_id", turnID, "err", err)
		h.notify(conn, "llm.error", map[string]any{
			"turn_id": turnID,
			"code":    rpc.CodeLLMUnavailable,
			"message": err.Error(),
		})
		return
	}

	reply := strings.TrimSpace(full.String())
	h.mu.Lock()
	if sess := h.sessions[key]; sess != nil {
		sess.hist = append(sess.hist, Message{Role: "assistant", Content: reply})
		if len(sess.hist) > maxHistoryMessages {
			sess.hist = append([]Message(nil), sess.hist[len(sess.hist)-maxHistoryMessages:]...)
		}
	}
	h.mu.Unlock()

	observability.DialogTurn(ctx, h.log, meta.ConversationID, meta.CallID, "assistant", reply,
		"root_call_id", meta.RootCallID, "phase", meta.Phase, "turn_id", turnID)

	h.notify(conn, "llm.done", map[string]any{
		"turn_id":  turnID,
		"text":     reply,
		"provider": h.client.Name(),
		"model":    h.client.Model(),
	})
}

type resetParams struct {
	CallID string `json:"call_id,omitempty"`
}

func (h *ChatHandlers) handleReset(_ context.Context, conn *rpc.Conn, params json.RawMessage) (any, *rpc.Error) {
	var p resetParams
	if rpcErr := rpc.DecodeParams(params, &p); rpcErr != nil {
		return nil, rpcErr
	}
	key := historyKey(conn, p.CallID)
	h.mu.Lock()
	if sess := h.sessions[key]; sess != nil {
		if sess.cancel != nil {
			sess.cancel()
		}
		delete(h.sessions, key)
	}
	h.mu.Unlock()
	return map[string]bool{"ok": true}, nil
}

func (h *ChatHandlers) sessionLocked(key string) *chatSession {
	sess, ok := h.sessions[key]
	if !ok {
		sess = &chatSession{}
		h.sessions[key] = sess
	}
	return sess
}

func historyKey(conn *rpc.Conn, callID string) string {
	if callID != "" {
		return "call:" + callID
	}
	return "conn:" + conn.ID()
}

func (h *ChatHandlers) notify(conn *rpc.Conn, method string, params map[string]any) {
	n, err := rpc.NewNotification(method, params)
	if err != nil {
		h.log.Debug("llm notify encode failed", "method", method, "err", err)
		return
	}
	if err := conn.SendNotification(n); err != nil {
		h.log.Debug("llm notify send failed", "method", method, "err", err)
	}
}

func shortID() string {
	return strings.ReplaceAll(uuid.NewString(), "-", "")[:8]
}

func (h *ChatHandlers) requestMeta(callID string) RequestMeta {
	if callID == "" || h.mgr == nil {
		return RequestMeta{CallID: callID}
	}
	c, err := h.mgr.Get(callID)
	if err != nil {
		return RequestMeta{CallID: callID}
	}
	return RequestMetaFromCall(c)
}

// DefaultChatTimeout is the suggested upper bound for waiting on llm.done.
const DefaultChatTimeout = 60 * time.Second
