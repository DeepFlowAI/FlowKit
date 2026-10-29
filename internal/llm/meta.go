package llm

import (
	"encoding/json"
	"os"
	"strings"

	"github.com/DeepFlowAI/flowkit/internal/session"
)

// RequestMeta is forwarded to the upstream LLM as HTTP headers and used for logging.
type RequestMeta struct {
	CallID         string
	ConversationID string
	RootCallID     string
	Phase          string
	From           string
	To             string
}

// RequestMetaFromCall builds meta from a live Call.
func RequestMetaFromCall(c *session.Call) RequestMeta {
	if c == nil {
		return RequestMeta{}
	}
	return RequestMeta{
		CallID:         c.ID(),
		ConversationID: c.ConversationID(),
		RootCallID:     c.RootCallID(),
		Phase:          c.Phase(),
		From:           c.From(),
		To:             c.To(),
	}
}

// ExtraHeadersFromEnv parses LLM_EXTRA_HEADERS as a JSON object, e.g.
// {"X-Tenant-Id":"prod","X-Channel":"sip"}.
func ExtraHeadersFromEnv() map[string]string {
	raw := strings.TrimSpace(os.Getenv("LLM_EXTRA_HEADERS"))
	if raw == "" {
		return nil
	}
	var m map[string]string
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		k = strings.TrimSpace(k)
		if k != "" {
			out[k] = v
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// ExtraBodyFromEnv parses LLM_EXTRA_BODY as a JSON object merged into the
// chat/completions request body, e.g. to disable qwen thinking:
// {"chat_template_kwargs":{"enable_thinking":false}}.
func ExtraBodyFromEnv() map[string]any {
	raw := strings.TrimSpace(os.Getenv("LLM_EXTRA_BODY"))
	if raw == "" {
		return nil
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		return nil
	}
	if len(m) == 0 {
		return nil
	}
	return m
}
