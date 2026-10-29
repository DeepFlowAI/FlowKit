package llm

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/DeepFlowAI/flowkit/internal/observability"
)

// OpenAICompatConfig configures an OpenAI-compatible chat/completions API.
type OpenAICompatConfig struct {
	ProviderName string // logical provider id exposed via llm.status
	APIKey       string
	Model        string
	BaseURL      string // e.g. https://api.deepseek.com/v1
	ExtraHeaders map[string]string
	ExtraBody    map[string]any // merged into request body (model/messages/stream win)
	// ExtendInstanceID, when true, injects extendData.instanceId = sid (the SIP
	// call id) per request. Required by voice backends that key a pre-generation
	// cache on it (e.g. DeepFlow OpenAPI aliyun robot). Off by default.
	ExtendInstanceID bool
}

// OpenAICompatClient implements Provider against OpenAI-compatible SSE APIs.
type OpenAICompatClient struct {
	providerName     string
	apiKey           string
	model            string
	baseURL          string
	extraHeaders     map[string]string
	extraBody        map[string]any
	extendInstanceID bool
	httpClient       *http.Client
}

// NewOpenAICompatClient constructs a client. baseURL should not include a
// trailing slash.
func NewOpenAICompatClient(cfg OpenAICompatConfig) (*OpenAICompatClient, error) {
	if cfg.APIKey == "" {
		return nil, fmt.Errorf("llm: api_key required")
	}
	if cfg.BaseURL == "" {
		return nil, fmt.Errorf("llm: base_url required")
	}
	model := cfg.Model
	if model == "" {
		return nil, fmt.Errorf("llm: model required")
	}
	name := cfg.ProviderName
	if name == "" {
		name = "openai_compat"
	}
	return &OpenAICompatClient{
		providerName: name,
		apiKey:       cfg.APIKey,
		model:        model,
		baseURL:      strings.TrimRight(cfg.BaseURL, "/"),
		extraHeaders:     cfg.ExtraHeaders,
		extraBody:        cfg.ExtraBody,
		extendInstanceID: cfg.ExtendInstanceID,
		httpClient:       &http.Client{},
	}, nil
}

func (c *OpenAICompatClient) Name() string  { return c.providerName }
func (c *OpenAICompatClient) Model() string { return c.model }

// StreamChat implements Provider.
func (c *OpenAICompatClient) StreamChat(ctx context.Context, meta RequestMeta, messages []Message, tokenCh chan<- string) error {
	defer close(tokenCh)

	payload := make(map[string]any, len(c.extraBody)+3)
	for k, v := range c.extraBody {
		payload[k] = v
	}
	payload["model"] = c.model
	payload["messages"] = messages
	payload["stream"] = true
	// Per-call extendData.instanceId (= sid) so a pre-generation cache can be hit.
	// Merge over any static extendData from extraBody without mutating the shared map.
	if c.extendInstanceID && meta.CallID != "" {
		ext := map[string]any{}
		if e, ok := payload["extendData"].(map[string]any); ok {
			for k, v := range e {
				ext[k] = v
			}
		}
		ext["instanceId"] = meta.CallID
		payload["extendData"] = ext
	}
	// Ask for token usage in the final SSE chunk (OpenAI/vLLM/DeepSeek compatible).
	if _, ok := payload["stream_options"]; !ok {
		payload["stream_options"] = map[string]any{"include_usage": true}
	}
	body, _ := json.Marshal(payload)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.baseURL+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Accept", "text/event-stream")
	c.applyMetaHeaders(req, meta)

	observability.LLMRequest(ctx, slog.Default(), req.Method, req.URL.String(), req.Header, body,
		"conversation_id", meta.ConversationID,
		"call_id", meta.CallID,
		"root_call_id", meta.RootCallID,
		"phase", meta.Phase,
		"provider", c.providerName,
		"model", c.model,
	)

	start := time.Now()
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("llm request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		errBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("llm HTTP %d: %s", resp.StatusCode, string(errBody))
	}

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	var (
		gotFirst    bool
		deltas      int
		contentRune int
		usage       *usageStats
	)
	logCompleted := func() {
		fields := []any{
			"call_id", meta.CallID,
			"conversation_id", meta.ConversationID,
			"root_call_id", meta.RootCallID,
			"phase", meta.Phase,
			"provider", c.providerName,
			"model", c.model,
			"total_ms", time.Since(start).Milliseconds(),
			"deltas", deltas,
			"content_chars", contentRune,
		}
		if usage != nil {
			fields = append(fields,
				"prompt_tokens", usage.PromptTokens,
				"completion_tokens", usage.CompletionTokens,
				"total_tokens", usage.TotalTokens)
		}
		slog.Default().Info("llm.completed", fields...)
	}

	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		data := strings.TrimPrefix(line, "data: ")
		if data == "[DONE]" {
			logCompleted()
			return nil
		}

		var chunk struct {
			Choices []struct {
				Delta struct {
					Content string `json:"content"`
				} `json:"delta"`
			} `json:"choices"`
			Usage *usageStats `json:"usage"`
		}
		if err := json.Unmarshal([]byte(data), &chunk); err != nil {
			continue
		}
		if chunk.Usage != nil {
			usage = chunk.Usage
		}
		if len(chunk.Choices) == 0 {
			continue
		}
		delta := chunk.Choices[0].Delta.Content
		if delta == "" {
			continue
		}
		if !gotFirst {
			gotFirst = true
			slog.Default().Info("llm.first_token",
				"call_id", meta.CallID,
				"conversation_id", meta.ConversationID,
				"root_call_id", meta.RootCallID,
				"phase", meta.Phase,
				"provider", c.providerName,
				"model", c.model,
				"ttft_ms", time.Since(start).Milliseconds())
		}
		deltas++
		contentRune += utf8.RuneCountInString(delta)
		select {
		case tokenCh <- delta:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("llm SSE read: %w", err)
	}
	logCompleted()
	return nil
}

// usageStats mirrors the OpenAI streaming usage object (present in the final
// chunk when stream_options.include_usage is set).
type usageStats struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

func (c *OpenAICompatClient) applyMetaHeaders(req *http.Request, meta RequestMeta) {
	if meta.ConversationID != "" {
		req.Header.Set("X-Conversation-ID", meta.ConversationID)
	}
	if meta.CallID != "" {
		req.Header.Set("X-SID", meta.CallID)
	}
	if meta.RootCallID != "" {
		req.Header.Set("X-Root-Call-Id", meta.RootCallID)
	}
	if meta.Phase != "" {
		req.Header.Set("X-Phase", meta.Phase)
	}
	if meta.From != "" {
		req.Header.Set("X-Caller-Number", meta.From)
	}
	if meta.To != "" {
		req.Header.Set("X-Called-Number", meta.To)
	}
	for k, v := range c.extraHeaders {
		req.Header.Set(k, v)
	}
}
