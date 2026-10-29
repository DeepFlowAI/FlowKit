package llm

import (
	"fmt"
	"os"
	"strings"

	"github.com/DeepFlowAI/flowkit/internal/config"
)

const defaultSystemPrompt = "你是 flowkit 智能语音助手，请用简洁、口语化的中文回答（每次回复不要超过 80 字）。"

// envTrue reports whether env var name is set to a truthy value (1/true/yes/on).
func envTrue(name string) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(name))) {
	case "1", "true", "yes", "on":
		return true
	}
	return false
}

// ProviderSettings is the kernel LLM configuration (YAML + env).
type ProviderSettings struct {
	Provider     string
	SystemPrompt string
	APIKey       string
	Model        string
	BaseURL      string
}

// SettingsFromEnv reads LLM_* env vars.
func SettingsFromEnv() ProviderSettings {
	s := ProviderSettings{
		Provider:     strings.TrimSpace(os.Getenv("LLM_PROVIDER")),
		SystemPrompt: os.Getenv("LLM_SYSTEM_PROMPT"),
		APIKey:       strings.TrimSpace(os.Getenv("LLM_API_KEY")),
		Model:        strings.TrimSpace(os.Getenv("LLM_MODEL")),
		BaseURL:      strings.TrimSpace(os.Getenv("LLM_BASE_URL")),
	}
	if s.SystemPrompt == "" {
		s.SystemPrompt = defaultSystemPrompt
	}
	return s
}

// MergeSettings combines YAML llm config with LLM_* env (env wins when set).
func MergeSettings(cfg config.LLMConfig) ProviderSettings {
	env := SettingsFromEnv()
	s := ProviderSettings{
		Provider:     cfg.Provider,
		SystemPrompt: cfg.SystemPrompt,
		APIKey:       env.APIKey,
		Model:        env.Model,
		BaseURL:      env.BaseURL,
	}
	if cfg.Config != nil {
		if v := stringVal(cfg.Config["api_key"]); v != "" {
			s.APIKey = v
		}
		if v := stringVal(cfg.Config["model"]); v != "" {
			s.Model = v
		}
		if v := stringVal(cfg.Config["base_url"]); v != "" {
			s.BaseURL = v
		}
	}
	if env.Provider != "" {
		s.Provider = env.Provider
	}
	if env.SystemPrompt != "" && env.SystemPrompt != defaultSystemPrompt {
		s.SystemPrompt = env.SystemPrompt
	} else if s.SystemPrompt == "" {
		s.SystemPrompt = env.SystemPrompt
	}
	if s.SystemPrompt == "" {
		s.SystemPrompt = defaultSystemPrompt
	}
	return s
}

func stringVal(v any) string {
	if s, ok := v.(string); ok {
		return strings.TrimSpace(s)
	}
	return ""
}

// NewProvider builds a Provider from settings. Empty provider or missing API
// key returns (nil, nil) — LLM RPC stays registered but reports disabled.
func NewProvider(s ProviderSettings) (Provider, error) {
	provider := strings.TrimSpace(s.Provider)
	if provider == "" || provider == "mock" || provider == "disabled" {
		if s.APIKey != "" {
			return nil, fmt.Errorf("llm: LLM_API_KEY set but LLM_PROVIDER is empty")
		}
		return nil, nil
	}
	if s.APIKey == "" {
		return nil, nil
	}

	baseURL := s.BaseURL
	model := s.Model
	name := provider

	switch provider {
	case "deepseek":
		if baseURL == "" {
			baseURL = "https://api.deepseek.com/v1"
		}
		if model == "" {
			model = "deepseek-chat"
		}
	case "openai":
		if baseURL == "" {
			baseURL = "https://api.openai.com/v1"
		}
		name = "openai"
	case "openai_compat":
		name = "openai_compat"
	default:
		name = provider
	}

	if baseURL == "" {
		return nil, fmt.Errorf("llm: provider %q requires base_url (LLM_BASE_URL)", provider)
	}
	if model == "" {
		return nil, fmt.Errorf("llm: provider %q requires model (LLM_MODEL)", provider)
	}

	return NewOpenAICompatClient(OpenAICompatConfig{
		ProviderName:     name,
		APIKey:           s.APIKey,
		Model:            model,
		BaseURL:          baseURL,
		ExtraHeaders:     ExtraHeadersFromEnv(),
		ExtraBody:        ExtraBodyFromEnv(),
		ExtendInstanceID: envTrue("LLM_EXTEND_INSTANCE_ID"),
	})
}
