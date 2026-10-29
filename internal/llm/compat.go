package llm

// Legacy aliases kept for older callers. Prefer NewProvider + OpenAICompatClient.

// DeepSeekClient is an OpenAI-compatible client preset for DeepSeek.
type DeepSeekClient = OpenAICompatClient

// NewDeepSeekClient constructs a DeepSeek-backed client (learn examples only).
func NewDeepSeekClient(apiKey, model string) *OpenAICompatClient {
	if model == "" {
		model = "deepseek-chat"
	}
	c, err := NewOpenAICompatClient(OpenAICompatConfig{
		ProviderName: "deepseek",
		APIKey:       apiKey,
		Model:        model,
		BaseURL:      "https://api.deepseek.com/v1",
	})
	if err != nil {
		panic(err)
	}
	return c
}
