package llm

import "context"

// Provider streams chat completions from a configured LLM backend.
type Provider interface {
	// Name returns the provider id (e.g. "deepseek", "openai_compat").
	Name() string
	// Model returns the model id sent to the backend.
	Model() string
	// StreamChat pushes token deltas to tokenCh until done or error; tokenCh
	// is closed on return.
	StreamChat(ctx context.Context, meta RequestMeta, messages []Message, tokenCh chan<- string) error
}
