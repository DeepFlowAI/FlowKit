package llm

// Message is one turn in an OpenAI-style chat history.
type Message struct {
	Role    string `json:"role"` // "system" / "user" / "assistant"
	Content string `json:"content"`
}
