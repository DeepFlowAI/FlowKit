package llm

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewProvider_Disabled(t *testing.T) {
	p, err := NewProvider(ProviderSettings{})
	require.NoError(t, err)
	require.Nil(t, p)
}

func TestNewProvider_KeyWithoutProvider(t *testing.T) {
	_, err := NewProvider(ProviderSettings{APIKey: "sk-x"})
	require.Error(t, err)
}

func TestNewProvider_DeepseekPreset(t *testing.T) {
	p, err := NewProvider(ProviderSettings{
		Provider: "deepseek",
		APIKey:   "sk-x",
	})
	require.NoError(t, err)
	require.NotNil(t, p)
	require.Equal(t, "deepseek", p.Name())
	require.Equal(t, "deepseek-chat", p.Model())
}

func TestNewProvider_OpenAICompatRequiresBaseURL(t *testing.T) {
	_, err := NewProvider(ProviderSettings{
		Provider: "openai_compat",
		APIKey:   "sk-x",
		Model:    "gpt-4o-mini",
	})
	require.Error(t, err)
}
