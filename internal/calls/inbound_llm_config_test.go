package calls

import (
	"testing"

	"github.com/DeepFlowAI/flowkit/internal/dialogpolicy"
	"github.com/stretchr/testify/require"
)

func TestShouldBargePartial(t *testing.T) {
	a := &InboundLLMAgent{cfg: InboundLLMConfig{BargeInMinChars: 2}}

	require.False(t, a.shouldBargePartial(""))
	require.False(t, a.shouldBargePartial("  "))
	require.False(t, a.shouldBargePartial("你"))
	require.True(t, a.shouldBargePartial("你好"))

	a.cfg.BargeInMinChars = 0
	require.True(t, a.shouldBargePartial("你"))
}

func TestLoadInboundLLMConfigFromEnv(t *testing.T) {
	t.Setenv("SIP_LLM_BARGE_IN_MIN_CHARS", "0")
	t.Setenv("SIP_LLM_BARGE_IN_ON_START", "true")
	p, err := dialogpolicy.LoadFromEnv()
	require.NoError(t, err)
	dialogpolicy.Set(p)
	t.Cleanup(dialogpolicy.ResetForTest)
	cfg := LoadInboundLLMConfig("hi")
	require.Equal(t, 0, cfg.BargeInMinChars)
	require.True(t, cfg.BargeInOnStart)
	require.Equal(t, "hi", cfg.Greeting)
}
