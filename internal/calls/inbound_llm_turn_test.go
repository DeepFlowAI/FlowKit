package calls

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAcceptUserTurn(t *testing.T) {
	require.False(t, acceptUserTurn(""))
	require.False(t, acceptUserTurn("嗯"))
	require.False(t, acceptUserTurn("  啊  "))
	require.False(t, acceptUserTurn("，"))
	require.False(t, acceptUserTurn("goodo"))
	require.False(t, acceptUserTurn("？，"))
	require.True(t, acceptUserTurn("你好"))
	require.True(t, acceptUserTurn("，你好"))
	require.True(t, acceptUserTurn("你会干什么"))
}

func TestLikelyEchoASR(t *testing.T) {
	require.True(t, likelyEchoASR("你好", "您好，你好，我是助手"))
	require.True(t, likelyEchoASR("你好", "你好"))
	require.False(t, likelyEchoASR("你会干什么", "你好，我是助手"))
}
