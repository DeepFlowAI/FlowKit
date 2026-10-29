package llm

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"github.com/DeepFlowAI/flowkit/internal/rpc"
	"github.com/stretchr/testify/require"
)

func testConn() *rpc.Conn {
	c := &rpc.Conn{}
	c.MarkHelloOK()
	return c
}

func TestHandleStatus_Disabled(t *testing.T) {
	reg := rpc.NewRegistry(slog.Default())
	RegisterChat(reg, nil, "sys", slog.Default(), nil)

	req, _ := rpc.NewRequest("llm.status", nil, "1")
	resp := reg.ProcessRequest(context.Background(), testConn(), req)
	require.Nil(t, resp.Error)
	var res map[string]any
	require.NoError(t, json.Unmarshal(resp.Result, &res))
	require.Equal(t, false, res["enabled"])
}

func TestHandleStatus_Enabled(t *testing.T) {
	reg := rpc.NewRegistry(slog.Default())
	client := NewDeepSeekClient("key", "deepseek-chat")
	RegisterChat(reg, client, "hello", slog.Default(), nil)

	req, _ := rpc.NewRequest("llm.status", nil, "1")
	resp := reg.ProcessRequest(context.Background(), testConn(), req)
	require.Nil(t, resp.Error)
	var res map[string]any
	require.NoError(t, json.Unmarshal(resp.Result, &res))
	require.Equal(t, true, res["enabled"])
	require.Equal(t, "deepseek", res["provider"])
	require.Equal(t, "deepseek-chat", res["model"])
}

func TestHandleChat_RequiresKey(t *testing.T) {
	reg := rpc.NewRegistry(slog.Default())
	RegisterChat(reg, nil, "sys", slog.Default(), nil)

	req, _ := rpc.NewRequest("llm.chat", map[string]string{"text": "hi"}, "1")
	resp := reg.ProcessRequest(context.Background(), testConn(), req)
	require.NotNil(t, resp.Error)
	require.Equal(t, rpc.CodeLLMUnavailable, resp.Error.Code)
}

func TestHistoryKey(t *testing.T) {
	conn := testConn()
	require.True(t, strings.HasPrefix(historyKey(conn, ""), "conn:"))
	require.Equal(t, "call:abc", historyKey(conn, "abc"))
}
