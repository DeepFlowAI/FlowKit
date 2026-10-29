package rpc

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/config"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
)

func startAuthServer(t *testing.T, apiKey string) *Server {
	t.Helper()
	srv := New(config.RPCConfig{Listen: "127.0.0.1:0", APIKey: apiKey}, NewRegistry(quietLogger()), quietLogger())
	srv.RegisterHTTP("/media/pcm", srv.RequireAuth(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})))
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = srv.Start(ctx) }()
	select {
	case <-srv.Ready():
	case <-time.After(2 * time.Second):
		t.Fatal("server failed to bind within 2s")
	}
	t.Cleanup(cancel)
	return srv
}

func dialWS(t *testing.T, srv *Server, header http.Header) (*websocket.Conn, int) {
	t.Helper()
	ws, resp, err := websocket.DefaultDialer.Dial("ws://"+srv.Addr()+"/ws", header)
	if err != nil {
		require.NotNil(t, resp, "dial error without HTTP response: %v", err)
		return nil, resp.StatusCode
	}
	t.Cleanup(func() { _ = ws.Close() })
	return ws, resp.StatusCode
}

func TestWSAuth_RejectsMissingAndWrongKey(t *testing.T) {
	srv := startAuthServer(t, "s3cret")

	cases := map[string]http.Header{
		"no header":    nil,
		"wrong key":    {"Authorization": {"Bearer nope"}},
		"wrong scheme": {"Authorization": {"Basic s3cret"}},
		"bare key":     {"Authorization": {"s3cret"}},
	}
	for name, h := range cases {
		t.Run(name, func(t *testing.T) {
			ws, code := dialWS(t, srv, h)
			require.Nil(t, ws)
			require.Equal(t, http.StatusUnauthorized, code)
		})
	}
}

func TestWSAuth_AcceptsBearerKey(t *testing.T) {
	srv := startAuthServer(t, "s3cret")
	ws, code := dialWS(t, srv, http.Header{"Authorization": {"Bearer s3cret"}})
	require.NotNil(t, ws)
	require.Equal(t, http.StatusSwitchingProtocols, code)
}

func TestWSAuth_QueryKeyForBrowsers(t *testing.T) {
	srv := startAuthServer(t, "s3cret")
	dial := func(query string, header http.Header) int {
		ws, resp, err := websocket.DefaultDialer.Dial("ws://"+srv.Addr()+"/ws"+query, header)
		if err == nil {
			_ = ws.Close()
		}
		require.NotNil(t, resp)
		return resp.StatusCode
	}
	require.Equal(t, http.StatusSwitchingProtocols, dial("?api_key=s3cret", nil))
	require.Equal(t, http.StatusUnauthorized, dial("?api_key=nope", nil))
	require.Equal(t, http.StatusUnauthorized, dial("?api_key=", nil))
	// A wrong header is not rescued by a correct query key.
	require.Equal(t, http.StatusUnauthorized, dial("?api_key=s3cret", http.Header{"Authorization": {"Bearer nope"}}))
}

func TestWSAuth_NoKeyConfiguredAllowsAll(t *testing.T) {
	srv := startAuthServer(t, "")
	ws, _ := dialWS(t, srv, nil)
	require.NotNil(t, ws)
}

func TestRequireAuth_ProtectsExtraRoutesButNotHealth(t *testing.T) {
	srv := startAuthServer(t, "s3cret")
	base := "http://" + srv.Addr()

	resp, err := http.Get(base + "/media/pcm")
	require.NoError(t, err)
	_ = resp.Body.Close()
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)

	req, _ := http.NewRequest(http.MethodGet, base+"/media/pcm", nil)
	req.Header.Set("Authorization", "Bearer s3cret")
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	_ = resp.Body.Close()
	require.Equal(t, http.StatusNoContent, resp.StatusCode)

	resp, err = http.Get(base + "/health")
	require.NoError(t, err)
	_ = resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}
