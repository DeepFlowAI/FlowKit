package wsjson

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
)

func TestCallFailsWhenServerCloses(t *testing.T) {
	up := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()
		// Read one request then drop the connection without responding.
		_, _, _ = c.ReadMessage()
		_ = c.Close()
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	ctx := context.Background()
	client, err := Connect(ctx, wsURL)
	require.NoError(t, err)
	defer client.Close()

	start := time.Now()
	_, err = client.Call(ctx, "test.hang", map[string]any{})
	require.Error(t, err)
	require.Less(t, time.Since(start), 2*time.Second, "pending RPC must fail promptly on disconnect")
}

func TestCloseFailsPending(t *testing.T) {
	up := websocket.Upgrader{}
	accepted := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()
		close(accepted)
		// Hold open; never respond.
		for {
			if _, _, err := c.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	client, err := Connect(context.Background(), wsURL)
	require.NoError(t, err)
	<-accepted

	done := make(chan error, 1)
	go func() {
		_, err := client.Call(context.Background(), "test.hang", json.RawMessage(`{}`))
		done <- err
	}()
	time.Sleep(20 * time.Millisecond)
	_ = client.Close()

	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Call did not unblock after Close")
	}
}
