package llm

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

// sseOnce serves one content chunk then [DONE], capturing the request body.
func sseServer(t *testing.T, gotBody *[]byte, gotAuth *string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*gotBody, _ = io.ReadAll(r.Body)
		*gotAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: [DONE]\n\n")
	}))
}

func runChat(t *testing.T, cfg OpenAICompatConfig, meta RequestMeta) {
	t.Helper()
	c, err := NewOpenAICompatClient(cfg)
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	ch := make(chan string, 8)
	if err := c.StreamChat(context.Background(), meta, []Message{{Role: "user", Content: "x"}}, ch); err != nil {
		t.Fatalf("StreamChat: %v", err)
	}
}

func TestOpenAICompat_InjectsExtendInstanceID(t *testing.T) {
	var body []byte
	var auth string
	srv := sseServer(t, &body, &auth)
	defer srv.Close()

	runChat(t, OpenAICompatConfig{
		APIKey: "tok", Model: "m", BaseURL: srv.URL, ExtendInstanceID: true,
	}, RequestMeta{CallID: "sid-123"})

	var got map[string]any
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("body: %v", err)
	}
	ext, ok := got["extendData"].(map[string]any)
	if !ok || ext["instanceId"] != "sid-123" {
		t.Fatalf("extendData.instanceId not injected: %s", body)
	}
	if auth != "Bearer tok" {
		t.Fatalf("auth = %q", auth)
	}
}

func TestOpenAICompat_NoExtendDataByDefault(t *testing.T) {
	var body []byte
	var auth string
	srv := sseServer(t, &body, &auth)
	defer srv.Close()

	runChat(t, OpenAICompatConfig{
		APIKey: "tok", Model: "m", BaseURL: srv.URL, // ExtendInstanceID off
	}, RequestMeta{CallID: "sid-123"})

	var got map[string]any
	_ = json.Unmarshal(body, &got)
	if _, ok := got["extendData"]; ok {
		t.Fatalf("extendData should be absent when off: %s", body)
	}
}

func TestSessionInitializer_PostsSessionStart(t *testing.T) {
	var body []byte
	var auth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ = io.ReadAll(r.Body)
		auth = r.Header.Get("Authorization")
		_, _ = io.WriteString(w, `{"success":true}`)
	}))
	defer srv.Close()

	s := &SessionInitializer{url: srv.URL, agentID: "agt", event: "session_start", client: srv.Client()}
	s.Init(context.Background(), "sid-9", "13800000000", "57550000")

	var got map[string]any
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("body: %v", err)
	}
	if got["instanceId"] != "sid-9" || got["event"] != "session_start" || got["aiAgentId"] != "agt" {
		t.Fatalf("session_start payload wrong: %s", body)
	}
	if auth != "Bearer agt" {
		t.Fatalf("auth = %q", auth)
	}
	// userData carries the call metadata as a JSON string.
	var ud map[string]string
	if err := json.Unmarshal([]byte(got["userData"].(string)), &ud); err != nil {
		t.Fatalf("userData: %v", err)
	}
	if ud["user_phone"] != "13800000000" || ud["trunk_number"] != "57550000" || ud["callid"] != "sid-9" {
		t.Fatalf("userData wrong: %v", ud)
	}
}

func TestSessionInitializer_NilSafe(t *testing.T) {
	var s *SessionInitializer
	s.Init(context.Background(), "sid", "from", "to") // must not panic
}
