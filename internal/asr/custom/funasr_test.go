package custom

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/asr"
	"github.com/gorilla/websocket"
)

// fakeFunASR upgrades the connection, records the config frame, waits for one
// binary PCM frame, then replies with an online (partial) and offline (final)
// result. The recorded config is sent on cfgCh.
func fakeFunASR(t *testing.T, cfgCh chan<- map[string]any) *httptest.Server {
	t.Helper()
	up := websocket.Upgrader{}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()

		_, data, err := c.ReadMessage()
		if err != nil {
			return
		}
		var cfg map[string]any
		_ = json.Unmarshal(data, &cfg)
		cfgCh <- cfg

		// Wait for at least one binary frame before responding.
		for {
			mt, _, err := c.ReadMessage()
			if err != nil {
				return
			}
			if mt == websocket.BinaryMessage {
				break
			}
		}
		_ = c.WriteJSON(map[string]any{"mode": "2pass-online", "text": "你好"})
		_ = c.WriteJSON(map[string]any{"mode": "2pass-offline", "text": "你好世界"})
		// Hold the connection open until the client closes it.
		for {
			if _, _, err := c.ReadMessage(); err != nil {
				return
			}
		}
	}))
}

func wsURL(s *httptest.Server) string {
	return "ws" + strings.TrimPrefix(s.URL, "http")
}

func collect(t *testing.T, ch <-chan asr.Event, n int) []asr.Event {
	t.Helper()
	var evs []asr.Event
	timeout := time.After(3 * time.Second)
	for len(evs) < n {
		select {
		case ev, ok := <-ch:
			if !ok {
				return evs
			}
			evs = append(evs, ev)
		case <-timeout:
			t.Fatalf("timed out waiting for %d events, got %d", n, len(evs))
		}
	}
	return evs
}

func TestFunASR_PartialThenFinal(t *testing.T) {
	cfgCh := make(chan map[string]any, 1)
	srv := fakeFunASR(t, cfgCh)
	defer srv.Close()

	p, err := New(Config{BaseURL: wsURL(srv), Hotwords: "flowkit 20"})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := p.SendPCM([]int16{1, 2, 3, 4}); err != nil {
		t.Fatal(err)
	}

	evs := collect(t, p.Events(), 2)
	if evs[0].Type != asr.EventPartial || evs[0].Text != "你好" {
		t.Errorf("event[0] = %+v, want Partial 你好", evs[0])
	}
	if evs[1].Type != asr.EventFinal || evs[1].Text != "你好世界" {
		t.Errorf("event[1] = %+v, want Final 你好世界", evs[1])
	}
	if evs[0].Sequence == evs[1].Sequence {
		t.Error("sequence numbers should be monotonic/distinct")
	}

	cfg := <-cfgCh
	if cfg["mode"] != "2pass" {
		t.Errorf("mode = %v, want 2pass", cfg["mode"])
	}
	if cfg["wav_format"] != "pcm" {
		t.Errorf("wav_format = %v, want pcm", cfg["wav_format"])
	}
	if cfg["audio_fs"].(float64) != 16000 {
		t.Errorf("audio_fs = %v, want 16000", cfg["audio_fs"])
	}
	if cfg["hotwords"] != "flowkit 20" {
		t.Errorf("hotwords = %v, want 'flowkit 20'", cfg["hotwords"])
	}

	if err := p.Stop(); err != nil {
		t.Fatal(err)
	}
	// Events channel must close after Stop.
	if _, ok := <-p.Events(); ok {
		// drain any buffered, then it should close
		for range p.Events() {
		}
	}
}

func TestFunASR_SendBeforeStart(t *testing.T) {
	p, _ := New(Config{BaseURL: "ws://127.0.0.1:1"})
	if err := p.SendPCM([]int16{1}); err != asr.ErrNotStarted {
		t.Errorf("err = %v, want ErrNotStarted", err)
	}
}

func TestFunASR_StartDialError(t *testing.T) {
	p, _ := New(Config{BaseURL: "ws://127.0.0.1:1"})
	if err := p.Start(context.Background()); err == nil {
		t.Error("expected dial error")
	}
	// events channel should be closed on failed start
	if _, ok := <-p.Events(); ok {
		t.Error("events channel should be closed after failed Start")
	}
}

func TestNew_RequiresBaseURL(t *testing.T) {
	if _, err := New(Config{}); err == nil {
		t.Error("expected error when BaseURL empty")
	}
}

func TestFunASR_DefaultsApplied(t *testing.T) {
	p, err := New(Config{BaseURL: "ws://x"})
	if err != nil {
		t.Fatal(err)
	}
	if p.cfg.Mode != "2pass" {
		t.Errorf("Mode default = %q, want 2pass", p.cfg.Mode)
	}
	if p.cfg.SampleRate != 16000 {
		t.Errorf("SampleRate default = %d, want 16000", p.cfg.SampleRate)
	}
}

// fakeFunASRNoRead upgrades, consumes the config frame, then never reads again
// so client binary writes eventually block on a full TCP window.
func fakeFunASRNoRead(t *testing.T) *httptest.Server {
	t.Helper()
	up := websocket.Upgrader{}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()
		_, _, _ = c.ReadMessage() // config only
		select {}                  // never read PCM — half-open peer
	}))
}

// TestFunASR_SendPCMWriteDeadline ensures a half-open FunASR peer cannot
// block SendPCM forever under writeMu (control-plane stall).
func TestFunASR_SendPCMWriteDeadline(t *testing.T) {
	srv := fakeFunASRNoRead(t)
	defer srv.Close()

	p, err := New(Config{BaseURL: wsURL(srv)})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Large frames fill the send buffer faster so WriteMessage hits the deadline.
	chunk := make([]int16, 16*1024)
	deadline := time.Now().Add(writeTimeout + 3*time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		lastErr = p.SendPCM(chunk)
		if lastErr != nil {
			break
		}
	}
	if lastErr == nil {
		t.Fatal("expected SendPCM to fail with write deadline against non-reading peer")
	}

	stopDone := make(chan struct{})
	go func() {
		_ = p.Stop()
		close(stopDone)
	}()
	select {
	case <-stopDone:
	case <-time.After(writeTimeout + 2*time.Second):
		t.Fatal("Stop blocked longer than writeTimeout — writeMu deadlock?")
	}
}
