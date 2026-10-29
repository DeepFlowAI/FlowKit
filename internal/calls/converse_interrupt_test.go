package calls

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/llm"
	"github.com/DeepFlowAI/flowkit/internal/session"
	"github.com/stretchr/testify/require"
)

func TestHeardText(t *testing.T) {
	cases := []struct {
		name      string
		played    []playedSpeech
		leadPadMs int64
		want      string
	}{
		{"nothing played", nil, 0, ""},
		{"cut before any audio", []playedSpeech{{text: "您好。", playedMs: 0}}, 0, ""},
		{
			// 4 chars in 800ms → 200ms/char, so 600ms of the next sentence is 3 chars.
			"rate from finished sentence",
			[]playedSpeech{{text: "您好，请", playedMs: 800, completed: true}, {text: "携带身份证到前台", playedMs: 600}},
			0, "您好，请携带身",
		},
		{"default rate", []playedSpeech{{text: "携带身份证到前台", playedMs: 440}}, 0, "携带"},
		{"lead pad is not speech", []playedSpeech{{text: "携带身份证到前台", playedMs: 1440}}, 1000, "携带"},
		{"capped at the sentence", []playedSpeech{{text: "好的", playedMs: 5000}}, 0, "好的"},
		{
			"cut between sentences",
			[]playedSpeech{{text: "好的。", playedMs: 600, completed: true}},
			0, "好的。",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, heardText(tc.played, tc.leadPadMs))
		})
	}
}

func TestInterruptURL(t *testing.T) {
	require.Equal(t, "https://vs/api/v1/agents/3/turn/interrupt",
		interruptURL("https://vs/api/v1/agents/3/turn/stream"))
	require.Empty(t, interruptURL("https://agent.example/turn"))
}

// fakePlaySink plays every prepared sentence in full except the one named cut.
type fakePlaySink struct {
	cut    string
	padded bool
}

func (s *fakePlaySink) Prepare(text string) (llm.PreparedSpeech, error) {
	return &PreparedSay{text: text}, nil
}

func (s *fakePlaySink) Play(p llm.PreparedSpeech) error {
	ps := p.(*PreparedSay)
	ps.playedMs = int64(len([]rune(ps.text))) * 100
	if ps.text == s.cut {
		ps.playedMs /= 2
		return ErrPlaybackInterrupted
	}
	return nil
}

func (s *fakePlaySink) PadTurnTail() { s.padded = true }

func TestPlayTracker_RecordsPlaybackAndForwardsTailPad(t *testing.T) {
	inner := &fakePlaySink{cut: "第二句"}
	tr := &playTracker{SpeechSink: inner}
	for _, text := range []string{"第一句", "第二句"} {
		ps, err := tr.Prepare(text)
		require.NoError(t, err)
		_ = tr.Play(ps)
	}
	tr.PadTurnTail()

	require.Equal(t, []playedSpeech{
		{text: "第一句", playedMs: 300, completed: true},
		{text: "第二句", playedMs: 150},
	}, tr.played)
	require.True(t, inner.padded)
	require.Equal(t, "第一句第", heardText(tr.played, 0))
}

func TestReportBargeIn_PostsHeardTextWhileLoopRuns(t *testing.T) {
	got := make(chan converseInterruptRequest, 1)
	var gotAuth, gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotPath = r.URL.Path
		var body converseInterruptRequest
		_ = json.NewDecoder(r.Body).Decode(&body)
		got <- body
	}))
	defer srv.Close()

	a := &converseAgent{
		callID: "call-9", turnURL: srv.URL + "/agents/3/turn/stream", token: "sk-1",
		loopCtx: context.Background(), log: slog.Default(),
	}
	a.reportBargeIn("t1", "您好")

	select {
	case body := <-got:
		require.Equal(t, converseInterruptRequest{SessionID: "call-9", TurnID: "t1", HeardText: "您好"}, body)
	case <-time.After(3 * time.Second):
		t.Fatal("no interrupt report")
	}
	require.Equal(t, "/agents/3/turn/interrupt", gotPath)
	require.Equal(t, "Bearer sk-1", gotAuth)
}

// A turn cancelled mid-reply (barge-in) is reported under the same turn_id the
// stream request carried.
func TestConverseDoTurn_BargeInReportsSameTurnID(t *testing.T) {
	capture := &synthCapture{}
	_, mgr, h := newParityStack(t, capture)
	call := mgr.Create(session.DirectionInbound)
	_ = bindCollectingIO(t, call)

	streamed := make(chan converseTurnRequest, 1)
	reported := make(chan converseInterruptRequest, 1)
	mux := http.NewServeMux()
	mux.HandleFunc("/turn/stream", func(w http.ResponseWriter, r *http.Request) {
		var body converseTurnRequest
		_ = json.NewDecoder(r.Body).Decode(&body)
		streamed <- body
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"type\":\"delta\",\"text\":\"您好，需要先在官网预约。\"}\n\n"))
		w.(http.Flusher).Flush()
		<-r.Context().Done() // still generating when the caller barges in
	})
	mux.HandleFunc("/turn/interrupt", func(w http.ResponseWriter, r *http.Request) {
		var body converseInterruptRequest
		_ = json.NewDecoder(r.Body).Decode(&body)
		reported <- body
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	a := &converseAgent{h: h, callID: call.ID(), turnURL: srv.URL + "/turn/stream",
		loopCtx: context.Background(), log: quietLogger()}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		a.doTurn(ctx, "怎么办理", 900, true)
		close(done)
	}()
	req := <-streamed
	require.NotEmpty(t, req.TurnID)
	require.Eventually(t, func() bool { return len(capture.all()) > 0 }, 3*time.Second, 10*time.Millisecond)
	cancel()
	<-done

	select {
	case got := <-reported:
		require.Equal(t, call.ID(), got.SessionID)
		require.Equal(t, req.TurnID, got.TurnID)
	case <-time.After(3 * time.Second):
		t.Fatal("no interrupt report")
	}
}

func TestReportBargeIn_SkippedWhenLoopEnded(t *testing.T) {
	hit := make(chan struct{}, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { hit <- struct{}{} }))
	defer srv.Close()

	loopCtx, cancel := context.WithCancel(context.Background())
	cancel() // hangup / handoff: not an interruption
	a := &converseAgent{callID: "c", turnURL: srv.URL + "/turn/stream", loopCtx: loopCtx, log: slog.Default()}
	a.reportBargeIn("t1", "")

	select {
	case <-hit:
		t.Fatal("reported after the loop ended")
	case <-time.After(200 * time.Millisecond):
	}
}
