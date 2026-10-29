package calls

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/llm"
)

// Barge-in reports for call.converse_external_agent: when the caller cuts a
// reply off, tell the agent which turn it was and what the caller had heard,
// so its conversation record can mark where the interruption happened.

const (
	// defaultMsPerChar is the speech-rate guess for a cut-off sentence when no
	// sentence of the same turn finished playing (CosyVoice zh ≈ 4.5 chars/s).
	defaultMsPerChar       = 220
	interruptReportTimeout = 5 * time.Second
)

type playedSpeech struct {
	text      string
	playedMs  int64
	completed bool
}

// playTracker wraps a turn's SpeechSink and records every sentence it plays.
// StreamSentences calls Play from one goroutine and waits for it before
// returning, so played is safe to read once StreamSentences is done.
type playTracker struct {
	llm.SpeechSink
	leadPadMs int64 // leading silence on the first sentence, not speech
	played    []playedSpeech
}

func (t *playTracker) Play(p llm.PreparedSpeech) error {
	err := t.SpeechSink.Play(p)
	if ps, ok := p.(*PreparedSay); ok && ps != nil {
		t.played = append(t.played, playedSpeech{text: ps.text, playedMs: ps.PlayedMs(), completed: err == nil})
	}
	return err
}

// PadTurnTail forwards to the wrapped sink; StreamSentences looks for it by
// type assertion, which embedding the interface alone would hide.
func (t *playTracker) PadTurnTail() {
	if p, ok := t.SpeechSink.(interface{ PadTurnTail() }); ok {
		p.PadTurnTail()
	}
}

// heardText estimates what the caller heard: each sentence that finished
// playing, plus the share of the cut-off one that its played audio covers.
// The speech rate comes from this turn's finished sentences when it has any.
func heardText(played []playedSpeech, leadPadMs int64) string {
	var b strings.Builder
	var doneMs, doneChars int64
	for i, p := range played {
		ms := p.playedMs
		if i == 0 {
			ms -= leadPadMs
		}
		if ms < 0 {
			ms = 0
		}
		runes := []rune(p.text)
		if p.completed {
			b.WriteString(p.text)
			doneMs += ms
			doneChars += int64(len(runes))
			continue
		}
		perChar := int64(defaultMsPerChar)
		if doneChars > 0 && doneMs >= doneChars {
			perChar = doneMs / doneChars
		}
		n := ms / perChar
		if n > int64(len(runes)) {
			n = int64(len(runes))
		}
		b.WriteString(string(runes[:n]))
		break
	}
	return b.String()
}

// interruptURL derives the agent's barge-in endpoint from its turn URL
// (VoiceSpeed: …/turn/stream → …/turn/interrupt). Empty for any other turn
// URL shape: that agent gets no reports.
func interruptURL(turnURL string) string {
	const suffix = "/turn/stream"
	if !strings.HasSuffix(turnURL, suffix) {
		return ""
	}
	return strings.TrimSuffix(turnURL, suffix) + "/turn/interrupt"
}

type converseInterruptRequest struct {
	SessionID string `json:"session_id"`
	TurnID    string `json:"turn_id,omitempty"` // empty: the fixed greeting
	HeardText string `json:"heard_text"`
}

// reportBargeIn reports a cancelled reply if the caller cut it off, which is
// when the converse loop is still running: hangup and handoff end the loop and
// are not interruptions. turnID "" is the fixed greeting.
func (a *converseAgent) reportBargeIn(turnID, heard string) {
	if a.loopCtx == nil || a.loopCtx.Err() != nil {
		return
	}
	url := interruptURL(a.turnURL)
	if url == "" {
		return
	}
	body, _ := json.Marshal(converseInterruptRequest{SessionID: a.callID, TurnID: turnID, HeardText: heard})
	// Fire and forget: the next turn must not wait on it, and a failed report
	// only loses the mark.
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), interruptReportTimeout)
		defer cancel()
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			return
		}
		req.Header.Set("Content-Type", "application/json")
		if a.token != "" {
			req.Header.Set("Authorization", "Bearer "+a.token)
		}
		resp, err := converseHTTPClient.Do(req)
		if err != nil {
			a.log.Warn("converse interrupt report failed", "turn_id", turnID, "err", err)
			return
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			a.log.Warn("converse interrupt report rejected", "turn_id", turnID, "status", resp.StatusCode)
		}
	}()
}
