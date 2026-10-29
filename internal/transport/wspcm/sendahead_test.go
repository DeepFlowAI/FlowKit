package wspcm

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
)

// dialPair returns a server-side MediaIO built with opts and the client end.
func dialPair(t *testing.T, opts ...Option) (*MediaIO, *websocket.Conn) {
	t.Helper()
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	mioCh := make(chan *MediaIO, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		mioCh <- New(ws, 4, 64, slog.New(slog.NewTextHandler(io.Discard, nil)), opts...)
	}))
	t.Cleanup(srv.Close)
	client, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(srv.URL, "http"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	mio := <-mioCh
	t.Cleanup(func() { _ = mio.Close() })
	return mio, client
}

type wireMsg struct {
	audio   int // samples, for binary frames
	control playbackControl
}

func readUntil(t *testing.T, c *websocket.Conn, want func([]wireMsg) bool) []wireMsg {
	t.Helper()
	var got []wireMsg
	for !want(got) {
		_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
		typ, data, err := c.ReadMessage()
		require.NoError(t, err)
		if typ == websocket.BinaryMessage {
			got = append(got, wireMsg{audio: len(data) / 2})
			continue
		}
		var pc playbackControl
		require.NoError(t, json.Unmarshal(data, &pc))
		got = append(got, wireMsg{control: pc})
	}
	return got
}

func TestSendAheadForwardsPlaybackControlInOrder(t *testing.T) {
	mio, client := dialPair(t, WithSendAhead(200*time.Millisecond))

	mio.OutgoingPCM() <- make([]int16, 50*frameSamples) // 1 s
	// The lead arrives at once, far ahead of real time.
	start := time.Now()
	readUntil(t, client, func(m []wireMsg) bool { return len(m) >= 10 })
	require.Less(t, time.Since(start), 100*time.Millisecond)

	mio.DuckOutgoing(0.25)
	mio.StopOutgoing()
	got := readUntil(t, client, func(m []wireMsg) bool {
		return len(m) > 1 && m[len(m)-2].control.Action == "flush"
	})
	var actions []string
	var gains []float64
	for _, m := range got {
		if m.control.Type != "" {
			require.Equal(t, "playback", m.control.Type)
			actions = append(actions, m.control.Action)
			if m.control.Action == "gain" {
				gains = append(gains, m.control.Gain)
			}
		}
	}
	// A duck lasts one utterance: after a stop the next reply must not start
	// at the ducked level.
	require.Equal(t, []string{"gain", "flush", "gain"}, actions)
	require.Equal(t, []float64{0.25, 1}, gains)
	require.Zero(t, mio.PlaybackStats().DuckEvents, "the client applies the gain, not the pacer")
}

func TestWithoutSendAheadNoControlFrames(t *testing.T) {
	mio, client := dialPair(t)

	mio.OutgoingPCM() <- make([]int16, 3*frameSamples)
	mio.PauseOutgoing()
	mio.ResumeOutgoing()
	mio.StopOutgoing()
	mio.OutgoingPCM() <- make([]int16, frameSamples)
	got := readUntil(t, client, func(m []wireMsg) bool { return len(m) >= 2 })
	for _, m := range got {
		require.Empty(t, m.control.Type, "real-time clients get audio only")
	}
}
