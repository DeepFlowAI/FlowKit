package calls

import (
	"context"
	"testing"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/tts"
	"github.com/stretchr/testify/require"
)

// waitAudibleClosed reports whether the prepared utterance became audible
// within d.
func waitAudibleClosed(p *PreparedSay, d time.Duration) bool {
	select {
	case <-p.audible:
		return true
	case <-time.After(d):
		return false
	}
}

func TestRelayAudibleSignalsOnFirstChunkAndForwardsAll(t *testing.T) {
	in := make(chan tts.PCMChunk)
	out := make(chan tts.PCMChunk, 4)
	audible := make(chan struct{})
	go relayAudible(context.Background(), in, out, audible)

	select {
	case <-audible:
		t.Fatal("audible closed before any chunk arrived")
	case <-time.After(20 * time.Millisecond):
	}

	in <- tts.PCMChunk{Samples: []int16{1, 2}}
	select {
	case <-audible:
	case <-time.After(time.Second):
		t.Fatal("audible not closed after the first chunk")
	}

	in <- tts.PCMChunk{Samples: []int16{3}, Final: true}
	close(in)

	var got []tts.PCMChunk
	for chunk := range out {
		got = append(got, chunk)
	}
	require.Len(t, got, 2)
	require.Equal(t, []int16{1, 2}, got[0].Samples)
	require.Equal(t, []int16{3}, got[1].Samples)
	require.True(t, got[1].Final)
}

func TestRelayAudibleSignalsWhenSynthesisProducesNothing(t *testing.T) {
	in := make(chan tts.PCMChunk)
	out := make(chan tts.PCMChunk, 1)
	audible := make(chan struct{})
	go relayAudible(context.Background(), in, out, audible)

	// A synthesis that fails after the request was accepted must not strand a
	// waiter holding back the next sentence.
	close(in)
	select {
	case <-audible:
	case <-time.After(time.Second):
		t.Fatal("audible not closed after synthesis ended empty")
	}
	_, open := <-out
	require.False(t, open, "out should be closed")
}

func TestWaitAudibleReturnsOnTimeoutAndCancel(t *testing.T) {
	p := &PreparedSay{audible: make(chan struct{})}

	start := time.Now()
	p.WaitAudible(context.Background(), 30*time.Millisecond)
	require.GreaterOrEqual(t, time.Since(start), 30*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.WaitAudible(ctx, time.Minute)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("WaitAudible ignored a cancelled context")
	}

	close(p.audible)
	require.True(t, waitAudibleClosed(p, time.Second))
}

func TestWaitAudibleOnNilIsSafe(t *testing.T) {
	var p *PreparedSay
	p.WaitAudible(context.Background(), time.Millisecond)
	(&PreparedSay{}).WaitAudible(context.Background(), time.Millisecond)
}
