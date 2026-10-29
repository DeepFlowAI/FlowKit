package webrtc

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/transport"
	"github.com/DeepFlowAI/flowkit/internal/transport/pacer"
	"github.com/stretchr/testify/require"
)

// compile-time interface checks — WebRTC MediaIO embeds the shared pacer.
var (
	_ transport.MediaIO = (*pcmMediaIO)(nil)
	_ pacer.Sink        = (*pcmMediaIO)(nil)
)

// newBare builds a pcmMediaIO without a local track (SendFrame fails closed).
// Used to unit-test pause/stop/close via the embedded pacer.Outgoing.
func newBare(inBuf, outBuf int) *pcmMediaIO {
	return newPCMMediaIO(nil, nil, inBuf, outBuf)
}

// TestPCMMediaIOLosslessPause verifies pause drains OutgoingPCM into the pacer
// residual so producers never block, and Resume continues without dropping.
func TestPCMMediaIOLosslessPause(t *testing.T) {
	m := newBare(1, 8)
	t.Cleanup(func() { _ = m.Close() })

	m.PauseOutgoing()

	const n = 24
	for i := 0; i < n; i++ {
		select {
		case m.OutgoingPCM() <- []int16{int16(i), 2, 3}:
		case <-time.After(200 * time.Millisecond):
			t.Fatalf("OutgoingPCM blocked while paused at write %d/%d", i+1, n)
		}
	}

	m.ResumeOutgoing()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	// nil localTrack → SendFrame errors → pacer goes idle; Wait must still return.
	require.NoError(t, m.WaitPlaybackIdle(ctx))
}

// TestCloseDoesNotCloseOutgoingUnderProducers races producers against Close.
// Close must not close the outgoing channel (send-on-closed panic).
func TestCloseDoesNotCloseOutgoingUnderProducers(t *testing.T) {
	m := newBare(4, 8)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				select {
				case m.OutgoingPCM() <- []int16{1, 2, 3}:
				case <-m.done:
					return
				default:
				}
			}
		}()
	}
	time.Sleep(5 * time.Millisecond)
	require.NoError(t, m.Close())
	wg.Wait()
	// No incoming pump started → Close closes incoming so range consumers unblock.
	_, ok := <-m.incoming
	require.False(t, ok)
}
