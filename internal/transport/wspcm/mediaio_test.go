package wspcm

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/transport"
	"github.com/DeepFlowAI/flowkit/internal/transport/pacer"
	"github.com/stretchr/testify/require"
)

// compile-time interface checks — WS-PCM MediaIO embeds the shared pacer.
var (
	_ transport.MediaIO = (*MediaIO)(nil)
	_ pacer.Sink        = (*MediaIO)(nil)
)

// newBare builds a MediaIO without a WebSocket (SendFrame fails closed). Used to
// unit-test pause/stop/close via the embedded pacer.Outgoing.
func newBare() *MediaIO {
	m := &MediaIO{
		incoming: make(chan []int16, 4),
		done:     make(chan struct{}),
	}
	m.Outgoing.Bind(m, pacer.WithQueue(8))
	return m
}

func TestPauseDrainsOutgoing(t *testing.T) {
	m := newBare()
	t.Cleanup(func() {
		_ = m.Close()
		m.closeIncoming()
	})
	m.PauseOutgoing()

	// Past channel capacity: pacer must drain into its residual while paused.
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
	// nil ws → SendFrame errors → pacer goes idle; Wait must still return.
	require.NoError(t, m.WaitPlaybackIdle(ctx))
}

func TestStopOutgoingUnpausesAndDrops(t *testing.T) {
	m := newBare()
	t.Cleanup(func() {
		_ = m.Close()
		m.closeIncoming()
	})
	m.PauseOutgoing()

	frame := make([]int16, pacer.FrameSamples)
	for i := range frame {
		frame[i] = 100
	}
	select {
	case m.OutgoingPCM() <- frame:
	case <-time.After(time.Second):
		t.Fatal("could not enqueue while paused")
	}

	m.StopOutgoing()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	start := time.Now()
	require.NoError(t, m.WaitPlaybackIdle(ctx))
	require.LessOrEqual(t, time.Since(start), 500*time.Millisecond,
		"StopOutgoing must force idle promptly (also clears pause)")
}

// TestCloseDoesNotCloseOutgoingUnderProducers races WriteOutgoing-style sends
// against Close. Close must not close the outgoing channel (send-on-closed panic).
func TestCloseDoesNotCloseOutgoingUnderProducers(t *testing.T) {
	m := newBare()
	var wg sync.WaitGroup
	out := m.OutgoingPCM()
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				select {
				case out <- []int16{1, 2, 3}:
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
	// Pump-less Close leaves incoming open; close it so range consumers unblock.
	m.closeIncoming()
	_, ok := <-m.incoming
	require.False(t, ok, "incoming must be closed after closeIncoming")
}

// TestCloseUnblocksIncomingConsumer ensures Close + pump exit closes incoming
// so Call.pumpIncoming's range loop terminates. Bare MediaIO has no pump, so
// we simulate the pump's defer closeIncoming after Close.
func TestCloseUnblocksIncomingConsumer(t *testing.T) {
	m := newBare()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range m.incoming {
		}
	}()
	require.NoError(t, m.Close())
	m.closeIncoming() // mirrors runIncomingPump defer
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("incoming consumer did not unblock after closeIncoming")
	}
}
