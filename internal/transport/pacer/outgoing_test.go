package pacer_test

import (
	"context"
	"testing"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/transport/pacer"
	"github.com/stretchr/testify/require"
)

func TestOutgoingUnboundIsSafe(t *testing.T) {
	var o pacer.Outgoing
	require.Nil(t, o.OutgoingPCM())
	require.Nil(t, o.Pacer())
	require.Equal(t, pacer.Stats{}, o.PlaybackStats())
	o.PauseOutgoing()
	o.ResumeOutgoing()
	o.StopOutgoing()
	o.DuckOutgoing(0.5)
	o.RestoreOutgoing()
	o.SetPlaybackFeeding(true)
	o.SetSentTap(nil)
	o.Close() // no-op
	require.ErrorIs(t, o.WaitPlaybackIdle(context.Background()), pacer.ErrClosed)
}

// TestOutgoingWaitIdleAfterEnqueueBlocksUntilSent is the real call-site shape:
// enqueue then WaitPlaybackIdle on the same goroutine with no sink polling.
// It must not return before the last frame has been handed to Sink.
func TestOutgoingWaitIdleAfterEnqueueBlocksUntilSent(t *testing.T) {
	sink := &fakeSink{}
	var o pacer.Outgoing
	o.Bind(sink, pacer.WithQueue(8))
	t.Cleanup(o.Close)

	const n = 5
	for i := 0; i < n; i++ {
		select {
		case o.OutgoingPCM() <- frame(int16(i + 1)):
		case <-time.After(time.Second):
			t.Fatal("OutgoingPCM blocked")
		}
	}

	require.NoError(t, o.WaitPlaybackIdle(context.Background()))
	require.Equal(t, n, sink.len(),
		"WaitPlaybackIdle returned before all enqueued frames were sent")
}

func TestOutgoingSecondBindDoesNotLeak(t *testing.T) {
	sink := &fakeSink{}
	var o pacer.Outgoing
	o.Bind(sink)
	first := o.Pacer()
	require.NotNil(t, first)
	o.Bind(sink) // discarded
	require.Same(t, first, o.Pacer())
	o.Close()
}
