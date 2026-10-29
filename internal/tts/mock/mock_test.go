package mock

import (
	"context"
	"testing"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/tts"
	"github.com/stretchr/testify/require"
)

// compile-time interface check
var _ tts.Provider = (*Mock)(nil)

func TestMock_Synthesize_EmitsChunksProportionalToText(t *testing.T) {
	m := New()
	ch, err := m.Synthesize(context.Background(), "你好", tts.Options{})
	require.NoError(t, err)

	total := 0
	chunkCount := 0
	var sawFinal bool
	for c := range ch {
		total += len(c.Samples)
		chunkCount++
		if c.Final {
			sawFinal = true
		}
	}
	// 2 chars × 800 samples/char = 1600 samples expected
	require.Equal(t, 1600, total)
	require.Greater(t, chunkCount, 0)
	require.True(t, sawFinal, "last chunk must be marked Final")
	require.Equal(t, int64(1), m.CallCount.Load())
}

func TestMock_Synthesize_EmptyText_Errors(t *testing.T) {
	m := New()
	_, err := m.Synthesize(context.Background(), "", tts.Options{})
	require.ErrorIs(t, err, tts.ErrEmptyText)
}

func TestMock_Synthesize_AfterClose_Errors(t *testing.T) {
	m := New()
	require.NoError(t, m.Close())
	_, err := m.Synthesize(context.Background(), "hi", tts.Options{})
	require.ErrorIs(t, err, tts.ErrClosed)
}

func TestMock_Synthesize_ContextCancellation_StopsEarly(t *testing.T) {
	m := New()
	ctx, cancel := context.WithCancel(context.Background())

	// Long text so synthesis takes a while if consumer is slow
	ch, err := m.Synthesize(ctx, "我们要测试取消行为这段文字稍长", tts.Options{})
	require.NoError(t, err)

	// Read one chunk then cancel
	<-ch
	cancel()

	// Drain — channel must close promptly
	closed := false
	timeout := time.After(time.Second)
loop:
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				closed = true
				break loop
			}
		case <-timeout:
			break loop
		}
	}
	require.True(t, closed, "channel must close after cancellation")
}

func TestMock_Close_Idempotent(t *testing.T) {
	m := New()
	require.NoError(t, m.Close())
	require.NoError(t, m.Close())
}
