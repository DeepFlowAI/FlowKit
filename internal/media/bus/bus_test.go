package bus

import (
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(discardWriter{}, &slog.HandlerOptions{Level: slog.LevelError + 1}))
}

type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }

func mkFrame(seq int, src string) Frame {
	return Frame{
		Samples:   make([]int16, SamplesPerFrame),
		Timestamp: time.Duration(seq) * FrameDuration,
		Source:    src,
	}
}

func TestBus_Subscribe_ReceivesPublishedFrames(t *testing.T) {
	b := New(quietLogger())
	defer b.Close()

	s := b.Subscribe()
	require.NotNil(t, s)
	require.Equal(t, 1, b.SubscriberCount())

	b.Publish(mkFrame(1, "trunk"))
	b.Publish(mkFrame(2, "trunk"))

	got1 := <-s.Channel()
	got2 := <-s.Channel()
	require.Equal(t, "trunk", got1.Source)
	require.Equal(t, time.Duration(1)*FrameDuration, got1.Timestamp)
	require.Equal(t, time.Duration(2)*FrameDuration, got2.Timestamp)
}

func TestBus_MultipleSubscribers_AllReceiveEveryFrame(t *testing.T) {
	b := New(quietLogger())
	defer b.Close()

	const N = 5
	subs := make([]*Subscriber, N)
	for i := 0; i < N; i++ {
		subs[i] = b.Subscribe()
	}

	b.Publish(mkFrame(1, "tts"))
	b.Publish(mkFrame(2, "tts"))

	for i, s := range subs {
		got1 := <-s.Channel()
		got2 := <-s.Channel()
		require.Equal(t, time.Duration(1)*FrameDuration, got1.Timestamp, "subscriber %d frame 1", i)
		require.Equal(t, time.Duration(2)*FrameDuration, got2.Timestamp, "subscriber %d frame 2", i)
	}
}

func TestBus_SlowSubscriber_DropsFramesWithoutBlocking(t *testing.T) {
	b := New(quietLogger())
	defer b.Close()

	fast := b.Subscribe()
	slow := b.Subscribe()
	_ = slow // never read — buffer fills, frames get dropped

	const total = SubscriberBufferSize * 5

	// Drain the fast subscriber concurrently — it must NOT block the publisher.
	var got atomic.Int32
	go func() {
		for range fast.Channel() {
			got.Add(1)
		}
	}()

	// The key property: Publish completes promptly even when slow has a full
	// buffer. Measure the publish loop wall time — if Publish blocked on slow,
	// this would take much longer than a few ms.
	start := time.Now()
	for i := 0; i < total; i++ {
		b.Publish(mkFrame(i, "x"))
	}
	elapsed := time.Since(start)
	require.Less(t, elapsed, 500*time.Millisecond, "Publish must not block on slow subscriber (took %v)", elapsed)

	// Slow subscriber must have dropped at least (total - bufferSize) frames
	require.GreaterOrEqual(t, slow.Dropped(), uint64(total-SubscriberBufferSize),
		"slow subscriber should have dropped frames")
	require.GreaterOrEqual(t, b.DroppedTotal(), slow.Dropped(),
		"DroppedTotal must include per-subscriber drops")

	// Fast subscriber must have received SOME frames (drops allowed due to
	// goroutine scheduling, but should be substantial).
	time.Sleep(50 * time.Millisecond) // flush window
	require.Greater(t, got.Load(), int32(0), "fast subscriber should have received at least some frames")
}

func TestBus_DroppedTotal_SurvivesUnsubscribe(t *testing.T) {
	b := New(quietLogger())
	defer b.Close()

	slow := b.Subscribe()
	for i := 0; i < SubscriberBufferSize+3; i++ {
		b.Publish(mkFrame(i, "x"))
	}
	dropped := slow.Dropped()
	require.Greater(t, dropped, uint64(0))
	require.Equal(t, dropped, b.DroppedTotal())

	b.Unsubscribe(slow)
	require.Equal(t, dropped, b.DroppedTotal(), "lifetime total must survive Unsubscribe")
}

func TestBus_Unsubscribe_StopsDelivery(t *testing.T) {
	b := New(quietLogger())
	defer b.Close()

	s := b.Subscribe()
	require.Equal(t, 1, b.SubscriberCount())

	b.Publish(mkFrame(1, "x"))
	<-s.Channel()

	b.Unsubscribe(s)
	require.Equal(t, 0, b.SubscriberCount())
	require.True(t, s.IsClosed())

	// Publishing after unsubscribe should not panic; channel is closed
	b.Publish(mkFrame(2, "x"))
	_, ok := <-s.Channel()
	require.False(t, ok, "channel must be closed after unsubscribe")
}

func TestBus_Unsubscribe_Idempotent(t *testing.T) {
	b := New(quietLogger())
	defer b.Close()
	s := b.Subscribe()
	b.Unsubscribe(s)
	b.Unsubscribe(s) // second call must not panic
}

func TestBus_Unsubscribe_NilSafe(t *testing.T) {
	b := New(quietLogger())
	b.Unsubscribe(nil) // must not panic
}

func TestBus_Close_ClosesAllSubscribers(t *testing.T) {
	b := New(quietLogger())
	s1 := b.Subscribe()
	s2 := b.Subscribe()

	b.Close()

	_, ok1 := <-s1.Channel()
	_, ok2 := <-s2.Channel()
	require.False(t, ok1)
	require.False(t, ok2)
	require.True(t, s1.IsClosed())
	require.True(t, s2.IsClosed())
}

func TestBus_PublishAfterClose_NoOp(t *testing.T) {
	b := New(quietLogger())
	b.Close()
	b.Publish(mkFrame(1, "x")) // must not panic
}

func TestBus_SubscribeAfterClose_ReturnsNil(t *testing.T) {
	b := New(quietLogger())
	b.Close()
	require.Nil(t, b.Subscribe())
}

func TestBus_Concurrent_PublishersAndSubscribers(t *testing.T) {
	b := New(quietLogger())
	defer b.Close()

	const subs = 5
	subscribers := make([]*Subscriber, subs)
	for i := 0; i < subs; i++ {
		subscribers[i] = b.Subscribe()
	}

	const publishers = 4
	const framesPer = 50

	// Drain subscribers in background
	drained := make([]atomic.Int64, subs)
	var drainWG sync.WaitGroup
	for i := 0; i < subs; i++ {
		drainWG.Add(1)
		go func(i int) {
			defer drainWG.Done()
			for range subscribers[i].Channel() {
				drained[i].Add(1)
			}
		}(i)
	}

	var pubWG sync.WaitGroup
	for p := 0; p < publishers; p++ {
		pubWG.Add(1)
		go func(p int) {
			defer pubWG.Done()
			for j := 0; j < framesPer; j++ {
				b.Publish(mkFrame(p*1000+j, "x"))
			}
		}(p)
	}
	pubWG.Wait()

	// Allow a short flush window then close
	time.Sleep(50 * time.Millisecond)
	b.Close()
	drainWG.Wait()

	// Each subscriber should have received SOME frames (drops are allowed
	// because publishers are concurrent and fast)
	for i := 0; i < subs; i++ {
		require.Greater(t, drained[i].Load(), int64(0), "subscriber %d got no frames", i)
	}
}

func TestSubscriber_AccessorsAreSafe(t *testing.T) {
	b := New(quietLogger())
	defer b.Close()
	s := b.Subscribe()
	require.NotEmpty(t, s.ID())
	require.Equal(t, uint64(0), s.Dropped())
	require.False(t, s.IsClosed())
}
