package pacer_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/transport/pacer"
	"github.com/stretchr/testify/require"
)

// fakeSink records each SendFrame with a monotonic timestamp.
type fakeSink struct {
	mu      sync.Mutex
	frames  [][]int16
	times   []time.Time
	entered chan struct{} // closed on first SendFrame entry (optional)
	block   chan struct{} // if non-nil, SendFrame waits for it once before record
}

func (s *fakeSink) SendFrame(samples []int16) error {
	s.mu.Lock()
	if s.entered != nil {
		close(s.entered)
		s.entered = nil
	}
	block := s.block
	s.block = nil
	s.mu.Unlock()

	if block != nil {
		<-block
	}

	s.mu.Lock()
	cp := append([]int16(nil), samples...)
	s.frames = append(s.frames, cp)
	s.times = append(s.times, time.Now())
	s.mu.Unlock()
	return nil
}

func (s *fakeSink) snapshot() (frames [][]int16, times []time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	frames = make([][]int16, len(s.frames))
	for i, f := range s.frames {
		frames[i] = append([]int16(nil), f...)
	}
	times = append([]time.Time(nil), s.times...)
	return frames, times
}

func (s *fakeSink) len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.frames)
}

func newPacer(t *testing.T, sink pacer.Sink, opts ...pacer.Option) *pacer.Pacer {
	t.Helper()
	p := pacer.New(sink, opts...)
	t.Cleanup(p.Close)
	return p
}

func frame(v int16) []int16 {
	out := make([]int16, pacer.FrameSamples)
	for i := range out {
		out[i] = v
	}
	return out
}

func samples(n int, v int16) []int16 {
	out := make([]int16, n)
	for i := range out {
		out[i] = v
	}
	return out
}

func waitFrames(t *testing.T, s *fakeSink, n int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if s.len() >= n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d frames; got %d", n, s.len())
}

func TestTailFrameZeroPadded(t *testing.T) {
	sink := &fakeSink{}
	p := newPacer(t, sink)

	// 1.5 frames of non-zero samples → expect 2 full frames; second half silence.
	half := pacer.FrameSamples / 2
	p.OutgoingPCM() <- samples(pacer.FrameSamples+half, 1000)

	waitFrames(t, sink, 2, time.Second)
	require.NoError(t, p.WaitIdle(context.Background()))

	frames, _ := sink.snapshot()
	require.Len(t, frames, 2)
	require.Len(t, frames[0], pacer.FrameSamples)
	require.Len(t, frames[1], pacer.FrameSamples)
	for i, s := range frames[0] {
		require.Equal(t, int16(1000), s, "frame0[%d]", i)
	}
	for i := 0; i < half; i++ {
		require.Equal(t, int16(1000), frames[1][i], "frame1 head[%d]", i)
	}
	for i := half; i < pacer.FrameSamples; i++ {
		require.Equal(t, int16(0), frames[1][i], "frame1 pad[%d]", i)
	}
}

func TestPauseDoesNotBlockProducer(t *testing.T) {
	sink := &fakeSink{}
	const queue = 8
	p := newPacer(t, sink, pacer.WithQueue(queue))

	p.Pause() // sync: send goroutine is now in pause-drain

	const n = 64 // far beyond queue capacity
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < n; i++ {
			p.OutgoingPCM() <- frame(int16(i + 1))
		}
	}()

	select {
	case <-done:
		// Producer finished — was not blocked on a full queue.
	case <-time.After(2 * time.Second):
		t.Fatal("producer blocked while paused; pause must drain OutgoingPCM")
	}

	p.Resume()
	waitFrames(t, sink, n, 5*time.Second)
	require.NoError(t, p.WaitIdle(context.Background()))

	frames, _ := sink.snapshot()
	require.Len(t, frames, n)
	for i, f := range frames {
		require.Equal(t, int16(i+1), f[0], "frame order %d", i)
	}
}

func TestSendPhaseFirstFrameImmediate(t *testing.T) {
	// Hold the first SendFrame so we can observe pacing without measuring
	// wall-clock gaps: after it returns, a paced sender waits a full
	// FrameDuration before the next call; a bursting sender dumps the queue.
	entered := make(chan struct{})
	release := make(chan struct{})
	sink := &fakeSink{entered: entered, block: release}
	p := newPacer(t, sink)

	const burst = 3
	for i := 0; i < burst; i++ {
		p.OutgoingPCM() <- frame(int16(i + 1))
	}

	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("first SendFrame never entered — send-then-wait should start immediately")
	}
	require.Equal(t, 0, sink.len(), "blocked SendFrame must not have recorded yet")

	close(release)
	waitFrames(t, sink, 1, time.Second)
	// Half a pace period: paced send still waiting; burst would have flushed all.
	time.Sleep(pacer.FrameDuration / 2)
	require.Equal(t, 1, sink.len(), "must not burst remaining frames after the first send")

	waitFrames(t, sink, burst, time.Second)
	require.NoError(t, p.WaitIdle(context.Background()))
	frames, _ := sink.snapshot()
	require.Len(t, frames, burst)
	for i, f := range frames {
		require.Equal(t, int16(i+1), f[0], "frame order %d", i)
	}
}

func TestFeedingUnderrunEmitsSilenceThenIdle(t *testing.T) {
	sink := &fakeSink{}
	p := newPacer(t, sink)

	p.SetFeeding(true)
	p.OutgoingPCM() <- frame(7)
	waitFrames(t, sink, 1, time.Second)

	// Queue empty while feeding → silence frames keep the stream alive.
	waitFrames(t, sink, 3, time.Second)
	frames, _ := sink.snapshot()
	require.Equal(t, int16(7), frames[0][0])
	require.True(t, allZero(frames[1]), "expected underrun silence")
	require.True(t, allZero(frames[2]), "expected underrun silence")
	st := p.Snapshot()
	require.GreaterOrEqual(t, st.SilenceFrames, uint64(2))
	require.Equal(t, uint64(1), st.UnderrunEvents, "one dry spell is one underrun event")

	// Audio resumes, then runs dry again → a second event.
	p.OutgoingPCM() <- frame(8)
	require.Eventually(t, func() bool { return p.Snapshot().UnderrunEvents == 2 }, time.Second, 5*time.Millisecond)

	// End feeding with empty queue → become idle (no more silence).
	p.SetFeeding(false)
	idleCtx, idleCancel := context.WithTimeout(context.Background(), time.Second)
	defer idleCancel()
	require.NoError(t, p.WaitIdle(idleCtx))

	nAfterIdle := sink.len()
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, nAfterIdle, sink.len(), "must not keep sending silence after feeding ends")
}

func TestStopDiscardsAndAllowsReplay(t *testing.T) {
	sink := &fakeSink{}
	p := newPacer(t, sink)

	p.Pause()
	time.Sleep(5 * time.Millisecond)
	for i := 0; i < 10; i++ {
		p.OutgoingPCM() <- frame(42)
	}
	// Stop while paused with a large backlog.
	p.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	require.NoError(t, p.WaitIdle(ctx), "Stop must make WaitIdle return immediately")

	st := p.Snapshot()
	require.GreaterOrEqual(t, st.FramesDropped, uint64(10))
	require.Equal(t, 0, sink.len(), "stopped audio must not be sent")

	// New audio after Stop still plays.
	p.OutgoingPCM() <- frame(99)
	waitFrames(t, sink, 1, time.Second)
	require.NoError(t, p.WaitIdle(context.Background()))
	frames, _ := sink.snapshot()
	require.Len(t, frames, 1)
	require.Equal(t, int16(99), frames[0][0])
}

func TestStopDuringSendCompletesCurrentFrame(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	sink := &fakeSink{entered: entered, block: release}
	p := newPacer(t, sink)

	p.OutgoingPCM() <- frame(1)
	p.OutgoingPCM() <- frame(2)

	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("SendFrame never entered")
	}

	stopDone := make(chan struct{})
	go func() {
		defer close(stopDone)
		p.Stop()
	}()

	// Stop must wait for the in-flight SendFrame (cannot preempt Sink).
	select {
	case <-stopDone:
		t.Fatal("Stop returned while SendFrame still in flight")
	case <-time.After(30 * time.Millisecond):
	}

	close(release)
	select {
	case <-stopDone:
	case <-time.After(time.Second):
		t.Fatal("Stop did not return after in-flight SendFrame completed")
	}

	require.NoError(t, p.WaitIdle(context.Background()))
	frames, _ := sink.snapshot()
	// The in-flight frame is delivered; the queued second frame is dropped.
	require.Len(t, frames, 1)
	require.Equal(t, int16(1), frames[0][0])
}

func TestCloseThenWriteDoesNotPanic(t *testing.T) {
	sink := &fakeSink{}
	p := pacer.New(sink, pacer.WithQueue(8))
	p.Close()
	p.Close() // idempotent

	require.NotPanics(t, func() {
		for i := 0; i < 4; i++ {
			select {
			case p.OutgoingPCM() <- frame(1):
			case <-time.After(50 * time.Millisecond):
				// Buffer full after close is fine; must not panic.
			}
		}
	})
}

func TestSentTapAtSendTime(t *testing.T) {
	sink := &fakeSink{}
	p := newPacer(t, sink)

	var (
		mu      sync.Mutex
		tapVals []int16
	)
	p.SetSentTap(func(samples []int16) {
		mu.Lock()
		defer mu.Unlock()
		tapVals = append(tapVals, samples[0])
	})

	// Enqueue a burst; tap must fire once per sent frame, not both at enqueue.
	p.OutgoingPCM() <- frame(11)
	p.OutgoingPCM() <- frame(22)
	waitFrames(t, sink, 1, time.Second)
	mu.Lock()
	require.Equal(t, []int16{11}, tapVals, "second tap must not fire at enqueue time")
	mu.Unlock()

	waitFrames(t, sink, 2, time.Second)
	require.NoError(t, p.WaitIdle(context.Background()))

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []int16{11, 22}, tapVals)
	frames, _ := sink.snapshot()
	require.Equal(t, frames[0][0], tapVals[0])
	require.Equal(t, frames[1][0], tapVals[1])
}

func TestDuckDoesNotMutateCallerSlice(t *testing.T) {
	sink := &fakeSink{}
	p := newPacer(t, sink)

	src := frame(10000)
	p.Duck(0.5)
	p.OutgoingPCM() <- src
	waitFrames(t, sink, 1, time.Second)
	require.NoError(t, p.WaitIdle(context.Background()))

	require.Equal(t, int16(10000), src[0], "caller slice must be unchanged")
	frames, _ := sink.snapshot()
	require.InDelta(t, 5000, float64(frames[0][0]), 2)
}

func TestPacingDoesNotDrift(t *testing.T) {
	sink := &fakeSink{}
	p := newPacer(t, sink)

	const n = 40
	for i := 0; i < n; i++ {
		p.OutgoingPCM() <- frame(int16(i + 1))
	}
	waitFrames(t, sink, n, 3*time.Second)
	require.NoError(t, p.WaitIdle(context.Background()))

	_, times := sink.snapshot()
	require.Len(t, times, n)

	elapsed := times[n-1].Sub(times[0])
	expected := time.Duration(n-1) * pacer.FrameDuration
	// Allow scheduling jitter but reject systematic drift (e.g. sleep-per-frame slip).
	require.InDelta(t, float64(expected), float64(elapsed), float64(25*time.Millisecond),
		"elapsed=%v expected=%v", elapsed, expected)
}

func TestWaitIdleContextCanceled(t *testing.T) {
	sink := &fakeSink{}
	p := newPacer(t, sink)
	p.SetFeeding(true)
	p.OutgoingPCM() <- frame(1)
	waitFrames(t, sink, 1, time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	err := p.WaitIdle(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	p.SetFeeding(false)
	require.NoError(t, p.WaitIdle(context.Background()))
}

// TestWaitIdleCancelLeavesOthersUnaffected: a ctx-canceled WaitIdle must return
// promptly without preventing a later waiter from observing true idle, and
// without disturbing concurrent waiters still blocked on the same burst.
func TestWaitIdleCancelLeavesOthersUnaffected(t *testing.T) {
	sink := &fakeSink{}
	p := newPacer(t, sink)

	// ~200 ms of audio so WaitIdle must block past the cancel.
	big := make([]int16, pacer.FrameSamples*10)
	for i := range big {
		big[i] = 1
	}
	p.OutgoingPCM() <- big

	keeperDone := make(chan error, 1)
	go func() {
		keeperDone <- p.WaitIdle(context.Background())
	}()

	ctx, cancel := context.WithCancel(context.Background())
	canceled := make(chan error, 1)
	go func() {
		canceled <- p.WaitIdle(ctx)
	}()

	time.Sleep(30 * time.Millisecond) // let both probes register waiters
	cancel()

	select {
	case err := <-canceled:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("canceled WaitIdle did not return")
	}

	select {
	case err := <-keeperDone:
		require.NoError(t, err, "surviving waiter must still reach idle")
	case <-time.After(2 * time.Second):
		t.Fatal("surviving WaitIdle did not complete after playback")
	}
	require.Equal(t, 10, sink.len())

	// Pacer accepts a fresh burst after the abandoned waiter was signalled.
	p.OutgoingPCM() <- frame(9)
	require.NoError(t, p.WaitIdle(context.Background()))
	require.Equal(t, 11, sink.len())
}

// TestWaitIdleAfterEnqueueBlocksUntilSent catches the race where WaitIdle
// returns after the send goroutine has taken a chunk from OutgoingPCM but
// before markActive — a multi-core window that looks like "queue empty and
// idle" while audio has not been sent yet. Real call shape: enqueue then
// WaitIdle immediately; it must not return before every frame has been sent.
func TestWaitIdleAfterEnqueueBlocksUntilSent(t *testing.T) {
	sink := &fakeSink{}
	p := newPacer(t, sink)

	const n = 5
	for i := 0; i < n; i++ {
		p.OutgoingPCM() <- frame(int16(i + 1))
	}

	require.NoError(t, p.WaitIdle(context.Background()))
	require.Equal(t, n, sink.len(),
		"WaitIdle returned before all enqueued frames were sent")
}

// TestWaitIdleConcurrentCallers checks that overlapping WaitIdle probes each
// sync with the send goroutine and all observe the same completed playback.
func TestWaitIdleConcurrentCallers(t *testing.T) {
	sink := &fakeSink{}
	p := newPacer(t, sink)

	const n = 8
	for i := 0; i < n; i++ {
		p.OutgoingPCM() <- frame(int16(i + 1))
	}

	const callers = 8
	errs := make(chan error, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- p.WaitIdle(context.Background())
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.Equal(t, n, sink.len())
}

// slowFirstSink delays only the first SendFrame to simulate a transport stall.
type slowFirstSink struct {
	fakeSink
	delay         time.Duration
	firstReturned chan struct{} // closed after the slow first SendFrame finishes
	n             int
}

func (s *slowFirstSink) SendFrame(samples []int16) error {
	s.n++
	if s.n == 1 && s.delay > 0 {
		time.Sleep(s.delay)
	}
	err := s.fakeSink.SendFrame(samples)
	if s.n == 1 && s.firstReturned != nil {
		close(s.firstReturned)
		s.firstReturned = nil
	}
	return err
}

func TestSlowSendFrameDoesNotBurstCatchUp(t *testing.T) {
	// Stall the first SendFrame by 5 frame periods. The pacer must not dump the
	// queued frames back-to-back to "catch up"; it waits a fresh FrameDuration between
	// sends. Assert on PaceResets and the frame count in a short window after
	// the stall — not on wall-clock gaps between individual frames.
	firstReturned := make(chan struct{})
	sink := &slowFirstSink{
		delay:         100 * time.Millisecond,
		firstReturned: firstReturned,
	}
	p := newPacer(t, sink)

	const n = 6
	for i := 0; i < n; i++ {
		p.OutgoingPCM() <- frame(int16(i + 1))
	}

	select {
	case <-firstReturned:
	case <-time.After(2 * time.Second):
		t.Fatal("slow first SendFrame never completed")
	}

	// One-and-a-half pace periods after the stall returns: a paced sender
	// delivers at most one more frame; a catch-up burst has already flushed
	// the rest of the queue (often before this sleep even starts).
	time.Sleep(pacer.FrameDuration + pacer.FrameDuration/2)
	got := sink.len()
	require.LessOrEqual(t, got, 2,
		"catch-up burst dumped too many frames after slow SendFrame (got %d)", got)

	st := p.Snapshot()
	require.GreaterOrEqual(t, st.PaceResets, uint64(1), "slow SendFrame must realign the pace clock")

	require.NoError(t, p.WaitIdle(context.Background()))
	require.Equal(t, n, sink.len(), "all enqueued frames must still be delivered")
	require.Equal(t, uint64(n), p.Snapshot().FramesSent)
}

func TestModeratelySlowSendFrameKeepsFollowingPacketsPaced(t *testing.T) {
	// A stall shorter than the old catch-up window used to make the next RTP
	// packets run back-to-back. Carrier G.711 jitter buffers may render that as
	// a gap followed by clipped/choppy speech. Every packet after a completed
	// write must retain a real pacing interval, regardless of prior lateness.
	sink := &slowFirstSink{delay: 35 * time.Millisecond}
	p := newPacer(t, sink)

	for i := 0; i < 4; i++ {
		p.OutgoingPCM() <- frame(int16(i + 1))
	}
	require.NoError(t, p.WaitIdle(context.Background()))

	_, sentAt := sink.snapshot()
	require.Len(t, sentAt, 4)
	for i := 1; i < len(sentAt); i++ {
		require.GreaterOrEqual(t, sentAt[i].Sub(sentAt[i-1]), 15*time.Millisecond,
			"late writes must not be followed by catch-up packet bursts")
	}
}

func TestFramesDroppedCountsSamplesNotChunks(t *testing.T) {
	sink := &fakeSink{}
	p := newPacer(t, sink)

	p.Pause()
	// 10 quarter-frames = 2.5 frames → ceil = 3 dropped frames, not 10.
	const quarters = 10
	for i := 0; i < quarters; i++ {
		p.OutgoingPCM() <- samples(pacer.FrameSamples/4, 1)
	}
	p.Stop()

	st := p.Snapshot()
	require.Equal(t, uint64(3), st.FramesDropped)
	require.Equal(t, 0, sink.len())
}

func TestStopRestoresDuckGain(t *testing.T) {
	sink := &fakeSink{}
	p := newPacer(t, sink)

	p.Duck(0.5)
	p.OutgoingPCM() <- frame(10000)
	waitFrames(t, sink, 1, time.Second)
	frames, _ := sink.snapshot()
	require.InDelta(t, 5000, float64(frames[0][0]), 2, "precondition: Duck attenuates")

	p.Stop()
	p.OutgoingPCM() <- frame(10000)
	require.NoError(t, p.WaitIdle(context.Background()))

	frames, _ = sink.snapshot()
	require.Len(t, frames, 2)
	require.Equal(t, int16(10000), frames[1][0],
		"Stop must restore unity gain so the next utterance is not ducked")
}

func TestDuckCountsEventsAndDuckedFrames(t *testing.T) {
	sink := &fakeSink{}
	p := newPacer(t, sink)

	p.Duck(0.25)
	p.OutgoingPCM() <- frame(1000)
	p.OutgoingPCM() <- frame(1000)
	waitFrames(t, sink, 2, time.Second)
	p.Restore()
	p.OutgoingPCM() <- frame(1000)
	require.NoError(t, p.WaitIdle(context.Background()))

	st := p.Snapshot()
	require.Equal(t, uint64(1), st.DuckEvents)
	require.Equal(t, uint64(2), st.DuckedFrames, "only frames sent while gain<1 count")
	require.Equal(t, uint64(3), st.FramesSent)
}

func TestDuckGainAboveOneNotCounted(t *testing.T) {
	sink := &fakeSink{}
	p := newPacer(t, sink)

	// gain>1 clamps to unity (a restore), not a duck.
	p.Duck(2)
	p.OutgoingPCM() <- frame(1000)
	require.NoError(t, p.WaitIdle(context.Background()))

	st := p.Snapshot()
	require.Equal(t, uint64(0), st.DuckEvents)
	require.Equal(t, uint64(0), st.DuckedFrames)
}

func TestPauseResumeCountsOnceAndAccruesPausedMs(t *testing.T) {
	sink := &fakeSink{}
	p := newPacer(t, sink)

	p.Pause()
	time.Sleep(60 * time.Millisecond)
	p.Resume()

	st := p.Snapshot()
	require.Equal(t, uint64(1), st.PauseEvents)
	require.GreaterOrEqual(t, st.PausedMs, uint64(40))

	// Settled: PausedMs must not keep growing after Resume.
	time.Sleep(40 * time.Millisecond)
	require.Equal(t, st.PausedMs, p.Snapshot().PausedMs)
}

func TestPauseStopSettlesPausedMs(t *testing.T) {
	sink := &fakeSink{}
	p := newPacer(t, sink)

	p.Pause()
	time.Sleep(60 * time.Millisecond)
	p.Stop()

	st := p.Snapshot()
	require.Equal(t, uint64(1), st.PauseEvents)
	require.GreaterOrEqual(t, st.PausedMs, uint64(40), "Stop must settle the open pause interval")

	// No growth and no double count after Stop.
	time.Sleep(40 * time.Millisecond)
	require.Equal(t, st.PausedMs, p.Snapshot().PausedMs)
}

func TestPauseCloseSettlesPausedMs(t *testing.T) {
	sink := &fakeSink{}
	p := newPacer(t, sink)

	p.Pause()
	time.Sleep(60 * time.Millisecond)
	p.Close()

	st := p.Snapshot()
	require.Equal(t, uint64(1), st.PauseEvents)
	require.GreaterOrEqual(t, st.PausedMs, uint64(40), "Close must settle the open pause interval")
}

func TestIdempotentPauseCountsOnce(t *testing.T) {
	sink := &fakeSink{}
	p := newPacer(t, sink)

	p.Pause()
	p.Pause()
	p.Pause()
	p.Resume()

	require.Equal(t, uint64(1), p.Snapshot().PauseEvents,
		"repeated Pause without an interleaving Resume is one pause event")
}

func TestSendGapOverrunsZeroAtNormalPace(t *testing.T) {
	sink := &fakeSink{}
	p := newPacer(t, sink)

	const n = 6
	for i := 0; i < n; i++ {
		p.OutgoingPCM() <- frame(int16(i + 1))
	}
	require.NoError(t, p.WaitIdle(context.Background()))

	st := p.Snapshot()
	require.Equal(t, uint64(0), st.SendGapOverruns)
	require.Equal(t, uint64(0), st.MaxSendGapMs)
}

// slowNthSink delays only the n-th SendFrame to open a mid-burst send gap.
type slowNthSink struct {
	fakeSink
	delay time.Duration
	nth   int
	n     int
}

func (s *slowNthSink) SendFrame(samples []int16) error {
	s.n++
	if s.n == s.nth {
		time.Sleep(s.delay)
	}
	return s.fakeSink.SendFrame(samples)
}

func TestSendGapOverrunCounted(t *testing.T) {
	sink := &slowNthSink{delay: 60 * time.Millisecond, nth: 3}
	p := newPacer(t, sink)

	const n = 5
	for i := 0; i < n; i++ {
		p.OutgoingPCM() <- frame(int16(i + 1))
	}
	require.NoError(t, p.WaitIdle(context.Background()))

	st := p.Snapshot()
	require.Equal(t, uint64(1), st.SendGapOverruns, "the 60 ms stall is one >40 ms gap")
	require.GreaterOrEqual(t, st.MaxSendGapMs, uint64(40))
}

func TestPauseDoesNotCountAsSendGap(t *testing.T) {
	// Pause mid-burst: the wire hole from the pause must land in PausedMs,
	// not in SendGapOverruns. The blocked first SendFrame only paces the test;
	// it is released before Pause, which is sync and cannot ack while the send
	// goroutine is stuck inside Sink.
	entered := make(chan struct{})
	release := make(chan struct{})
	sink := &fakeSink{entered: entered, block: release}
	p := newPacer(t, sink)

	const n = 4
	for i := 0; i < n; i++ {
		p.OutgoingPCM() <- frame(int16(i + 1))
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("SendFrame never entered")
	}
	close(release)
	p.Pause()
	time.Sleep(100 * time.Millisecond) // well over the 40 ms gap threshold
	p.Resume()

	require.NoError(t, p.WaitIdle(context.Background()))
	require.Equal(t, n, sink.len())

	st := p.Snapshot()
	require.Equal(t, uint64(1), st.PauseEvents)
	require.GreaterOrEqual(t, st.PausedMs, uint64(80))
	require.Equal(t, uint64(0), st.SendGapOverruns, "pause holes are not send-gap overruns")
	require.Equal(t, uint64(0), st.MaxSendGapMs)
}

func TestUnderrunHoldLimitEndsBurst(t *testing.T) {
	const holdLimit = 5 // keep the test fast; production default is 250
	sink := &fakeSink{}
	p := newPacer(t, sink, pacer.WithUnderrunHoldLimit(holdLimit))

	p.SetFeeding(true)
	p.OutgoingPCM() <- frame(7)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, p.WaitIdle(ctx), "underrun cap must end the burst so WaitIdle returns")

	frames, _ := sink.snapshot()
	// 1 real frame + holdLimit silence, then idle (no more silence).
	require.Len(t, frames, 1+holdLimit)
	require.Equal(t, int16(7), frames[0][0])
	for i := 1; i < len(frames); i++ {
		require.True(t, allZero(frames[i]), "frame %d should be underrun silence", i)
	}

	st := p.Snapshot()
	require.Equal(t, uint64(holdLimit), st.SilenceFrames)
	require.Equal(t, uint64(1), st.UnderrunLimitHits)

	n := sink.len()
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, n, sink.len(), "must not keep sending silence after the hold cap")
}

func allZero(f []int16) bool {
	for _, s := range f {
		if s != 0 {
			return false
		}
	}
	return true
}

func TestIdleKeepalivePacesSilenceWhenIdle(t *testing.T) {
	sink := &fakeSink{}
	p := newPacer(t, sink, pacer.WithIdleKeepalive(true))

	waitFrames(t, sink, 4, 2*time.Second)

	// WaitIdle stays a queue-drain probe: keepalive must not block it.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, p.WaitIdle(ctx))

	frames, times := sink.snapshot()
	require.GreaterOrEqual(t, len(frames), 4)
	for i, f := range frames {
		require.Len(t, f, pacer.FrameSamples)
		require.True(t, allZero(f), "frame %d should be all-zero PCM", i)
	}
	for i := 1; i < 4; i++ {
		gap := times[i].Sub(times[i-1])
		require.Greater(t, gap, 5*time.Millisecond, "keepalive must be paced, not a burst")
		require.Less(t, gap, 3*pacer.FrameDuration, "keepalive gap %v exceeds the frame cadence", gap)
	}

	st := p.Snapshot()
	require.GreaterOrEqual(t, st.KeepaliveFrames, uint64(4))
	require.Equal(t, st.FramesSent, st.KeepaliveFrames, "only keepalive frames were sent")
	require.Zero(t, st.SilenceFrames, "keepalive must not count as underrun hold")
}

func TestIdleKeepaliveAudioResumesAtFrameBoundary(t *testing.T) {
	sink := &fakeSink{}
	p := newPacer(t, sink, pacer.WithIdleKeepalive(true))

	waitFrames(t, sink, 3, 2*time.Second) // keepalive cadence running
	p.OutgoingPCM() <- frame(7)

	// The audio frame joins at the next frame boundary; after it drains,
	// keepalive silence continues without a wire break.
	waitFrames(t, sink, 6, 2*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, p.WaitIdle(ctx))

	frames, times := sink.snapshot()
	audioAt := -1
	for i, f := range frames {
		if f[0] == 7 {
			audioAt = i
			break
		}
	}
	require.Greater(t, audioAt, 0, "audio frame must follow keepalive silence")
	for i, s := range frames[audioAt] {
		require.Equal(t, int16(7), s, "audio frame sample %d", i)
	}
	require.Less(t, times[audioAt].Sub(times[audioAt-1]), 3*pacer.FrameDuration,
		"audio must resume within a frame period of the last keepalive frame")
	for i := audioAt + 1; i < len(frames); i++ {
		require.True(t, allZero(frames[i]), "frame %d after the audio must be silence again", i)
	}
}

func TestIdleKeepaliveDisabledBlocks(t *testing.T) {
	sink := &fakeSink{}
	p := newPacer(t, sink) // keepalive off by default

	time.Sleep(3 * pacer.FrameDuration)
	require.Zero(t, sink.len(), "idle pacer without keepalive must not send")

	// The old blocking behavior still wakes for audio.
	p.OutgoingPCM() <- frame(1)
	waitFrames(t, sink, 1, 2*time.Second)
	require.NoError(t, p.WaitIdle(context.Background()))
	require.Zero(t, p.Snapshot().KeepaliveFrames)
}

func TestIdleKeepalivePauseFreezesResumeContinues(t *testing.T) {
	sink := &fakeSink{}
	p := newPacer(t, sink, pacer.WithIdleKeepalive(true))

	waitFrames(t, sink, 2, 2*time.Second)
	p.Pause() // sync: the send goroutine is now paused
	n := sink.len()

	require.Never(t, func() bool { return sink.len() != n },
		3*pacer.FrameDuration, 5*time.Millisecond,
		"pause is an intentional stop: no keepalive frames while paused")

	p.Resume()
	waitFrames(t, sink, n+2, 2*time.Second)
}

func TestSendAheadBurstsLeadThenRealTime(t *testing.T) {
	sink := &fakeSink{}
	p := newPacer(t, sink, pacer.WithSendAhead(200*time.Millisecond))

	start := time.Now()
	p.OutgoingPCM() <- samples(30*pacer.FrameSamples, 1000) // 600 ms
	waitFrames(t, sink, 10, time.Second)
	require.Less(t, time.Since(start), 80*time.Millisecond,
		"the 200 ms lead should go out without pacing")

	require.NoError(t, p.WaitIdle(context.Background()))
	elapsed := time.Since(start)
	require.Equal(t, 30, sink.len())
	// 600 ms of audio, 200 ms of it sent ahead: the rest is paced in real time.
	require.Greater(t, elapsed, 330*time.Millisecond)
	require.Less(t, elapsed, 550*time.Millisecond)
}

func TestSendAheadWaitsForAudioInsteadOfSilenceWhileAhead(t *testing.T) {
	sink := &fakeSink{}
	p := newPacer(t, sink, pacer.WithSendAhead(200*time.Millisecond))
	p.SetFeeding(true)

	p.OutgoingPCM() <- samples(10*pacer.FrameSamples, 1000) // exactly the lead
	waitFrames(t, sink, 10, time.Second)
	// The source falls behind for less than the lead the peer still holds.
	time.Sleep(100 * time.Millisecond)
	p.OutgoingPCM() <- samples(5*pacer.FrameSamples, 2000)
	waitFrames(t, sink, 15, time.Second)
	p.SetFeeding(false)
	require.NoError(t, p.WaitIdle(context.Background()))

	frames, _ := sink.snapshot()
	for i, f := range frames {
		require.False(t, allZero(f), "frame %d is underrun silence the peer did not need", i)
	}
	require.Zero(t, p.Snapshot().UnderrunEvents)
}

func TestSendAheadUnderrunSilenceIsRealTime(t *testing.T) {
	sink := &fakeSink{}
	p := newPacer(t, sink, pacer.WithSendAhead(100*time.Millisecond))
	p.SetFeeding(true)

	p.OutgoingPCM() <- samples(2*pacer.FrameSamples, 1000)
	time.Sleep(400 * time.Millisecond) // source dry far longer than the lead
	p.SetFeeding(false)
	require.NoError(t, p.WaitIdle(context.Background()))

	frames, times := sink.snapshot()
	var silent []time.Time
	for i, f := range frames {
		if allZero(f) {
			silent = append(silent, times[i])
		}
	}
	require.NotEmpty(t, silent)
	require.Equal(t, uint64(1), p.Snapshot().UnderrunEvents)
	// Silence is held at the frame rate, never burst ahead of the audio.
	for i := 1; i < len(silent); i++ {
		require.Greater(t, silent[i].Sub(silent[i-1]), 10*time.Millisecond, "silence frame %d", i)
	}
}

func TestSendAheadNextUtteranceDoesNotStackLead(t *testing.T) {
	sink := &fakeSink{}
	p := newPacer(t, sink, pacer.WithSendAhead(200*time.Millisecond))

	p.OutgoingPCM() <- samples(20*pacer.FrameSamples, 1000) // 400 ms
	require.NoError(t, p.WaitIdle(context.Background()))
	// The burst went idle about 200 ms ahead of real time; the peer is still
	// playing that lead when the next utterance arrives right away.
	before := sink.len()
	p.OutgoingPCM() <- samples(50*pacer.FrameSamples, 2000) // 1 s
	time.Sleep(60 * time.Millisecond)
	// Only the part of the lead the peer has played since may go out.
	require.LessOrEqual(t, sink.len()-before, 5)
}

func TestSendAheadResumeDoesNotStackLead(t *testing.T) {
	sink := &fakeSink{}
	p := newPacer(t, sink, pacer.WithSendAhead(200*time.Millisecond))

	p.OutgoingPCM() <- samples(50*pacer.FrameSamples, 1000) // 1 s
	waitFrames(t, sink, 10, time.Second)
	p.Pause() // the peer pauses holding the ~200 ms lead
	before := sink.len()
	time.Sleep(100 * time.Millisecond)
	p.Resume()
	time.Sleep(60 * time.Millisecond)
	// Still ahead by the carried lead: only real-time frames since resume.
	require.LessOrEqual(t, sink.len()-before, 5)
}
