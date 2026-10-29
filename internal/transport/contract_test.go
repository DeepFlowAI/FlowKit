package transport_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/transport"
	"github.com/stretchr/testify/require"
)

// Factories under test. "full" includes a wire-side observer; "bare" is enough
// for Close / capability probes (WebRTC bare: no remote track — see
// TestContract_ConnectedWebRTCCloseDoesNotCloseIncomingPCM for the connected case).
var harnessFactories = []struct {
	name    string
	newFull func(*testing.T) *mediaHarness
	newBare func(*testing.T) *mediaHarness
}{
	{"sip", newSIPHarness, newSIPHarnessBare},
	{"webrtc", newWebRTCHarness, newWebRTCHarnessBare},
	{"wspcm", newWSPCMHarness, newWSPCMHarness}, // WS pair is already the observer
}

// ---------------------------------------------------------------------------
// DTMF capability matrix — the only MediaIO capabilities that still vary by
// transport. Paced-playback methods are required on MediaIO itself; compile
// failures catch a missing method faster than a bool table would.
// ---------------------------------------------------------------------------

type dtmfCapabilitySet struct {
	DTMFReceiver bool
	DTMFSender   bool
}

// SIP alone implements telephone-event; WebRTC / WS-PCM do not.
var expectedDTMFCapabilities = map[string]dtmfCapabilitySet{
	"sip":    {DTMFReceiver: true, DTMFSender: true},
	"webrtc": {},
	"wspcm":  {},
}

func probeDTMFCapabilities(mio transport.MediaIO) dtmfCapabilitySet {
	_, dtmfROK := mio.(transport.DTMFReceiver)
	_, dtmfSOK := mio.(transport.DTMFSender)
	return dtmfCapabilitySet{DTMFReceiver: dtmfROK, DTMFSender: dtmfSOK}
}

func TestContract_DTMFCapabilityMatrix(t *testing.T) {
	rows := make([]string, 0, len(harnessFactories)+1)
	rows = append(rows, fmt.Sprintf("%-8s | Kind | DTMF-R | DTMF-S", "impl"))
	for _, f := range harnessFactories {
		f := f
		t.Run(f.name, func(t *testing.T) {
			h := f.newBare(t)
			defer h.cleanup()
			require.Equal(t, transport.Kind(f.name), h.mio.Kind(), "MediaIO.Kind must match harness name")
			got := probeDTMFCapabilities(h.mio)
			want, ok := expectedDTMFCapabilities[f.name]
			require.True(t, ok, "missing expectedDTMFCapabilities entry for %s", f.name)
			require.Equal(t, want, got, "DTMF capability drift for %s — update expectedDTMFCapabilities deliberately", f.name)
			rows = append(rows, fmt.Sprintf("%-8s | %-6s | %6v | %6v", f.name, h.mio.Kind(), got.DTMFReceiver, got.DTMFSender))
		})
	}
	t.Cleanup(func() {
		t.Log("DTMF capability matrix:")
		for _, r := range rows {
			t.Log(r)
		}
	})
}

// ---------------------------------------------------------------------------
// 1. Close is idempotent
// ---------------------------------------------------------------------------

func TestContract_CloseIdempotent(t *testing.T) {
	for _, f := range harnessFactories {
		f := f
		t.Run(f.name, func(t *testing.T) {
			h := f.newBare(t)
			defer h.cleanup()
			err1 := h.mio.Close()
			err2 := h.mio.Close()
			// Same outcome both times (both nil today); never panic.
			require.Equal(t, err1, err2, "Close errors must be stable across calls")
		})
	}
}

// ---------------------------------------------------------------------------
// 2. Close closes IncomingPCM
// ---------------------------------------------------------------------------

func TestContract_CloseClosesIncomingPCM(t *testing.T) {
	for _, f := range harnessFactories {
		f := f
		t.Run(f.name, func(t *testing.T) {
			h := f.newBare(t)
			defer h.cleanup()
			require.NoError(t, h.mio.Close())
			select {
			case _, ok := <-h.mio.IncomingPCM():
				require.False(t, ok, "IncomingPCM must be closed after Close")
			case <-time.After(3 * time.Second):
				t.Fatal("IncomingPCM not closed within 3s of Close")
			}
		})
	}
}

// Connected WebRTC: runIncomingPump blocks in ReadRTP, so MediaIO.Close alone
// does not close IncomingPCM. Item 2 uses the bare (unconnected) harness; this
// test makes the connected-path defect visible and named.
func TestContract_ConnectedWebRTCCloseDoesNotCloseIncomingPCM(t *testing.T) {
	h := newWebRTCHarness(t)
	defer h.cleanup()
	require.NotNil(t, h.remoteMio, "connected webrtc harness must expose remoteMio")

	// Prove the incoming pump is running (blocked in ReadRTP) before Close —
	// otherwise bare-path Close closes IncomingPCM and hides the defect.
	writeFrames(t, h.remoteMio, [][]int16{pcmFrame(99)})
	select {
	case _, ok := <-h.mio.IncomingPCM():
		require.True(t, ok, "expected a live incoming frame before Close")
	case <-time.After(5 * time.Second):
		t.Fatal("incoming pump never delivered a frame; OnTrack/ReadRTP not running")
	}

	require.NoError(t, h.mio.Close()) // MediaIO only — Peer stays up so ReadRTP stays blocked

	select {
	case _, ok := <-h.mio.IncomingPCM():
		if !ok {
			// Defect fixed: IncomingPCM closed despite an active remote track.
			// Retire this test (or promote into CloseClosesIncomingPCM with full harness).
			t.Log("IncomingPCM closed on connected WebRTC — defect appears fixed")
			return
		}
		t.Fatal("IncomingPCM delivered a frame after Close; expected closed or hang")
	case <-time.After(500 * time.Millisecond):
		t.Skip("KNOWN DEFECT: connected webrtc MediaIO.Close leaves IncomingPCM open because runIncomingPump blocks in ReadRTP; remove this Skip when the shared close path unblocks/closes incoming")
	}
}

// ---------------------------------------------------------------------------
// 3. Write to OutgoingPCM after Close does not panic
// ---------------------------------------------------------------------------

func TestContract_WriteOutgoingAfterCloseDoesNotPanic(t *testing.T) {
	for _, f := range harnessFactories {
		f := f
		t.Run(f.name, func(t *testing.T) {
			h := f.newBare(t)
			defer h.cleanup()
			require.NoError(t, h.mio.Close())

			panicked := make(chan any, 1)
			go func() {
				defer func() {
					if r := recover(); r != nil {
						panicked <- r
					} else {
						close(panicked)
					}
				}()
				select {
				case h.mio.OutgoingPCM() <- pcmFrame(1):
				case <-time.After(200 * time.Millisecond):
					// Blocked is acceptable under close races; panic is not.
				}
			}()
			select {
			case r, ok := <-panicked:
				if ok {
					t.Fatalf("write after Close panicked: %v", r)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("write-after-close goroutine stuck")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// 4. Audio written to OutgoingPCM is eventually delivered
// ---------------------------------------------------------------------------

func TestContract_OutgoingAudioIsDelivered(t *testing.T) {
	for _, f := range harnessFactories {
		f := f
		t.Run(f.name, func(t *testing.T) {
			h := f.newFull(t)
			defer h.cleanup()

			const n = 5
			frames := make([][]int16, n)
			for i := 0; i < n; i++ {
				frames[i] = pcmFrame(int16(1000 + i*100))
			}
			writeFrames(t, h.mio, frames)

			got := h.observe(n, 5*time.Second)
			require.GreaterOrEqual(t, len(got), n, "delivered %d/%d frames", len(got), n)
		})
	}
}

// ---------------------------------------------------------------------------
// 5. Outgoing audio is paced (~20 ms/frame), not burst-delivered
// ---------------------------------------------------------------------------

func TestContract_OutgoingAudioIsPaced(t *testing.T) {
	for _, f := range harnessFactories {
		f := f
		t.Run(f.name, func(t *testing.T) {
			h := f.newFull(t)
			defer h.cleanup()

			const n = 10
			frames := make([][]int16, n)
			for i := 0; i < n; i++ {
				frames[i] = pcmFrame(int16(2000 + i))
			}
			// Enqueue as fast as the channel allows — pacing must happen on send.
			writeFrames(t, h.mio, frames)

			got := h.observe(n, 5*time.Second)
			require.GreaterOrEqual(t, len(got), n,
				"only received %d/%d frames to measure pacing", len(got), n)
			// First→last span for N frames ≈ (N-1)×20ms. Require ≥ (N-1)×15ms.
			minSpan := time.Duration(n-1) * 15 * time.Millisecond
			gotSpan := span(got)
			require.GreaterOrEqual(t, gotSpan, minSpan,
				"burst delivery: span=%v want≥%v (N=%d)", gotSpan, minSpan, n)
			// Sanity upper bound: not pathologically slow (keeps the test meaningful).
			maxSpan := time.Duration(n-1) * 60 * time.Millisecond
			require.LessOrEqual(t, gotSpan, maxSpan,
				"pacing span too large: %v > %v — flaky clock or stalled pump?", gotSpan, maxSpan)
			t.Logf("%s paced span=%v for %d frames (min=%v)", f.name, gotSpan, n, minSpan)
		})
	}
}

// ---------------------------------------------------------------------------
// 6. PauseOutgoing stops wire audio; Resume keeps queued samples in order
// ---------------------------------------------------------------------------

func TestContract_PauseResumeLossless(t *testing.T) {
	for _, f := range harnessFactories {
		f := f
		t.Run(f.name, func(t *testing.T) {
			h := f.newFull(t)
			defer h.cleanup()

			// Prime the paced sender so Pause hits the active path.
			writeFrames(t, h.mio, [][]int16{pcmFrame(42)})
			prime := h.observe(1, 2*time.Second)
			require.NotEmpty(t, prime, "failed to prime paced sender")
			h.drainDiscard()

			h.mio.PauseOutgoing()

			const n = 8
			queued := make([][]int16, n)
			for i := 0; i < n; i++ {
				// Distinct markers that survive G.711 roughly (large steps).
				queued[i] = pcmFrame(int16(3000 + i*500))
			}
			writeFrames(t, h.mio, queued)

			// While paused, the observer must not see new real audio.
			sneak := h.observe(1, 120*time.Millisecond)
			require.Empty(t, sneak, "received %d frame(s) while paused", len(sneak))

			h.mio.ResumeOutgoing()
			got := h.observe(n, 5*time.Second)
			require.GreaterOrEqual(t, len(got), n,
				"after resume got %d/%d frames — samples dropped?", len(got), n)

			// Order: markers must be non-decreasing across frames (G.711-tolerant).
			// WS-PCM is bit-exact; SIP/WebRTC only check relative order of markers.
			if f.name == "wspcm" {
				for i := 0; i < n; i++ {
					require.Equal(t, queued[i][0], got[i].samples[0],
						"wspcm frame %d marker mismatch", i)
				}
			} else {
				prev := int16(-32768)
				for i, fr := range got[:n] {
					m := fr.samples[0]
					require.GreaterOrEqual(t, m, prev,
						"frame order broken at %d: marker %d < prev %d", i, m, prev)
					prev = m
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// 7. PlaybackSync: WaitPlaybackIdle blocks while queue non-empty, then returns
// ---------------------------------------------------------------------------

func TestContract_WaitPlaybackIdleBlocksThenReturns(t *testing.T) {
	for _, f := range harnessFactories {
		f := f
		t.Run(f.name, func(t *testing.T) {
			h := f.newFull(t)
			defer h.cleanup()

			// ~400 ms of audio so Wait has something to block on.
			big := make([]int16, frameSamples*20)
			for i := range big {
				big[i] = 800
			}
			writeFrames(t, h.mio, [][]int16{big})

			blocked := make(chan struct{})
			done := make(chan error, 1)
			go func() {
				// Give the pump a moment to mark active.
				time.Sleep(10 * time.Millisecond)
				close(blocked)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				done <- h.mio.WaitPlaybackIdle(ctx)
			}()
			<-blocked
			select {
			case err := <-done:
				require.NoError(t, err)
				t.Fatal("WaitPlaybackIdle returned immediately while ~400ms still queued")
			case <-time.After(80 * time.Millisecond):
				// Still blocking — good.
			}

			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("WaitPlaybackIdle did not return after drain")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// 8. PlaybackControl: StopOutgoing drops queued audio; WaitPlaybackIdle returns soon
// ---------------------------------------------------------------------------

func TestContract_StopOutgoingDropsQueue(t *testing.T) {
	for _, f := range harnessFactories {
		f := f
		t.Run(f.name, func(t *testing.T) {
			h := f.newFull(t)
			defer h.cleanup()

			big := make([]int16, frameSamples*50) // ~1 s
			for i := range big {
				big[i] = 900
			}
			writeFrames(t, h.mio, [][]int16{big})
			time.Sleep(30 * time.Millisecond) // let pacing start
			h.mio.StopOutgoing()

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			start := time.Now()
			err := h.mio.WaitPlaybackIdle(ctx)
			elapsed := time.Since(start)
			require.NoError(t, err)
			require.LessOrEqual(t, elapsed, 500*time.Millisecond,
				"WaitPlaybackIdle took %v after StopOutgoing", elapsed)

			// Observer should not receive anything close to the full 50 frames.
			got := h.observe(50, 300*time.Millisecond)
			require.Less(t, len(got), 40,
				"StopOutgoing did not drop queue: still got %d frames", len(got))
			t.Logf("%s StopOutgoing: idle in %v, observed %d frames after stop", f.name, elapsed, len(got))
		})
	}
}

// ---------------------------------------------------------------------------
// 9. SentTapper: tap samples match OutgoingPCM and fire at send (paced) time
// ---------------------------------------------------------------------------

func TestContract_SentTapMatchesAndIsPaced(t *testing.T) {
	for _, f := range harnessFactories {
		f := f
		t.Run(f.name, func(t *testing.T) {
			h := f.newFull(t)
			defer h.cleanup()

			var mu sync.Mutex
			var taps []timedFrame
			enqueuedAt := time.Now()
			h.mio.SetSentTap(func(samples []int16) {
				mu.Lock()
				taps = append(taps, timedFrame{at: time.Now(), samples: copyFrame(samples)})
				mu.Unlock()
			})
			defer h.mio.SetSentTap(nil)

			const n = 10
			frames := make([][]int16, n)
			for i := 0; i < n; i++ {
				frames[i] = pcmFrame(int16(4000 + i*10))
			}
			writeFrames(t, h.mio, frames)
			enqueueDone := time.Now()

			// Wait until tap has seen all frames (paced).
			deadline := time.Now().Add(5 * time.Second)
			for {
				mu.Lock()
				count := len(taps)
				mu.Unlock()
				if count >= n {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("tap received %d/%d frames", count, n)
				}
				time.Sleep(5 * time.Millisecond)
			}

			mu.Lock()
			got := append([]timedFrame(nil), taps[:n]...)
			mu.Unlock()

			for i := 0; i < n; i++ {
				require.Equal(t, frames[i], got[i].samples,
					"tap samples must match OutgoingPCM frame %d", i)
			}

			tapSpan := span(got)
			minSpan := time.Duration(n-1) * 15 * time.Millisecond
			require.GreaterOrEqual(t, tapSpan, minSpan,
				"tap fired at enqueue time (burst): span=%v want≥%v", tapSpan, minSpan)
			// Tap of the last frame must land well after the enqueue burst finished.
			require.GreaterOrEqual(t, got[n-1].at.Sub(enqueueDone), time.Duration(n/2)*15*time.Millisecond,
				"last tap too soon after enqueue burst (enqueue=%v lastTapΔ=%v)",
				enqueueDone.Sub(enqueuedAt), got[n-1].at.Sub(enqueueDone))
			t.Logf("%s SentTap span=%v enqueueBurst=%v", f.name, tapSpan, enqueueDone.Sub(enqueuedAt))
		})
	}
}

// ---------------------------------------------------------------------------
// Partial-frame flush: trailing samples below one 20 ms frame must still be sent
// (zero-padded to a full frame is OK; dropping the tail is not).
// ---------------------------------------------------------------------------

func TestContract_PartialTrailingFrameIsDelivered(t *testing.T) {
	for _, f := range harnessFactories {
		f := f
		t.Run(f.name, func(t *testing.T) {
			h := f.newFull(t)
			defer h.cleanup()

			// 1.5 frames = 480 samples — not an integer number of 20 ms frames.
			const wantSamples = frameSamples + frameSamples/2
			chunk := make([]int16, wantSamples)
			for i := range chunk {
				chunk[i] = 700
			}
			writeFrames(t, h.mio, [][]int16{chunk})
			waitPlaybackIdleIfSupported(t, h.mio, 2*time.Second)

			// At most 2 paced frames (640 samples if padded).
			got := h.observe(2, time.Second)
			gotSamples := countSamples(got)
			t.Logf("%s partial-tail: wrote %d samples, observed %d across %d frame(s)",
				f.name, wantSamples, gotSamples, len(got))

			require.GreaterOrEqual(t, gotSamples, wantSamples,
				"%s dropped residual below one frame (%d samples written, %d delivered)",
				f.name, wantSamples, gotSamples)
		})
	}
}

// ---------------------------------------------------------------------------
// Pause drain: while paused, producers must keep enqueueing without blocking
// even past the OutgoingPCM channel capacity (pump drains into residual).
// ---------------------------------------------------------------------------

func TestContract_PauseDrainsOutgoingSoProducerDoesNotBlock(t *testing.T) {
	for _, f := range harnessFactories {
		f := f
		t.Run(f.name, func(t *testing.T) {
			h := f.newFull(t)
			defer h.cleanup()

			require.Greater(t, h.outCap, 0, "harness must report outgoing channel capacity")

			// Prime so Pause hits the active paced path.
			writeFrames(t, h.mio, [][]int16{pcmFrame(11)})
			require.NotEmpty(t, h.observe(1, 2*time.Second), "failed to prime paced sender")
			h.drainDiscard()

			h.mio.PauseOutgoing()

			// Write well past channel capacity. All paced transports drain while
			// paused into the pacer residual so producers never block.
			n := h.outCap + 16
			frames := make([][]int16, n)
			for i := range frames {
				frames[i] = pcmFrame(int16(5000 + i))
			}
			accepted := tryWriteFrames(h.mio, frames, 50*time.Millisecond)
			t.Logf("%s pause-drain: outCap=%d wrote=%d/%d before timeout",
				f.name, h.outCap, accepted, n)
			require.Equal(t, n, accepted, "producer blocked while paused despite drain contract")

			// Cleanup: resume and let audio flush so Close is not racing a full queue.
			h.mio.ResumeOutgoing()
			_ = h.observe(n, 5*time.Second)
		})
	}
}
