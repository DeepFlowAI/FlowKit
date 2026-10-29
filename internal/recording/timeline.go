package recording

import (
	"sync"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/media/bus"
)

const localTimelineGap = 4 * bus.FrameDuration

// localTimelineMaxLead is how far ahead of the wall clock a side may run.
// Some lead is real: audio sent ahead of real time is played later. A backlog
// that lands at once after a network stall is not, and counting it on would
// leave this side ahead for the rest of the call, pushing the other side's
// frames behind the encoder holdback. Frames beyond the lead are dropped
// instead. Kept under half the holdback so neither side can be pushed out.
const localTimelineMaxLead = 500 * time.Millisecond

// localTimeline gives media frames a stable wall-aware timestamp. Consecutive
// frames stay consecutive even when callbacks or packets arrive a few
// milliseconds early or late; a real media pause still advances the clock.
// timestamp reports false for a frame too far ahead of the wall clock to keep.
type localTimeline struct {
	mu sync.Mutex

	started  bool
	lastWall time.Duration
	next     time.Duration

	resyncs int
	maxJump time.Duration // largest forward resync, for diagnostics
}

// resyncStats reports how often the timeline snapped forward to the wall clock
// and the largest such jump.
func (t *localTimeline) resyncStats() (int, time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.resyncs, t.maxJump
}

func (t *localTimeline) timestamp(wall time.Duration, samples int) (time.Duration, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	frameDuration := time.Duration(samples) * time.Second / bus.SampleRate
	if !t.started {
		t.started = true
		t.lastWall = wall
		t.next = time.Duration(SlotIndex(wall)) * bus.FrameDuration
	}
	// Resync on a real pause, and also when a stream delivered slightly slower
	// than real time has drifted a full gap behind: frame counting alone never
	// catches up, and the encoder drops frames that lag past its holdback.
	wallSlot := time.Duration(SlotIndex(wall)) * bus.FrameDuration
	if wall-t.lastWall >= localTimelineGap || wallSlot-t.next >= localTimelineGap {
		if wallSlot > t.next {
			t.resyncs++
			t.maxJump = max(t.maxJump, wallSlot-t.next)
			t.next = wallSlot
		}
	}
	t.lastWall = wall
	if t.next-wallSlot > localTimelineMaxLead {
		return 0, false
	}
	ts := t.next
	t.next += frameDuration
	return ts, true
}
