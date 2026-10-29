package recording

import (
	"sync"
	"testing"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/media/bus"
)

func TestStereoEncoder_ConcurrentLR(t *testing.T) {
	e := NewStereoEncoder()
	samples := make([]int16, bus.SamplesPerFrame)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(2)
		slot := time.Duration(i) * bus.FrameDuration
		go func(ts time.Duration) {
			defer wg.Done()
			_ = e.WriteRemote(ts, append([]int16(nil), samples...))
		}(slot)
		go func(ts time.Duration) {
			defer wg.Done()
			_ = e.WriteLocal(ts, append([]int16(nil), samples...))
		}(slot)
	}
	wg.Wait()
	_ = e.Flush()
}
