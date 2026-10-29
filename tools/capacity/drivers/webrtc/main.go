// WebRTC offer load driver for Flowkit capacity tests (S2).
//
// Each session opens a WS connection, runs system.hello → webrtc.offer → ICE → hold → call.hangup.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/transport/webrtc"
	"github.com/DeepFlowAI/flowkit/tools/capacity/drivers/wsjson"
	pwebrtc "github.com/pion/webrtc/v4"
)

type result struct {
	Driver       string         `json:"driver"`
	TargetN      int            `json:"target_n"`
	Completed    int            `json:"completed"`
	Failed       int            `json:"failed"`
	SuccessRate  float64        `json:"success_rate"`
	SetupP50Ms   float64        `json:"setup_p50_ms"`
	SetupP99Ms   float64        `json:"setup_p99_ms"`
	AvgSetupMs   float64        `json:"avg_setup_ms"`
	StatusCounts map[string]int `json:"status_counts"`
	Errors       []string       `json:"errors"`
	WallSeconds  float64        `json:"wall_seconds"`
}

type iceNotifyParams struct {
	CallID    string                     `json:"call_id"`
	Candidate pwebrtc.ICECandidateInit   `json:"candidate"`
}

func main() {
	wsURL := flag.String("ws", "ws://127.0.0.1:9600/ws", "Flowkit WebSocket URL")
	n := flag.Int("n", 10, "number of concurrent sessions")
	cps := flag.Int("cps", 10, "sessions per second ramp rate")
	hold := flag.Duration("hold", 30*time.Second, "hold after ICE connected")
	timeout := flag.Duration("timeout", 20*time.Second, "per-session timeout")
	stun := flag.String("stun", "stun:stun.l.google.com:19302", "STUN server URL (empty disables)")
	flag.Parse()

	if *n <= 0 {
		log.Fatal("-n must be > 0")
	}
	if *cps <= 0 {
		*cps = *n
	}

	var iceServers []string
	if *stun != "" {
		iceServers = []string{*stun}
	}

	t0 := time.Now()
	var (
		wg           sync.WaitGroup
		completed    atomic.Int32
		statusMu     sync.Mutex
		statusCounts = map[string]int{}
		setupMu      sync.Mutex
		setupMs      []float64
		errorsMu     sync.Mutex
		errors       []string
	)

	tick := time.NewTicker(time.Second / time.Duration(*cps))
	defer tick.Stop()
	launch := make(chan struct{}, *n)
	go func() {
		for i := 0; i < *n; i++ {
			<-tick.C
			launch <- struct{}{}
		}
		close(launch)
	}()

	for i := 0; i < *n; i++ {
		<-launch
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			setup, err := runSession(*wsURL, iceServers, *hold, *timeout, idx)
			if err != nil {
				errorsMu.Lock()
				errors = append(errors, err.Error())
				errorsMu.Unlock()
				statusMu.Lock()
				statusCounts["error"]++
				statusMu.Unlock()
				return
			}
			completed.Add(1)
			setupMu.Lock()
			setupMs = append(setupMs, setup)
			setupMu.Unlock()
			statusMu.Lock()
			statusCounts["200"]++
			statusMu.Unlock()
		}(i)
	}
	wg.Wait()

	out := result{
		Driver:       "webrtc",
		TargetN:      *n,
		Completed:    int(completed.Load()),
		Failed:       *n - int(completed.Load()),
		SuccessRate:  float64(completed.Load()) / float64(*n),
		SetupP50Ms:   percentile(setupMs, 0.50),
		SetupP99Ms:   percentile(setupMs, 0.99),
		AvgSetupMs:   average(setupMs),
		StatusCounts: statusCounts,
		Errors:       uniqueStrings(errors, 20),
		WallSeconds:  time.Since(t0).Seconds(),
	}
	raw, _ := json.Marshal(out)
	fmt.Printf("__CAPACITY_RESULT__=%s\n", raw)
	if out.Completed < *n {
		os.Exit(1)
	}
}

func runSession(wsURL string, iceServers []string, hold, timeout time.Duration, idx int) (float64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout+hold)
	defer cancel()

	client, err := wsjson.Connect(ctx, wsURL)
	if err != nil {
		return 0, fmt.Errorf("session %d: ws: %w", idx, err)
	}
	defer func() { _ = client.Close() }()

	if _, err := client.Call(ctx, "system.hello", map[string]string{
		"sdk": "capacity-webrtc", "version": "0.1",
	}); err != nil {
		return 0, fmt.Errorf("session %d: hello: %w", idx, err)
	}

	peer, err := webrtc.New(webrtc.Config{
		ICEServers:       iceServers,
		ICEGatherMaxWait: 2 * time.Second,
	})
	if err != nil {
		return 0, fmt.Errorf("session %d: peer: %w", idx, err)
	}
	defer func() { _ = peer.Close() }()

	var callID string
	var pendingLocal []pwebrtc.ICECandidateInit
	var pendingRemote []pwebrtc.ICECandidateInit
	var mu sync.Mutex

	flushLocal := func() {
		mu.Lock()
		cid := callID
		pending := append([]pwebrtc.ICECandidateInit(nil), pendingLocal...)
		pendingLocal = nil
		mu.Unlock()
		if cid == "" {
			return
		}
		for _, c := range pending {
			_, _ = client.Call(ctx, "webrtc.ice", map[string]any{
				"call_id": cid, "candidate": c,
			})
		}
	}

	peer.OnLocalICECandidate(func(c pwebrtc.ICECandidateInit) {
		mu.Lock()
		if callID == "" {
			pendingLocal = append(pendingLocal, c)
			mu.Unlock()
			return
		}
		mu.Unlock()
		_, _ = client.Call(ctx, "webrtc.ice", map[string]any{
			"call_id": callID, "candidate": c,
		})
	})

	client.OnNotification("webrtc.ice", func(raw json.RawMessage) {
		var p iceNotifyParams
		if json.Unmarshal(raw, &p) != nil || p.Candidate.Candidate == "" {
			return
		}
		mu.Lock()
		cid := callID
		mu.Unlock()
		if cid == "" || p.CallID != cid {
			mu.Lock()
			pendingRemote = append(pendingRemote, p.Candidate)
			mu.Unlock()
			return
		}
		_ = peer.AddICECandidate(p.Candidate)
	})

	connected := make(chan struct{}, 1)
	peer.OnConnected(func() {
		select {
		case connected <- struct{}{}:
		default:
		}
	})

	tStart := time.Now()
	offerSDP, err := peer.CreateOffer(ctx)
	if err != nil {
		return 0, fmt.Errorf("session %d: create offer: %w", idx, err)
	}

	raw, err := client.Call(ctx, "webrtc.offer", map[string]string{"sdp": offerSDP})
	if err != nil {
		return 0, fmt.Errorf("session %d: offer: %w", idx, err)
	}
	var ans struct {
		CallID string `json:"call_id"`
		SDP    string `json:"sdp"`
	}
	if err := json.Unmarshal(raw, &ans); err != nil {
		return 0, fmt.Errorf("session %d: decode offer result: %w", idx, err)
	}
	mu.Lock()
	callID = ans.CallID
	remotePending := append([]pwebrtc.ICECandidateInit(nil), pendingRemote...)
	pendingRemote = nil
	mu.Unlock()
	flushLocal()
	for _, c := range remotePending {
		_ = peer.AddICECandidate(c)
	}
	if err := peer.SetRemoteAnswer(ctx, ans.SDP); err != nil {
		return 0, fmt.Errorf("session %d: set answer: %w", idx, err)
	}

	select {
	case <-connected:
	case <-ctx.Done():
		return 0, fmt.Errorf("session %d: ice not connected", idx)
	}
	setup := float64(time.Since(tStart).Milliseconds())

	select {
	case <-time.After(hold):
	case <-ctx.Done():
	}

	if _, err := client.Call(ctx, "call.hangup", map[string]string{
		"call_id": callID, "reason": "capacity_end",
	}); err != nil {
		return setup, fmt.Errorf("session %d: hangup: %w", idx, err)
	}
	return setup, nil
}

func percentile(values []float64, p float64) float64 {
	if len(values) == 0 {
		return 0
	}
	cp := append([]float64(nil), values...)
	sort.Float64s(cp)
	idx := int(float64(len(cp)-1) * p)
	if idx < 0 {
		idx = 0
	}
	return cp[idx]
}

func average(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	var sum float64
	for _, v := range values {
		sum += v
	}
	return sum / float64(len(values))
}

func uniqueStrings(in []string, max int) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(in))
	for _, s := range in {
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
		if len(out) >= max {
			break
		}
	}
	return out
}
