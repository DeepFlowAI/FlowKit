// S4 mock CC load driver: one WS orchestrator + concurrent SIP inbound calls.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/DeepFlowAI/flowkit/tools/capacity/drivers/result"
	"github.com/DeepFlowAI/flowkit/tools/capacity/drivers/sipclient"
	"github.com/DeepFlowAI/flowkit/tools/capacity/drivers/wsjson"
	sipmsg "github.com/emiago/sipgo/sip"
)

type incomingParams struct {
	CallID    string `json:"call_id"`
	Transport string `json:"transport"`
}

type playEndParams struct {
	CallID string `json:"call_id"`
	PlayID string `json:"play_id"`
}

type orchestrator struct {
	client       *wsjson.Client
	sayText      string
	listenWait   time.Duration
	completed    atomic.Int32
	setupMu      sync.Mutex
	setupMs      []float64
	errorsMu     sync.Mutex
	errors       []string
	inFlight     sync.WaitGroup
	playEnds     sync.Map // call_id -> chan struct{}
}

func main() {
	wsURL := flag.String("ws", "ws://127.0.0.1:9600/ws", "Flowkit WebSocket URL")
	sipTarget := flag.String("sip-target", "127.0.0.1:5060", "SIP host:port")
	sipUser := flag.String("sip-user", "flowkit", "SIP request-URI user")
	n := flag.Int("n", 5, "concurrent SIP calls")
	cps := flag.Int("cps", 5, "calls per second")
	sayText := flag.String("say", "capacity test", "call.say text")
	listenSec := flag.Int("listen-seconds", 5, "seconds to wait after call.listen")
	timeout := flag.Duration("timeout", 60*time.Second, "overall timeout")
	flag.Parse()

	if *n <= 0 {
		log.Fatal("-n must be > 0")
	}
	if *cps <= 0 {
		*cps = *n
	}

	host, portStr, err := net.SplitHostPort(*sipTarget)
	if err != nil {
		log.Fatalf("bad sip-target: %v", err)
	}
	port, _ := strconv.Atoi(portStr)
	targetURI := sipmsg.Uri{User: *sipUser, Host: host, Port: port}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout+time.Duration(*n)*time.Second)
	defer cancel()

	orch, err := startOrchestrator(ctx, *wsURL, *sayText, time.Duration(*listenSec)*time.Second)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = orch.client.Close() }()

	t0 := time.Now()
	var wg sync.WaitGroup
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
			sipCtx, sipCancel := context.WithTimeout(ctx, *timeout)
			defer sipCancel()
			if err := sipclient.WaitRemoteHangup(sipCtx, *sipTarget, targetURI, idx); err != nil && sipCtx.Err() == nil {
				orch.recordError(fmt.Errorf("sip %d: %w", idx, err))
			}
		}(i)
	}
	wg.Wait()
	orch.inFlight.Wait()

	collector := result.Collector{
		TargetN:      *n,
		SetupMs:      orch.setupMs,
		StatusCounts: map[string]int{},
		Errors:       orch.errors,
	}
	done := int(orch.completed.Load())
	if done > 0 {
		collector.StatusCounts["200"] = done
	}
	if len(orch.errors) > 0 {
		collector.StatusCounts["error"] = len(orch.errors)
	}

	result.Emit(result.Payload{
		Driver:       "mock_cc",
		TargetN:      *n,
		Completed:    done,
		Failed:       *n - done,
		StatusCounts: collector.StatusCounts,
		Errors:       result.UniqueStrings(orch.errors, 20),
		WallSeconds:  time.Since(t0).Seconds(),
		SetupMs:      orch.setupMs,
	})
}

func startOrchestrator(ctx context.Context, wsURL, sayText string, listenWait time.Duration) (*orchestrator, error) {
	client, err := wsjson.Connect(ctx, wsURL)
	if err != nil {
		return nil, err
	}
	if _, err := client.Call(ctx, "system.hello", map[string]string{
		"sdk": "capacity-mock-cc", "version": "0.1",
	}); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("hello: %w", err)
	}
	o := &orchestrator{
		client:     client,
		sayText:    sayText,
		listenWait: listenWait,
	}
	client.OnNotification("call.play_end", func(raw json.RawMessage) {
		var p playEndParams
		if json.Unmarshal(raw, &p) != nil || p.CallID == "" {
			return
		}
		if ch, ok := o.playEnds.Load(p.CallID); ok {
			select {
			case ch.(chan struct{}) <- struct{}{}:
			default:
			}
		}
	})
	client.OnNotification("call.incoming", func(raw json.RawMessage) {
		var p incomingParams
		if json.Unmarshal(raw, &p) != nil || p.CallID == "" {
			return
		}
		if p.Transport != "" && p.Transport != "sip" {
			return
		}
		o.inFlight.Add(1)
		go o.runWorkflow(context.Background(), p.CallID)
	})
	return o, nil
}

func (o *orchestrator) runWorkflow(ctx context.Context, callID string) {
	defer o.inFlight.Done()
	wctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	t0 := time.Now()

	steps := []struct {
		name string
		fn   func() error
	}{
		{"answer", func() error {
			_, err := o.client.Call(wctx, "call.answer", map[string]string{"call_id": callID})
			return err
		}},
		{"say", func() error {
			_, err := o.client.Call(wctx, "call.say", map[string]string{
				"call_id": callID, "text": o.sayText,
			})
			return err
		}},
	}
	for _, step := range steps {
		if err := step.fn(); err != nil {
			o.recordError(fmt.Errorf("call %s %s: %w", callID[:8], step.name, err))
			return
		}
	}

	playEnd := make(chan struct{}, 1)
	o.playEnds.Store(callID, playEnd)
	defer o.playEnds.Delete(callID)

	select {
	case <-playEnd:
	case <-time.After(45 * time.Second):
		o.recordError(fmt.Errorf("call %s play_end timeout", callID[:8]))
		return
	case <-wctx.Done():
		o.recordError(fmt.Errorf("call %s play_end ctx: %w", callID[:8], wctx.Err()))
		return
	}

	if _, err := o.client.Call(wctx, "call.listen", map[string]any{
		"call_id": callID, "timeout_ms": int(o.listenWait.Milliseconds()),
	}); err != nil {
		o.recordError(fmt.Errorf("call %s listen: %w", callID[:8], err))
		return
	}
	select {
	case <-time.After(o.listenWait + 2*time.Second):
	case <-wctx.Done():
		o.recordError(fmt.Errorf("call %s listen wait: %w", callID[:8], wctx.Err()))
		return
	}

	if _, err := o.client.Call(wctx, "call.hangup", map[string]string{
		"call_id": callID, "reason": "capacity_end",
	}); err != nil {
		o.recordError(fmt.Errorf("call %s hangup: %w", callID[:8], err))
		return
	}

	o.completed.Add(1)
	o.setupMu.Lock()
	o.setupMs = append(o.setupMs, float64(time.Since(t0).Milliseconds()))
	o.setupMu.Unlock()
}

func (o *orchestrator) recordError(err error) {
	o.errorsMu.Lock()
	o.errors = append(o.errors, err.Error())
	o.errorsMu.Unlock()
}
