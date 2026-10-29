// S3 bridge load driver: SIP inbound + WebRTC offer + call.bridge per pair.
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

	"github.com/DeepFlowAI/flowkit/tools/capacity/drivers/capwebrtc"
	"github.com/DeepFlowAI/flowkit/tools/capacity/drivers/result"
	"github.com/DeepFlowAI/flowkit/tools/capacity/drivers/sipclient"
	"github.com/DeepFlowAI/flowkit/tools/capacity/drivers/wsjson"
	sipmsg "github.com/emiago/sipgo/sip"
)

type incomingParams struct {
	CallID    string `json:"call_id"`
	Transport string `json:"transport"`
}

// sipAssigner maps broadcast call.incoming events to waiting pair goroutines (FIFO).
type sipAssigner struct {
	mu   sync.Mutex
	wait []chan string
	seen sync.Map
}

func (a *sipAssigner) register() chan string {
	ch := make(chan string, 1)
	a.mu.Lock()
	a.wait = append(a.wait, ch)
	a.mu.Unlock()
	return ch
}

func (a *sipAssigner) deliver(callID string) {
	if _, loaded := a.seen.LoadOrStore(callID, true); loaded {
		return
	}
	a.mu.Lock()
	if len(a.wait) == 0 {
		a.mu.Unlock()
		return
	}
	ch := a.wait[0]
	a.wait = a.wait[1:]
	a.mu.Unlock()
	select {
	case ch <- callID:
	default:
	}
}

func (a *sipAssigner) onIncoming(raw json.RawMessage) {
	var p incomingParams
	if json.Unmarshal(raw, &p) != nil || p.CallID == "" {
		return
	}
	if p.Transport != "" && p.Transport != "sip" {
		return
	}
	a.deliver(p.CallID)
}

func main() {
	wsURL := flag.String("ws", "ws://127.0.0.1:9600/ws", "Flowkit WebSocket URL")
	sipTarget := flag.String("sip-target", "127.0.0.1:5060", "SIP host:port")
	sipUser := flag.String("sip-user", "flowkit", "SIP request-URI user")
	n := flag.Int("n", 5, "concurrent bridged pairs")
	cps := flag.Int("cps", 5, "pairs per second")
	hold := flag.Duration("hold", 30*time.Second, "hold after bridge")
	timeout := flag.Duration("timeout", 45*time.Second, "per-pair timeout")
	stun := flag.String("stun", "stun:stun.l.google.com:19302", "STUN URL")
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

	var ice []string
	if *stun != "" {
		ice = []string{*stun}
	}

	assigner := &sipAssigner{}
	t0 := time.Now()
	var (
		wg          sync.WaitGroup
		completed   atomic.Int32
		collector   result.Collector
		collectorMu sync.Mutex
	)
	collector.TargetN = *n
	collector.StatusCounts = map[string]int{}

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
			setup, err := runPair(*wsURL, *sipTarget, targetURI, ice, *hold, *timeout, idx, assigner)
			collectorMu.Lock()
			defer collectorMu.Unlock()
			if err != nil {
				collector.AddError(err.Error())
				return
			}
			completed.Add(1)
			collector.AddOK()
			collector.AddSetup(float64(setup.Milliseconds()))
		}(i)
	}
	wg.Wait()

	result.Emit(result.Payload{
		Driver:       "bridge",
		TargetN:      *n,
		Completed:    int(completed.Load()),
		Failed:       *n - int(completed.Load()),
		StatusCounts: collector.StatusCounts,
		Errors:       result.UniqueStrings(collector.Errors, 20),
		WallSeconds:  time.Since(t0).Seconds(),
		SetupMs:      collector.SetupMs,
	})
}

func runPair(
	wsURL, sipListen string,
	targetURI sipmsg.Uri,
	ice []string,
	hold, timeout time.Duration,
	idx int,
	assigner *sipAssigner,
) (time.Duration, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout+hold+10*time.Second)
	defer cancel()

	sipReady := assigner.register()

	client, err := wsjson.Connect(ctx, wsURL)
	if err != nil {
		return 0, fmt.Errorf("pair %d: ws: %w", idx, err)
	}
	defer func() { _ = client.Close() }()

	client.OnNotification("call.incoming", assigner.onIncoming)

	if _, err := client.Call(ctx, "system.hello", map[string]string{
		"sdk": "capacity-bridge", "version": "0.1",
	}); err != nil {
		return 0, fmt.Errorf("pair %d: hello: %w", idx, err)
	}

	tStart := time.Now()
	sipHoldDone := make(chan struct{})
	sipDone := make(chan struct{})
	go func() {
		defer close(sipDone)
		sipCtx, sipCancel := context.WithTimeout(ctx, hold+timeout+30*time.Second)
		defer sipCancel()
		_ = sipclient.PlaceInbound(sipCtx, sipListen, targetURI, idx, sipclient.HoldUntil{
			Done:   sipHoldDone,
			MaxDur: hold + timeout + 30*time.Second,
		})
	}()

	webrtcSess, _, err := capwebrtc.RunOffer(ctx, client, ice, true)
	if err != nil {
		close(sipHoldDone)
		<-sipDone
		return 0, fmt.Errorf("pair %d: webrtc: %w", idx, err)
	}
	defer webrtcSess.Close()

	if err := capwebrtc.Answer(ctx, client, webrtcSess.CallID); err != nil {
		close(sipHoldDone)
		<-sipDone
		return 0, fmt.Errorf("pair %d: answer webrtc: %w", idx, err)
	}

	var sipCallID string
	select {
	case sipCallID = <-sipReady:
	case <-time.After(timeout):
		return 0, fmt.Errorf("pair %d: sip call.incoming timeout", idx)
	case <-ctx.Done():
		return 0, ctx.Err()
	}

	if _, err := client.Call(ctx, "call.bridge", map[string]string{
		"call_id": sipCallID, "other_call_id": webrtcSess.CallID,
	}); err != nil {
		close(sipHoldDone)
		<-sipDone
		return 0, fmt.Errorf("pair %d: bridge: %w", idx, err)
	}
	setup := time.Since(tStart)

	select {
	case <-time.After(hold):
	case <-ctx.Done():
	}

	close(sipHoldDone)
	_ = capwebrtc.Hangup(ctx, client, sipCallID)
	_ = capwebrtc.Hangup(ctx, client, webrtcSess.CallID)
	<-sipDone
	return setup, nil
}
