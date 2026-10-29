// SIP inbound load driver for Flowkit capacity tests (S1).
//
// Sends concurrent INVITEs to a running Flowkit SIP UA, holds each call, then BYE.
// Emits a single JSON result line prefixed with __CAPACITY_RESULT__= on stdout.
//
// Usage:
//
//	go run ./tools/capacity/drivers/sip \
//	  -target 127.0.0.1:5060 \
//	  -n 50 -cps 10 -hold 30s
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/transport/sip"
	"github.com/emiago/sipgo"
	sipmsg "github.com/emiago/sipgo/sip"
	"github.com/google/uuid"
)

type result struct {
	Driver           string         `json:"driver"`
	TargetN          int            `json:"target_n"`
	Completed        int            `json:"completed"`
	Failed           int            `json:"failed"`
	SuccessRate      float64        `json:"success_rate"`
	SetupP50Ms       float64        `json:"setup_p50_ms"`
	SetupP99Ms       float64        `json:"setup_p99_ms"`
	AvgSetupMs       float64        `json:"avg_setup_ms"`
	StatusCounts     map[string]int `json:"status_counts"`
	Errors           []string       `json:"errors"`
	WallSeconds      float64        `json:"wall_seconds"`
}

func main() {
	target := flag.String("target", "127.0.0.1:5060", "Flowkit SIP listen host:port")
	user := flag.String("user", "flowkit", "SIP request-URI user")
	n := flag.Int("n", 10, "number of concurrent calls")
	cps := flag.Int("cps", 10, "calls per second ramp rate")
	hold := flag.Duration("hold", 30*time.Second, "time to keep each call up after 200 OK")
	timeout := flag.Duration("timeout", 15*time.Second, "per-call INVITE timeout")
	stagger := flag.Duration("stagger", 0, "delay between starting each call goroutine")
	flag.Parse()

	if *n <= 0 {
		log.Fatal("-n must be > 0")
	}
	if *cps <= 0 {
		*cps = *n
	}

	host, portStr, err := net.SplitHostPort(*target)
	if err != nil {
		log.Fatalf("bad -target: %v", err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		log.Fatalf("bad port in -target: %v", err)
	}
	targetURI := sipmsg.Uri{User: *user, Host: host, Port: port}

	t0 := time.Now()
	var (
		wg            sync.WaitGroup
		completed     atomic.Int32
		statusMu      sync.Mutex
		statusCounts  = map[string]int{}
		setupMu       sync.Mutex
		setupMs       []float64
		errorsMu      sync.Mutex
		errors        []string
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
		if *stagger > 0 && i > 0 {
			time.Sleep(*stagger)
		}
		<-launch
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			setup, code, runErr := runCall(*target, targetURI, *hold, *timeout, idx)
			if runErr != nil {
				errorsMu.Lock()
				errors = append(errors, runErr.Error())
				errorsMu.Unlock()
			}
			key := fmt.Sprintf("%d", code)
			if code == 0 {
				key = "error"
			}
			statusMu.Lock()
			statusCounts[key]++
			statusMu.Unlock()
			if code == 200 {
				completed.Add(1)
				setupMu.Lock()
				setupMs = append(setupMs, setup)
				setupMu.Unlock()
			}
		}(i)
	}

	wg.Wait()
	wall := time.Since(t0).Seconds()

	setupMu.Lock()
	p50, p99, avg := percentile(setupMs, 0.50), percentile(setupMs, 0.99), average(setupMs)
	setupMu.Unlock()

	done := int(completed.Load())
	out := result{
		Driver:       "sip",
		TargetN:      *n,
		Completed:    done,
		Failed:       *n - done,
		SuccessRate:  float64(done) / float64(*n),
		SetupP50Ms:   p50,
		SetupP99Ms:   p99,
		AvgSetupMs:   avg,
		StatusCounts: statusCounts,
		Errors:       uniqueStrings(errors, 20),
		WallSeconds:  wall,
	}
	raw, _ := json.Marshal(out)
	fmt.Printf("__CAPACITY_RESULT__=%s\n", raw)
	if done < *n {
		os.Exit(1)
	}
}

func runCall(listenAddr string, targetURI sipmsg.Uri, hold, timeout time.Duration, idx int) (setupMs float64, statusCode int, err error) {
	clientUA, err := sipgo.NewUA(
		sipgo.WithUserAgent("flowkit-capacity"),
		sipgo.WithUserAgentHostname("127.0.0.1"),
	)
	if err != nil {
		return 0, 0, fmt.Errorf("session %d: NewUA: %w", idx, err)
	}
	defer func() { _ = clientUA.Close() }()

	client, err := sipgo.NewClient(clientUA)
	if err != nil {
		return 0, 0, fmt.Errorf("session %d: NewClient: %w", idx, err)
	}
	defer func() { _ = client.Close() }()

	rtpPC, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		return 0, 0, fmt.Errorf("session %d: rtp listen: %w", idx, err)
	}
	defer func() { _ = rtpPC.Close() }()
	rtpPort := rtpPC.LocalAddr().(*net.UDPAddr).Port

	offer := sip.BuildAnswerPCMU("127.0.0.1", rtpPort, time.Now().UnixNano()+int64(idx))
	invite := sipmsg.NewRequest(sipmsg.INVITE, targetURI)
	invite.SetTransport("udp")
	invite.SetDestination(listenAddr)
	invite.SetBody([]byte(offer))
	invite.AppendHeader(sipmsg.NewHeader("Content-Type", "application/sdp"))
	invite.AppendHeader(&sipmsg.ContactHeader{
		Address: sipmsg.Uri{User: "cap", Host: "127.0.0.1", Port: 6000 + idx},
	})
	// UUID Call-ID avoids CN trunk probe-leg filtering (numeric X-Y-Z IDs get 486).
	invite.RemoveHeader("Call-ID")
	callID := sipmsg.CallIDHeader(uuid.NewString())
	invite.AppendHeader(&callID)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	tStart := time.Now()
	tx, err := client.TransactionRequest(ctx, invite)
	if err != nil {
		return 0, 0, fmt.Errorf("session %d: invite: %w", idx, err)
	}
	defer tx.Terminate()

	var res *sipmsg.Response
	for {
		select {
		case r, ok := <-tx.Responses():
			if !ok {
				return 0, 0, fmt.Errorf("session %d: no final response", idx)
			}
			if r.StatusCode >= 200 {
				res = r
				goto gotFinal
			}
		case <-ctx.Done():
			return 0, 0, fmt.Errorf("session %d: timeout waiting for 200", idx)
		}
	}
gotFinal:
	setupMs = float64(time.Since(tStart).Milliseconds())
	statusCode = int(res.StatusCode)
	if statusCode != 200 {
		return setupMs, statusCode, fmt.Errorf("session %d: status %d", idx, statusCode)
	}
	if err := sendACK(client, invite, res, listenAddr); err != nil {
		return setupMs, statusCode, fmt.Errorf("session %d: ack: %w", idx, err)
	}

	// Silence RTP keeps the media path warm without heavy CPU.
	go pumpSilence(rtpPC, hold+2*time.Second)

	select {
	case <-time.After(hold):
	case <-ctx.Done():
	}

	if err := sendBYE(client, invite, res, listenAddr); err != nil {
		return setupMs, statusCode, fmt.Errorf("session %d: bye: %w", idx, err)
	}
	return setupMs, statusCode, nil
}

func sendACK(client *sipgo.Client, invite *sipmsg.Request, res *sipmsg.Response, dest string) error {
	recipient := invite.Recipient
	if contact := res.Contact(); contact != nil {
		recipient = contact.Address
	}
	ack := sipmsg.NewRequest(sipmsg.ACK, recipient)
	ack.SetDestination(dest)
	ack.SetTransport(invite.Transport())
	if h := invite.From(); h != nil {
		ack.AppendHeader(sipmsg.HeaderClone(h))
	}
	if h := res.To(); h != nil {
		ack.AppendHeader(sipmsg.HeaderClone(h))
	}
	if h := invite.CallID(); h != nil {
		ack.AppendHeader(sipmsg.HeaderClone(h))
	}
	if h := invite.CSeq(); h != nil {
		ack.AppendHeader(sipmsg.HeaderClone(h))
	}
	if cseq := ack.CSeq(); cseq != nil {
		cseq.MethodName = sipmsg.ACK
	}
	if h := invite.Contact(); h != nil {
		ack.AppendHeader(sipmsg.HeaderClone(h))
	}
	if v := invite.Via(); v != nil {
		ack.AppendHeader(sipmsg.HeaderClone(v))
	}
	return client.WriteRequest(ack)
}

func sendBYE(client *sipgo.Client, invite *sipmsg.Request, res *sipmsg.Response, dest string) error {
	recipient := invite.Recipient
	if contact := res.Contact(); contact != nil {
		recipient = contact.Address
	}
	bye := sipmsg.NewRequest(sipmsg.BYE, recipient)
	bye.SetDestination(dest)
	bye.SetTransport(invite.Transport())
	for _, h := range []sipmsg.Header{
		invite.From(), res.To(), invite.CallID(),
	} {
		if h != nil {
			bye.AppendHeader(sipmsg.HeaderClone(h))
		}
	}
	cseq := invite.CSeq()
	if cseq != nil {
		byeCSeq := sipmsg.HeaderClone(cseq).(*sipmsg.CSeqHeader)
		byeCSeq.SeqNo++
		byeCSeq.MethodName = sipmsg.BYE
		bye.AppendHeader(byeCSeq)
	}
	if v := invite.Via(); v != nil {
		bye.AppendHeader(sipmsg.HeaderClone(v))
	}
	return client.WriteRequest(bye)
}

func pumpSilence(pc net.PacketConn, d time.Duration) {
	buf := make([]byte, 172) // minimal RTP-ish payload
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		_, _ = pc.WriteTo(buf, pc.LocalAddr())
		time.Sleep(20 * time.Millisecond)
	}
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
