package sip

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/emiago/sipgo"
	"github.com/emiago/sipgo/sip"
	"github.com/stretchr/testify/require"
)

// TestSIP_Soak_ConcurrentInbound accepts multiple independent callers in parallel.
func TestSIP_Soak_ConcurrentInbound(t *testing.T) {
	const sessions = 5

	contactHost, contactPort, contactCleanup := startByeContact(t)
	defer contactCleanup()
	listenAddr, sigState, mgr, _, teardown := startTestUA(t)
	defer teardown()

	host, portStr, _ := net.SplitHostPort(listenAddr)
	port, _ := strconv.Atoi(portStr)
	targetURI := sip.Uri{User: "flowkit", Host: host, Port: port}

	var wg sync.WaitGroup
	var ok200 atomic.Int32
	var busy486 atomic.Int32

	for i := 0; i < sessions; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			clientUA, err := sipgo.NewUA(
				sipgo.WithUserAgent("soak-client"),
				sipgo.WithUserAgentHostname("127.0.0.1"),
			)
			if err != nil {
				t.Logf("session %d: NewUA: %v", idx, err)
				return
			}
			defer clientUA.Close()
			client, err := sipgo.NewClient(clientUA)
			if err != nil {
				t.Logf("session %d: NewClient: %v", idx, err)
				return
			}
			defer client.Close()

			rtpPC, err := net.ListenPacket("udp", "127.0.0.1:0")
			if err != nil {
				t.Logf("session %d: rtp listen: %v", idx, err)
				return
			}
			defer rtpPC.Close()
			rtpPort := rtpPC.LocalAddr().(*net.UDPAddr).Port

			offer := BuildAnswerPCMU("127.0.0.1", rtpPort, time.Now().UnixNano()+int64(idx))
			invite := sip.NewRequest(sip.INVITE, targetURI)
			invite.SetTransport("udp")
			invite.SetDestination(listenAddr)
			invite.SetBody([]byte(offer))
			invite.AppendHeader(sip.NewHeader("Content-Type", "application/sdp"))
			invite.AppendHeader(&sip.ContactHeader{
				Address: sip.Uri{User: "soak", Host: contactHost, Port: contactPort},
			})

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			tx, err := client.TransactionRequest(ctx, invite)
			if err != nil {
				t.Logf("session %d: TransactionRequest: %v", idx, err)
				return
			}
			defer tx.Terminate()

			for {
				select {
				case res, more := <-tx.Responses():
					if !more {
						t.Logf("session %d: response channel closed", idx)
						return
					}
					if res.StatusCode >= 200 {
						switch int(res.StatusCode) {
						case 200:
							ok200.Add(1)
							_ = sendInviteACK(client, invite, res, listenAddr)
						case 486:
							busy486.Add(1)
						default:
							t.Logf("session %d: unexpected %d", idx, res.StatusCode)
						}
						return
					}
				case <-ctx.Done():
					t.Logf("session %d: timeout", idx)
					return
				}
			}
		}(i)
	}
	wg.Wait()

	require.Equal(t, int32(sessions), ok200.Load(), "each caller should be accepted")
	require.Equal(t, 0, int(busy486.Load()), "independent callers should not get 486")
	require.Equal(t, sessions, sigState.ActiveCalls())
	require.Equal(t, sessions, mgr.Count())

	for _, id := range mgr.IDs() {
		_ = mgr.Close(id, "soak_end")
	}
	require.Eventually(t, func() bool { return sigState.ActiveCalls() == 0 }, 3*time.Second, 50*time.Millisecond)
}

// TestSIP_Soak_TrunkProbeLegsRejected rejects numeric Call-ID probe legs while
// accepting the UUID media leg (CN trunk parallel INVITE pattern).
func TestSIP_Soak_TrunkProbeLegsRejected(t *testing.T) {
	const probes = 4

	contactHost, contactPort, contactCleanup := startByeContact(t)
	defer contactCleanup()
	listenAddr, sigState, mgr, _, teardown := startTestUA(t)
	defer teardown()

	host, portStr, _ := net.SplitHostPort(listenAddr)
	port, _ := strconv.Atoi(portStr)
	targetURI := sip.Uri{User: "flowkit", Host: host, Port: port}

	var wg sync.WaitGroup
	var ok200 atomic.Int32
	var busy486 atomic.Int32

	sendInvite := func(idx int, sipCallID string, pt int) {
		defer wg.Done()

		clientUA, err := sipgo.NewUA(
			sipgo.WithUserAgent("soak-client"),
			sipgo.WithUserAgentHostname("127.0.0.1"),
		)
		if err != nil {
			t.Logf("session %d: NewUA: %v", idx, err)
			return
		}
		defer clientUA.Close()
		client, err := sipgo.NewClient(clientUA)
		if err != nil {
			t.Logf("session %d: NewClient: %v", idx, err)
			return
		}
		defer client.Close()

		rtpPC, err := net.ListenPacket("udp", "127.0.0.1:0")
		if err != nil {
			t.Logf("session %d: rtp listen: %v", idx, err)
			return
		}
		defer rtpPC.Close()
		rtpPort := rtpPC.LocalAddr().(*net.UDPAddr).Port

		var offer string
		if pt == 8 {
			offer = BuildAnswerG711("127.0.0.1", rtpPort, time.Now().UnixNano()+int64(idx), 8, 0)
		} else {
			offer = BuildAnswerPCMU("127.0.0.1", rtpPort, time.Now().UnixNano()+int64(idx))
		}
		invite := sip.NewRequest(sip.INVITE, targetURI)
		invite.SetTransport("udp")
		invite.SetDestination(listenAddr)
		invite.SetBody([]byte(offer))
		invite.AppendHeader(sip.NewHeader("Content-Type", "application/sdp"))
		invite.AppendHeader(sip.NewHeader("Call-ID", sipCallID))
		invite.AppendHeader(&sip.ContactHeader{
			Address: sip.Uri{User: "soak", Host: contactHost, Port: contactPort},
		})

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		tx, err := client.TransactionRequest(ctx, invite)
		if err != nil {
			t.Logf("session %d: TransactionRequest: %v", idx, err)
			return
		}
		defer tx.Terminate()

		for {
			select {
			case res, more := <-tx.Responses():
				if !more {
					return
				}
				if res.StatusCode >= 200 {
					switch int(res.StatusCode) {
					case 200:
						ok200.Add(1)
						_ = sendInviteACK(client, invite, res, listenAddr)
					case 486:
						busy486.Add(1)
					default:
						t.Logf("session %d: unexpected %d", idx, res.StatusCode)
					}
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}

	wg.Add(1)
	go sendInvite(0, "481cc9d6-cf72-123f-c08d-b4e9b8fac0c5", 8)
	for i := 0; i < probes; i++ {
		wg.Add(1)
		go sendInvite(i+1, fmt.Sprintf("1448750201-%d-1036032193", i), 0)
	}
	wg.Wait()

	require.Equal(t, int32(1), ok200.Load(), "UUID media leg should be accepted")
	require.Equal(t, int32(probes), busy486.Load(), "numeric probe legs should get 486")
	require.Equal(t, 1, sigState.ActiveCalls())
	require.Equal(t, 1, mgr.Count())

	for _, id := range mgr.IDs() {
		_ = mgr.Close(id, "soak_end")
	}
	require.Eventually(t, func() bool { return sigState.ActiveCalls() == 0 }, 3*time.Second, 50*time.Millisecond)
}

func TestSIP_Soak_InboundCapacityLimit(t *testing.T) {
	t.Setenv("SIP_MAX_INBOUND_CALLS", "2")

	contactHost, contactPort, contactCleanup := startByeContact(t)
	defer contactCleanup()
	listenAddr, sigState, mgr, _, teardown := startTestUA(t)
	defer teardown()

	host, portStr, _ := net.SplitHostPort(listenAddr)
	port, _ := strconv.Atoi(portStr)
	targetURI := sip.Uri{User: "flowkit", Host: host, Port: port}

	var wg sync.WaitGroup
	var ok200 atomic.Int32
	var busy486 atomic.Int32

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			clientUA, err := sipgo.NewUA(
				sipgo.WithUserAgent("soak-client"),
				sipgo.WithUserAgentHostname("127.0.0.1"),
			)
			if err != nil {
				return
			}
			defer clientUA.Close()
			client, err := sipgo.NewClient(clientUA)
			if err != nil {
				return
			}
			defer client.Close()

			rtpPC, err := net.ListenPacket("udp", "127.0.0.1:0")
			if err != nil {
				return
			}
			defer rtpPC.Close()
			rtpPort := rtpPC.LocalAddr().(*net.UDPAddr).Port

			offer := BuildAnswerG711("127.0.0.1", rtpPort, time.Now().UnixNano()+int64(idx), 8, 0)
			invite := sip.NewRequest(sip.INVITE, targetURI)
			invite.SetTransport("udp")
			invite.SetDestination(listenAddr)
			invite.SetBody([]byte(offer))
			invite.AppendHeader(sip.NewHeader("Content-Type", "application/sdp"))
			invite.AppendHeader(&sip.ContactHeader{
				Address: sip.Uri{User: "soak", Host: contactHost, Port: contactPort},
			})

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			tx, err := client.TransactionRequest(ctx, invite)
			if err != nil {
				return
			}
			defer tx.Terminate()

			for {
				select {
				case res, more := <-tx.Responses():
					if !more {
						return
					}
					if res.StatusCode >= 200 {
						switch int(res.StatusCode) {
						case 200:
							ok200.Add(1)
							_ = sendInviteACK(client, invite, res, listenAddr)
						case 486:
							busy486.Add(1)
						}
						return
					}
				case <-ctx.Done():
					return
				}
			}
		}(i)
	}
	wg.Wait()

	require.Equal(t, int32(2), ok200.Load())
	require.Equal(t, int32(1), busy486.Load())
	require.Equal(t, 2, sigState.ActiveCalls())

	for _, id := range mgr.IDs() {
		_ = mgr.Close(id, "soak_end")
	}
	require.Eventually(t, func() bool { return sigState.ActiveCalls() == 0 }, 3*time.Second, 50*time.Millisecond)
}
