package sip

import (
	"fmt"
	"os"
	"testing"
	"time"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	code := m.Run()
	if code != 0 {
		os.Exit(code)
	}

	opts := []goleak.Option{
		// THIRD-PARTY (emiago/sipgo): TransportUDP read loop. Top of stack is
		// usually internal/poll.runtime_pollWait; match the sipgo frame so we
		// do not blanket-ignore all network pollWait. Exits when the UDP
		// PacketConn is closed.
		goleak.IgnoreAnyFunction("github.com/emiago/sipgo/sip.(*TransportUDP).readListenerConnection"),

		// THIRD-PARTY (emiago/sipgo): after a final INVITE response the server
		// transaction stays in TerminateGracefully until Timer H (64*T1 ≈ 32s
		// with stock timers). Library retransmission absorption, not a flowkit
		// leak.
		goleak.IgnoreTopFunction("github.com/emiago/sipgo/sip.(*ServerTx).TerminateGracefully"),
	}

	// Inbound hangup WriteBye uses byeTimeout (2s) when the peer does not
	// answer; tests answer BYE via startByeContact so suite-end clear is
	// usually well under that. Wait is byeTimeout + margin.
	deadline := time.Now().Add(byeTimeout + 500*time.Millisecond)
	var err error
	for {
		err = goleak.Find(opts...)
		if err == nil {
			os.Exit(0)
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	fmt.Fprintf(os.Stderr, "goleak: Errors on successful test run: %v\n", err)
	os.Exit(1)
}
