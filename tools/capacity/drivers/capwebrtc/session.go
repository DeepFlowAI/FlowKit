// Package capwebrtc drives webrtc.offer over the Flowkit JSON-RPC WebSocket API.
package capwebrtc

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/transport/webrtc"
	"github.com/DeepFlowAI/flowkit/tools/capacity/drivers/wsjson"
	pwebrtc "github.com/pion/webrtc/v4"
)

type iceNotifyParams struct {
	CallID    string                 `json:"call_id"`
	Candidate pwebrtc.ICECandidateInit `json:"candidate"`
}

// OfferSession keeps the local peer alive until Close (required for bridge/hold).
type OfferSession struct {
	CallID string
	peer   *webrtc.Peer
}

// Close releases the local WebRTC peer.
func (s *OfferSession) Close() {
	if s == nil || s.peer == nil {
		return
	}
	_ = s.peer.Close()
	s.peer = nil
}

// RunOffer completes SDP + ICE and optionally waits until connected.
// Caller must Close the returned session when the leg is finished.
func RunOffer(ctx context.Context, client *wsjson.Client, iceServers []string, waitConnected bool) (*OfferSession, time.Duration, error) {
	peer, err := webrtc.New(webrtc.Config{
		ICEServers:       iceServers,
		ICEGatherMaxWait: 2 * time.Second,
	})
	if err != nil {
		return nil, 0, err
	}

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
		cid := callID
		mu.Unlock()
		_, _ = client.Call(ctx, "webrtc.ice", map[string]any{
			"call_id": cid, "candidate": c,
		})
	})

	client.OnNotification("webrtc.ice", func(raw json.RawMessage) {
		var p iceNotifyParams
		if json.Unmarshal(raw, &p) != nil || p.Candidate.Candidate == "" {
			return
		}
		mu.Lock()
		cid := callID
		if cid == "" || p.CallID != cid {
			pendingRemote = append(pendingRemote, p.Candidate)
			mu.Unlock()
			return
		}
		mu.Unlock()
		_ = peer.AddICECandidate(p.Candidate)
	})

	connected := make(chan struct{}, 1)
	if waitConnected {
		peer.OnConnected(func() {
			select {
			case connected <- struct{}{}:
			default:
			}
		})
	}

	t0 := time.Now()
	offerSDP, err := peer.CreateOffer(ctx)
	if err != nil {
		_ = peer.Close()
		return nil, 0, fmt.Errorf("create offer: %w", err)
	}

	raw, err := client.Call(ctx, "webrtc.offer", map[string]string{"sdp": offerSDP})
	if err != nil {
		_ = peer.Close()
		return nil, 0, fmt.Errorf("webrtc.offer: %w", err)
	}
	var ans struct {
		CallID string `json:"call_id"`
		SDP    string `json:"sdp"`
	}
	if err := json.Unmarshal(raw, &ans); err != nil {
		_ = peer.Close()
		return nil, 0, fmt.Errorf("decode offer result: %w", err)
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
		_ = peer.Close()
		return nil, 0, fmt.Errorf("set answer: %w", err)
	}

	if waitConnected {
		select {
		case <-connected:
		case <-ctx.Done():
			_ = peer.Close()
			return nil, 0, fmt.Errorf("ice not connected: %w", ctx.Err())
		}
	}
	return &OfferSession{CallID: ans.CallID, peer: peer}, time.Since(t0), nil
}

// Answer marks a WebRTC leg active (required before call.bridge).
func Answer(ctx context.Context, client *wsjson.Client, callID string) error {
	_, err := client.Call(ctx, "call.answer", map[string]string{"call_id": callID})
	return err
}

// Hangup ends a call via RPC.
func Hangup(ctx context.Context, client *wsjson.Client, callID string) error {
	_, err := client.Call(ctx, "call.hangup", map[string]string{
		"call_id": callID, "reason": "capacity_end",
	})
	return err
}
