package sip

import (
	"context"
	"fmt"
	"time"

	"github.com/emiago/sipgo"
	"github.com/emiago/sipgo/sip"
)

// dialogDoer sends in-dialog SIP requests (re-INVITE for hold).
type dialogDoer interface {
	Do(ctx context.Context, req *sip.Request) (*sip.Response, error)
}

// SetCallHold applies or clears SIP hold for the flowkit call_id via re-INVITE
// (RFC 3264 sendonly / sendrecv). WebRTC-only calls are no-ops.
func (s *Signal) SetCallHold(flowCallID string, hold bool) error {
	cs := s.findCallByFlowID(flowCallID)
	if cs == nil || cs.dialog == nil || cs.localPort == 0 {
		return nil
	}
	doer, ok := cs.dialog.(dialogDoer)
	if !ok {
		return nil
	}
	target, ok := reinviteTarget(cs.dialog)
	if !ok {
		return nil
	}
	var sdp string
	if hold {
		sdp = BuildAnswerG711Hold(s.ua.ExtIP(), cs.localPort, time.Now().UnixNano(), cs.g711PT, cs.dtmfPT)
	} else {
		sdp = BuildAnswerG711(s.ua.ExtIP(), cs.localPort, time.Now().UnixNano(), cs.g711PT, cs.dtmfPT)
	}
	req := sip.NewRequest(sip.INVITE, target)
	req.SetBody([]byte(sdp))
	req.AppendHeader(sip.NewHeader("Content-Type", "application/sdp"))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res, err := doer.Do(ctx, req)
	if err != nil {
		return fmt.Errorf("sip hold reinvite: %w", err)
	}
	if res == nil || res.StatusCode != 200 {
		code := 0
		if res != nil {
			code = int(res.StatusCode)
		}
		return fmt.Errorf("sip hold reinvite: status %d", code)
	}
	return nil
}

func (s *Signal) findCallByFlowID(flowCallID string) *callState {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, cs := range s.calls {
		if cs.flowCall != nil && cs.flowCall.ID() == flowCallID {
			return cs
		}
	}
	return nil
}

func reinviteTarget(dialog dialogCloser) (sip.Uri, bool) {
	switch d := dialog.(type) {
	case *sipgo.DialogServerSession:
		if d.InviteRequest == nil || d.InviteRequest.From() == nil {
			return sip.Uri{}, false
		}
		return d.InviteRequest.From().Address, true
	case *sipgo.DialogClientSession:
		if d.InviteRequest == nil {
			return sip.Uri{}, false
		}
		return d.InviteRequest.Recipient, true
	default:
		return sip.Uri{}, false
	}
}
