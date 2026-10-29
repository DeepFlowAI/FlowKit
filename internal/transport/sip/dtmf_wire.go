package sip

import (
	"github.com/DeepFlowAI/flowkit/internal/rpc"
	"github.com/DeepFlowAI/flowkit/internal/session"
	flowrtp "github.com/DeepFlowAI/flowkit/internal/media/rtp"
)

// NegotiatedDTMFPT returns the RFC 4733 payload type to use for MediaIO.
// When the remote SDP offers telephone-event, reuse that PT; otherwise use
// the default 101 that BuildAnswerG711 includes in the answer.
func NegotiatedDTMFPT(offer *SDP) uint8 {
	if pt := offer.TelephoneEventPT(); pt != 0 {
		return pt
	}
	return flowrtp.PayloadTypeTelephoneEvent
}

// WireDTMF connects inbound DTMF detection to session events and optional
// JSON-RPC broadcast.
func WireDTMF(mio *MediaIO, call *session.Call, bc Broadcaster) {
	if mio == nil || call == nil || mio.dtmfPT == 0 {
		return
	}
	mio.SetDTMFHandler(func(digit string) {
		call.Emit(session.NewDTMF(digit))
		if bc == nil {
			return
		}
		n, err := rpc.NewNotification("call.dtmf", map[string]any{
			"call_id": call.ID(),
			"digit":   digit,
		})
		if err == nil {
			bc.BroadcastNotification(n)
		}
	})
}
