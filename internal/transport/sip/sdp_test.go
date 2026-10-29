package sip

import (
	"strings"
	"testing"

	flowrtp "github.com/DeepFlowAI/flowkit/internal/media/rtp"
	"github.com/stretchr/testify/require"
)

func TestParseSDP_Linphone(t *testing.T) {
	body := strings.Join([]string{
		"v=0",
		"o=linphone 1234 1234 IN IP4 192.168.1.20",
		"s=Talk",
		"c=IN IP4 192.168.1.20",
		"t=0 0",
		"m=audio 7078 RTP/AVP 0 8 96 97",
		"a=rtpmap:0 PCMU/8000",
		"a=rtpmap:8 PCMA/8000",
		"a=rtpmap:96 telephone-event/8000",
		"a=fmtp:96 0-16",
		"a=rtpmap:97 opus/48000/2",
		"a=ptime:20",
		"a=sendrecv",
		"",
	}, "\r\n")

	got, err := ParseSDP(body)
	require.NoError(t, err)
	require.Equal(t, "192.168.1.20", got.ConnectIP)
	require.Equal(t, 7078, got.AudioPort)
	require.Equal(t, []int{0, 8, 96, 97}, got.PayloadList)
	require.True(t, got.HasPCMU())
	require.Equal(t, uint8(96), got.TelephoneEventPT())
}

func TestParseSDP_Asterisk_PCMA_Only(t *testing.T) {
	body := strings.Join([]string{
		"v=0",
		"o=asterisk 555 555 IN IP4 10.0.0.5",
		"s=session",
		"c=IN IP4 10.0.0.5",
		"t=0 0",
		"m=audio 16000 RTP/AVP 8",
		"a=rtpmap:8 PCMA/8000",
		"",
	}, "\n")

	got, err := ParseSDP(body)
	require.NoError(t, err)
	require.Equal(t, 16000, got.AudioPort)
	require.False(t, got.HasPCMU(), "PCMA-only offer must report no PCMU")
	pt, ok := got.NegotiatedG711()
	require.True(t, ok)
	require.Equal(t, 8, pt)
}

func TestBuildAnswerG711_PCMA(t *testing.T) {
	answer := BuildAnswerG711("192.168.1.10", 16384, 42, 8, 96)
	require.Contains(t, answer, "m=audio 16384 RTP/AVP 8 96")
	require.Contains(t, answer, "a=rtpmap:8 PCMA/8000")
	require.Contains(t, answer, "a=rtpmap:96 telephone-event/8000")
	parsed, err := ParseSDP(answer)
	require.NoError(t, err)
	require.True(t, parsed.HasPCMA())
}

func TestParseSDP_NoAudioLine_Errors(t *testing.T) {
	body := "v=0\r\no=x 1 1 IN IP4 1.1.1.1\r\nc=IN IP4 1.1.1.1\r\nt=0 0\r\n"
	_, err := ParseSDP(body)
	require.Error(t, err)
}

func TestParseSDP_NoCLine_Errors(t *testing.T) {
	body := "v=0\r\no=x 1 1 IN IP4 1.1.1.1\r\nt=0 0\r\nm=audio 5000 RTP/AVP 0\r\n"
	_, err := ParseSDP(body)
	require.Error(t, err)
}

func TestParseSDP_EmptyBody_Errors(t *testing.T) {
	_, err := ParseSDP("")
	require.Error(t, err)
}

func TestParseSDP_BadPort_Errors(t *testing.T) {
	body := "v=0\r\no=x 1 1 IN IP4 1.1.1.1\r\nc=IN IP4 1.1.1.1\r\nt=0 0\r\nm=audio xyz RTP/AVP 0\r\n"
	_, err := ParseSDP(body)
	require.Error(t, err)
}

func TestBuildAnswerG711Hold_Sendonly(t *testing.T) {
	answer := BuildAnswerG711Hold("192.168.1.10", 16384, 42, 0, 96)
	require.Contains(t, answer, "a=sendonly\r\n")
	require.NotContains(t, answer, "a=sendrecv")
}

func TestBuildAnswerPCMU_StructureValid(t *testing.T) {
	answer := BuildAnswerPCMU("192.168.1.10", 16384, 42)

	require.Contains(t, answer, "v=0\r\n")
	require.Contains(t, answer, "o=flowkit 42 42 IN IP4 192.168.1.10")
	require.Contains(t, answer, "c=IN IP4 192.168.1.10")
	require.Contains(t, answer, "m=audio 16384 RTP/AVP 0")
	require.Contains(t, answer, "a=rtpmap:0 PCMU/8000")
	require.Contains(t, answer, "a=ptime:20")
	require.Contains(t, answer, "a=sendrecv")

	// Should be reparsable by our own parser
	parsed, err := ParseSDP(answer)
	require.NoError(t, err)
	require.Equal(t, "192.168.1.10", parsed.ConnectIP)
	require.Equal(t, 16384, parsed.AudioPort)
	require.Equal(t, []int{0, 101}, parsed.PayloadList)
	require.True(t, parsed.HasPCMU())
}

func TestParseSDP_TelephoneEvent(t *testing.T) {
	body := strings.Join([]string{
		"v=0",
		"o=x 1 1 IN IP4 1.1.1.1",
		"c=IN IP4 1.1.1.1",
		"t=0 0",
		"m=audio 5000 RTP/AVP 0 96",
		"a=rtpmap:0 PCMU/8000",
		"a=rtpmap:96 telephone-event/8000",
		"",
	}, "\r\n")
	got, err := ParseSDP(body)
	require.NoError(t, err)
	require.Equal(t, uint8(96), got.TelephoneEventPT())
}

func TestNegotiatedDTMFPT(t *testing.T) {
	withEvent := &SDP{telephonePT: 96}
	require.Equal(t, uint8(96), NegotiatedDTMFPT(withEvent))
	require.Equal(t, flowrtp.PayloadTypeTelephoneEvent, NegotiatedDTMFPT(&SDP{}))
}

func TestBuildAnswerPCMU_DefaultSessionID(t *testing.T) {
	answer := BuildAnswerPCMU("1.2.3.4", 5000, 0)
	require.Contains(t, answer, "o=flowkit 1 1 IN IP4 1.2.3.4")
}
