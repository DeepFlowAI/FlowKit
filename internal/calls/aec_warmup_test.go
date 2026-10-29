package calls

import (
	"testing"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/dialogpolicy"
	"github.com/DeepFlowAI/flowkit/internal/session"
	"github.com/DeepFlowAI/flowkit/internal/transport"
	"github.com/DeepFlowAI/flowkit/internal/transport/fakeio"
	"github.com/stretchr/testify/require"
)

type sipKindIO struct{ *fakeio.FakeIO }

func (sipKindIO) Kind() transport.Kind { return transport.KindSIP }

func TestInAECWarmup_FirstSpeechOnSIPOnly(t *testing.T) {
	dialogpolicy.ResetForTest()
	t.Cleanup(dialogpolicy.ResetForTest)
	_, mgr, _ := newTestStack(t)

	sipCall := mgr.Create(session.DirectionInbound)
	require.NoError(t, sipCall.BindMediaIO(sipKindIO{fakeio.New(8)}))
	t.Cleanup(func() { _ = sipCall.Hangup("done") })
	cs := &callState{}
	now := time.Now()

	require.False(t, inAECWarmup(sipCall, cs, now), "no bot audio yet: nothing to echo")

	first := now.Add(-time.Second)
	cs.firstAudioNano.Store(first.UnixNano())
	require.True(t, inAECWarmup(sipCall, cs, first.Add(2500*time.Millisecond)),
		"echo onset ~2.5s after the first audio falls in the default window")
	require.False(t, inAECWarmup(sipCall, cs, first.Add(3500*time.Millisecond)))
	require.False(t, inAECWarmup(sipCall, cs, first.Add(-100*time.Millisecond)),
		"speech that began before the bot spoke is the caller's")
	require.False(t, inAECWarmup(sipCall, cs, time.Time{}))

	browser := mgr.Create(session.DirectionInbound)
	require.NoError(t, browser.BindMediaIO(fakeio.New(8)))
	t.Cleanup(func() { _ = browser.Hangup("done") })
	require.False(t, inAECWarmup(browser, cs, first.Add(time.Second)),
		"non-SIP transports cancel echo locally")

	p := dialogpolicy.Defaults()
	p.AECWarmupMs = 0
	dialogpolicy.Set(&p)
	require.False(t, inAECWarmup(sipCall, cs, first.Add(time.Second)), "0 disables the window")
}

func TestLikelyEchoOverPlayback(t *testing.T) {
	bot := "您好，您现在套餐是109元，这个套餐快到期了，我们现在有个优惠"
	// Leaked playback that ASR returned with characters dropped or misheard.
	require.True(t, likelyEchoOverPlayback("现在套餐是109这个套餐快到期", bot))
	require.True(t, likelyEchoOverPlayback("这个套餐快到其了", "", bot))
	// The echo can trail into the next sentence; the previous one still counts.
	require.True(t, likelyEchoOverPlayback("这个套餐快到期了", bot, "您看愿意办理吗？"))

	// Real interjections from recorded calls stay accepted.
	for _, user := range []string{
		"九十块钱吗",           // quotes the bot, but too short to judge
		"流量有多的吗",          // shares only "流量"/"吗"
		"呃，我有不接受的权利吗",     // over "您看这个用量可以接受吗"
		"嗯，可以帮我办理下",       // over "您看是否愿意办理"
		"这是空的，不是风，一开始是一直没有任何声音啊",
	} {
		require.False(t, likelyEchoOverPlayback(user,
			"含100GB流量和800分钟语音，合约期12个月，您看愿意办理吗？",
			"您看这个用量可以接受吗？您看是否愿意办理，每月九十块钱。"), user)
	}
	require.False(t, likelyEchoOverPlayback("这个套餐快到期了"), "no bot text, no echo")
}
