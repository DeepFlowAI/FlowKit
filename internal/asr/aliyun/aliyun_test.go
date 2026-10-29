package aliyun

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/DeepFlowAI/flowkit/internal/asr"
	"github.com/stretchr/testify/require"
)

// compile-time interface check
var _ asr.Provider = (*Provider)(nil)

func TestNew_RequiresCredentials(t *testing.T) {
	cases := map[string]Config{
		"missing AK":     {AccessKeySecret: "s", AppKey: "ak"},
		"missing secret": {AccessKeyID: "id", AppKey: "ak"},
		"missing appkey": {AccessKeyID: "id", AccessKeySecret: "s"},
	}
	for name, cfg := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := New(cfg)
			require.Error(t, err)
		})
	}
}

func TestNew_DefaultsRegion(t *testing.T) {
	p, err := New(Config{
		AccessKeyID:     "AKID",
		AccessKeySecret: "AKSECRET",
		AppKey:          "appkey",
	})
	require.NoError(t, err)
	require.Equal(t, "cn-shanghai", p.cfg.Region)
}

func TestSendPCM_BeforeStart_Errors(t *testing.T) {
	p, err := New(Config{
		AccessKeyID:     "AKID",
		AccessKeySecret: "AKSECRET",
		AppKey:          "appkey",
	})
	require.NoError(t, err)

	err = p.SendPCM([]int16{1, 2, 3})
	require.ErrorIs(t, err, asr.ErrNotStarted)
}

func TestSendPCM_AfterStop_Errors(t *testing.T) {
	p, err := New(Config{
		AccessKeyID:     "AKID",
		AccessKeySecret: "AKSECRET",
		AppKey:          "appkey",
	})
	require.NoError(t, err)

	require.NoError(t, p.Stop())
	err = p.SendPCM([]int16{1})
	// Either ErrStopped (preferred) or ErrNotStarted is acceptable —
	// the contract is "no PCM after Stop".
	require.Error(t, err)
}

func TestStop_Idempotent(t *testing.T) {
	p, err := New(Config{
		AccessKeyID:     "AKID",
		AccessKeySecret: "AKSECRET",
		AppKey:          "appkey",
	})
	require.NoError(t, err)
	require.NoError(t, p.Stop())
	require.NoError(t, p.Stop())
}

func TestTranslate_AllKnownEvents(t *testing.T) {
	cases := []struct {
		raw     ASREvent
		want    asr.EventType
		wantTxt string
	}{
		{ASREvent{Type: "sentence_begin"}, asr.EventVADStart, ""},
		{ASREvent{Type: "result_changed", Text: "你"}, asr.EventPartial, "你"},
		{ASREvent{Type: "sentence_end", Text: "你好"}, asr.EventFinal, "你好"},
		{ASREvent{Type: "completed"}, asr.EventVADEnd, ""},
		{ASREvent{Type: "started"}, asr.EventVADStart, ""},
	}
	for _, c := range cases {
		t.Run(c.raw.Type, func(t *testing.T) {
			ev := translate(c.raw)
			require.Equal(t, c.want, ev.Type)
			require.Equal(t, c.wantTxt, ev.Text)
			require.False(t, ev.At.IsZero())
		})
	}
}

func TestTranslate_UnknownEvent_BecomesError(t *testing.T) {
	ev := translate(ASREvent{Type: "made_up_event_name"})
	require.Equal(t, asr.EventError, ev.Type)
	require.NotNil(t, ev.Err)
}

func TestInt16ToLEBytes(t *testing.T) {
	in := []int16{0, 1, -1, 32767, -32768}
	out := int16ToLEBytes(in)
	require.Len(t, out, len(in)*2)

	for i, s := range in {
		got := int16(binary.LittleEndian.Uint16(out[i*2:]))
		require.Equal(t, s, got, "sample %d", i)
	}
}

// Start would hit the real Aliyun service; we'd need either real credentials
// or a fake WebSocket server. M1 leaves that to integration tests (the
// existing aliyun package is exercised by integration tests).
//
// The smoke-style "Start with bogus creds returns an error" assertion is
// fragile in CI (DNS may resolve sandboxed). Skip.
func TestStart_NotInvokedHere(t *testing.T) {
	t.Skip("Start is exercised by integration tests; unit-level " +
		"would need a fake WebSocket server")
	_ = context.Background()
}
