package aliyun

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/DeepFlowAI/flowkit/internal/tts"
	"github.com/stretchr/testify/require"
)

// compile-time interface check
var _ tts.Provider = (*Provider)(nil)

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

func TestNew_AppliesDefaults(t *testing.T) {
	p, err := New(Config{
		AccessKeyID:     "AKID",
		AccessKeySecret: "AKSECRET",
		AppKey:          "appkey",
	})
	require.NoError(t, err)
	require.Equal(t, "cn-shanghai", p.cfg.Region)
	require.Equal(t, 16000, p.cfg.DefaultSampleRate)
	require.Equal(t, 50, p.cfg.DefaultVolume)
	require.Equal(t, "siyue", p.cfg.DefaultVoice)
}

func TestSynthesize_EmptyText_Errors(t *testing.T) {
	p, err := New(Config{
		AccessKeyID:     "AKID",
		AccessKeySecret: "AKSECRET",
		AppKey:          "appkey",
	})
	require.NoError(t, err)

	_, err = p.Synthesize(context.Background(), "", tts.Options{})
	require.ErrorIs(t, err, tts.ErrEmptyText)
}

func TestSynthesize_AfterClose_Errors(t *testing.T) {
	p, err := New(Config{
		AccessKeyID:     "AKID",
		AccessKeySecret: "AKSECRET",
		AppKey:          "appkey",
	})
	require.NoError(t, err)
	require.NoError(t, p.Close())

	_, err = p.Synthesize(context.Background(), "hi", tts.Options{})
	require.ErrorIs(t, err, tts.ErrClosed)
}

func TestPickStr(t *testing.T) {
	require.Equal(t, "override", pickStr("override", "fallback"))
	require.Equal(t, "fallback", pickStr("", "fallback"))
}

func TestPickInt(t *testing.T) {
	require.Equal(t, 5, pickInt(5, 99))
	require.Equal(t, 99, pickInt(0, 99))
	require.Equal(t, 99, pickInt(-3, 99))
}

func TestBytesToInt16LE_Roundtrip(t *testing.T) {
	samples := []int16{0, 1, -1, 32767, -32768, 12345}
	buf := make([]byte, len(samples)*2)
	for i, s := range samples {
		binary.LittleEndian.PutUint16(buf[i*2:], uint16(s))
	}
	got := bytesToInt16LE(buf)
	require.Equal(t, samples, got)
}

func TestBytesToInt16LE_OddLength_TruncatesGracefully(t *testing.T) {
	got := bytesToInt16LE([]byte{0x01, 0x00, 0x02}) // 3 bytes = 1 sample, leftover ignored
	require.Len(t, got, 1)
	require.Equal(t, int16(1), got[0])
}

func TestClose_Idempotent(t *testing.T) {
	p, err := New(Config{
		AccessKeyID:     "AKID",
		AccessKeySecret: "AKSECRET",
		AppKey:          "appkey",
	})
	require.NoError(t, err)
	require.NoError(t, p.Close())
	require.NoError(t, p.Close())
}
