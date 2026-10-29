// tts.go：Aliyun NLS REST 语音合成（一句话合成）客户端。
//
// 协议参考：https://help.aliyun.com/document_detail/84435.html
//
// 设计选择：用 REST 而不是 WebSocket Streaming TTS——
//   - REST 是一次 POST、一次 Response，简单
//   - 教学场景下"等 1 秒拿到全部音频再播"完全可接受
//   - WebSocket 流式 TTS 适合做"边合成边播"的低延迟场景，
//     等后面阶段做实时对话再上
//
// 单次请求最大 300 字符；超过要分句调用。
package aliyun

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/auth/aliyun"
)

// TTSConfig 是一次合成的配置。
type TTSConfig struct {
	Text       string // 要合成的文字（≤300 字符）
	Voice      string // 发音人，如 "siyue"
	Format     string // "mp3" / "wav" / "pcm"（不要 opus，REST 不支持）
	SampleRate int    // 8000 / 16000 / 24000
	Volume     int    // 0~100
	SpeechRate int    // -500~500，0 是正常速度
}

// TTSClient 是 NLS REST TTS 客户端。线程安全。
type TTSClient struct {
	tm         *auth.TokenManager
	appKey     string
	region     string
	httpClient *http.Client
}

// NewTTSClient 创建客户端。
func NewTTSClient(tm *auth.TokenManager, appKey, region string) *TTSClient {
	if region == "" {
		region = "cn-shanghai"
	}
	return &TTSClient{
		tm:         tm,
		appKey:     appKey,
		region:     region,
		httpClient: &http.Client{Timeout: 30 * time.Second},
	}
}

// Synthesize 调一次合成，返回完整音频字节（MP3/WAV/PCM 视 cfg.Format 而定）。
//
// 接口说明：
//
//	POST https://nls-gateway-cn-shanghai.aliyuncs.com/stream/v1/tts
//	Header: X-NLS-Token: <token>
//	        Content-Type: application/json
//	Body:   {"appkey":"...","text":"...","format":"mp3",...}
//
// 成功响应：
//   - HTTP 200 + Content-Type: audio/mpeg|audio/wav|audio/pcm
//   - Body 是二进制音频
//
// 失败响应：
//   - HTTP 200 + Content-Type: application/json + 错误 JSON（坑！）
//   - 我们靠 Content-Type 判断成功还是失败
func (c *TTSClient) Synthesize(ctx context.Context, cfg TTSConfig) ([]byte, error) {
	token, err := c.tm.Get()
	if err != nil {
		return nil, fmt.Errorf("拿 Token: %w", err)
	}

	endpoint := fmt.Sprintf("https://nls-gateway-%s.aliyuncs.com/stream/v1/tts", c.region)

	bodyMap := map[string]any{
		"appkey":      c.appKey,
		"text":        cfg.Text,
		"token":       token, // 也可以放 header，放 body 也行
		"format":      cfg.Format,
		"sample_rate": cfg.SampleRate,
		"voice":       cfg.Voice,
		"volume":      cfg.Volume,
		"speech_rate": cfg.SpeechRate,
		"pitch_rate":  0,
	}
	bodyBytes, _ := json.Marshal(bodyMap)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-NLS-Token", token)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("请求 TTS: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("读响应 body: %w", err)
	}

	contentType := resp.Header.Get("Content-Type")

	// 注意：失败时 Aliyun 也返回 HTTP 200，但 Content-Type 是 application/json
	if resp.StatusCode != 200 || isJSONResponse(contentType) {
		return nil, fmt.Errorf("TTS 失败 (HTTP %d, ct=%s): %s",
			resp.StatusCode, contentType, string(respBody))
	}

	if len(respBody) == 0 {
		return nil, fmt.Errorf("TTS 返回空音频")
	}

	return respBody, nil
}

func isJSONResponse(contentType string) bool {
	return len(contentType) >= 16 && contentType[:16] == "application/json"
}
