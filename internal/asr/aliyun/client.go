// asr.go：Aliyun NLS 实时语音识别（Speech Transcriber）客户端。
//
// 协议参考：https://help.aliyun.com/document_detail/84428.html
//
// 总体流程：
//
//	1. 客户端 → wss://nls-gateway-cn-shanghai.aliyuncs.com/ws/v1?token=...
//	2. 客户端 → JSON: StartTranscription（指定 format/sample_rate 等）
//	3. 服务端 → JSON: TranscriptionStarted（确认）
//	4. 客户端 → BINARY: PCM 块 1（推荐 100ms 一片）
//	   客户端 → BINARY: PCM 块 2
//	   ...
//	5. 服务端 → JSON: TranscriptionResultChanged（中间结果，逐字增长）
//	   服务端 → JSON: SentenceBegin / SentenceEnd（句子分界）
//	   服务端 → JSON: TranscriptionCompleted（全部识别完）
//	6. 客户端 → JSON: StopTranscription
//	7. 关 WebSocket
//
// 与 TTS 不同：ASR 是"半双工流式"——客户端不停推音频，服务端不停推识别结果，两边同时进行。
package aliyun

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/auth/aliyun"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

// ASRConfig 是一次识别会话的配置。
type ASRConfig struct {
	Format                  string // "pcm" / "opus"，我们用 "pcm"
	SampleRate              int    // 8000 / 16000
	EnableIntermediateResult bool  // 中间结果（边说边出，逐字增长）
	EnablePunctuation        bool  // 自动标点
	EnableITN                bool  // ITN（Inverse Text Normalization）：把"二零二六"转成"2026"
	// MaxSentenceSilenceMs，当 > 0 时设置 NLS 尾点静音阈值（200–2000ms，默认 800ms）：
	// 说话停顿超过此时长即判一句结束。调小=判停更快、打断更灵敏，但更易把中途停顿误判为说完。
	MaxSentenceSilenceMs int
}

// DefaultASRConfig 返回一个适合中文语音对话场景的默认配置。
func DefaultASRConfig() ASRConfig {
	return ASRConfig{
		Format:                   "pcm",
		SampleRate:               16000,
		EnableIntermediateResult: true,
		EnablePunctuation:        true,
		EnableITN:                true,
	}
}

// ASREvent 是从 NLS 推回来的识别结果。
type ASREvent struct {
	Type       string // "started" / "result_changed" / "sentence_begin" / "sentence_end" / "completed"
	Text       string // 识别出的文字（result_changed 是中间结果；sentence_end 是该句最终结果）
	SentenceID int    // 句子序号（同一次会话内自增）
	BeginTime  int    // 该句起点（毫秒）
	EndTime    int    // 该句终点（毫秒，sentence_end 时才有）
}

// ASRClient 是 NLS ASR WebSocket 客户端。一个客户端 = 一次识别会话。
//
// 用法：
//
//	c, err := aliyun.NewASRClient(ctx, tm, appKey, region, aliyun.DefaultASRConfig())
//	if err != nil { panic(err) }
//	defer c.Close()
//
//	// 一边推音频
//	go func() {
//	    for pcm := range pcmCh { c.SendPCM(pcm) }
//	    c.StopAndWait()  // 把缓冲推完，再等服务端 Completed
//	}()
//
//	// 一边收识别结果
//	for ev := range c.Events() {
//	    fmt.Println(ev.Type, ev.Text)
//	}
// asrStartTimeout is how long NewASRClient waits for TranscriptionStarted
// before failing the session setup.
const asrStartTimeout = 10 * time.Second

type ASRClient struct {
	conn   *websocket.Conn
	appKey string
	taskID string
	events chan ASREvent

	readyOnce sync.Once
	ready     chan struct{}
	readyErr  error

	closeOnce sync.Once
	errMu     sync.Mutex
	closeErr  error

	stopping atomic.Bool
}

func (c *ASRClient) setCloseErr(err error) {
	if err == nil {
		return
	}
	c.errMu.Lock()
	if c.closeErr == nil {
		c.closeErr = err
	}
	c.errMu.Unlock()
}

func (c *ASRClient) getCloseErr() error {
	c.errMu.Lock()
	defer c.errMu.Unlock()
	return c.closeErr
}

func (c *ASRClient) signalReady(err error) {
	c.readyOnce.Do(func() {
		c.readyErr = err
		close(c.ready)
	})
}

// NewASRClient 创建并启动一次 ASR 会话——返回时已收到 TranscriptionStarted，
// 可以立即往 SendPCM 推音频。
func NewASRClient(ctx context.Context, tm *auth.TokenManager, appKey, region string, cfg ASRConfig) (*ASRClient, error) {
	token, err := tm.Get()
	if err != nil {
		return nil, fmt.Errorf("拿 Token: %w", err)
	}
	if region == "" {
		region = "cn-shanghai"
	}

	q := url.Values{}
	q.Set("token", token)
	wsURL := fmt.Sprintf("wss://nls-gateway-%s.aliyuncs.com/ws/v1?%s", region, q.Encode())

	dialer := websocket.Dialer{HandshakeTimeout: 10 * time.Second}
	conn, resp, err := dialer.DialContext(ctx, wsURL, http.Header{})
	if err != nil {
		if resp != nil {
			return nil, fmt.Errorf("WebSocket 握手 HTTP %d: %w", resp.StatusCode, err)
		}
		return nil, fmt.Errorf("WebSocket 握手: %w", err)
	}

	return startASRClient(ctx, conn, appKey, cfg)
}

// startASRClient wires up an ASR session on an established WebSocket. The
// caller owns closing conn when this returns an error.
func startASRClient(ctx context.Context, conn *websocket.Conn, appKey string, cfg ASRConfig) (*ASRClient, error) {
	c := &ASRClient{
		conn:   conn,
		appKey: appKey,
		taskID: strings.ReplaceAll(uuid.NewString(), "-", ""),
		events: make(chan ASREvent, 32),
		ready:  make(chan struct{}),
	}

	// Start read loop before StartTranscription so we cannot miss a fast reply.
	go c.readLoop()

	payload := map[string]any{
		"format":                            cfg.Format,
		"sample_rate":                       cfg.SampleRate,
		"enable_intermediate_result":        cfg.EnableIntermediateResult,
		"enable_punctuation_prediction":     cfg.EnablePunctuation,
		"enable_inverse_text_normalization": cfg.EnableITN,
	}
	if cfg.MaxSentenceSilenceMs > 0 {
		payload["max_sentence_silence"] = cfg.MaxSentenceSilenceMs
	}
	startMsg := map[string]any{
		"header": map[string]any{
			"appkey":     appKey,
			"namespace":  "SpeechTranscriber",
			"name":       "StartTranscription",
			"task_id":    c.taskID,
			"message_id": strings.ReplaceAll(uuid.NewString(), "-", ""),
		},
		"payload": payload,
	}
	if err := conn.WriteJSON(startMsg); err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("发送 StartTranscription: %w", err)
	}

	waitCtx := ctx
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		waitCtx, cancel = context.WithTimeout(ctx, asrStartTimeout)
		defer cancel()
	}

	select {
	case <-c.ready:
		if c.readyErr != nil {
			_ = c.Close()
			return nil, c.readyErr
		}
		return c, nil
	case <-waitCtx.Done():
		_ = c.Close()
		return nil, fmt.Errorf("等待 TranscriptionStarted: %w", waitCtx.Err())
	}
}

// SendPCM 推一片 PCM 音频。线程不安全，请只在一个 goroutine 里调用。
func (c *ASRClient) SendPCM(pcm []byte) error {
	<-c.ready
	if c.readyErr != nil {
		return c.readyErr
	}
	return c.conn.WriteMessage(websocket.BinaryMessage, pcm)
}

// StopAndWait 发 StopTranscription，然后阻塞等服务端 TranscriptionCompleted。
// 调用后 SendPCM 不应该再被调用。
func (c *ASRClient) StopAndWait(ctx context.Context) error {
	c.stopping.Store(true)
	stopMsg := map[string]any{
		"header": map[string]any{
			"appkey":     c.appKey,
			"namespace":  "SpeechTranscriber",
			"name":       "StopTranscription",
			"task_id":    c.taskID,
			"message_id": strings.ReplaceAll(uuid.NewString(), "-", ""),
		},
	}
	if err := c.conn.WriteJSON(stopMsg); err != nil {
		return fmt.Errorf("发送 StopTranscription: %w", err)
	}
	// 等 events channel 被 readLoop 关闭（意味着收到了 Completed 或断连）
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case _, ok := <-c.events:
			if !ok {
				return c.getCloseErr()
			}
			// 没关掉就继续读（消费掉残余事件）
		}
	}
}

// Events 返回识别结果事件流。会话结束（成功或失败）后这个 channel 会被 close。
func (c *ASRClient) Events() <-chan ASREvent {
	return c.events
}

// Close 强制关闭 WebSocket。SendPCM 之后必调用。
func (c *ASRClient) Close() error {
	c.closeOnce.Do(func() {
		c.stopping.Store(true)
		c.setCloseErr(c.conn.Close())
	})
	return c.getCloseErr()
}

// readLoop 持续从 WebSocket 读消息，把识别结果转成 ASREvent 推到 events channel。
// 收到 TranscriptionCompleted 或网络错误时关闭 channel 并退出。
func (c *ASRClient) readLoop() {
	var loopErr error
	defer func() {
		c.setCloseErr(loopErr)
		c.signalReady(loopErr)
		close(c.events)
	}()

	for {
		_, data, err := c.conn.ReadMessage()
		if err != nil {
			if !websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				loopErr = err
				slog.Default().Warn("aliyun asr: websocket read error",
					"task_id", c.taskID, "err", err)
			}
			return
		}

		var msg struct {
			Header struct {
				Name      string `json:"name"`
				Status    int    `json:"status"`
				StatusMsg string `json:"status_message"`
			} `json:"header"`
			Payload struct {
				Result    string `json:"result"`
				Index     int    `json:"index"`
				BeginTime int    `json:"begin_time"`
				Time      int    `json:"time"`
			} `json:"payload"`
		}
		if err := json.Unmarshal(data, &msg); err != nil {
			continue // 忽略畸形消息
		}

		switch msg.Header.Name {
		case "TranscriptionStarted":
			c.signalReady(nil)
			c.events <- ASREvent{Type: "started"}
		case "SentenceBegin":
			c.events <- ASREvent{Type: "sentence_begin",
				SentenceID: msg.Payload.Index, BeginTime: msg.Payload.BeginTime}
		case "TranscriptionResultChanged":
			c.events <- ASREvent{Type: "result_changed",
				SentenceID: msg.Payload.Index,
				Text:       msg.Payload.Result,
				BeginTime:  msg.Payload.BeginTime,
				EndTime:    msg.Payload.Time}
		case "SentenceEnd":
			c.events <- ASREvent{Type: "sentence_end",
				SentenceID: msg.Payload.Index,
				Text:       msg.Payload.Result,
				BeginTime:  msg.Payload.BeginTime,
				EndTime:    msg.Payload.Time}
		case "TranscriptionCompleted":
			c.events <- ASREvent{Type: "completed"}
			return
		case "TaskFailed":
			if isNLSIdleTimeout(msg.Header.Status, msg.Header.StatusMsg, string(data)) {
				c.logTaskFailed("debug", msg.Header.Status, msg.Header.StatusMsg, data)
				return
			}
			loopErr = fmt.Errorf("ASR 任务失败 (status %d): %s",
				msg.Header.Status, msg.Header.StatusMsg)
			c.events <- ASREvent{Type: "task_failed", Text: msg.Header.StatusMsg}
			c.logTaskFailed("warn", msg.Header.Status, msg.Header.StatusMsg, data)
			return
		}
	}
}

func isNLSIdleTimeout(status int, statusMsg, raw string) bool {
	if status != 40000004 {
		return false
	}
	combined := statusMsg + raw
	return strings.Contains(combined, "IDLE_TIMEOUT")
}

func (c *ASRClient) logTaskFailed(level string, status int, statusMsg string, raw []byte) {
	args := []any{
		"task_id", c.taskID,
		"status", status,
		"status_message", statusMsg,
		"raw", string(raw),
	}
	switch level {
	case "debug":
		slog.Default().Debug("aliyun asr: session ended", args...)
	default:
		slog.Default().Warn("aliyun asr: TaskFailed from NLS", args...)
	}
}
