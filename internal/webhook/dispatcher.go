// Package webhook 把内核事件可靠地推送到客户配置的 HTTP 端点。
//
// 设计要点：
//
//  1. 异步：业务线程调 Dispatch() 不阻塞，事件入队 → 后台 worker 推
//  2. 签名：每个 POST 带 X-Webhook-Signature: HMAC-SHA256(secret, body)
//     接收方可以验证"这个请求确实来自我配置的 flowkit"
//  3. 重试：失败时 3 次指数退避（1s/5s/30s）
//  4. 队列上限：满了直接丢——比阻塞业务好
package webhook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"
)

// Event 一条要推送的事件。
type Event struct {
	Event     string         `json:"event"`      // "call.user_said" 等
	SessionID string         `json:"session_id"`
	Timestamp time.Time      `json:"timestamp"`
	Payload   map[string]any `json:"payload,omitempty"`
}

// Dispatcher 是 Webhook 的发送器。
//
// 没配 URL 时所有 Dispatch 都是 no-op——这样上层代码不用关心"webhook 开了没"。
type Dispatcher struct {
	url    string
	secret string
	queue  chan Event
	client *http.Client
}

// New 创建 Dispatcher。url == "" 时返回的 dispatcher 是 no-op。
// 调用 Run() 启动 worker。
func New(url, secret string, queueSize int) *Dispatcher {
	if queueSize <= 0 {
		queueSize = 256
	}
	d := &Dispatcher{
		url:    url,
		secret: secret,
		queue:  make(chan Event, queueSize),
		client: &http.Client{Timeout: 10 * time.Second},
	}
	return d
}

// Enabled 报告 dispatcher 是否真的会发送（即配置了 url）。
func (d *Dispatcher) Enabled() bool { return d.url != "" }

// Dispatch 入队一条事件。非阻塞——队列满了就丢弃 + 打日志。
func (d *Dispatcher) Dispatch(ev Event) {
	if !d.Enabled() {
		return
	}
	if ev.Timestamp.IsZero() {
		ev.Timestamp = time.Now().UTC()
	}
	select {
	case d.queue <- ev:
	default:
		log.Printf("⚠️  webhook 队列满，丢弃 event=%s session=%s", ev.Event, ev.SessionID)
	}
}

// Run 启动 worker——阻塞运行，传 ctx 控制退出。
// 通常在 main 里 `go dispatcher.Run(ctx)` 一次。
func (d *Dispatcher) Run(ctx context.Context) {
	if !d.Enabled() {
		return
	}
	log.Printf("📨 Webhook dispatcher 启动 → %s", d.url)
	for {
		select {
		case <-ctx.Done():
			log.Println("📭 Webhook dispatcher 退出")
			return
		case ev := <-d.queue:
			d.deliverWithRetry(ctx, ev)
		}
	}
}

// deliverWithRetry 发一条事件，失败按 1s/5s/30s 重试，3 次都失败就放弃。
func (d *Dispatcher) deliverWithRetry(ctx context.Context, ev Event) {
	body, _ := json.Marshal(ev)
	backoffs := []time.Duration{0, 1 * time.Second, 5 * time.Second, 30 * time.Second}

	for i, wait := range backoffs {
		if wait > 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(wait):
			}
		}
		err := d.deliverOnce(ctx, body, ev.Event)
		if err == nil {
			if i > 0 {
				log.Printf("✅ webhook 重试 #%d 成功: %s", i, ev.Event)
			}
			return
		}
		log.Printf("⚠️  webhook 投递失败（第 %d 次）: %v", i+1, err)
	}
	log.Printf("❌ webhook 放弃: event=%s session=%s（3 次都失败）", ev.Event, ev.SessionID)
}

func (d *Dispatcher) deliverOnce(ctx context.Context, body []byte, eventName string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "flowkit-webhook/1.0")
	req.Header.Set("X-Webhook-Event", eventName)
	if d.secret != "" {
		req.Header.Set("X-Webhook-Signature", sign(body, d.secret))
	}

	resp, err := d.client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(respBody))
	}
	return nil
}

// sign 计算 HMAC-SHA256 签名，base64 编码。
//
// 接收方验证（Python 示例）：
//
//	import hmac, hashlib, base64
//	expected = base64.b64encode(hmac.new(secret.encode(), body, hashlib.sha256).digest()).decode()
//	if hmac.compare_digest(expected, request.headers["X-Webhook-Signature"]):
//	    ... 是合法请求 ...
func sign(body []byte, secret string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}
