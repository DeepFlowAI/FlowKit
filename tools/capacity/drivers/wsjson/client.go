// Package wsjson is a minimal JSON-RPC 2.0 client over WebSocket for capacity drivers.
package wsjson

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

const writeTimeout = 5 * time.Second

// Client speaks JSON-RPC over a single WebSocket connection.
type Client struct {
	conn    *websocket.Conn
	writeMu sync.Mutex

	nextID  atomic.Int64
	pending sync.Map // id string -> chan *Response

	notifMu sync.RWMutex
	notifs  map[string]func(params json.RawMessage)

	readDone chan struct{}
	failOnce sync.Once
}

type Response struct {
	Result json.RawMessage
	Error  *RPCError
}

type RPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e *RPCError) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("rpc %d: %s", e.Code, e.Message)
}

// Connect dials wsURL (e.g. ws://127.0.0.1:9600/ws) and starts the read loop.
// FLOWKIT_API_KEY, when set, is sent as a Bearer token.
func Connect(ctx context.Context, wsURL string) (*Client, error) {
	dialer := websocket.Dialer{}
	var header http.Header
	if key := os.Getenv("FLOWKIT_API_KEY"); key != "" {
		header = http.Header{"Authorization": {"Bearer " + key}}
	}
	conn, _, err := dialer.DialContext(ctx, wsURL, header)
	if err != nil {
		return nil, fmt.Errorf("ws dial: %w", err)
	}
	c := &Client{
		conn:     conn,
		notifs:   make(map[string]func(json.RawMessage)),
		readDone: make(chan struct{}),
	}
	go c.readLoop()
	return c, nil
}

func (c *Client) failPending(err error) {
	c.failOnce.Do(func() {
		c.pending.Range(func(key, value any) bool {
			ch := value.(chan *Response)
			select {
			case ch <- &Response{Error: &RPCError{Code: -1, Message: err.Error()}}:
			default:
			}
			c.pending.Delete(key)
			return true
		})
	})
}

func (c *Client) readLoop() {
	defer close(c.readDone)
	defer c.failPending(fmt.Errorf("ws closed"))
	for {
		_, data, err := c.conn.ReadMessage()
		if err != nil {
			return
		}
		var probe struct {
			Method string          `json:"method"`
			ID     json.RawMessage `json:"id"`
			Result json.RawMessage `json:"result"`
			Error  *RPCError       `json:"error"`
		}
		if err := json.Unmarshal(data, &probe); err != nil {
			continue
		}
		if probe.Method != "" && len(probe.ID) == 0 {
			c.notifMu.RLock()
			fn := c.notifs[probe.Method]
			c.notifMu.RUnlock()
			if fn != nil {
				var n struct {
					Params json.RawMessage `json:"params"`
				}
				_ = json.Unmarshal(data, &n)
				fn(n.Params)
			}
			continue
		}
		if len(probe.ID) == 0 {
			continue
		}
		var idStr string
		if err := json.Unmarshal(probe.ID, &idStr); err != nil || idStr == "" {
			continue
		}
		if ch, ok := c.pending.Load(idStr); ok {
			ch.(chan *Response) <- &Response{Result: probe.Result, Error: probe.Error}
		}
	}
}

// OnNotification registers a handler for server-push notifications.
func (c *Client) OnNotification(method string, fn func(params json.RawMessage)) {
	c.notifMu.Lock()
	c.notifs[method] = fn
	c.notifMu.Unlock()
}

func (c *Client) writeJSON(v any) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	_ = c.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	return c.conn.WriteJSON(v)
}

// Call sends a JSON-RPC request and waits for the matching response.
func (c *Client) Call(ctx context.Context, method string, params any) (json.RawMessage, error) {
	id := fmt.Sprintf("cap-%d", c.nextID.Add(1))
	ch := make(chan *Response, 1)
	c.pending.Store(id, ch)
	defer c.pending.Delete(id)

	frame := map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"method":  method,
		"params":  params,
	}
	if params == nil {
		frame["params"] = map[string]any{}
	}
	if err := c.writeJSON(frame); err != nil {
		return nil, fmt.Errorf("ws write: %w", err)
	}
	select {
	case resp := <-ch:
		if resp.Error != nil {
			return nil, resp.Error
		}
		return resp.Result, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.readDone:
		return nil, fmt.Errorf("ws closed")
	}
}

// Notify sends a JSON-RPC notification (no response).
func (c *Client) Notify(method string, params any) error {
	frame := map[string]any{
		"jsonrpc": "2.0",
		"method":  method,
		"params":  params,
	}
	if params == nil {
		frame["params"] = map[string]any{}
	}
	return c.writeJSON(frame)
}

// Close shuts the WebSocket down and fails any in-flight RPCs.
func (c *Client) Close() error {
	err := c.conn.Close()
	c.failPending(fmt.Errorf("ws closed"))
	<-c.readDone
	return err
}
