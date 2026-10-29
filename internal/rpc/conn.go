package rpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

// Conn represents a single bidirectional JSON-RPC connection over either
// WebSocket or Unix Socket. The Conn owns one goroutine for reads and one for
// writes; it is safe to call Send* from any goroutine.
type Conn struct {
	id     string
	server *Server
	log    *slog.Logger

	// Exactly one of ws / unix is set.
	ws   *websocket.Conn
	unix net.Conn

	sendCh    chan []byte
	closeOnce sync.Once
	closeCh   chan struct{}

	helloOK atomic.Bool
}

// ID returns the unique connection identifier.
func (c *Conn) ID() string { return c.id }

// IsHelloOK reports whether the client has completed system.hello.
func (c *Conn) IsHelloOK() bool { return c.helloOK.Load() }

// MarkHelloOK flips the helloOK flag once the system.hello handler succeeds.
func (c *Conn) MarkHelloOK() { c.helloOK.Store(true) }

// CloseChan is closed when the connection terminates (for cleanup hooks).
func (c *Conn) CloseChan() <-chan struct{} { return c.closeCh }

// Close shuts the connection down idempotently.
func (c *Conn) Close() error {
	var err error
	c.closeOnce.Do(func() {
		close(c.closeCh)
		if c.ws != nil {
			err = c.ws.Close()
		}
		if c.unix != nil {
			if e := c.unix.Close(); e != nil && err == nil {
				err = e
			}
		}
	})
	return err
}

// SendRaw queues a pre-encoded frame for transmission. Returns an error if
// the connection is closed or the send buffer is full.
func (c *Conn) SendRaw(frame []byte) error {
	select {
	case <-c.closeCh:
		return errors.New("rpc: connection closed")
	case c.sendCh <- frame:
		return nil
	default:
		return errors.New("rpc: send buffer full")
	}
}

// SendResponse encodes and queues a Response.
func (c *Conn) SendResponse(resp *Response) error {
	if resp == nil {
		return nil
	}
	resp.JSONRPC = Version
	frame, err := json.Marshal(resp)
	if err != nil {
		return fmt.Errorf("rpc: marshal response: %w", err)
	}
	return c.SendRaw(frame)
}

// SendNotification encodes and queues a server-initiated Notification.
func (c *Conn) SendNotification(n *Notification) error {
	if n == nil {
		return nil
	}
	n.JSONRPC = Version
	frame, err := json.Marshal(n)
	if err != nil {
		return fmt.Errorf("rpc: marshal notification: %w", err)
	}
	return c.SendRaw(frame)
}

const (
	readBufSize  = 4096
	writeBufSize = 4096
	sendQueueSz  = 64
	pingPeriod   = 25 * time.Second
	pongWait     = 60 * time.Second
	writeTimeout = 10 * time.Second
)

// newWSConn wraps an upgraded *websocket.Conn.
func newWSConn(server *Server, ws *websocket.Conn) *Conn {
	c := &Conn{
		id:      uuid.NewString(),
		server:  server,
		log:     server.log.With("conn_id", "ws-..."),
		ws:      ws,
		sendCh:  make(chan []byte, sendQueueSz),
		closeCh: make(chan struct{}),
	}
	c.log = server.log.With("conn_id", c.id, "transport", "ws")
	return c
}

// newUnixConn wraps a Unix Socket net.Conn.
func newUnixConn(server *Server, unix net.Conn) *Conn {
	c := &Conn{
		id:      uuid.NewString(),
		server:  server,
		unix:    unix,
		sendCh:  make(chan []byte, sendQueueSz),
		closeCh: make(chan struct{}),
	}
	c.log = server.log.With("conn_id", c.id, "transport", "unix")
	return c
}

// run starts the read and write goroutines and blocks until the connection ends.
func (c *Conn) run(ctx context.Context) {
	defer func() {
		_ = c.Close()
		c.server.removeConn(c.id)
	}()

	var wg sync.WaitGroup
	wg.Add(2)

	// Hello timeout watcher
	helloTimeout := c.server.cfg.HelloTimeout
	if helloTimeout > 0 {
		go func() {
			select {
			case <-time.After(helloTimeout):
				if !c.helloOK.Load() {
					c.log.Warn("hello timeout, closing")
					_ = c.Close()
				}
			case <-c.closeCh:
			}
		}()
	}

	go func() {
		defer wg.Done()
		c.writeLoop(ctx)
	}()
	go func() {
		defer wg.Done()
		c.readLoop(ctx)
	}()
	wg.Wait()
}

func (c *Conn) readLoop(ctx context.Context) {
	if c.ws != nil {
		c.readWS(ctx)
		return
	}
	c.readUnix(ctx)
}

func (c *Conn) readWS(ctx context.Context) {
	c.ws.SetReadLimit(1 << 20) // 1 MiB max frame
	_ = c.ws.SetReadDeadline(time.Now().Add(pongWait))
	c.ws.SetPongHandler(func(string) error {
		return c.ws.SetReadDeadline(time.Now().Add(pongWait))
	})

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.closeCh:
			return
		default:
		}

		_, data, err := c.ws.ReadMessage()
		if err != nil {
			c.log.Debug("ws read ended", "err", err)
			return
		}
		c.dispatch(ctx, data)
	}
}

func (c *Conn) readUnix(ctx context.Context) {
	dec := json.NewDecoder(c.unix)
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.closeCh:
			return
		default:
		}
		var msg json.RawMessage
		if err := dec.Decode(&msg); err != nil {
			c.log.Debug("unix read ended", "err", err)
			return
		}
		c.dispatch(ctx, msg)
	}
}

// dispatch parses one inbound frame and routes to the Handler.
func (c *Conn) dispatch(ctx context.Context, raw []byte) {
	in, err := Decode(raw)
	if err != nil {
		c.log.Debug("decode error", "err", err)
		// JSON-RPC says: on parse error, send response with null id
		_ = c.SendResponse(NewErrorResponse(nil, ErrParseError(err.Error())))
		return
	}

	switch in.Kind {
	case KindRequest:
		if err := ValidateID(in.Request.ID); err != nil {
			_ = c.SendResponse(NewErrorResponse(in.Request.ID, ErrInvalidRequest(err.Error())))
			return
		}
		go func() {
			defer func() {
				if r := recover(); r != nil {
					c.log.Error("handler panic", "method", in.Request.Method, "panic", r)
					_ = c.SendResponse(NewErrorResponse(in.Request.ID, ErrInternal("handler panic")))
				}
			}()
			resp := c.server.handler.ProcessRequest(ctx, c, in.Request)
			if resp != nil {
				resp.ID = in.Request.ID
				if err := c.SendResponse(resp); err != nil {
					c.log.Warn("send response failed", "err", err)
				}
			}
		}()
	case KindNotification:
		go func() {
			defer func() {
				if r := recover(); r != nil {
					c.log.Error("notification handler panic", "method", in.Notification.Method, "panic", r)
				}
			}()
			c.server.handler.ProcessNotification(ctx, c, in.Notification)
		}()
	case KindResponse:
		// Server should rarely receive responses; ignore (a future RPC client
		// embedded in flowkit would route these via a pending-request map).
		c.log.Debug("ignoring unexpected response frame")
	}
}

func (c *Conn) writeLoop(ctx context.Context) {
	if c.ws != nil {
		c.writeWS(ctx)
		return
	}
	c.writeUnix(ctx)
}

func (c *Conn) writeWS(ctx context.Context) {
	ping := time.NewTicker(pingPeriod)
	defer ping.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.closeCh:
			return
		case frame := <-c.sendCh:
			_ = c.ws.SetWriteDeadline(time.Now().Add(writeTimeout))
			if err := c.ws.WriteMessage(websocket.TextMessage, frame); err != nil {
				c.log.Debug("ws write ended", "err", err)
				return
			}
		case <-ping.C:
			_ = c.ws.SetWriteDeadline(time.Now().Add(writeTimeout))
			if err := c.ws.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func (c *Conn) writeUnix(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.closeCh:
			return
		case frame := <-c.sendCh:
			// Newline-delimited JSON over Unix Socket — pairs with json.Decoder.
			out := append(frame, '\n')
			if _, err := c.unix.Write(out); err != nil {
				c.log.Debug("unix write ended", "err", err)
				return
			}
		}
	}
}
