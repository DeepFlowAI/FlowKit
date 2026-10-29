package rpc

import "context"

// Handler dispatches inbound frames to business logic. The server passes each
// parsed Request / Notification to the registered Handler. Real method
// registration lives in task ⑤ (`registry.go`).
type Handler interface {
	// ProcessRequest handles a JSON-RPC request and returns the response.
	// Returning nil signals that the handler will send the response asynchronously
	// via Conn (or that the request was dropped, which the server logs).
	ProcessRequest(ctx context.Context, conn *Conn, req *Request) *Response

	// ProcessNotification handles a notification. No response is expected.
	ProcessNotification(ctx context.Context, conn *Conn, n *Notification)
}

// HandlerFunc wraps two callbacks into a Handler. Useful for tests.
type HandlerFunc struct {
	OnRequest      func(context.Context, *Conn, *Request) *Response
	OnNotification func(context.Context, *Conn, *Notification)
}

func (h HandlerFunc) ProcessRequest(ctx context.Context, c *Conn, r *Request) *Response {
	if h.OnRequest == nil {
		return NewErrorResponse(r.ID, ErrMethodNotFound(r.Method))
	}
	return h.OnRequest(ctx, c, r)
}

func (h HandlerFunc) ProcessNotification(ctx context.Context, c *Conn, n *Notification) {
	if h.OnNotification != nil {
		h.OnNotification(ctx, c, n)
	}
}
