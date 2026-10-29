package rpc

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
)

// MethodFunc is the signature business code provides for each JSON-RPC method.
// Returning a non-nil *Error yields an error response; returning result=nil
// yields a successful response with `{}` body.
type MethodFunc func(ctx context.Context, conn *Conn, params json.RawMessage) (result any, err *Error)

// NotificationFunc handles a server-receiving notification. No response is
// produced. Panics are recovered by the Conn layer.
type NotificationFunc func(ctx context.Context, conn *Conn, params json.RawMessage)

// Registry routes inbound requests/notifications to registered methods.
// It satisfies the Handler interface.
type Registry struct {
	log *slog.Logger

	mu            sync.RWMutex
	methods       map[string]MethodFunc
	notifications map[string]NotificationFunc

	// preHelloAllowed methods may be invoked before system.hello succeeds.
	preHelloAllowed map[string]bool
}

// NewRegistry constructs a Registry whose default pre-hello allow list is
// just {"system.hello"}.
func NewRegistry(log *slog.Logger) *Registry {
	if log == nil {
		log = slog.Default()
	}
	return &Registry{
		log:           log,
		methods:       make(map[string]MethodFunc),
		notifications: make(map[string]NotificationFunc),
		preHelloAllowed: map[string]bool{
			"system.hello": true,
		},
	}
}

// Register registers a method. Overwriting an existing method is allowed but
// logged as a warning.
func (r *Registry) Register(method string, fn MethodFunc) {
	if method == "" || fn == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, dup := r.methods[method]; dup {
		r.log.Warn("rpc: overwriting method", "method", method)
	}
	r.methods[method] = fn
}

// RegisterNotification registers a notification handler.
func (r *Registry) RegisterNotification(method string, fn NotificationFunc) {
	if method == "" || fn == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, dup := r.notifications[method]; dup {
		r.log.Warn("rpc: overwriting notification handler", "method", method)
	}
	r.notifications[method] = fn
}

// AllowPreHello marks a method as callable before system.hello has succeeded.
func (r *Registry) AllowPreHello(method string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.preHelloAllowed[method] = true
}

// HasMethod reports whether the given method is registered (test helper).
func (r *Registry) HasMethod(method string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.methods[method]
	return ok
}

// ProcessRequest implements Handler. It looks up the method, applies the
// hello gate, calls the handler, and returns a response.
func (r *Registry) ProcessRequest(ctx context.Context, conn *Conn, req *Request) *Response {
	r.mu.RLock()
	fn, ok := r.methods[req.Method]
	allowedPreHello := r.preHelloAllowed[req.Method]
	r.mu.RUnlock()

	if !ok {
		return NewErrorResponse(req.ID, ErrMethodNotFound(req.Method))
	}
	if !allowedPreHello && !conn.IsHelloOK() {
		return NewErrorResponse(req.ID, ErrNotReady())
	}

	result, rpcErr := fn(ctx, conn, req.Params)
	if rpcErr != nil {
		return NewErrorResponse(req.ID, rpcErr)
	}
	resp, err := NewResponse(req.ID, result)
	if err != nil {
		return NewErrorResponse(req.ID, ErrInternal(fmt.Sprintf("encode result: %v", err)))
	}
	return resp
}

// ProcessNotification implements Handler.
func (r *Registry) ProcessNotification(ctx context.Context, conn *Conn, n *Notification) {
	r.mu.RLock()
	fn, ok := r.notifications[n.Method]
	r.mu.RUnlock()
	if !ok {
		r.log.Debug("rpc: unknown notification", "method", n.Method)
		return
	}
	fn(ctx, conn, n.Params)
}

// DecodeParams unmarshals params into the provided struct, returning a
// well-formed ErrInvalidParams on failure. A zero-length params field with a
// non-nil destination is treated as "{}".
func DecodeParams(raw json.RawMessage, into any) *Error {
	if into == nil {
		return nil
	}
	if len(raw) == 0 {
		raw = json.RawMessage("{}")
	}
	if err := json.Unmarshal(raw, into); err != nil {
		return ErrInvalidParams(err.Error())
	}
	return nil
}
