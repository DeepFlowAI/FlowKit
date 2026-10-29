package rpc

import (
	"encoding/json"
	"errors"
	"fmt"
)

// Version is the JSON-RPC protocol version we accept and emit.
const Version = "2.0"

// Request is a JSON-RPC 2.0 request frame. An empty ID makes it a notification.
type Request struct {
	JSONRPC string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
	ID      json.RawMessage `json:"id,omitempty"`
}

// IsNotification reports whether the request omits its id (per JSON-RPC 2.0).
// Spec: "A Notification is a Request object without an 'id' member."
func (r *Request) IsNotification() bool {
	return len(r.ID) == 0
}

// Response is a JSON-RPC 2.0 response frame. Exactly one of Result or Error
// must be set.
type Response struct {
	JSONRPC string          `json:"jsonrpc"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *Error          `json:"error,omitempty"`
	ID      json.RawMessage `json:"id"` // present even when value is null
}

// Notification is server-initiated; it carries no id.
type Notification struct {
	JSONRPC string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

// Error is the JSON-RPC error object.
type Error struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("rpc %d: %s", e.Code, e.Message)
}

// NewError returns a new Error with the given code and message.
func NewError(code int, msg string) *Error {
	return &Error{Code: code, Message: msg}
}

// NewErrorf returns a new Error with a formatted message.
func NewErrorf(code int, format string, args ...any) *Error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...)}
}

// NewRequest builds a Request with arbitrary params and a JSON-encodable id.
// Pass id=nil to construct a notification.
func NewRequest(method string, params any, id any) (*Request, error) {
	r := &Request{JSONRPC: Version, Method: method}
	if params != nil {
		raw, err := json.Marshal(params)
		if err != nil {
			return nil, fmt.Errorf("rpc: encode params: %w", err)
		}
		r.Params = raw
	}
	if id != nil {
		raw, err := json.Marshal(id)
		if err != nil {
			return nil, fmt.Errorf("rpc: encode id: %w", err)
		}
		r.ID = raw
	}
	return r, nil
}

// NewNotification is shorthand for NewRequest(method, params, nil).
func NewNotification(method string, params any) (*Notification, error) {
	n := &Notification{JSONRPC: Version, Method: method}
	if params != nil {
		raw, err := json.Marshal(params)
		if err != nil {
			return nil, fmt.Errorf("rpc: encode params: %w", err)
		}
		n.Params = raw
	}
	return n, nil
}

// NewResponse builds a successful response. result may be nil for void calls.
func NewResponse(id json.RawMessage, result any) (*Response, error) {
	resp := &Response{JSONRPC: Version, ID: ensureID(id)}
	if result == nil {
		// Use {} instead of omitting Result so clients can distinguish "no result"
		// from "error" reliably.
		resp.Result = json.RawMessage("{}")
		return resp, nil
	}
	raw, err := json.Marshal(result)
	if err != nil {
		return nil, fmt.Errorf("rpc: encode result: %w", err)
	}
	resp.Result = raw
	return resp, nil
}

// NewErrorResponse builds a failure response.
func NewErrorResponse(id json.RawMessage, err *Error) *Response {
	return &Response{JSONRPC: Version, ID: ensureID(id), Error: err}
}

// ensureID returns id as-is, or `null` if absent (per spec: when id can't be
// determined, e.g. parse error, response uses null id).
func ensureID(id json.RawMessage) json.RawMessage {
	if len(id) == 0 {
		return json.RawMessage("null")
	}
	return id
}

// Kind enumerates the three inbound JSON-RPC frame shapes.
type Kind int

const (
	KindUnknown      Kind = iota
	KindRequest           // has method + id
	KindNotification      // has method + no id
	KindResponse          // has result or error
)

// Inbound is a parsed frame whose shape is one of {Request, Notification, Response}.
type Inbound struct {
	Kind         Kind
	Request      *Request
	Notification *Notification
	Response     *Response
}

// Decode parses a raw JSON-RPC frame and classifies it.
func Decode(raw []byte) (*Inbound, error) {
	var probe struct {
		JSONRPC string          `json:"jsonrpc"`
		Method  string          `json:"method"`
		Result  json.RawMessage `json:"result"`
		Error   *Error          `json:"error"`
		ID      json.RawMessage `json:"id"`
	}
	if err := json.Unmarshal(raw, &probe); err != nil {
		return nil, fmt.Errorf("rpc: decode: %w", err)
	}
	if probe.JSONRPC != Version {
		return nil, fmt.Errorf("rpc: unexpected jsonrpc version %q (want %q)", probe.JSONRPC, Version)
	}

	switch {
	case probe.Method != "" && len(probe.ID) > 0:
		var r Request
		if err := json.Unmarshal(raw, &r); err != nil {
			return nil, fmt.Errorf("rpc: decode request: %w", err)
		}
		return &Inbound{Kind: KindRequest, Request: &r}, nil
	case probe.Method != "":
		var n Notification
		if err := json.Unmarshal(raw, &n); err != nil {
			return nil, fmt.Errorf("rpc: decode notification: %w", err)
		}
		return &Inbound{Kind: KindNotification, Notification: &n}, nil
	case len(probe.Result) > 0 || probe.Error != nil:
		var r Response
		if err := json.Unmarshal(raw, &r); err != nil {
			return nil, fmt.Errorf("rpc: decode response: %w", err)
		}
		return &Inbound{Kind: KindResponse, Response: &r}, nil
	}

	return nil, errors.New("rpc: frame has neither method nor result/error")
}
