package rpc

// JSON-RPC standard error codes (-32700 to -32603) and Flowkit-specific
// extensions (-32001 to -32099).
const (
	// Standard
	CodeParseError     = -32700
	CodeInvalidRequest = -32600
	CodeMethodNotFound = -32601
	CodeInvalidParams  = -32602
	CodeInternalError  = -32603

	// Flowkit extensions
	CodeNotReady          = -32001
	CodeCallNotFound      = -32002
	CodeCallStateInvalid  = -32003
	CodeASRUnavailable    = -32004
	CodeTTSUnavailable    = -32005
	CodeSIPDialogError    = -32006
	CodeWebRTCICEFailed   = -32007
	CodeRateLimited       = -32008
	CodeShuttingDown      = -32009
	CodeLLMUnavailable    = -32010
)

// codeNames is used for human-readable error rendering in logs.
var codeNames = map[int]string{
	CodeParseError:        "parse_error",
	CodeInvalidRequest:    "invalid_request",
	CodeMethodNotFound:    "method_not_found",
	CodeInvalidParams:     "invalid_params",
	CodeInternalError:     "internal_error",
	CodeNotReady:          "not_ready",
	CodeCallNotFound:      "call_not_found",
	CodeCallStateInvalid:  "call_state_invalid",
	CodeASRUnavailable:    "asr_unavailable",
	CodeTTSUnavailable:    "tts_unavailable",
	CodeSIPDialogError:    "sip_dialog_error",
	CodeWebRTCICEFailed:   "webrtc_ice_failed",
	CodeRateLimited:       "rate_limited",
	CodeShuttingDown:      "shutting_down",
	CodeLLMUnavailable:    "llm_unavailable",
}

// CodeName returns the short symbolic name for a code; "unknown" if unrecognised.
func CodeName(code int) string {
	if name, ok := codeNames[code]; ok {
		return name
	}
	return "unknown"
}

// Convenience constructors for the most common errors.

func ErrParseError(detail string) *Error    { return NewErrorf(CodeParseError, "parse error: %s", detail) }
func ErrInvalidRequest(d string) *Error     { return NewErrorf(CodeInvalidRequest, "invalid request: %s", d) }
func ErrMethodNotFound(method string) *Error {
	return NewErrorf(CodeMethodNotFound, "method not found: %s", method)
}
func ErrInvalidParams(detail string) *Error { return NewErrorf(CodeInvalidParams, "invalid params: %s", detail) }
func ErrInternal(detail string) *Error      { return NewErrorf(CodeInternalError, "internal: %s", detail) }
func ErrNotReady() *Error                   { return NewError(CodeNotReady, "client has not completed system.hello") }
func ErrCallNotFound(id string) *Error      { return NewErrorf(CodeCallNotFound, "call not found: %s", id) }
func ErrCallStateInvalid(d string) *Error   { return NewErrorf(CodeCallStateInvalid, "call state invalid: %s", d) }
func ErrShuttingDown() *Error               { return NewError(CodeShuttingDown, "server is shutting down") }
