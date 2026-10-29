package rpc

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// --- Request ---

func TestNewRequest_HappyPath(t *testing.T) {
	r, err := NewRequest("call.say", map[string]string{"text": "hi"}, "req-1")
	require.NoError(t, err)
	require.Equal(t, "2.0", r.JSONRPC)
	require.Equal(t, "call.say", r.Method)
	require.JSONEq(t, `{"text":"hi"}`, string(r.Params))
	require.Equal(t, `"req-1"`, string(r.ID))
	require.False(t, r.IsNotification())
}

func TestNewRequest_AsNotification(t *testing.T) {
	r, err := NewRequest("system.health", nil, nil)
	require.NoError(t, err)
	require.True(t, r.IsNotification())
	require.Equal(t, 0, len(r.ID))
}

func TestNewRequest_IntegerID(t *testing.T) {
	r, err := NewRequest("call.answer", nil, 42)
	require.NoError(t, err)
	require.Equal(t, "42", string(r.ID))
}

// --- Notification ---

func TestNewNotification(t *testing.T) {
	n, err := NewNotification("call.ringing", map[string]string{"call_id": "abc"})
	require.NoError(t, err)
	require.Equal(t, "call.ringing", n.Method)
	require.JSONEq(t, `{"call_id":"abc"}`, string(n.Params))
}

// --- Response ---

func TestNewResponse_WithResult(t *testing.T) {
	id := json.RawMessage(`"req-7"`)
	r, err := NewResponse(id, map[string]string{"status": "ok"})
	require.NoError(t, err)
	require.Equal(t, "2.0", r.JSONRPC)
	require.JSONEq(t, `{"status":"ok"}`, string(r.Result))
	require.Equal(t, `"req-7"`, string(r.ID))
	require.Nil(t, r.Error)
}

func TestNewResponse_NilResult_EmitsEmptyObject(t *testing.T) {
	r, err := NewResponse(json.RawMessage(`"x"`), nil)
	require.NoError(t, err)
	require.JSONEq(t, `{}`, string(r.Result))
}

func TestNewErrorResponse(t *testing.T) {
	r := NewErrorResponse(json.RawMessage(`"req-8"`), NewError(CodeInvalidParams, "missing text"))
	require.NotNil(t, r.Error)
	require.Equal(t, CodeInvalidParams, r.Error.Code)
	require.Contains(t, r.Error.Message, "missing text")
}

func TestNewErrorResponse_NullIDWhenAbsent(t *testing.T) {
	r := NewErrorResponse(nil, ErrParseError("bad json"))
	require.Equal(t, "null", string(r.ID))
}

// --- Error type ---

func TestError_ErrorString(t *testing.T) {
	require.Equal(t, "rpc -32602: bad params", NewError(CodeInvalidParams, "bad params").Error())
	require.Equal(t, "", (*Error)(nil).Error())
}

func TestCodeName(t *testing.T) {
	require.Equal(t, "method_not_found", CodeName(CodeMethodNotFound))
	require.Equal(t, "call_not_found", CodeName(CodeCallNotFound))
	require.Equal(t, "unknown", CodeName(-1))
}

// --- Decode ---

func TestDecode_Request(t *testing.T) {
	raw := []byte(`{"jsonrpc":"2.0","method":"call.say","params":{"text":"hi"},"id":"r1"}`)
	in, err := Decode(raw)
	require.NoError(t, err)
	require.Equal(t, KindRequest, in.Kind)
	require.NotNil(t, in.Request)
	require.Equal(t, "call.say", in.Request.Method)
	require.False(t, in.Request.IsNotification())
}

func TestDecode_Notification(t *testing.T) {
	raw := []byte(`{"jsonrpc":"2.0","method":"call.ringing","params":{"call_id":"abc"}}`)
	in, err := Decode(raw)
	require.NoError(t, err)
	require.Equal(t, KindNotification, in.Kind)
	require.Equal(t, "call.ringing", in.Notification.Method)
}

func TestDecode_Response_Result(t *testing.T) {
	raw := []byte(`{"jsonrpc":"2.0","result":{"ok":true},"id":"r1"}`)
	in, err := Decode(raw)
	require.NoError(t, err)
	require.Equal(t, KindResponse, in.Kind)
	require.NotNil(t, in.Response)
	require.JSONEq(t, `{"ok":true}`, string(in.Response.Result))
}

func TestDecode_Response_Error(t *testing.T) {
	raw := []byte(`{"jsonrpc":"2.0","error":{"code":-32601,"message":"no such method"},"id":null}`)
	in, err := Decode(raw)
	require.NoError(t, err)
	require.Equal(t, KindResponse, in.Kind)
	require.NotNil(t, in.Response.Error)
	require.Equal(t, CodeMethodNotFound, in.Response.Error.Code)
}

func TestDecode_BadJSON(t *testing.T) {
	_, err := Decode([]byte(`not json`))
	require.Error(t, err)
}

func TestDecode_WrongVersion(t *testing.T) {
	raw := []byte(`{"jsonrpc":"1.0","method":"x","id":"1"}`)
	_, err := Decode(raw)
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "version")
}

func TestDecode_EmptyFrame(t *testing.T) {
	raw := []byte(`{"jsonrpc":"2.0"}`)
	_, err := Decode(raw)
	require.Error(t, err, "frame with neither method nor result must fail")
}

// --- Roundtrip ---

func TestRoundtrip_Request(t *testing.T) {
	original, err := NewRequest("call.originate", map[string]string{"uri": "sip:1001@host"}, "req-9")
	require.NoError(t, err)

	wire, err := json.Marshal(original)
	require.NoError(t, err)

	in, err := Decode(wire)
	require.NoError(t, err)
	require.Equal(t, KindRequest, in.Kind)
	require.Equal(t, original.Method, in.Request.Method)
	require.JSONEq(t, string(original.Params), string(in.Request.Params))
	require.Equal(t, string(original.ID), string(in.Request.ID))
}

func TestRoundtrip_Notification(t *testing.T) {
	original, err := NewNotification("call.speech_partial",
		map[string]any{"call_id": "abc", "text": "你"})
	require.NoError(t, err)
	wire, err := json.Marshal(original)
	require.NoError(t, err)
	in, err := Decode(wire)
	require.NoError(t, err)
	require.Equal(t, KindNotification, in.Kind)
}

func TestRoundtrip_ErrorResponse(t *testing.T) {
	original := NewErrorResponse(json.RawMessage(`"x"`), ErrCallNotFound("abc"))
	wire, err := json.Marshal(original)
	require.NoError(t, err)
	in, err := Decode(wire)
	require.NoError(t, err)
	require.Equal(t, KindResponse, in.Kind)
	require.NotNil(t, in.Response.Error)
	require.Equal(t, CodeCallNotFound, in.Response.Error.Code)
}

// --- ID validation ---

func TestValidateID(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		ok   bool
	}{
		{"absent", "", true},
		{"string", `"req-1"`, true},
		{"int", `42`, true},
		{"float", `1.5`, true},
		{"null", `null`, true},
		{"array", `[1,2]`, false},
		{"object", `{"x":1}`, false},
		{"bool", `true`, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := ValidateID(json.RawMessage(c.raw))
			if c.ok {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestIDString(t *testing.T) {
	require.Equal(t, "req-1", IDString(json.RawMessage(`"req-1"`)))
	require.Equal(t, "42", IDString(json.RawMessage(`42`)))
	require.Equal(t, "", IDString(nil))
}
