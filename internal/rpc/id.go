package rpc

import (
	"encoding/json"
	"fmt"
)

// ValidateID enforces the JSON-RPC 2.0 rule: id MUST be string, number, or null.
// Returns nil for absent (notification) or valid ids; an error otherwise.
func ValidateID(raw json.RawMessage) error {
	if len(raw) == 0 {
		return nil // notification — absent id is fine
	}

	// Try string
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return nil
	}
	// Try number (float64 covers ints + floats in JSON)
	var n float64
	if err := json.Unmarshal(raw, &n); err == nil {
		return nil
	}
	// Try null
	var v any
	if err := json.Unmarshal(raw, &v); err == nil && v == nil {
		return nil
	}

	return fmt.Errorf("rpc: id must be string, number, or null; got %s", string(raw))
}

// IDString returns a stable string form of a request id for logging and routing.
// For string ids, returns the unquoted value; for numbers, the JSON form;
// for null/absent, returns "".
func IDString(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	return string(raw) // numbers as their JSON form, e.g. "42"
}
