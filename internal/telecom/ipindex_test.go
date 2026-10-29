package telecom

import "testing"

func TestIPIndex_LongestPrefixWins(t *testing.T) {
	idx := NewIPIndex()
	idx.Add("203.0.113.0/24", 5060, "wide")
	idx.Add("203.0.113.0/28", 5060, "narrow")

	id, ok := idx.Lookup("203.0.113.5", 5060)
	if !ok || id != "narrow" {
		t.Errorf("longest prefix: got %q ok=%v want narrow,true", id, ok)
	}
	// Outside /28, /24 still catches.
	id, ok = idx.Lookup("203.0.113.50", 5060)
	if !ok || id != "wide" {
		t.Errorf("fallthrough: got %q ok=%v want wide,true", id, ok)
	}
}

func TestIPIndex_TieBreakByTrunkID(t *testing.T) {
	idx := NewIPIndex()
	idx.Add("203.0.113.10", 5060, "zeta")
	idx.Add("203.0.113.10", 5060, "alpha")

	id, ok := idx.Lookup("203.0.113.10", 5060)
	if !ok || id != "alpha" {
		t.Errorf("tie-break: got %q ok=%v want alpha,true", id, ok)
	}
}

func TestIPIndex_PortAnyAndExact(t *testing.T) {
	idx := NewIPIndex()
	idx.Add("203.0.113.10", 0, "any-port")
	idx.Add("203.0.113.20", 5070, "specific")

	if id, ok := idx.Lookup("203.0.113.10", 12345); !ok || id != "any-port" {
		t.Errorf("port 0 should match anything: %q ok=%v", id, ok)
	}
	if id, ok := idx.Lookup("203.0.113.20", 5070); !ok || id != "specific" {
		t.Errorf("exact port: %q ok=%v", id, ok)
	}
	if _, ok := idx.Lookup("203.0.113.20", 5060); ok {
		t.Error("wrong port should not match")
	}
}

func TestIPIndex_UnknownIP(t *testing.T) {
	idx := NewIPIndex()
	idx.Add("203.0.113.0/24", 5060, "t1")
	if _, ok := idx.Lookup("198.51.100.1", 5060); ok {
		t.Error("unknown IP should not match")
	}
	if _, ok := idx.Lookup("not-an-ip", 5060); ok {
		t.Error("invalid IP should return ok=false")
	}
}

func TestIPIndex_InvalidEntryIgnored(t *testing.T) {
	idx := NewIPIndex()
	idx.Add("not-an-ip", 0, "bad")
	idx.Add("203.0.113.0/99", 0, "bad-cidr")
	idx.Add("203.0.113.10", 0, "")
	idx.Add("", 0, "empty")
	if idx.Len() != 0 {
		t.Errorf("invalid entries should be ignored, got %d", idx.Len())
	}
}
