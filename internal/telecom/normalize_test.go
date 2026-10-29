package telecom

import "testing"

func TestNormalizeE164(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"+8602112345678", "8602112345678"},
		{"tel:+8602112345678", "8602112345678"},
		{"TEL:13800138000", "13800138000"},
		{"+86 21-1234 5678", "862112345678"},
		{"(021) 1234.5678", "02112345678"},
		{"13800138000", "13800138000"},
		// non-digit content survives unchanged (extension / SIP user / alpha)
		{"support", "support"},
		{"1234*5", "1234*5"},
		{"alice@example.com", "alice@example.com"},
		{"", ""},
		{"   ", ""},
		// purely-symbol input falls back to trimmed original
		{"+--", "+--"},
	}
	for _, c := range cases {
		got := NormalizeE164(c.in)
		if got != c.want {
			t.Errorf("NormalizeE164(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}
