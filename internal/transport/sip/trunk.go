package sip

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/emiago/sipgo/sip"
)

// TrunkConfig holds outbound routing defaults from flowkit.yaml sip.trunk_*.
type TrunkConfig struct {
	Server       string
	Port         int
	User         string
	Pass         string
	Realm        string
	DID          string // default caller ID (主叫外显)
	CalleePrefix string // prepended to bare callee digits (e.g. 035)
}

// OutboundResolverFn looks up a per-trunk OutboundProfile by trunk_id and
// returns it as a TrunkConfig. The empty trunk_id means "use the default"
// (e.g. system_outbound). Returns false when not found / not ready.
//
// This function-typed resolver avoids importing internal/telecom from this
// package; the catalog wires it via SetOutboundResolver in main.go.
type OutboundResolverFn func(trunkID string) (TrunkConfig, bool)

// SetTrunkConfig wires trunk defaults used when originate URIs omit a host
// AND no OutboundResolver is wired (legacy single-trunk path).
func (s *Signal) SetTrunkConfig(cfg TrunkConfig) {
	s.trunk = cfg
}

// SetOutboundResolver wires per-trunk_id outbound routing. When set, originate
// queries this first; falls back to SetTrunkConfig only if the resolver
// returns false AND the requested trunk_id is empty.
func (s *Signal) SetOutboundResolver(fn OutboundResolverFn) {
	s.mu.Lock()
	s.outboundResolver = fn
	s.mu.Unlock()
}

// ErrTrunkNotFound indicates the requested trunk_id is not in the catalog.
var ErrTrunkNotFound = errors.New("originate: trunk_id not found")

// ErrTrunkNotReady indicates the trunk exists but is disabled or has no
// outbound profile.
var ErrTrunkNotReady = errors.New("originate: trunk not ready for outbound")

// pickTrunkConfig returns the routing config for a call.originate request.
//
//   - trunkID != "": MUST resolve via resolver or return ErrTrunkNotFound /
//     ErrTrunkNotReady. Never silently fall back to s.trunk (would route to
//     wrong gateway).
//   - trunkID == "": prefer resolver(""); fall back to s.trunk; finally
//     error if neither is configured.
func (s *Signal) pickTrunkConfig(trunkID string) (TrunkConfig, error) {
	s.mu.Lock()
	resolver := s.outboundResolver
	fallback := s.trunk
	s.mu.Unlock()

	if trunkID != "" {
		if resolver == nil {
			return TrunkConfig{}, ErrTrunkNotFound
		}
		cfg, ok := resolver(trunkID)
		if !ok {
			return TrunkConfig{}, ErrTrunkNotFound
		}
		if !cfg.OutboundTrunkReady() {
			return TrunkConfig{}, ErrTrunkNotReady
		}
		return cfg, nil
	}

	if resolver != nil {
		if cfg, ok := resolver(""); ok && cfg.OutboundTrunkReady() {
			return cfg, nil
		}
	}
	if fallback.OutboundTrunkReady() {
		return fallback, nil
	}
	return TrunkConfig{}, fmt.Errorf("originate: no outbound trunk configured")
}

func (s *Signal) resolveOriginate(uri, callerID, trunkID string) (target sip.Uri, fromUser string, routing TrunkConfig, err error) {
	raw := strings.TrimSpace(uri)
	if raw == "" {
		return sip.Uri{}, "", TrunkConfig{}, fmt.Errorf("originate: empty uri")
	}

	// Pre-parse to learn whether routing is required.
	isDial := isDialString(raw)
	var parsed sip.Uri
	if !isDial {
		parsed, err = parseTargetURI(raw)
		if err != nil {
			return sip.Uri{}, "", TrunkConfig{}, err
		}
	}
	needsTrunkHost := isDial || parsed.Host == ""

	switch {
	case trunkID != "":
		// Explicit trunk_id MUST resolve. Required regardless of URI form
		// because the SIP From / auth creds bind to that trunk.
		routing, err = s.pickTrunkConfig(trunkID)
	case needsTrunkHost:
		routing, err = s.pickTrunkConfig("")
	default:
		// Full URI + no trunk_id: try default for auth creds, but absence
		// is OK — fully-qualified destinations may not require auth.
		if cfg, perr := s.pickTrunkConfig(""); perr == nil {
			routing = cfg
		}
	}
	if err != nil {
		return sip.Uri{}, "", TrunkConfig{}, err
	}

	fromUser = strings.TrimSpace(callerID)
	if fromUser == "" {
		fromUser = strings.TrimSpace(routing.DID)
	}
	if fromUser == "" {
		fromUser = strings.TrimSpace(routing.User)
	}

	if isDial {
		t, err := buildTrunkURI(routing, raw)
		return t, fromUser, routing, err
	}
	if parsed.Host == "" {
		t, err := buildTrunkURI(routing, parsed.User)
		return t, fromUser, routing, err
	}
	return parsed, fromUser, routing, nil
}

func buildTrunkURI(cfg TrunkConfig, user string) (sip.Uri, error) {
	user = applyCalleePrefix(cfg.CalleePrefix, strings.TrimSpace(user))
	if user == "" {
		return sip.Uri{}, fmt.Errorf("originate: empty destination")
	}
	host, port, err := trunkHostPort(cfg)
	if err != nil {
		return sip.Uri{}, err
	}
	return sip.Uri{User: user, Host: host, Port: port}, nil
}

// OutboundTrunkReady reports whether outbound originate has a gateway configured.
func (c TrunkConfig) OutboundTrunkReady() bool {
	return strings.TrimSpace(c.Server) != ""
}

func trunkHostPort(cfg TrunkConfig) (host string, port int, err error) {
	server := strings.TrimSpace(cfg.Server)
	if server == "" {
		return "", 0, fmt.Errorf("originate: trunk server not configured")
	}
	port = cfg.Port
	if port <= 0 {
		port = 5060
	}
	if h, p, splitErr := net.SplitHostPort(server); splitErr == nil {
		host = h
		if pp, convErr := strconv.Atoi(p); convErr == nil && pp > 0 {
			port = pp
		}
		return host, port, nil
	}
	return server, port, nil
}

func applyCalleePrefix(prefix, user string) string {
	user = strings.TrimSpace(user)
	prefix = strings.TrimSpace(prefix)
	if prefix == "" || user == "" {
		return user
	}
	if strings.HasPrefix(user, prefix) {
		return user
	}
	return prefix + user
}

// isDialString reports bare callee digits (e.g. 5–15 digit phone numbers).
func isDialString(s string) bool {
	if len(s) < 5 || len(s) > 15 {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}
