package config

import (
	"errors"
	"fmt"
	"net"
	"os"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

// isLoopbackListen reports whether a host:port listen address only accepts
// connections from the local machine. An empty host binds every interface.
func isLoopbackListen(listen string) bool {
	host, _, err := net.SplitHostPort(listen)
	if err != nil || host == "" {
		return false
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// Load reads a YAML file, expands ${VAR} / ${VAR:-default} placeholders
// against the process environment, merges in defaults, and validates the
// result. An empty path returns Default() unchanged (useful for tests).
func Load(path string) (*Config, error) {
	cfg := Default()
	if path == "" {
		return cfg, cfg.Validate()
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: read %s: %w", path, err)
	}

	expanded := expandEnv(string(raw))

	if err := yaml.Unmarshal([]byte(expanded), cfg); err != nil {
		return nil, fmt.Errorf("config: parse %s: %w", path, err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("config: validate %s: %w", path, err)
	}
	return cfg, nil
}

// LoadBytes is the in-memory equivalent of Load — used by tests.
func LoadBytes(raw []byte) (*Config, error) {
	cfg := Default()
	if err := yaml.Unmarshal([]byte(expandEnv(string(raw))), cfg); err != nil {
		return nil, fmt.Errorf("config: parse: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("config: validate: %w", err)
	}
	return cfg, nil
}

// envPlaceholder matches ${NAME} or ${NAME:-default}.
// The default may contain anything except '}'.
var envPlaceholder = regexp.MustCompile(`\$\{([A-Z_][A-Z0-9_]*)(?::-([^}]*))?\}`)

// expandEnv replaces ${VAR} and ${VAR:-default} occurrences in s with the
// corresponding environment variable. A missing variable with no default
// expands to an empty string.
func expandEnv(s string) string {
	return envPlaceholder.ReplaceAllStringFunc(s, func(match string) string {
		groups := envPlaceholder.FindStringSubmatch(match)
		name, def := groups[1], groups[2]
		if v, ok := os.LookupEnv(name); ok {
			return v
		}
		return def
	})
}

// Validate checks invariants the kernel relies on.
func (c *Config) Validate() error {
	var errs []string

	if c.RPC.Listen == "" && c.RPC.Unix == "" {
		errs = append(errs, "rpc: at least one of rpc.listen / rpc.unix is required")
	}
	if c.RPC.Listen != "" && !strings.Contains(c.RPC.Listen, ":") {
		errs = append(errs, fmt.Sprintf("rpc.listen %q must be host:port", c.RPC.Listen))
	}
	if c.RPC.Listen != "" && c.RPC.APIKey == "" && !c.RPC.AllowInsecureNoAuth && !isLoopbackListen(c.RPC.Listen) {
		errs = append(errs, fmt.Sprintf(
			"rpc.listen %q is not loopback: set rpc.api_key (FLOWKIT_API_KEY), or FLOWKIT_ALLOW_INSECURE_NO_AUTH=true on a trusted network",
			c.RPC.Listen))
	}
	if c.RPC.HelloTimeout < 0 {
		errs = append(errs, "rpc.hello_timeout must be >= 0")
	}

	if !knownProvider(c.ASR.Provider) {
		errs = append(errs, fmt.Sprintf("asr.provider %q is not a recognised provider", c.ASR.Provider))
	}
	if !knownProvider(c.TTS.Provider) {
		errs = append(errs, fmt.Sprintf("tts.provider %q is not a recognised provider", c.TTS.Provider))
	}

	switch c.Store.Type {
	case "local", "redis":
	default:
		errs = append(errs, fmt.Sprintf("store.type %q must be 'local' or 'redis'", c.Store.Type))
	}
	if c.Store.Type == "redis" && c.Store.Redis.Addr == "" {
		errs = append(errs, "store.redis.addr required when store.type=redis")
	}

	if c.Node.ID == "" {
		errs = append(errs, "node.id required")
	}

	if c.SIP.TrunkPort < 0 || c.SIP.TrunkPort > 65535 {
		errs = append(errs, fmt.Sprintf("sip.trunk_port %d out of range (SIP_TRUNK_PORT)", c.SIP.TrunkPort))
	}
	if c.SIP.TrunkOutboundPort < 0 || c.SIP.TrunkOutboundPort > 65535 {
		errs = append(errs, fmt.Sprintf("sip.trunk_outbound_port %d out of range (SIP_TRUNK_OUTBOUND_PORT)", c.SIP.TrunkOutboundPort))
	}

	switch strings.ToLower(c.Observability.LogLevel) {
	case "", "debug", "info", "warn", "error":
	default:
		errs = append(errs, fmt.Sprintf("observability.log_level %q invalid", c.Observability.LogLevel))
	}

	if len(errs) == 0 {
		return nil
	}
	return errors.New("config invalid: " + strings.Join(errs, "; "))
}

func knownProvider(name string) bool {
	switch name {
	case "aliyun", "custom", "mock", "":
		return true
	}
	return false
}
