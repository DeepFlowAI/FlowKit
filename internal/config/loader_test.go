package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDefault_PassesValidation(t *testing.T) {
	require.NoError(t, Default().Validate())
}

func TestLoadBytes_HappyPath(t *testing.T) {
	yaml := []byte(`
rpc:
  listen: "127.0.0.1:9700"
  hello_timeout: 3s
asr:
  provider: mock
tts:
  provider: aliyun
store:
  type: local
node:
  id: flowkit-test
  addr: localhost:9700
observability:
  log_level: debug
`)
	cfg, err := LoadBytes(yaml)
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1:9700", cfg.RPC.Listen)
	require.Equal(t, 3*time.Second, cfg.RPC.HelloTimeout)
	require.Equal(t, "mock", cfg.ASR.Provider)
	require.Equal(t, "aliyun", cfg.TTS.Provider)
	require.Equal(t, "flowkit-test", cfg.Node.ID)
}

func TestLoadBytes_DefaultsApply(t *testing.T) {
	// Empty doc → all defaults
	cfg, err := LoadBytes([]byte(""))
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1:9600", cfg.RPC.Listen, "default listen not applied")
	require.Equal(t, "mock", cfg.ASR.Provider, "default asr provider not applied")
	require.Equal(t, "local", cfg.Store.Type)
}

func TestLoadBytes_EnvExpansion(t *testing.T) {
	t.Setenv("FK_TEST_NODE_ID", "flowkit-from-env")
	t.Setenv("FK_TEST_REDIS_PASS", "secret")

	yaml := []byte(`
rpc:
  listen: "127.0.0.1:9600"
node:
  id: "${FK_TEST_NODE_ID}"
  addr: "${FK_TEST_NODE_ADDR:-fallback-host:9600}"
store:
  type: redis
  redis:
    addr: "redis:6379"
    password: "${FK_TEST_REDIS_PASS}"
`)
	cfg, err := LoadBytes(yaml)
	require.NoError(t, err)
	require.Equal(t, "flowkit-from-env", cfg.Node.ID, "env var should expand")
	require.Equal(t, "fallback-host:9600", cfg.Node.Addr, "missing env should use :- default")
	require.Equal(t, "secret", cfg.Store.Redis.Password)
}

func TestLoadBytes_ValidationFails(t *testing.T) {
	cases := map[string][]byte{
		"no listen and no unix": []byte(`
rpc:
  listen: ""
  unix: ""
node:
  id: flowkit-x
`),
		"unknown asr provider": []byte(`
rpc:
  listen: "127.0.0.1:9600"
asr:
  provider: "made-up"
node:
  id: flowkit-x
`),
		"redis store without addr": []byte(`
rpc:
  listen: "127.0.0.1:9600"
store:
  type: redis
node:
  id: flowkit-x
`),
		"missing node id": []byte(`
rpc:
  listen: "127.0.0.1:9600"
node:
  id: ""
`),
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := LoadBytes(raw)
			require.Error(t, err, "expected validation failure")
		})
	}
}

func TestLoadBytes_PublicListenRequiresAPIKey(t *testing.T) {
	base := "node:\n  id: x\nrpc:\n  listen: %q\n%s"
	cases := []struct {
		name   string
		listen string
		extra  string
		ok     bool
	}{
		{"loopback without key", "127.0.0.1:9600", "", true},
		{"localhost without key", "localhost:9600", "", true},
		{"ipv6 loopback without key", "[::1]:9600", "", true},
		{"all interfaces without key", "0.0.0.0:9600", "", false},
		{"empty host without key", ":9600", "", false},
		{"public with key", "0.0.0.0:9600", "  api_key: k\n", true},
		{"public with explicit opt-out", "0.0.0.0:9600", "  allow_insecure_no_auth: true\n", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := LoadBytes([]byte(fmt.Sprintf(base, tc.listen, tc.extra)))
			if tc.ok {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, "FLOWKIT_API_KEY")
			}
		})
	}
}

func TestLoad_FromFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "flowkit.yaml")
	body := `
rpc:
  listen: "127.0.0.1:9600"
node:
  id: file-test
`
	require.NoError(t, os.WriteFile(path, []byte(body), 0o644))

	cfg, err := Load(path)
	require.NoError(t, err)
	require.Equal(t, "file-test", cfg.Node.ID)
}

func TestLoad_FileNotFound(t *testing.T) {
	_, err := Load("/no/such/file.yaml")
	require.Error(t, err)
}

func TestLoad_EmptyPath_ReturnsDefaults(t *testing.T) {
	cfg, err := Load("")
	require.NoError(t, err)
	require.Equal(t, "flowkit-1", cfg.Node.ID)
}

func TestExpandEnv_PreservesNonPlaceholders(t *testing.T) {
	out := expandEnv("plain text without $vars or {braces}")
	require.Equal(t, "plain text without $vars or {braces}", out)
}

func TestExpandEnv_HandlesMultipleOccurrences(t *testing.T) {
	t.Setenv("A", "alpha")
	t.Setenv("B", "beta")
	out := expandEnv("${A}-${B}-${A}")
	require.Equal(t, "alpha-beta-alpha", out)
}

func TestKnownProvider(t *testing.T) {
	require.True(t, knownProvider("aliyun"))
	require.True(t, knownProvider("mock"))
	require.True(t, knownProvider(""), "empty should be allowed (falls back to default)")
	require.False(t, knownProvider("openai"))
	require.False(t, knownProvider("nuance"))
}

func TestExpandEnv_RejectsLowercaseVarsByDesign(t *testing.T) {
	// We deliberately match only UPPER_CASE env-style names so $home / ${tmp}
	// in shell snippets embedded in comments don't get rewritten by accident.
	t.Setenv("LOWER", "x")
	out := expandEnv("${lower}")
	require.Equal(t, "${lower}", out, "lowercase placeholder should stay literal")
	// Sanity: it's not a regex artefact — uppercase still works.
	require.Equal(t, "x", expandEnv("${LOWER}"))
}

// keep strings import used (golangci-lint friendly placeholder if needed later)
var _ = strings.ToLower
