package config

import "time"

// Config is the top-level Flowkit configuration. Loaded from YAML, with
// ${VAR} and ${VAR:-default} expansion against environment variables.
type Config struct {
	RPC           RPCConfig           `yaml:"rpc"`
	SIP           SIPConfig           `yaml:"sip"`
	WebRTC        WebRTCConfig        `yaml:"webrtc"`
	ASR           ProviderConfig      `yaml:"asr"`
	TTS           ProviderConfig      `yaml:"tts"`
	Agent         AgentConfig         `yaml:"agent"`
	LLM           LLMConfig           `yaml:"llm"`
	Store         StoreConfig         `yaml:"store"`
	Node          NodeConfig          `yaml:"node"`
	Observability ObservabilityConfig `yaml:"observability"`
	Telecom       TelecomConfig       `yaml:"telecom"`
	Recording     RecordingConfig     `yaml:"recording"`
}

// RecordingConfig controls call recording.
type RecordingConfig struct {
	Enabled           bool                   `yaml:"enabled"`
	StartOn           string                 `yaml:"start_on"` // answered | ringing
	CaptureEarlyMedia bool                   `yaml:"capture_early_media"`
	Encoding          string                 `yaml:"encoding"` // v0.1: pcm16 only
	Layout            string                 `yaml:"layout"`   // stereo | mono_mix
	FlushInterval     time.Duration          `yaml:"flush_interval"`
	MaxDuration       time.Duration          `yaml:"max_duration"`
	DropOnOverrun     bool                   `yaml:"drop_on_overrun"`
	Storage           RecordingStorageConfig `yaml:"storage"`
}

// RecordingStorageConfig selects the upload backend via factory (aliyun_oss | local).
type RecordingStorageConfig struct {
	Backend   string                   `yaml:"backend"`
	AliyunOSS RecordingAliyunOSSConfig `yaml:"aliyun_oss"`
	Local     RecordingLocalConfig     `yaml:"local"`
}

// RecordingAliyunOSSConfig is used when Storage.Backend == "aliyun_oss".
type RecordingAliyunOSSConfig struct {
	Endpoint     string `yaml:"endpoint"`
	Bucket       string `yaml:"bucket"`
	KeyTemplate  string `yaml:"key_template"`
	AccessKey    string `yaml:"access_key"`
	AccessSecret string `yaml:"access_secret"`
	SSE          bool   `yaml:"sse"`
}

// RecordingLocalConfig is used when Storage.Backend == "local".
type RecordingLocalConfig struct {
	Dir       string `yaml:"dir"`
	OrphanDir string `yaml:"orphan_dir"`
}

// TelecomConfig controls the multi-SIP Trunk Catalog.
type TelecomConfig struct {
	Register   TelecomRegisterConfig   `yaml:"register"`
	Static     TelecomStaticConfig     `yaml:"static"`
	Inbound    TelecomInboundConfig    `yaml:"inbound"`
	Extensions TelecomExtensionsConfig `yaml:"extensions"`
	Rooms      TelecomRoomsConfig      `yaml:"rooms"`
}

// TelecomRoomsConfig gates multi-party audio mixing "rooms" (three-way calls,
// barge-in, whisper, takeover, whole-chain hangup). When enabled, the room.*
// JSON-RPC methods are registered. Off by default — no effect on existing
// two-leg bridging when disabled.
type TelecomRoomsConfig struct {
	Enabled bool `yaml:"enabled"` // default false
}

// TelecomExtensionsConfig gates the trust-network SIP extension registrar:
// when enabled, inbound REGISTERs are recorded so calls can be originated to a
// phone by extension number. No effect on REGISTER acceptance (still 200 OK).
type TelecomExtensionsConfig struct {
	Enabled         bool `yaml:"enabled"`           // default false
	SweepIntervalMS int  `yaml:"sweep_interval_ms"` // default 5000
}

// TelecomRegisterConfig gates the HTTP API + heartbeat sweeper.
type TelecomRegisterConfig struct {
	Enabled         bool   `yaml:"enabled"`
	APIKey          string `yaml:"api_key"`
	LeaseMinSec     int    `yaml:"lease_ttl_sec_min"` // default 30
	LeaseMaxSec     int    `yaml:"lease_ttl_sec_max"` // default 300
	SweepIntervalMS int    `yaml:"sweep_interval_ms"` // default 5000
}

// TelecomStaticConfig gates the env-var-loaded `system` Provider.
type TelecomStaticConfig struct {
	Enabled bool `yaml:"enabled"` // default true (preserves legacy behavior)
}

// TelecomInboundConfig controls inbound peer matching.
type TelecomInboundConfig struct {
	RejectUnknownPeer bool   `yaml:"reject_unknown_peer"` // default true
	ToNormalization   string `yaml:"to_normalization"`    // "e164_strip_prefix" | "" (off)
}

// RPCConfig controls the JSON-RPC server.
type RPCConfig struct {
	Listen       string        `yaml:"listen"`        // e.g. "127.0.0.1:9600"
	Unix         string        `yaml:"unix"`          // e.g. "/tmp/flowkit.sock"; "" disables
	HelloTimeout time.Duration `yaml:"hello_timeout"` // max time to wait for system.hello after connect
	// APIKey is required as "Authorization: Bearer <key>" on /ws and
	// /media/pcm. Empty disables auth, which is only allowed on a loopback
	// listen address unless AllowInsecureNoAuth is set.
	APIKey              string `yaml:"api_key"`
	AllowInsecureNoAuth bool   `yaml:"allow_insecure_no_auth"`
}

// SIPConfig controls the SIP UA. Empty fields disable SIP transport.
type SIPConfig struct {
	Listen      string `yaml:"listen"`       // e.g. "0.0.0.0:5060"
	AltListen   string `yaml:"alt_listen"`   // optional 2nd UDP bind, e.g. "0.0.0.0:5070" (SIP-ALG bypass)
	ExtIP       string `yaml:"ext_ip"`       // public IP, "auto" to detect
	TrunkVendor string `yaml:"trunk_vendor"` // aliyun | tencent | twilio | signalwire | zadarma
	TrunkServer string `yaml:"trunk_server"` // e.g. "sip.example.com:5060"
	TrunkPort   int    `yaml:"trunk_port"`   // default 5060
	TrunkUser   string `yaml:"trunk_user"`
	TrunkPass   string `yaml:"trunk_pass"`
	TrunkRealm  string `yaml:"trunk_realm"`
	TrunkDID    string `yaml:"trunk_did"` // E.164 number
	// Outbound originate defaults (falls back to trunk_* when empty).
	TrunkOutboundServer       string `yaml:"trunk_outbound_server"`
	TrunkOutboundPort         int    `yaml:"trunk_outbound_port"`
	TrunkOutboundDID          string `yaml:"trunk_outbound_did"`
	TrunkOutboundCalleePrefix string `yaml:"trunk_outbound_callee_prefix"`
	TrunkOutboundUser         string `yaml:"trunk_outbound_user"`
	TrunkOutboundPass         string `yaml:"trunk_outbound_pass"`
	TrunkOutboundRealm        string `yaml:"trunk_outbound_realm"`
	AllowOriginate            bool   `yaml:"allow_originate"`
	Transport                 string `yaml:"transport"` // UDP | TCP | TLS
	// OutboundProxy is a next-hop SIP proxy (e.g. a Kamailio front-end) that
	// trunk outbound INVITEs are sent to at the transport layer. The
	// Request-URI still targets the trunk; only the UDP destination changes.
	// Empty (the default) sends directly to the trunk host:port.
	OutboundProxy string `yaml:"outbound_proxy"`
	// PhoneOutboundProxy is the next-hop for direct / extension originates
	// (empty trunk_id). Typically Kamailio's phone-facing port (5070) so
	// INVITEs share NAT pinholes with desk-phone REGISTER. Empty falls back
	// to OutboundProxy, then to sending directly to the Contact host:port.
	PhoneOutboundProxy string `yaml:"phone_outbound_proxy"`
	// InboundMode documents the recommended client policy: notify | bridge | llm.
	// The kernel always broadcasts call.incoming; it does not auto-route by mode.
	InboundMode string `yaml:"inbound_mode"`
}

// WebRTCConfig controls the Pion WebRTC peer factory.
type WebRTCConfig struct {
	ICEServers []string `yaml:"ice_servers"` // e.g. ["stun:stun.l.google.com:19302"]
	// NAT1To1IP rewrites host ICE candidates to this public IP (Docker / NAT).
	// Falls back to the hostname in INSTANCE_URL when unset.
	NAT1To1IP string `yaml:"nat1to1_ip"`
}

// ProviderConfig identifies a provider implementation plus its opaque sub-config.
type ProviderConfig struct {
	Provider string         `yaml:"provider"` // e.g. "aliyun", "mock"
	Config   map[string]any `yaml:"config"`   // provider-specific keys
}

// AgentConfig configures the standalone, call-less ASR/TTS exposed over RPC
// for non-telephony consumers (e.g. the VoiceSpeed agent console). When a
// provider is empty it falls back to the call-path asr/tts config — so one
// instance can serve, say, aliyun on SIP calls and the private FunASR/CosyVoice
// models on the standalone agent path.
type AgentConfig struct {
	ASR ProviderConfig `yaml:"asr"`
	TTS ProviderConfig `yaml:"tts"`
}

// LLMConfig selects an LLM provider and optional system prompt.
type LLMConfig struct {
	Provider     string         `yaml:"provider"`      // e.g. "deepseek", "openai_compat"
	SystemPrompt string         `yaml:"system_prompt"` // default assistant persona
	Config       map[string]any `yaml:"config"`        // api_key, model, base_url
}

// StoreConfig selects between LocalStore (single node) and RedisStore (multi-node).
type StoreConfig struct {
	Type  string      `yaml:"type"` // "local" or "redis"
	Redis RedisConfig `yaml:"redis"`
}

// RedisConfig is used when Store.Type == "redis" (M4).
type RedisConfig struct {
	Addr     string `yaml:"addr"`
	Password string `yaml:"password"`
	DB       int    `yaml:"db"`
	TLS      bool   `yaml:"tls"`
}

// NodeConfig identifies this flowkit instance among peers.
type NodeConfig struct {
	ID   string `yaml:"id"`   // unique, e.g. "flowkit-1"
	Addr string `yaml:"addr"` // peer-reachable, e.g. "flowkit-1:9600"
}

// ObservabilityConfig controls slog + OTel.
type ObservabilityConfig struct {
	ServiceName  string `yaml:"service_name"`  // e.g. "flowkit"
	OTLPEndpoint string `yaml:"otlp_endpoint"` // empty = no OTel export, slog stderr only
	LogLevel     string `yaml:"log_level"`     // "debug" | "info" | "warn" | "error"
}

// Default returns a Config populated with sensible defaults for local development.
func Default() *Config {
	return &Config{
		RPC: RPCConfig{
			Listen:       "127.0.0.1:9600",
			Unix:         "",
			HelloTimeout: 5 * time.Second,
		},
		SIP: SIPConfig{
			Transport:   "UDP",
			TrunkPort:   5060,
			ExtIP:       "auto",
			InboundMode: "notify",
		},
		WebRTC: WebRTCConfig{
			ICEServers: []string{"stun:stun.l.google.com:19302"},
		},
		ASR: ProviderConfig{Provider: "mock"},
		TTS: ProviderConfig{Provider: "mock"},
		Store: StoreConfig{
			Type:  "local",
			Redis: RedisConfig{DB: 0},
		},
		Node: NodeConfig{
			ID:   "flowkit-1",
			Addr: "localhost:9600",
		},
		Observability: ObservabilityConfig{
			ServiceName: "flowkit",
			LogLevel:    "info",
		},
		Telecom: TelecomConfig{
			Register: TelecomRegisterConfig{
				Enabled:         false,
				LeaseMinSec:     30,
				LeaseMaxSec:     300,
				SweepIntervalMS: 5000,
			},
			Static: TelecomStaticConfig{Enabled: true},
			Inbound: TelecomInboundConfig{
				RejectUnknownPeer: true,
				ToNormalization:   "e164_strip_prefix",
			},
			Extensions: TelecomExtensionsConfig{
				Enabled:         false,
				SweepIntervalMS: 5000,
			},
		},
	}
}
