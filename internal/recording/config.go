package recording

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/DeepFlowAI/flowkit/internal/config"
)

// Config is the runtime recording configuration (from flowkit.yaml + env).
type Config struct {
	Enabled           bool
	StartOn           string // answered | ringing
	CaptureEarlyMedia bool   // outbound provisional RTP only
	Encoding          string // pcm16 (v0.1)
	Layout            Layout // stereo | mono_mix
	FlushInterval     time.Duration
	MaxDuration       time.Duration
	DropOnOverrun     bool
	// RemoteGain scales the caller (remote/left) channel in the recording.
	// Telephony incoming audio is typically quieter than synthesized TTS, so the
	// caller can sound faint next to the agent; >1.0 boosts it (clamped to int16,
	// so high values soft-clip peaks). 1.0 = unchanged. Env: RECORDING_REMOTE_GAIN.
	RemoteGain float64
	Storage    StorageConfig
}

// StorageConfig selects the upload backend.
type StorageConfig struct {
	Backend   string // aliyun_oss | local
	AliyunOSS AliyunOSSConfig
	Local     LocalConfig
}

// AliyunOSSConfig holds OSS AppendObject settings.
type AliyunOSSConfig struct {
	Endpoint     string
	Bucket       string
	KeyTemplate  string
	AccessKey    string
	AccessSecret string
	SSE          bool
}

// LocalConfig holds on-disk recording paths.
type LocalConfig struct {
	Dir       string
	OrphanDir string
}

// FromYAML maps kernel config into recording.Config and applies OSS env fallbacks.
func FromYAML(c config.RecordingConfig) (Config, error) {
	layout, err := ParseLayout(c.Layout)
	if err != nil {
		return Config{}, err
	}
	out := Config{
		Enabled:           c.Enabled,
		StartOn:           c.StartOn,
		CaptureEarlyMedia: c.CaptureEarlyMedia,
		Encoding:          c.Encoding,
		Layout:            layout,
		FlushInterval:     c.FlushInterval,
		MaxDuration:       c.MaxDuration,
		DropOnOverrun:     c.DropOnOverrun,
		Storage: StorageConfig{
			Backend: c.Storage.Backend,
			AliyunOSS: AliyunOSSConfig{
				Endpoint:     c.Storage.AliyunOSS.Endpoint,
				Bucket:       c.Storage.AliyunOSS.Bucket,
				KeyTemplate:  c.Storage.AliyunOSS.KeyTemplate,
				AccessKey:    c.Storage.AliyunOSS.AccessKey,
				AccessSecret: c.Storage.AliyunOSS.AccessSecret,
				SSE:          c.Storage.AliyunOSS.SSE,
			},
			Local: LocalConfig{
				Dir:       c.Storage.Local.Dir,
				OrphanDir: c.Storage.Local.OrphanDir,
			},
		},
	}
	out.normalize()
	return out, nil
}

func (c *Config) normalize() {
	if c.StartOn == "" {
		c.StartOn = "answered"
	}
	if c.Encoding == "" {
		c.Encoding = "pcm16"
	}
	if c.Layout == "" {
		c.Layout = LayoutStereo
	}
	if c.FlushInterval <= 0 {
		c.FlushInterval = 4 * time.Second
	}
	if c.MaxDuration <= 0 {
		c.MaxDuration = 4 * time.Hour
	}
	if c.RemoteGain <= 0 {
		c.RemoteGain = 1.0
		if v := strings.TrimSpace(os.Getenv("RECORDING_REMOTE_GAIN")); v != "" {
			if g, err := strconv.ParseFloat(v, 64); err == nil && g > 0 {
				c.RemoteGain = g
			}
		}
	}
	if c.Storage.Backend == "" {
		c.Storage.Backend = "aliyun_oss"
	}
	if c.Storage.AliyunOSS.KeyTemplate == "" {
		c.Storage.AliyunOSS.KeyTemplate = "recordings/{date}/{conversation_id}/{call_id}.wav"
	}
	if c.Storage.Local.Dir == "" {
		c.Storage.Local.Dir = "/var/lib/flowkit/recordings"
	}
	if c.Storage.Local.OrphanDir == "" {
		c.Storage.Local.OrphanDir = c.Storage.Local.Dir + "/orphan"
	}
	if c.Storage.AliyunOSS.AccessKey == "" {
		c.Storage.AliyunOSS.AccessKey = os.Getenv("ALIYUN_ACCESS_KEY_ID")
	}
	if c.Storage.AliyunOSS.AccessSecret == "" {
		c.Storage.AliyunOSS.AccessSecret = os.Getenv("ALIYUN_ACCESS_KEY_SECRET")
	}
	if c.Storage.AliyunOSS.Endpoint == "" {
		c.Storage.AliyunOSS.Endpoint = os.Getenv("OSS_ENDPOINT")
	}
	if c.Storage.AliyunOSS.Bucket == "" {
		c.Storage.AliyunOSS.Bucket = os.Getenv("OSS_BUCKET")
	}
}

// Validate checks recording settings when Enabled is true.
func (c *Config) Validate() error {
	if !c.Enabled {
		return nil
	}
	switch strings.ToLower(c.StartOn) {
	case "answered", "ringing":
	default:
		return fmt.Errorf("recording: start_on %q must be answered or ringing", c.StartOn)
	}
	switch strings.ToLower(c.Encoding) {
	case "pcm16":
	default:
		return fmt.Errorf("recording: encoding %q not supported in v0.1 (use pcm16)", c.Encoding)
	}
	switch c.Layout {
	case LayoutStereo, LayoutMonoMix:
	default:
		return fmt.Errorf("recording: layout %q must be stereo or mono_mix", c.Layout)
	}
	switch strings.ToLower(c.Storage.Backend) {
	case "aliyun_oss":
		o := c.Storage.AliyunOSS
		if o.Endpoint == "" || o.Bucket == "" {
			return fmt.Errorf("recording: aliyun_oss requires endpoint and bucket (OSS_ENDPOINT, OSS_BUCKET)")
		}
		if o.AccessKey == "" || o.AccessSecret == "" {
			return fmt.Errorf("recording: aliyun_oss requires credentials (OSS_ACCESS_KEY/OSS_ACCESS_SECRET or ALIYUN_ACCESS_KEY_ID/ALIYUN_ACCESS_KEY_SECRET)")
		}
	case "local":
		if c.Storage.Local.Dir == "" {
			return fmt.Errorf("recording: local dir required")
		}
	default:
		return fmt.Errorf("recording: storage.backend %q unknown", c.Storage.Backend)
	}
	return nil
}
