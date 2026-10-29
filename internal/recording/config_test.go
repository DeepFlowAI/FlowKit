package recording

import (
	"strings"
	"testing"

	"github.com/DeepFlowAI/flowkit/internal/config"
)

func TestConfig_Validate_disabled(t *testing.T) {
	c := Config{Enabled: false}
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestConfig_Validate_local(t *testing.T) {
	c, err := FromYAML(config.RecordingConfig{
		Enabled: true,
		StartOn: "answered",
		Storage: config.RecordingStorageConfig{
			Backend: "local",
			Local:   config.RecordingLocalConfig{Dir: t.TempDir()},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestConfig_FromYAML_CaptureEarlyMedia(t *testing.T) {
	c, err := FromYAML(config.RecordingConfig{
		Enabled:           true,
		CaptureEarlyMedia: true,
		Storage: config.RecordingStorageConfig{
			Backend: "local",
			Local:   config.RecordingLocalConfig{Dir: t.TempDir()},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !c.CaptureEarlyMedia {
		t.Fatal("capture_early_media was not mapped from YAML")
	}
}

func TestConfig_Validate_layoutMonoMix(t *testing.T) {
	c, err := FromYAML(config.RecordingConfig{
		Enabled: true,
		Layout:  "mono_mix",
		Storage: config.RecordingStorageConfig{
			Backend: "local",
			Local:   config.RecordingLocalConfig{Dir: t.TempDir()},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	if c.Layout != LayoutMonoMix {
		t.Fatalf("layout=%q", c.Layout)
	}
}

func TestConfig_FromYAML_layoutDefaultStereo(t *testing.T) {
	c, err := FromYAML(config.RecordingConfig{
		Enabled: true,
		Storage: config.RecordingStorageConfig{
			Backend: "local",
			Local:   config.RecordingLocalConfig{Dir: t.TempDir()},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if c.Layout != LayoutStereo {
		t.Fatalf("layout=%q", c.Layout)
	}
}

func TestConfig_FromYAML_layoutInvalid(t *testing.T) {
	_, err := FromYAML(config.RecordingConfig{
		Enabled: true,
		Layout:  "foo",
	})
	if err == nil || !strings.Contains(err.Error(), "layout") {
		t.Fatalf("err=%v", err)
	}
}

func TestConfig_Validate_ossMissingCreds(t *testing.T) {
	c, err := FromYAML(config.RecordingConfig{
		Enabled: true,
		Storage: config.RecordingStorageConfig{
			Backend: "aliyun_oss",
			AliyunOSS: config.RecordingAliyunOSSConfig{
				Endpoint: "https://oss-cn-shanghai.aliyuncs.com",
				Bucket:   "b",
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	c.Storage.AliyunOSS.AccessKey = ""
	c.Storage.AliyunOSS.AccessSecret = ""
	if err := c.Validate(); err == nil {
		t.Fatal("expected error for missing credentials")
	}
}
