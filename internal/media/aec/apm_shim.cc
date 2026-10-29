//go:build aec

// Implementation of the C ABI declared in apm_shim.h over WebRTC APM (AEC3).
// Built only under the `aec` build tag (requires libwebrtc-audio-processing-dev).

#include "apm_shim.h"

#include <vector>

#include "modules/audio_processing/include/audio_processing.h"

using webrtc::AudioProcessing;
using webrtc::AudioProcessingBuilder;
using webrtc::StreamConfig;

namespace {

// Holds the APM plus a fixed StreamConfig and a scratch buffer for the reverse
// stream's (unused) output, so we never write into the caller's render frame.
struct Apm {
  rtc::scoped_refptr<AudioProcessing> apm;
  StreamConfig cfg;
  std::vector<int16_t> rev_out;

  Apm(int sr, int ch)
      : cfg(sr, ch), rev_out(static_cast<size_t>(sr / 100) * ch, 0) {}
};

}  // namespace

extern "C" {

apm_handle apm_create(int sample_rate_hz, int num_channels, int enable_aec3,
                      int enable_ns, int ns_level, int enable_agc) {
  Apm *self = new Apm(sample_rate_hz, num_channels);
  self->apm = AudioProcessingBuilder().Create();
  if (!self->apm) {
    delete self;
    return nullptr;
  }

  AudioProcessing::Config config;

  // Echo canceller: mobile_mode=false selects the full AEC3 (not the mobile AECM).
  config.echo_canceller.enabled = enable_aec3 != 0;
  config.echo_canceller.mobile_mode = false;

  config.noise_suppression.enabled = enable_ns != 0;
  switch (ns_level) {
    case 0:
      config.noise_suppression.level =
          AudioProcessing::Config::NoiseSuppression::kLow;
      break;
    case 1:
      config.noise_suppression.level =
          AudioProcessing::Config::NoiseSuppression::kModerate;
      break;
    case 3:
      config.noise_suppression.level =
          AudioProcessing::Config::NoiseSuppression::kVeryHigh;
      break;
    default:
      config.noise_suppression.level =
          AudioProcessing::Config::NoiseSuppression::kHigh;
      break;
  }

  // High-pass filter removes low-frequency line rumble; cheap and helps AEC/ASR.
  config.high_pass_filter.enabled = true;

  if (enable_agc != 0) {
    config.gain_controller1.enabled = true;
    config.gain_controller1.mode =
        AudioProcessing::Config::GainController1::kAdaptiveDigital;
  }

  self->apm->ApplyConfig(config);
  return reinterpret_cast<apm_handle>(self);
}

void apm_process_reverse(apm_handle h, const int16_t *frame) {
  if (!h) return;
  Apm *self = reinterpret_cast<Apm *>(h);
  self->apm->ProcessReverseStream(frame, self->cfg, self->cfg,
                                  self->rev_out.data());
}

void apm_set_stream_delay(apm_handle h, int delay_ms) {
  if (!h) return;
  reinterpret_cast<Apm *>(h)->apm->set_stream_delay_ms(delay_ms);
}

int apm_process_capture(apm_handle h, const int16_t *in, int16_t *out) {
  if (!h) return -1;
  Apm *self = reinterpret_cast<Apm *>(h);
  return self->apm->ProcessStream(in, self->cfg, self->cfg, out);
}

void apm_destroy(apm_handle h) {
  if (!h) return;
  delete reinterpret_cast<Apm *>(h);
}

}  // extern "C"
