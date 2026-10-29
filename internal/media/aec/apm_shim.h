// Thin C ABI over WebRTC's AudioProcessing module (APM), exposing AEC3 (+ optional
// NS/AGC) to cgo. APM is C++; cgo cannot call it directly, so this header declares
// a minimal extern "C" surface implemented in apm_shim.cc.
//
// Frames are 10 ms of interleaved 16-bit PCM (num_channels*sample_rate/100 samples),
// which is APM's fixed processing unit.
#ifndef FLOWKIT_APM_SHIM_H
#define FLOWKIT_APM_SHIM_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef void *apm_handle;

// apm_create builds an APM instance. Booleans are 0/1; ns_level is 0..3
// (low/moderate/high/veryHigh). Returns NULL on failure.
apm_handle apm_create(int sample_rate_hz, int num_channels,
                      int enable_aec3, int enable_ns, int ns_level,
                      int enable_agc);

// apm_process_reverse feeds one 10 ms far-end (played) frame as the echo reference.
void apm_process_reverse(apm_handle h, const int16_t *frame);

// apm_set_stream_delay sets the render/capture delay hint (ms); call before capture.
void apm_set_stream_delay(apm_handle h, int delay_ms);

// apm_process_capture cleans one 10 ms near-end (mic) frame: reads `in`, writes the
// echo-cancelled result to `out` (out may alias in). Returns 0 on success.
int apm_process_capture(apm_handle h, const int16_t *in, int16_t *out);

// apm_destroy releases the instance.
void apm_destroy(apm_handle h);

#ifdef __cplusplus
}
#endif

#endif // FLOWKIT_APM_SHIM_H
