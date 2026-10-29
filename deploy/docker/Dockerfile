# Flowkit kernel — multi-stage build (glibc + CGO for the Silero neural VAD and
# the WebRTC AEC3 echo canceller).
#   Stage 1: compile the binary (golang trixie, CGO_ENABLED=1). Links the AEC3
#            C++ shim against libwebrtc-audio-processing-dev; built with -tags aec.
#   Stage 2: Debian trixie runtime with ffmpeg + libonnxruntime.so (VAD) +
#            libwebrtc-audio-processing (AEC3, apt).
#
# Must be glibc (not Alpine/musl): onnxruntime ships glibc-only, and a CGO
# binary's libc must match the runtime's. libonnxruntime.so is vendored under
# deploy/docker/onnxruntime/ and COPYed in (avoids a GitHub fetch on the build host).
# AEC3 (libwebrtc-audio-processing 1.3) is only packaged from Debian trixie on,
# which is why both stages use trixie rather than bookworm.

# ─── Stage 1: build ─────────────────────────────────────────────────────
FROM golang:1.26-trixie AS builder

ARG GOPROXY=https://goproxy.io,direct
ARG APT_MIRROR=
ENV GOPROXY=${GOPROXY}
ENV CGO_ENABLED=1
ENV GOOS=linux

# AEC3 build deps: the WebRTC APM headers/lib, its C++ toolchain and pkg-config
# (the aec cgo package resolves the lib via `#cgo pkg-config: webrtc-audio-processing-1`).
RUN if [ -n "$APT_MIRROR" ]; then \
      sed -i "s|deb.debian.org|${APT_MIRROR}|g; s|security.debian.org|${APT_MIRROR}|g" \
        /etc/apt/sources.list.d/debian.sources 2>/dev/null || true; \
    fi \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
       libwebrtc-audio-processing-dev pkg-config g++ \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY cmd cmd
COPY internal internal

# -tags aec compiles the native AEC3 canceller (internal/media/aec). Without it
# a no-op stub is used, so AEC is a compile-time opt-in bound to this image.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build \
    -tags aec \
    -ldflags="-s -w" \
    -o /out/flowkit \
    ./cmd/flowkit

# ─── Stage 2: runtime (Debian trixie + ffmpeg + onnxruntime + AEC3) ─────
FROM debian:13-slim

ARG APT_MIRROR=

# ffmpeg — ASR/TTS codec conversion; curl — healthcheck; ca-certificates — OTLP;
# tzdata — log timestamps; libgomp1 — onnxruntime OpenMP runtime;
# libwebrtc-audio-processing-1-3 — AEC3 shared lib (pulls libabsl transitively).
RUN if [ -n "$APT_MIRROR" ]; then \
      sed -i "s|deb.debian.org|${APT_MIRROR}|g; s|security.debian.org|${APT_MIRROR}|g" \
        /etc/apt/sources.list.d/debian.sources 2>/dev/null || true; \
    fi \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
       ffmpeg ca-certificates curl tzdata libgomp1 libwebrtc-audio-processing-1-3 \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -r flowkit && useradd -r -g flowkit -d /app flowkit

# Silero VAD shared lib (CPU). flowkit loads it via VAD_ONNXRUNTIME_LIB.
COPY deploy/docker/onnxruntime/libonnxruntime.so /usr/lib/libonnxruntime.so

WORKDIR /app

COPY --from=builder /out/flowkit /app/flowkit
COPY flowkit.yaml /app/flowkit.yaml
COPY config/tts_lexicon.yaml /app/tts_lexicon.yaml

EXPOSE 9600
EXPOSE 5060/udp

USER flowkit

HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=3 \
    CMD curl -fsS http://127.0.0.1:9600/health || exit 1

ENTRYPOINT ["/app/flowkit", "-c", "/app/flowkit.yaml"]
