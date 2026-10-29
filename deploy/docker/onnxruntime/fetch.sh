#!/usr/bin/env bash
# 下载 libonnxruntime.so（Silero VAD 运行时依赖）到本目录，供 Docker 构建 COPY。
# 构建前需先跑一次（.so 不入库）。
set -e
VER="${ONNXRUNTIME_VERSION:-1.26.0}"
DIR="$(cd "$(dirname "$0")" && pwd)"
curl -fsSL -o /tmp/ort.tgz "https://github.com/microsoft/onnxruntime/releases/download/v${VER}/onnxruntime-linux-x64-${VER}.tgz"
tar xzf /tmp/ort.tgz -C /tmp
cp "/tmp/onnxruntime-linux-x64-${VER}/lib/libonnxruntime.so.${VER}" "$DIR/libonnxruntime.so"
rm -rf /tmp/ort.tgz "/tmp/onnxruntime-linux-x64-${VER}"
echo "onnxruntime $VER -> $DIR/libonnxruntime.so"
