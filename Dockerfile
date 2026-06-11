# syntax=docker/dockerfile:1.6
#
# mavsphere/layout-agent
#
# Fast build — starts from mavsphere/layout-agent-base which contains the
# pre-built GStreamer stack. Only the Go compile runs here (~10-30 seconds).
#
# To rebuild the base (only needed when GStreamer/Rust plugin versions change):
#   make docker-buildx-base
#
# Normal day-to-day agent build:
#   make docker-buildx

ARG BASE_IMAGE=mavsphere/layout-agent-base:latest

# ============================================================
# Stage 1: Build the layout agent (Go)
# ============================================================
FROM golang:1.26-bookworm AS agent-builder

WORKDIR /src

ARG TARGETOS
ARG TARGETARCH

ENV GOFLAGS=-mod=readonly
ENV GOTOOLCHAIN=local

COPY go.mod go.sum ./
COPY cmd     ./cmd
COPY pkg     ./pkg
COPY internal ./internal

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    set -eux; \
    go mod download; \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags="-s -w" \
    -o /out/mavsphere-layout-agent ./cmd/layoutagent

# ============================================================
# Stage 2: Final image — base + Go binary
# ============================================================
FROM ${BASE_IMAGE}

ARG DEBIAN_FRONTEND=noninteractive

ENV AGENT_CONFIG=/config/config.json

COPY --from=agent-builder /out/mavsphere-layout-agent /usr/local/bin/mavsphere-layout-agent

# Default config — operator should mount a real config.json over /config
RUN cat > /opt/mavsphere-layout-agent/default.config.json <<'CFGEOF'
{
  "layoutId": "1",
  "backendWsUrl": "wss://mavsphere.com/api/ws/agent",
  "backendUrl":   "https://mavsphere.com",
  "username":     "operator@example.com",
  "password":     "changeme",
  "dccEx": {
    "port":             "/dev/ttyUSB0",
    "baudRate":         115200,
    "commandTimeoutMs": 3000
  },
  "mqtt": {
    "brokerUrl":   "tcp://mosquitto:1883",
    "topicPrefix": "layout"
  },
  "janusUrl":          "wss://mavsphere.com/janus",
  "videoCodec":        "vp8",
  "h264Encoder":       "auto",
  "h264Profile":       "baseline",
  "h264BitrateBps":    1500000,
  "preferMjpg":        true,
  "videoWidth":        1280,
  "videoHeight":       720,
  "videoFps":          25,
  "webrtcStartBitrateBps": 800000,
  "webrtcMaxBitrateBps":   1500000,
  "webrtcMinBitrateBps":   150000,
  "cameras": [],
  "trains": [
    {
      "trainId":     0,
      "trainSlug":   "MyLoco",
      "dccAddress": 3,
      "displayName": "My Locomotive"
    }
  ],
  "allowControl": true,
  "failsafe": {
    "controlTimeoutMs": 450,
    "reissueStopMs":    500
  }
}
CFGEOF

RUN cat > /entrypoint.sh <<'EPEOF'
#!/bin/sh
set -e
CONFIG_PATH="${AGENT_CONFIG:-/config/config.json}"
if [ ! -f "$CONFIG_PATH" ]; then
  echo "[entrypoint] No config found at $CONFIG_PATH — copying default"
  mkdir -p "$(dirname "$CONFIG_PATH")"
  cp /opt/mavsphere-layout-agent/default.config.json "$CONFIG_PATH"
  echo "[entrypoint] Edit $CONFIG_PATH and restart the container"
fi

echo "[entrypoint] GStreamer version:"
gst-launch-1.0 --version || true

rm -f /root/.cache/gstreamer-1.0/registry.*.bin 2>/dev/null || true

for elem in janusvrwebrtcsink rtpgccbwe nicesrc nicesink webrtcbin; do
  gst-inspect-1.0 "$elem" >/dev/null 2>&1 || echo "[entrypoint] WARNING: $elem not found"
done

exec /usr/local/bin/mavsphere-layout-agent "$@"
EPEOF
RUN chmod +x /entrypoint.sh

# Web UI port
EXPOSE 8091

WORKDIR /config
ENTRYPOINT ["/entrypoint.sh"]
CMD ["-config", "/config/config.json"]
