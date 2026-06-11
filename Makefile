APP_NAME  ?= layout-agent
REGISTRY  ?= mavsphere
IMAGE     := $(REGISTRY)/$(APP_NAME)
BASE_IMAGE := $(REGISTRY)/$(APP_NAME)-base

TIMESTAMP := $(shell date -u +%Y-%m-%d_%H%M%S)
GIT_SHA   := $(shell git rev-parse --short HEAD 2>/dev/null || echo nogit)
TAG       ?= $(GIT_SHA)

# Base image tag — bump this when GST_VERSION / GST_PLUGINS_RS_REF / LIBNICE_VERSION change.
# Using a fixed label (not :latest) means agents built against an older base
# keep working even after you push a new base.
BASE_TAG  ?= latest

PLATFORMS := linux/amd64,linux/arm64
GO_IMAGE  ?= golang:1.26-bookworm

.PHONY: all build build-release build-debug \
        build-linux-amd64 build-linux-arm64 \
        go-mod-tidy docker-go-mod-tidy \
        docker-build docker-push docker-buildx docker-buildx-load \
        docker-buildx-base docker-buildx-base-load \
        buildx-setup docker-login docker-run \
        clean help

all: build-release

# ── Go module hygiene ────────────────────────────────────────────────────────
# Docker builds COPY go.mod + go.sum before they can run any Go command.
# Therefore go.sum must exist in the build context before docker/buildx starts.
# This target uses local Go if available, otherwise falls back to the official
# Go Docker image so a fresh Ubuntu host can still build the agent.

go-mod-tidy:
	@if command -v go >/dev/null 2>&1; then \
		echo "Running go mod tidy with local Go..."; \
		go mod tidy; \
	else \
		echo "Local Go not found; running go mod tidy via $(GO_IMAGE)..."; \
		docker run --rm \
		  --user "$$(id -u):$$(id -g)" \
		  -v "$$(pwd):/src" \
		  -w /src \
		  -e GOCACHE=/tmp/.gocache \
		  -e GOMODCACHE=/tmp/.gomodcache \
		  $(GO_IMAGE) \
		  go mod tidy; \
	fi
	@test -f go.sum || (echo "ERROR: go.sum was not generated" && exit 1)

# Explicit docker-only variant, useful when you want module tidy to use exactly
# the same Go version as the Docker build image.
docker-go-mod-tidy:
	docker run --rm \
	  --user "$$(id -u):$$(id -g)" \
	  -v "$$(pwd):/src" \
	  -w /src \
	  -e GOCACHE=/tmp/.gocache \
	  -e GOMODCACHE=/tmp/.gomodcache \
	  $(GO_IMAGE) \
	  go mod tidy
	@test -f go.sum || (echo "ERROR: go.sum was not generated" && exit 1)

# ── Local Go builds ──────────────────────────────────────────────────────────

build: go-mod-tidy
	@mkdir -p bin
	go build -o bin/$(APP_NAME) ./cmd/layoutagent
	@echo "Built bin/$(APP_NAME)"

build-release: go-mod-tidy
	@mkdir -p bin
	go build -trimpath -ldflags "-s -w" -o bin/$(APP_NAME) ./cmd/layoutagent
	@echo "Built bin/$(APP_NAME) (release)"

build-debug: go-mod-tidy
	@mkdir -p bin
	go build -gcflags "all=-N -l" -o bin/$(APP_NAME)-debug ./cmd/layoutagent
	@echo "Built bin/$(APP_NAME)-debug (debug)"

build-linux-amd64: go-mod-tidy
	@mkdir -p bin
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
	  go build -trimpath -ldflags "-s -w" -o bin/$(APP_NAME)-linux-amd64 ./cmd/layoutagent

build-linux-arm64: go-mod-tidy
	@mkdir -p bin
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 \
	  go build -trimpath -ldflags "-s -w" -o bin/$(APP_NAME)-linux-arm64 ./cmd/layoutagent

# ── Docker — single-platform (fast, for local testing) ───────────────────────

docker-build:
	docker build \
	  --build-arg BASE_IMAGE=$(BASE_IMAGE):$(BASE_TAG) \
	  --tag $(IMAGE):$(TAG) \
	  --tag $(IMAGE):latest \
	  .
	@echo "Built $(IMAGE):$(TAG)"

# ── Docker — multi-platform builder setup ────────────────────────────────────

buildx-setup:
	docker buildx inspect mavsphere-builder >/dev/null 2>&1 || \
	  docker buildx create --name mavsphere-builder --driver docker-container --bootstrap
	docker buildx use mavsphere-builder

# ── Base image — build ONLY when GStreamer/Rust plugin versions change ────────
#
# This is the slow build (~30 min). Run it once, push it, then forget about it
# until you need to upgrade GStreamer.
#
#   make docker-buildx-base              # uses BASE_TAG=latest
#   make docker-buildx-base BASE_TAG=1.26.9

docker-buildx-base: buildx-setup
	docker buildx build \
	  --platform $(PLATFORMS) \
	  --file Dockerfile.base \
	  --tag $(BASE_IMAGE):$(BASE_TAG) \
	  --push \
	  .
	@echo "Pushed $(BASE_IMAGE):$(BASE_TAG) [$(PLATFORMS)]"
	@echo ""
	@echo "Update BASE_TAG in your Makefile if you used a versioned tag."

# Load amd64 base locally (no push) — useful for local testing of the base build.
docker-buildx-base-load: buildx-setup
	docker buildx build \
	  --platform linux/amd64 \
	  --file Dockerfile.base \
	  --tag $(BASE_IMAGE):$(BASE_TAG) \
	  --load \
	  .

# ── Agent image — fast day-to-day build (Go only) ────────────────────────────
#
# Requires mavsphere/layout-agent-base:$(BASE_TAG) to already exist on Docker Hub.
# Build + push multi-arch:
#   make docker-buildx
#
# Build + load amd64 locally (no push):
#   make docker-buildx-load

docker-buildx: buildx-setup
	docker buildx build \
	  --platform $(PLATFORMS) \
	  --build-arg BASE_IMAGE=$(BASE_IMAGE):$(BASE_TAG) \
	  --tag $(IMAGE):$(TAG) \
	  --tag $(IMAGE):latest \
	  --push \
	  .
	@echo "Pushed $(IMAGE):$(TAG) [$(PLATFORMS)]"

# Build multi-arch, load amd64 into local docker (buildx limitation).
docker-buildx-load: buildx-setup
	docker buildx build \
	  --platform linux/amd64 \
	  --build-arg BASE_IMAGE=$(BASE_IMAGE):$(BASE_TAG) \
	  --tag $(IMAGE):$(TAG) \
	  --load \
	  .

# Push already-built single-platform tags.
docker-push:
	docker push $(IMAGE):$(TAG)
	docker push $(IMAGE):latest

# ── Run locally (mounts ./config as /config, passes through serial + video) ──
# Adjust --device flags for your hardware:
#   --device /dev/ttyUSB0   DCC-EX via USB serial
#   --device /dev/video0    Camera for WebRTC streaming
# Network host gives the container access to the local MQTT broker.

docker-run:
	docker run --rm -it \
	  --network host \
	  -v "$(PWD)/config:/config" \
	  --device /dev/video0 \
	  $(IMAGE):latest

# ── Housekeeping ─────────────────────────────────────────────────────────────

clean:
	rm -rf bin/

docker-login:
	@echo "Logging in to Docker Hub..."
	docker login -u $${DOCKERHUB_USERNAME} --password-stdin <<< $${DOCKERHUB_TOKEN}

help:
	@echo ""
	@echo "  make build                  Local Go build (host OS/arch)"
	@echo "  make build-release          Local Go build with -ldflags -s -w"
	@echo "  make build-linux-amd64      Cross-compile linux/amd64"
	@echo "  make build-linux-arm64      Cross-compile linux/arm64 (Raspberry Pi)"
	@echo ""
	@echo "  make go-mod-tidy            Sync go.mod/go.sum, using local Go or Docker fallback"
	@echo "  make docker-go-mod-tidy     Sync go.mod/go.sum using $(GO_IMAGE) only"
	@echo ""
	@echo "  ── Base image (slow, run rarely) ──────────────────────────────────"
	@echo "  make docker-buildx-base     Build + push GStreamer base image (~30 min)"
	@echo "  make docker-buildx-base-load  Build + load amd64 base locally (no push)"
	@echo ""
	@echo "  ── Agent image (fast, run often) ──────────────────────────────────"
	@echo "  make docker-build           Local single-platform build (no push)"
	@echo "  make docker-buildx          Multi-arch build + push (fast, Go only)"
	@echo "  make docker-buildx-load     Multi-arch build, load amd64 locally (no push)"
	@echo "  make docker-run             Run container with host network + ./config mounted"
	@echo ""
	@echo "  REGISTRY=$(REGISTRY)"
	@echo "  IMAGE=$(IMAGE)"
	@echo "  BASE_IMAGE=$(BASE_IMAGE)"
	@echo "  TAG=$(TAG)        (override: make docker-buildx TAG=v1.2.3)"
	@echo "  BASE_TAG=$(BASE_TAG)    (override: make docker-buildx-base BASE_TAG=1.26.9)"
	@echo "  GO_IMAGE=$(GO_IMAGE)"
	@echo ""
	@echo "  Hardware notes:"
	@echo "    DCC-EX serial: add --device /dev/ttyUSB0 to docker-run"
	@echo "    Camera:        add --device /dev/video0"
	@echo "    MQTT broker:   use --network host (or set mqttBrokerUrl to LAN IP)"
	@echo ""
