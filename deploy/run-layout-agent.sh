#!/usr/bin/env bash
# update.sh — install or update the MavSphere layout agent.
#
# First time:
#   1. Download the deploy/ folder from the MavSphere releases page
#   2. chmod +x deploy/update.sh
#   3. ./deploy/update.sh
#      — creates a config template and tells you what to fill in
#   4. Edit deploy/config/config.json with your MavSphere credentials
#   5. ./deploy/update.sh  (run again to actually start)
#
# Every subsequent time (to update to the latest release):
#   ./deploy/update.sh
#
# What it does:
#   - Pulls the latest layout-agent image from GHCR
#   - Stops and removes any existing agent containers
#   - Starts Mosquitto and the agent fresh with the new image
#   - Your config is always preserved — it lives in ./config/config.json
#     and is never modified or deleted by this script

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BOLD='\033[1m'
NC='\033[0m'

info()    { echo -e "${GREEN}▶ $*${NC}"; }
warning() { echo -e "${YELLOW}⚠  $*${NC}"; }
error()   { echo -e "${RED}✗  $*${NC}"; exit 1; }
header()  { echo -e "\n${BOLD}$*${NC}"; }

# ---- Detect host IP for display purposes ----
if [ -z "${HOST_IP:-}" ]; then
  HOST_IP="$(ip route get 1.1.1.1 2>/dev/null | awk '/src/ {
    for (i=1;i<=NF;i++) if ($i=="src") { print $(i+1); exit }
  }')"
fi

if [ -z "${HOST_IP:-}" ]; then
  HOST_IP="$(hostname -I 2>/dev/null | awk '{print $1}')"
fi

if [ -z "${HOST_IP:-}" ]; then
  HOST_IP="localhost"
fi

# ── Check Docker ──────────────────────────────────────────────────────────────
if ! command -v docker >/dev/null 2>&1; then
  warning "Docker is not installed."
  echo ""
  read -r -p "Install Docker now? [y/N] " response
  if [[ "$response" =~ ^[Yy]$ ]]; then
    info "Installing Docker..."
    curl -fsSL https://get.docker.com | sh
    sudo usermod -aG docker "$USER"
    echo ""
    warning "Docker installed. Log out and back in, then run ./update.sh again."
    exit 0
  else
    error "Docker is required. Install it from https://get.docker.com and try again."
  fi
fi

if ! docker compose version >/dev/null 2>&1; then
  error "Docker Compose v2 not found. Run: sudo apt install docker-compose-plugin"
fi

# ── First-run: create config if missing ──────────────────────────────────────
if [ ! -f "config/config.json" ]; then
  header "First time setup — creating config"
  mkdir -p config

  # Use example/template if present, otherwise write a minimal template
  if [ -f "config.json.example" ]; then
    cp config.json.example config/config.json
  elif [ -f "config.template.json" ]; then
    cp config.template.json config/config.json
  elif [ -f "../config.template.json" ]; then
    cp ../config.template.json config/config.json
  else
    cat > config/config.json << 'JSON'
{
  "layoutId":     "1",
  "backendUrl":   "https://mavsphere.com",
  "backendWsUrl": "wss://mavsphere.com/api/ws/agent",
  "username":     "your@email.com",
  "password":     "yourpassword",
  "dccEx": {
    "port":             "/dev/ttyUSB0",
    "baudRate":         115200,
    "commandTimeoutMs": 3000
  },
  "mqtt": {
    "brokerUrl":   "tcp://mosquitto:1883",
    "topicPrefix": "layout"
  },
  "cameras": [],
  "allowControl": true,
  "failsafe": {
    "controlTimeoutMs": 450,
    "reissueStopMs":    500
  }
}
JSON
  fi

  info "Created ${SCRIPT_DIR}/config/config.json"
  echo ""
  echo -e "  ${BOLD}Edit this file before continuing:${NC}"
  echo ""
  echo -e "  ${BOLD}${SCRIPT_DIR}/config/config.json${NC}"
  echo ""
  echo "    layoutId      — your layout ID (visible in the MavSphere UI)"
  echo "    backendUrl    — e.g. https://mavsphere.com"
  echo "    backendWsUrl  — e.g. wss://mavsphere.com/api/ws/agent"
  echo "    username      — your MavSphere login email"
  echo "    password      — your MavSphere password"
  echo "    dccEx.port    — serial device (/dev/ttyUSB0) or TCP target (192.168.1.x:2560)"
  echo ""
  echo "  Also edit ${SCRIPT_DIR}/docker-compose.yml if you need to expose hardware devices."
  echo ""
  echo "  Then run:  ./update.sh"
  echo ""
  exit 0
fi

# ── Pull latest image ─────────────────────────────────────────────────────────
header "Updating to latest release"
info "Pulling latest image..."
#docker pull ghcr.io/mavsphere/layout-agent:latest
docker pull ghcr.io/mavsphere/layout-agent:ip-cam

# ── Stop existing containers ──────────────────────────────────────────────────
info "Stopping existing containers..."
docker compose down --remove-orphans 2>/dev/null || true

# ── Start fresh ───────────────────────────────────────────────────────────────
info "Starting services..."
docker compose up -d

sleep 2
echo ""
info "Status:"
docker compose ps

echo ""
echo -e "  ${BOLD}Config UI:${NC}  http://${HOST_IP}:8091"
echo ""
info "Logs (Ctrl+C to stop watching — containers keep running):"
echo ""
docker compose logs -f --tail=50
