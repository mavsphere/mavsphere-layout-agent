#!/usr/bin/env bash
# run-layout-agent-with-sim.sh
#
# Runs the MavSphere layout-agent stack in simulator mode.
#
# Services:
#   - mosquitto
#   - simulator
#   - layout-agent
#
# Uses:
#   ./config-agent-with-sim/config.json   -> layout-agent config for simulator mode
#   ./config-simulator/config.json        -> simulator config
#
# Assumes everything runs on the same machine.
# The script auto-detects the machine LAN IP and updates simulator config so
# the simulator can reach the layout-agent UI/API over that IP.

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

COMPOSE_FILES=(-f docker-compose.yml -f docker-compose.sim.yml)

if ! command -v docker >/dev/null 2>&1; then
  warning "Docker is not installed."
  echo ""
  read -r -p "Install Docker now? [y/N] " response
  if [[ "$response" =~ ^[Yy]$ ]]; then
    info "Installing Docker..."
    curl -fsSL https://get.docker.com | sh
    sudo usermod -aG docker "$USER"
    echo ""
    warning "Docker installed. Log out and back in, then run ./deploy/run-layout-agent-with-sim.sh again."
    exit 0
  else
    error "Docker is required. Install it from https://get.docker.com and try again."
  fi
fi

if ! docker compose version >/dev/null 2>&1; then
  error "Docker Compose v2 not found. Run: sudo apt install docker-compose-plugin"
fi

if ! command -v python3 >/dev/null 2>&1; then
  error "python3 is required by this script"
fi

# ---- Detect host IP (used by simulator -> agent and agent -> simulator) ----
if [ -z "${HOST_IP:-}" ]; then
  HOST_IP="$(ip route get 1.1.1.1 2>/dev/null | awk '/src/ {
    for (i=1;i<=NF;i++) if ($i=="src") { print $(i+1); exit }
  }')"
fi

if [ -z "${HOST_IP:-}" ]; then
  HOST_IP="$(hostname -I 2>/dev/null | awk '{print $1}')"
fi

if [ -z "${HOST_IP:-}" ]; then
  error "Could not determine HOST_IP. Re-run with HOST_IP=x.x.x.x ./run-layout-agent-with-sim.sh"
fi

export HOST_IP
info "Using HOST_IP=$HOST_IP"

# Create layout-agent simulator-mode config if missing
if [ ! -f "config-agent-with-sim/config.json" ]; then
  header "First time simulator setup — creating layout-agent simulator config"
  mkdir -p config-agent-with-sim

  if [ -f "config-agent/config.json" ]; then
    cp config-agent/config.json config-agent-with-sim/config.json
  elif [ -f "../config/config.json" ]; then
    cp ../config/config.json config-agent-with-sim/config.json
  elif [ -f "../config.template.json" ]; then
    cp ../config.template.json config-agent-with-sim/config.json
  else
    error "Could not find a base layout-agent config to copy."
  fi

  warning "Created ./config-agent-with-sim/config.json"
  echo "Edit it so that:"
  echo "  - mqtt.brokerUrl points to your host broker, e.g. tcp://${HOST_IP}:1883"
  echo "  - dccEx.host uses ${HOST_IP}"
  echo "  - dccEx.port uses 2560"
  echo ""
  echo "Then run this script again."
  exit 0
fi

# Create simulator config if missing
if [ ! -f "config-simulator/config.json" ]; then
  header "First time simulator setup — creating simulator config"
  mkdir -p config-simulator

  cat > config-simulator/config.json <<JSON
{
  "layoutId": "1",
  "agentUrl": "http://${HOST_IP}:8091",
  "mqtt": {
    "brokerUrl": "tcp://mosquitto:1883",
    "topicPrefix": "layout"
  },
  "dccEx": {
    "listenAddr": "0.0.0.0:2560"
  },
  "simNodeId": "sim-node-01",
  "physicsTickMs": 100,
  "routePollMs": 500,
  "defaultBlockLengthMm": 500
}
JSON

  warning "Created ./config-simulator/config.json"
  echo "Edit layoutId if needed, then run this script again."
  exit 0
fi

# Keep simulator config aligned to current machine IP
info "Updating simulator config with current HOST_IP..."
python3 - <<PY
import json
from pathlib import Path

p = Path("config-simulator/config.json")
data = json.loads(p.read_text())

data["agentUrl"] = "http://${HOST_IP}:8091"

mqtt = data.setdefault("mqtt", {})
mqtt.setdefault("brokerUrl", "tcp://mosquitto:1883")
mqtt.setdefault("topicPrefix", "layout")

dccex = data.setdefault("dccEx", {})
dccex.setdefault("listenAddr", "0.0.0.0:2560")

p.write_text(json.dumps(data, indent=2) + "\n")
print("Updated config-simulator/config.json:")
print("  agentUrl =", data["agentUrl"])
PY

header "Updating to latest simulator release"
info "Pulling latest images..."
docker compose "${COMPOSE_FILES[@]}" pull

info "Stopping existing containers..."
docker compose "${COMPOSE_FILES[@]}" down --remove-orphans 2>/dev/null || true

info "Starting Mosquitto and simulator..."
docker compose "${COMPOSE_FILES[@]}" up -d mosquitto simulator

info "Waiting for simulator startup..."
for i in {1..30}; do
  if docker compose "${COMPOSE_FILES[@]}" ps simulator 2>/dev/null | grep -q "Up"; then
    break
  fi
  sleep 1
done

info "Waiting for simulator DCC-EX listener..."
for i in {1..30}; do
  if docker compose "${COMPOSE_FILES[@]}" logs simulator 2>/dev/null | grep -q "DCC-EX server listening on"; then
    info "Simulator reported DCC-EX listener ready"
    break
  fi
  sleep 1
  if [ "$i" -eq 30 ]; then
    warning "Simulator did not report DCC-EX readiness in time; starting layout-agent anyway."
  fi
done

info "Starting layout-agent..."
docker compose "${COMPOSE_FILES[@]}" up -d layout-agent

sleep 2
echo ""
info "Status:"
docker compose "${COMPOSE_FILES[@]}" ps

echo ""
echo -e "  ${BOLD}Host IP:${NC}    ${HOST_IP}"
echo -e "  ${BOLD}Config UI:${NC}  http://${HOST_IP}:8091"
echo ""
warning "This script assumes docker-compose.sim.yml publishes simulator DCC-EX on host port 2560."
warning "If agent still shows 'connect refused' to ${HOST_IP}:2560, add: ports: [\"2560:2560\"] to the simulator service."

echo ""
info "Logs (Ctrl+C to stop watching — containers keep running):"
echo ""
docker compose "${COMPOSE_FILES[@]}" logs -f --tail=80