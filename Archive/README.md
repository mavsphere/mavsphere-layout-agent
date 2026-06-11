# MavSphere Layout Agent

Local edge bridge between the MavSphere cloud backend, a DCC-EX command station, and ESP32 sensor nodes.

---

## Supported platforms

| Platform | Cameras | DCC-EX | Notes |
|----------|---------|--------|-------|
| Raspberry Pi 4 or 5 | 1–3 USB | ✅ | Good for small layouts |
| Raspberry Pi 5 | 1–4 USB | ✅ | Better CPU, same USB bandwidth limit |
| Ubuntu x86 mini PC (NUC, etc.) | 4–6 USB | ✅ | Recommended for larger layouts |
| Ubuntu 22.04 / 24.04 LTS (any x86) | 4–6 USB | ✅ | Full support |

**Ubuntu 22.04 or 24.04 LTS is the supported OS.** Raspberry Pi OS (Debian-based) also works well.

Windows is not supported. If you only have a Windows machine, run a small Ubuntu VM, or use a Raspberry Pi or cheap mini PC as the dedicated agent host. A Raspberry Pi 4 costs around £55 and handles most layouts comfortably.

**Camera guidance:**
- 1–2 cameras → Raspberry Pi 4 is fine
- 3–4 cameras → Raspberry Pi 5 or a low-power x86 mini PC
- 5–6 cameras → x86 mini PC (NUC or similar); all USB cameras share one controller on the Pi
- Multiple agent hosts are supported — a large layout can split cameras across two machines, each running their own agent instance connected to the same layout ID

---

## What it does

The layout agent runs on the machine at the layout and:

- Connects to the MavSphere backend over STOMP/WebSocket and keeps that connection alive
- Controls trains by sending DCC commands to a DCC-EX command station (USB serial or WiFi)
- Receives occupancy and RFID events from ESP32 sensor nodes via a local MQTT broker and forwards them to the backend
- Pushes signal aspect commands (RED/YELLOW/GREEN) to sensor nodes via MQTT so they can drive physical signal LEDs
- Streams camera video to the MavSphere UI via WebRTC
- Provides a local configuration web UI at port 8091

The agent is the **only** component that authenticates with the backend. Sensor nodes require no credentials.

---

## Architecture

```
[Home network — layout machine]

ESP32 sensor nodes
  │  mqtt tcp://machine-ip:1883  (LAN only, no auth needed)
  ▼
Local Mosquitto broker  (Docker, same machine as agent)
  │
Layout Agent  (Docker)
  ├── subscribes: occupancy, RFID, heartbeat from nodes
  └── publishes:  signal aspect commands to nodes
  │
  │  STOMP/WSS  ← the only outbound internet connection
  ▼
MavSphere Backend  (cloud)
  │
  ▼
MavSphere UI  (browser)
```

MQTT is purely local. The agent is the bridge between local sensor events and the cloud backend. The backend never touches MQTT directly.

### Component responsibilities

| Component    | Responsibility                                             |
|--------------|------------------------------------------------------------|
| Backend      | Layout model, routing, signalling engine, permissions      |
| Agent        | Hardware bridge — DCC-EX, cameras, MQTT sensor nodes      |
| Node (ESP32) | Sensor input and signal LED output only                    |

The train roster is provided by the backend at agent startup. Do not hardcode trains in config.json — this enables dynamic layouts, multiple agents, and consistent UI state.

---

## Deploying on the layout machine

### Quick start — download and run

You do not need to clone the repository. Download just the `deploy/` folder from the releases page, copy it to your layout machine, then:

```bash
chmod +x deploy/run-layout-agent.sh
./deploy/run-layout-agent.sh
```

The script will:
1. Check Docker is installed (and offer to install it if not)
2. Detect that this is the first run and create `deploy/config/config.json` from a template
3. Tell you exactly which fields to fill in, then exit

Fill in the config:

```bash
nano deploy/config/config.json
```

The fields you must set:

| Field | Example |
|-------|---------|
| `layoutId` | `"2"` — visible in the MavSphere UI |
| `backendUrl` | `"https://mavsphere.com"` |
| `backendWsUrl` | `"wss://mavsphere.com/api/ws/agent"` |
| `username` | your MavSphere login email |
| `password` | your MavSphere password |
| `dccEx.port` | `"/dev/ttyUSB0"` or `"192.168.1.x:2560"` for WiFi |

Leave `mqtt.brokerUrl` as `tcp://mosquitto:1883` — that is the local broker started by Docker Compose.

Then run the script again to start everything:

```bash
./deploy/run-layout-agent.sh
```

You should see in the logs:

```
[auth] login succeeded for 'youruser'
[trains] loaded N trains from backend
[topology] fetched N blocks, ...
[STOMP] connected
[STOMP] subscribed to /user/queue/layout/2/train/...
```

The layout goes Active in the MavSphere UI once the agent sends its first heartbeat.

### Adding hardware devices

Edit `deploy/docker-compose.yml` and uncomment the `devices:` section for your hardware:

```yaml
devices:
  - /dev/ttyUSB0:/dev/ttyUSB0   # DCC-EX via USB serial
  - /dev/video0:/dev/video0     # cameras — one line per camera
  - /dev/video2:/dev/video2
```

Find device names:
```bash
ls /dev/ttyUSB* /dev/ttyACM*   # DCC-EX
v4l2-ctl --list-devices          # cameras
```

Then run `./deploy/run-layout-agent.sh` again to restart with the new device config.

---

## Updating to a new release

```bash
./deploy/run-layout-agent.sh
```

The script pulls the latest image from Docker Hub, stops the running containers, and starts them fresh with the new image. Your config is never modified — it is a read-only mount into the container.

---

## Config reference

Config lives at `deploy/config/config.json`. Edit it directly or use the web UI at `http://machine-ip:8091`.

| Field | Description |
|-------|-------------|
| `layoutId` | ID of your layout (find it in the MavSphere UI) |
| `backendUrl` | Backend HTTP URL e.g. `https://mavsphere.com` |
| `backendWsUrl` | Backend WebSocket URL e.g. `wss://mavsphere.com/api/ws/agent` |
| `username` | Your MavSphere username |
| `password` | Your MavSphere password |
| `dccEx.port` | Serial path (`/dev/ttyUSB0`) or TCP address (`192.168.1.x:2560`) |
| `mqtt.brokerUrl` | `tcp://mosquitto:1883` — do not change when using Docker Compose |
| `mqtt.topicPrefix` | Must match your ESP32 nodes (default: `layout`) |

---

## DCC-EX connection modes

### USB serial (most common)

```json
"dccEx": {
  "port": "/dev/ttyUSB0",
  "baudRate": 115200
}
```

Add to `docker-compose.yml`:
```yaml
devices:
  - /dev/ttyUSB0:/dev/ttyUSB0
```

### WiFi / TCP

If your DCC-EX has a WiFi shield or is network-enabled:

```json
"dccEx": {
  "port": "192.168.1.100:2560"
}
```

No device mount needed.

---

## ESP32 sensor nodes

Flash firmware from the `mavsphere-sensor-node` repository.

Point each node's MQTT broker at `tcp://<machine-ip>:1883`. No username or password needed — the broker is local network only.

The `topicPrefix` on each node must match `mqtt.topicPrefix` in the agent config (default: `layout`).

### MQTT topic scheme

```
layout/node/{nodeId}/sensor/{sensorId}/state   ← agent subscribes
layout/node/{nodeId}/rfid/{readerId}/tag        ← agent subscribes
layout/node/{nodeId}/heartbeat                  ← agent subscribes
layout/node/{nodeId}/status                     ← agent subscribes
layout/node/{nodeId}/reply/+                    ← agent subscribes
layout/signal/{signalId}/aspect                 ← agent publishes (retained)
layout/node/{nodeId}/cmd/config/set             ← agent publishes
layout/node/{nodeId}/cmd/ping                   ← agent publishes
layout/node/{nodeId}/cmd/reboot                 ← agent publishes
```

`layout` is the configurable prefix. It must be consistent across the agent and all nodes.

---

## Cameras

Add a camera entry to `cameras` in config.json and mount the device in `docker-compose.yml`.

Find device names:
```bash
v4l2-ctl --list-devices
```

Example output:
```
USB Camera:
  /dev/video0
  /dev/video1

Second Camera:
  /dev/video2
  /dev/video3
```

Use one device path per physical camera. Skip the odd-numbered paths — `/dev/video0` and `/dev/video2` are the capture devices; `/dev/video1` and `/dev/video3` are metadata devices.

---

## Troubleshooting

### STOMP not connecting

Verify the backend is reachable:
```bash
curl http://your-backend:8081/actuator/health
```

Check `backendUrl` and `backendWsUrl` in config.json match exactly, then restart.

### MQTT not connecting

Check Mosquitto is running:
```bash
docker compose ps
docker compose logs mosquitto
```

### DCC-EX device not found

```
error gathering device information "/dev/ttyUSB0"
```

Check which device names exist:
```bash
ls /dev/ttyUSB* /dev/ttyACM*
```

Make sure the path in `docker-compose.yml` matches `dccEx.port` in config.json.

### Cameras not working

```bash
v4l2-ctl --list-devices
lsof /dev/video0    # check nothing else holds the device open
```

### Port already in use

```bash
lsof -i :8091       # config UI
lsof -i :1883       # MQTT
```

### Viewing logs

```bash
docker compose logs -f layout-agent          # follow live
docker compose logs --tail=100 layout-agent  # last 100 lines
docker compose logs -f mosquitto
```

---

## Development

### Build locally

```bash
make build                  # current platform
make build-release          # with size optimisations
make build-linux-arm64      # cross-compile for Raspberry Pi
make build-linux-amd64      # cross-compile for Linux x86
```

### Build and push Docker image (multi-arch)

The published image supports both `linux/amd64` and `linux/arm64` in a single tag.

```bash
export DOCKERHUB_USERNAME=youruser
export DOCKERHUB_TOKEN=yourtoken
make docker-login
make docker-buildx            # git SHA tag + latest
make docker-buildx TAG=v1.0.0 # explicit tag
```

### Deploy binary directly to Pi (without Docker)

```bash
make build-linux-arm64
scp bin/layout-agent-linux-arm64 pi@192.168.1.x:~/mavsphere/
```

Install Mosquitto on the Pi:
```bash
sudo apt install mosquitto mosquitto-clients
sudo systemctl enable mosquitto
```

Set `mqtt.brokerUrl` to `tcp://mosquitto:1883` in this case.

### Run for local testing

```bash
make docker-run
```

Uses `--network host` so `tcp://mosquitto:1883` reaches any Mosquitto on the host.

### Make targets

```
make build                  Build for current platform
make build-release          Build with -ldflags -s -w
make build-linux-amd64      Cross-compile linux/amd64
make build-linux-arm64      Cross-compile linux/arm64 (Raspberry Pi)
make docker-build           Single-platform Docker image (no push)
make docker-buildx          Multi-arch build + push to Docker Hub
make docker-buildx-load     Multi-arch build, load amd64 locally (no push)
make docker-run             Run with host network (dev/testing)
make clean                  Remove build outputs
make help                   Show all targets with notes
```

---

## Notes

- MQTT auth is not required on the local broker — the local network is the trust boundary
- The agent is the only component that authenticates with the backend; nodes never need credentials
- Train roster is fetched from the backend at startup — do not hardcode trains in config.json
- Designed for local-first operation: the agent continues handling hardware if the backend connection drops temporarily
- The DCC-EX failsafe stops trains automatically if the backend connection is lost for too long

---

## Future enhancements

- Multi-camera switching and stream layout mapping
- Automatic camera detection and hot-plug support
- Health and status dashboard in the local config UI
- RFID tag → train registration flow in the UI
