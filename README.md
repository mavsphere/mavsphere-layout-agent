# MavSphere Layout Agent

The **MavSphere Layout Agent** bridges local layout hardware (DCC-EX, sensors, cameras) to the MavSphere cloud backend.

It supports **two deployment modes**:

* **Real layout mode** — talks to real DCC-EX hardware
* **Simulator mode** — talks to `mavsphere-layout-simulator` instead of real hardware

---

## Quick start — download and run

You do not need to clone the repository. Download just the `deploy/` folder from the releases page and copy it to your layout machine, then:

```bash
chmod +x deploy/run-layout-agent.sh
chmod +x deploy/run-layout-agent-with-sim.sh
```

---

## Choose your mode

### 1. Real layout mode (default)

* Uses real DCC-EX hardware
* Uses config: `deploy/config-agent/config.json`

Run:

```bash
cd deploy
./run-layout-agent.sh
```

---

### 2. Simulator mode

* No hardware required
* Uses `mavsphere-layout-simulator`
* Uses configs:

  * `deploy/config-agent-with-sim/config.json`
  * `deploy/config-simulator/config.json`

Run:

```bash
cd deploy
./run-layout-agent-with-sim.sh
```

---

## First run behavior

On first run, the script will:

1. Check Docker is installed (and offer to install it if not)
2. Detect missing config files
3. Create default config(s)
4. Tell you exactly what to edit
5. Exit safely

---

## Configure the layout-agent

Edit the config:

```bash
nano deploy/config-agent/config.json
```

### Required fields

| Field          | Example                              |
| -------------- | ------------------------------------ |
| `layoutId`     | `"2"` — visible in the MavSphere UI  |
| `backendUrl`   | `"https://mavsphere.com"`            |
| `backendWsUrl` | `"wss://mavsphere.com/api/ws/agent"` |
| `username`     | your MavSphere login email           |
| `password`     | your MavSphere password              |

---

### DCC-EX configuration

#### Real hardware

```json
"dccEx": {
  "port": "/dev/ttyACM0"
}
```

or WiFi:

```json
"dccEx": {
  "port": "192.168.1.x:2560"
}
```

#### Simulator mode

```json
"dccEx": {
  "port": "simulator:2560"
}
```

---

### MQTT configuration

Leave as:

```json
"mqtt": {
  "brokerUrl": "tcp://mosquitto:1883",
  "topicPrefix": "layout"
}
```

* The layout-agent connects to Mosquitto via the **service name** inside Docker Compose.
* Sensor nodes outside Docker should still connect to the host IP:

```text
tcp://<host-ip>:1883
```

---

## Run again

After editing config:

* Real layout mode:

```bash
./run-layout-agent.sh
```

* Simulator mode:

```bash
./run-layout-agent-with-sim.sh
```

---

## Expected output

You should see logs like:

```
[auth] login succeeded for 'youruser'
[trains] loaded N trains from backend
[topology] fetched N blocks, ...
[STOMP] connected
[STOMP] subscribed to /user/queue/layout/2/train/...
[mqtt] connected
```

---

## When is it “working”?

The layout becomes **Active in the MavSphere UI** when:

1. The agent successfully logs in
2. Topology is loaded
3. STOMP is connected
4. First heartbeat is sent

* In simulator mode, trains appear active without real hardware
* In real mode, trains remain offline until DCC-EX is connected

---

## Deploy folder structure

```
deploy/
  docker-compose.yml
  docker-compose.sim.yml
  update.sh
  run-layout-agent-with-sim.sh
  config-agent/
    config.json
  config-agent-with-sim/
    config.json
  config-simulator/
    config.json
  mosquitto/
    mosquitto.conf
```

### Config folders

* `config-agent/config.json` — real layout mode
* `config-agent-with-sim/config.json` — simulator mode
* `config-simulator/config.json` — simulator configuration

---

## Docker Compose overview

### Base stack (`docker-compose.yml`)

* `mosquitto` — local MQTT broker for ESP32 sensor nodes
* `layout-agent` — main agent

### Simulator overlay (`docker-compose.sim.yml`)

* Adds `simulator` service
* Layout-agent points to `config-agent-with-sim/config.json`
* Simulator connects to agent and Mosquitto

---

## Hardware wiring

### DCC-EX

* **Serial (recommended):** Connect Arduino running DCC-EX to the Pi/server via USB
  Set `dccEx.port` to `/dev/ttyUSB0` (or `/dev/ttyACM*`)
* **TCP/WiFi:** If DCC-EX is on a WiFi shield, set `dccEx.port` to `"192.168.1.x:2560"`

### Sensor nodes (ESP32)

* Flash firmware from `mavsphere-sensor-node`
* Ensure `mqtt.topicPrefix` matches layout-agent config
* Topics:

```
layout/node/{nodeId}/sensor/{sensorId}/state
layout/node/{nodeId}/config
```

### Cameras

Before configuring cameras, see [CameraSetupGuide.md](./CameraSetupGuide.md) for commands to list, test, and select the correct camera devices.

* USB webcam connected to host
* Set `cameras[].device` to `/dev/video0` (or `/dev/video2`)
* Pass `--device /dev/video0` when running Docker

---

## Simulator integration

* Exposes a fake DCC-EX TCP endpoint
* Polls the layout-agent for topology and routes
* Publishes simulated sensor events over MQTT

Simulator connects to:

* `http://layout-agent:8091`
* `tcp://mosquitto:1883`

Enabled via:

* `deploy/docker-compose.sim.yml`
* `deploy/run-layout-agent-with-sim.sh`

---

## Script permissions

Make scripts executable if needed:

```bash
chmod +x deploy/update.sh
chmod +x deploy/run-layout-agent-with-sim.sh
```

---

## Notes

* Always check `docker ps` to ensure no stale containers
* Simulator mode can be used without hardware for development
* Real layout mode requires DCC-EX connected
* Ensure the config JSON is consistent with the mode you are running

---

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md) for the contribution workflow and licence terms.

The layout agent is MIT licensed with no restrictions.