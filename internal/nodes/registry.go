package nodes

import (
	"sync"
	"time"
)

type SensorState struct {
	Payload   map[string]any `json:"payload"`
	UpdatedAt time.Time      `json:"updatedAt"`
}

type ReplyState struct {
	Type      string         `json:"type"`
	Payload   map[string]any `json:"payload"`
	UpdatedAt time.Time      `json:"updatedAt"`
}

type NodeState struct {
	NodeID        string    `json:"nodeId"`
	Online        bool      `json:"online"`
	LastSeen      time.Time `json:"lastSeen"`
	LastHeartbeat time.Time `json:"lastHeartbeat"`
	LastSensor    time.Time `json:"lastSensor,omitempty"`
	LastRfid      time.Time `json:"lastRfidTime,omitempty"`

	IP            string `json:"ip"`
	RSSI          int    `json:"rssi"`
	WifiConnected bool   `json:"wifiConnected"`
	MqttConnected bool   `json:"mqttConnected"`
	ApMode        bool   `json:"apMode"`

	Sensors         map[string]SensorState `json:"sensors"`
	LastRfidPayload map[string]any         `json:"lastRfid,omitempty"`
	LastReply       *ReplyState            `json:"lastReply,omitempty"`
}

// Health returns "online", "stale", or "offline" based on heartbeat age.
func (n *NodeState) Health() string {
	if n == nil {
		return "offline"
	}
	age := time.Since(n.LastHeartbeat)
	switch {
	case age < 15*time.Second:
		return "online"
	case age < 60*time.Second:
		return "stale"
	default:
		return "offline"
	}
}

type Registry struct {
	mu    sync.RWMutex
	nodes map[string]*NodeState
}

func NewRegistry() *Registry {
	return &Registry{
		nodes: make(map[string]*NodeState),
	}
}

func (r *Registry) getOrCreate(nodeID string) *NodeState {
	n, ok := r.nodes[nodeID]
	if !ok {
		n = &NodeState{
			NodeID:  nodeID,
			Sensors: make(map[string]SensorState),
		}
		r.nodes[nodeID] = n
	}
	return n
}

// IsOnline returns true if the node sent a heartbeat within the last 15 seconds.
func (r *Registry) IsOnline(nodeID string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	n, ok := r.nodes[nodeID]
	if !ok {
		return false
	}
	return time.Since(n.LastHeartbeat) < 15*time.Second
}

func (r *Registry) UpdateHeartbeat(nodeID string, payload map[string]any) {
	r.mu.Lock()
	defer r.mu.Unlock()

	n := r.getOrCreate(nodeID)
	now := time.Now()
	n.Online = true
	n.LastSeen = now
	n.LastHeartbeat = now

	applyNodeStatusFields(n, payload)
}

func (r *Registry) UpdateStatus(nodeID string, payload map[string]any) {
	r.mu.Lock()
	defer r.mu.Unlock()

	n := r.getOrCreate(nodeID)
	n.Online = true
	n.LastSeen = time.Now()

	applyNodeStatusFields(n, payload)
}

func (r *Registry) UpdateSensor(nodeID, sensorID string, payload map[string]any) {
	r.mu.Lock()
	defer r.mu.Unlock()

	n := r.getOrCreate(nodeID)
	now := time.Now()
	n.Online = true
	n.LastSeen = now
	n.LastSensor = now
	n.Sensors[sensorID] = SensorState{
		Payload:   payload,
		UpdatedAt: now,
	}
}

func (r *Registry) UpdateRfid(nodeID string, payload map[string]any) {
	r.mu.Lock()
	defer r.mu.Unlock()

	n := r.getOrCreate(nodeID)
	now := time.Now()
	n.Online = true
	n.LastSeen = now
	n.LastRfid = now
	n.LastRfidPayload = payload
}

func (r *Registry) UpdateReply(nodeID, replyType string, payload map[string]any) {
	r.mu.Lock()
	defer r.mu.Unlock()

	n := r.getOrCreate(nodeID)
	n.Online = true
	n.LastSeen = time.Now()
	n.LastReply = &ReplyState{
		Type:      replyType,
		Payload:   payload,
		UpdatedAt: time.Now(),
	}
}

func (r *Registry) Snapshot() []*NodeState {
	r.mu.RLock()
	defer r.mu.RUnlock()

	out := make([]*NodeState, 0, len(r.nodes))
	for _, n := range r.nodes {
		copyNode := *n
		copyNode.Sensors = make(map[string]SensorState, len(n.Sensors))
		for k, v := range n.Sensors {
			copyNode.Sensors[k] = v
		}
		// Update derived Online flag based on heartbeat age
		copyNode.Online = copyNode.Health() != "offline"
		out = append(out, &copyNode)
	}
	return out
}

// applyNodeStatusFields extracts common status fields from a payload map.
func applyNodeStatusFields(n *NodeState, payload map[string]any) {
	if v, ok := payload["ipAddress"].(string); ok {
		n.IP = v
	}
	if v, ok := payload["ip"].(string); ok && n.IP == "" {
		n.IP = v
	}
	if v, ok := payload["rssi"].(float64); ok {
		n.RSSI = int(v)
	}
	if v, ok := payload["wifiConnected"].(bool); ok {
		n.WifiConnected = v
	}
	if v, ok := payload["mqttConnected"].(bool); ok {
		n.MqttConnected = v
	}
	if v, ok := payload["apMode"].(bool); ok {
		n.ApMode = v
	}
}

func (r *Registry) BuildBlockStateSnapshot() map[string]any {
	r.mu.RLock()
	defer r.mu.RUnlock()

	blockStates := make(map[string]any)

	for _, node := range r.nodes {
		for sensorID, sensor := range node.Sensors {
			state, _ := sensor.Payload["state"].(string)

			active := state == "OCCUPIED" || state == "BROKEN"

			blockStates[sensorID] = active
		}
	}

	return blockStates
}
