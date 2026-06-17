// Package backend manages the STOMP connection from the layout agent to MavSphere.
package backend

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/go-stomp/stomp"

	"github.com/mavsphere/mavsphere-layout-agent/internal/commandstation"
	"github.com/mavsphere/mavsphere-layout-agent/internal/mqtt"
	"github.com/mavsphere/mavsphere-layout-agent/pkg/auth"
	"github.com/mavsphere/mavsphere-layout-agent/pkg/config"
	agentmedia "github.com/mavsphere/mavsphere-layout-agent/pkg/media"
	"github.com/mavsphere/mavsphere-layout-agent/pkg/stomp/wsconn"
)

// ── Public message types ──────────────────────────────────────────────────────

// ControlMessage arrives from the backend for a specific train.
type ControlMessage struct {
	Type       string  `json:"type"`
	Speed      float64 `json:"speed"`
	Forward    bool    `json:"forward"`
	Function   int     `json:"function"`
	FunctionOn bool    `json:"functionOn"`
	RouteKey   string  `json:"routeKey,omitempty"`

	TurnoutID         string `json:"turnoutId,omitempty"`
	TurnoutDccAddress int    `json:"turnoutDccAddress,omitempty"`
	Position          string `json:"position,omitempty"`
}

// StreamCommand arrives from the backend to start/stop a camera stream.
type StreamCommand struct {
	Type       string               `json:"type"`
	CameraSlug string               `json:"cameraSlug"`
	RoomID     int64                `json:"roomId"`
	ICE        agentmedia.ICEConfig `json:"ice,omitempty"`
}

// TrainState is the richer state published from agent → backend → UI.
type TrainState struct {
	Online            bool            `json:"online"`
	RequestedSpeed    float64         `json:"requestedSpeed"`
	AppliedSpeed      float64         `json:"appliedSpeed"`
	Direction         string          `json:"direction"`
	MovementAuthority string          `json:"movementAuthority"`
	MovementReason    string          `json:"movementReason"`
	CurrentBlock      string          `json:"currentBlock,omitempty"`
	NextBlock         string          `json:"nextBlock,omitempty"`
	SignalAspect      string          `json:"signalAspect,omitempty"`
	ActiveRouteId     int64           `json:"activeRouteId,omitempty"`
	ActiveRouteName   string          `json:"activeRouteName,omitempty"`
	DccThrottleActive bool            `json:"dccThrottleActive"`
	Functions         map[string]bool `json:"functions,omitempty"`
	LastUpdateTime    string          `json:"lastUpdateTime"`
}

// NodeCommand is a command from the backend targeting a specific sensor node.
type NodeCommand struct {
	NodeID  string         `json:"nodeId"`
	Command string         `json:"command"`
	Payload map[string]any `json:"payload,omitempty"`
}

// ActiveRoute is a cached route received from a ROUTE-type STOMP event.
type ActiveRoute struct {
	RouteKey      string   `json:"routeKey"`
	TrainID       *int64   `json:"trainId"`
	Status        string   `json:"status"`
	BlockSequence []string `json:"blockSequence"`
	Timestamp     int64    `json:"timestamp"`
}

// BackendConnState tracks observable connection state for the status endpoint.
type BackendConnState struct {
	Connected         bool      `json:"connected"`
	LastConnect       time.Time `json:"lastConnect,omitempty"`
	LastDisconnect    time.Time `json:"lastDisconnect,omitempty"`
	LastHeartbeatSent time.Time `json:"lastHeartbeatSent,omitempty"`
	ReconnectCount    int       `json:"reconnectCount"`
}

// Handlers wired by main.go.
type Handlers struct {
	OnTrainControl  func(trainID int64, msg ControlMessage)
	OnStreamCommand func(cmd StreamCommand)
	OnNodeCommand   func(cmd NodeCommand)
	OnSignalUpdate  func(signals map[string]string)
	OnPolicy        func(payload map[string]any)
	OnConnected     func()
	TokenRefresh    func() (string, error)
}

// Session wraps the STOMP connection to the MavSphere backend.
type Session struct {
	cfg        *config.AgentConfig
	cs         commandstation.CommandStation
	csFailsafe *commandstation.Failsafe
	mqttBridge *mqtt.Bridge
	logger     *log.Logger
	handlers   Handlers

	// conn is guarded by connMu — never access directly.
	connMu sync.RWMutex
	conn   *stomp.Conn
	state  BackendConnState

	iceMu sync.RWMutex
	ice   agentmedia.ICEConfig

	requestedMu    sync.RWMutex
	requestedSpeed map[int]float64

	routesMu sync.RWMutex
	routes   map[string]*ActiveRoute
}

func NewSession(
	cfg *config.AgentConfig,
	cs commandstation.CommandStation,
	csFailsafe *commandstation.Failsafe,
	mqttBridge *mqtt.Bridge,
	handlers Handlers,
	logger *log.Logger,
) *Session {
	if logger == nil {
		logger = log.Default()
	}
	return &Session{
		cfg:            cfg,
		cs:             cs,
		csFailsafe:     csFailsafe,
		mqttBridge:     mqttBridge,
		handlers:       handlers,
		logger:         logger,
		requestedSpeed: make(map[int]float64),
		routes:         make(map[string]*ActiveRoute),
	}
}

// ── Connection state helpers ──────────────────────────────────────────────────

func (s *Session) setConn(c *stomp.Conn) {
	s.connMu.Lock()
	s.conn = c
	s.state.Connected = true
	s.state.LastConnect = time.Now()
	onConnected := s.handlers.OnConnected
	s.connMu.Unlock()

	if onConnected != nil {
		go onConnected()
	}
}

func (s *Session) clearConn() {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	s.conn = nil
	s.state.Connected = false
	s.state.LastDisconnect = time.Now()
	s.state.ReconnectCount++
}

func (s *Session) getConn() *stomp.Conn {
	s.connMu.RLock()
	defer s.connMu.RUnlock()
	return s.conn
}

// IsConnected returns true if there is an active STOMP connection.
func (s *Session) IsConnected() bool {
	s.connMu.RLock()
	defer s.connMu.RUnlock()
	return s.conn != nil && s.state.Connected
}

// ConnState returns a snapshot of connection state for the status endpoint.
func (s *Session) ConnState() BackendConnState {
	s.connMu.RLock()
	defer s.connMu.RUnlock()
	return s.state
}

// LastHeartbeatMs returns the last heartbeat time in Unix ms (0 if never sent).
func (s *Session) LastHeartbeatMs() int64 {
	s.connMu.RLock()
	defer s.connMu.RUnlock()
	if s.state.LastHeartbeatSent.IsZero() {
		return 0
	}
	return s.state.LastHeartbeatSent.UnixMilli()
}

// ── Publish helper ────────────────────────────────────────────────────────────

// publish sends a JSON body to a STOMP destination.
// Returns an error if not connected — callers may log and continue.
func (s *Session) publish(dest string, body []byte) error {
	conn := s.getConn()
	if conn == nil {
		return fmt.Errorf("not connected")
	}
	if err := conn.Send(dest, "application/json", body); err != nil {
		return err
	}
	s.connMu.Lock()
	s.state.LastHeartbeatSent = time.Now()
	s.connMu.Unlock()
	return nil
}

// ── Route cache ───────────────────────────────────────────────────────────────

// GetActiveRoutes returns a snapshot of LOCKED/ACTIVE/RELEASING routes.
func (s *Session) GetActiveRoutes() []ActiveRoute {
	s.routesMu.RLock()
	defer s.routesMu.RUnlock()
	out := make([]ActiveRoute, 0, len(s.routes))
	for _, r := range s.routes {
		out = append(out, *r)
	}
	return out
}

// ── Other accessors ───────────────────────────────────────────────────────────

// CurrentICE returns the most recently received ICE config.
func (s *Session) CurrentICE() agentmedia.ICEConfig {
	s.iceMu.RLock()
	defer s.iceMu.RUnlock()
	return s.ice
}

func (s *Session) SetRequestedSpeed(dccAddr int, speed float64) {
	s.requestedMu.Lock()
	s.requestedSpeed[dccAddr] = speed
	s.requestedMu.Unlock()
}

func (s *Session) GetRequestedSpeed(dccAddr int) float64 {
	s.requestedMu.RLock()
	defer s.requestedMu.RUnlock()
	return s.requestedSpeed[dccAddr]
}

// ── Run loop ──────────────────────────────────────────────────────────────────

// Run connects (with retry) and blocks until ctx is cancelled.
func (s *Session) Run(ctx context.Context, initialToken string) {
	token := initialToken
	backoff := 5 * time.Second // start at 5s — avoids burning through the login rate-limit window on rapid reconnects

	// Throttle token refresh so a reconnect storm cannot burn through the
	// backend's per-username login window (5 failures / 15 min).
	const tokenRefreshCooldown = 2 * time.Minute
	var lastTokenRefresh time.Time
	var rateLimitedUntil time.Time

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		err := s.connect(ctx, token)
		if err != nil {
			s.logger.Printf("[STOMP] connect error: %v — retry in %s", err, backoff)

			if s.handlers.TokenRefresh != nil {
				now := time.Now()

				// If we are still inside a backend-specified rate-limit window, skip the refresh.
				if now.Before(rateLimitedUntil) {
					remaining := rateLimitedUntil.Sub(now).Round(time.Second)
					s.logger.Printf("[STOMP] skipping token refresh — rate-limited for another %v", remaining)
				} else if now.Sub(lastTokenRefresh) < tokenRefreshCooldown {
					// Throttle: don't call Login more than once per cooldown period.
					nextAllowed := lastTokenRefresh.Add(tokenRefreshCooldown)
					s.logger.Printf("[STOMP] skipping token refresh — throttled until %s", nextAllowed.Format("15:04:05"))
				} else {
					lastTokenRefresh = now
					t, rErr := s.handlers.TokenRefresh()
					if rErr == nil {
						token = t
						s.logger.Printf("[STOMP] token refreshed")
					} else if errors.Is(rErr, auth.ErrRateLimited) {
						wait := 15 * time.Minute
						if rle, ok := auth.AsRateLimit(rErr); ok {
							wait = rle.RetryAfter
						}
						rateLimitedUntil = now.Add(wait)
						s.logger.Printf("[STOMP] token refresh rate limited — pausing until %s (%v)",
							rateLimitedUntil.Format("15:04:05"), wait.Round(time.Second))
					} else if errors.Is(rErr, auth.ErrTokenRevoked) {
						s.logger.Printf("[STOMP] token refresh: agent token revoked — restarting into pairing mode")
						// Don't duplicate the clear-token-and-save logic here; just exit
						// and let the next startup's auth.Login() call hit the same
						// ErrTokenRevoked path in main.go, which clears the token and
						// restarts into pairing.
						os.Exit(0)
					} else {
						s.logger.Printf("[STOMP] token refresh failed: %v", rErr)
					}
				}
			}

			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			if backoff < 30*time.Second {
				backoff *= 2
			}
			continue
		}

		backoff = 5 * time.Second // reset on successful connect
		s.logger.Printf("[STOMP] connected")
		s.runLoop(ctx)
		s.clearConn()
	}
}

func (s *Session) connect(ctx context.Context, token string) error {
	conn, err := wsconn.DialStompOverWebSocket(s.cfg.BackendWsURL, token, s.cfg.LayoutID)
	if err != nil {
		return fmt.Errorf("stomp dial: %w", err)
	}
	s.setConn(conn)
	return nil
}

func (s *Session) runLoop(ctx context.Context) {
	defer func() {
		conn := s.getConn()
		if conn != nil {
			_ = conn.Disconnect()
		}
	}()

	layoutID := s.cfg.LayoutID

	// Per-train control subscriptions
	for _, train := range s.cfg.Trains {
		if train.TrainID == 0 {
			s.logger.Printf("[STOMP] WARNING: train '%s' has no trainId — skipping subscribe", train.TrainSlug)
			continue
		}
		dest := fmt.Sprintf("/user/queue/layout/%s/train/%d/control", layoutID, train.TrainID)
		conn := s.getConn()
		sub, err := conn.Subscribe(dest, stomp.AckAuto)
		if err != nil {
			s.logger.Printf("[STOMP] subscribe error %s: %v", dest, err)
			continue
		}
		s.logger.Printf("[STOMP] subscribed to %s", dest)
		go s.safeSubscribe("train-control", func() {
			s.drainTrainControl(ctx, sub, train.DccAddress, train.TrainID)
		})
	}

	// Camera stream command subscriptions
	for _, cam := range s.cfg.Cameras {
		dest := fmt.Sprintf("/user/queue/layout/%s/camera/%s/commands", layoutID, cam.CameraID)
		conn := s.getConn()
		sub, err := conn.Subscribe(dest, stomp.AckAuto)
		if err != nil {
			s.logger.Printf("[STOMP] subscribe error %s: %v", dest, err)
			continue
		}
		go s.safeSubscribe("stream-commands", func() {
			s.drainStreamCommands(ctx, sub, cam.CameraID)
		})
	}

	conn := s.getConn()

	var policyCh <-chan *stomp.Message

	policySub, err := conn.Subscribe(
		fmt.Sprintf("/user/queue/layout/%s/policy", layoutID), stomp.AckAuto)
	if err != nil {
		s.logger.Printf("[STOMP] subscribe policy error: %v", err)
	} else {
		policyCh = policySub.C
	}

	nodeCommandSub, err := conn.Subscribe(
		fmt.Sprintf("/user/queue/layout/%s/node/commands", layoutID), stomp.AckAuto)
	if err != nil {
		s.logger.Printf("[STOMP] subscribe node commands error: %v", err)
	} else {
		s.logger.Printf("[STOMP] subscribed to node commands for layout %s", layoutID)
		go s.safeSubscribe("node-commands", func() {
			s.drainNodeCommands(ctx, nodeCommandSub)
		})
	}

	layoutStateSub, err := conn.Subscribe(
		fmt.Sprintf("/topic/layout/%s/state", layoutID), stomp.AckAuto)
	if err != nil {
		s.logger.Printf("[STOMP] subscribe layout state error: %v", err)
	} else {
		s.logger.Printf("[STOMP] subscribed to layout state for signal aspects")
		go s.safeSubscribe("layout-state", func() {
			s.drainLayoutState(ctx, layoutStateSub)
		})
	}

	s.sendHeartbeat()

	hbTicker := time.NewTicker(5 * time.Second)
	defer hbTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-hbTicker.C:
			s.sendHeartbeat()
		case msg, ok := <-policyCh:
			if !ok {
				s.logger.Println("[STOMP] policy sub closed — reconnecting")
				return
			}
			var payload map[string]any
			if err := json.Unmarshal(msg.Body, &payload); err == nil {
				if iceRaw, ok := payload["ice"]; ok {
					b, _ := json.Marshal(iceRaw)
					var ice agentmedia.ICEConfig
					if json.Unmarshal(b, &ice) == nil {
						s.iceMu.Lock()
						s.ice = ice
						s.iceMu.Unlock()
						s.logger.Printf("[STOMP] ICE config updated from policy push")
					}
				}
				s.handlers.OnPolicy(payload)
			}
		}
	}
}

// safeSubscribe runs fn in a goroutine with panic recovery.
func (s *Session) safeSubscribe(name string, fn func()) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				s.logger.Printf("[STOMP] panic in subscription '%s': %v", name, r)
			}
		}()
		fn()
	}()
}

// ── Dynamic train subscription ────────────────────────────────────────────────

// SubscribeNewTrains subscribes to the control queues for trains that are not
// yet subscribed in the current STOMP connection. Call this when new trains
// are detected by the periodic train-sync loop — no process restart needed.
func (s *Session) SubscribeNewTrains(ctx context.Context, trains []config.TrainConfig) {
	conn := s.getConn()
	if conn == nil {
		return // not connected; subscriptions are set up on next reconnect anyway
	}
	layoutID := s.cfg.LayoutID
	for _, train := range trains {
		if train.TrainID == 0 {
			s.logger.Printf("[STOMP] WARNING: new train '%s' has no trainId — skipping subscribe", train.TrainSlug)
			continue
		}
		dest := fmt.Sprintf("/user/queue/layout/%s/train/%d/control", layoutID, train.TrainID)
		sub, err := conn.Subscribe(dest, stomp.AckAuto)
		if err != nil {
			s.logger.Printf("[STOMP] subscribe error for new train %s: %v", dest, err)
			continue
		}
		s.logger.Printf("[STOMP] subscribed to new train control queue: %s", dest)
		t := train // capture loop var
		go s.safeSubscribe("train-control", func() {
			s.drainTrainControl(ctx, sub, t.DccAddress, t.TrainID)
		})
	}
}

// ── Heartbeat ─────────────────────────────────────────────────────────────────

func (s *Session) sendHeartbeat() {
	dest := fmt.Sprintf("/app/layout/%s/heartbeat", s.cfg.LayoutID)

	type trainEntry struct {
		TrainID      int64    `json:"trainId"`
		TrainSlug    string   `json:"trainSlug"`
		DisplayName  string   `json:"displayName"`
		DccAddress   int      `json:"dccAddress"`
		Online       bool     `json:"online"`
		Capabilities []string `json:"capabilities"`
	}
	type camEntry struct {
		CameraSlug string `json:"cameraSlug"`
		Label      string `json:"label"`
		CameraType string `json:"cameraType"`
		TrainSlug  string `json:"trainSlug,omitempty"`
		Online     bool   `json:"online"`
	}

	var trainCaps []string
	if s.cfg.Jmri.Enabled && s.cfg.Jmri.URL != "" {
		// JMRI mode: agent is a throttle proxy only — JMRI owns all layout state.
		trainCaps = []string{"TRAIN_CONTROL", "JMRI"}
	} else {
		// Native/DCC-EX mode: agent owns layout state, signals, and block occupancy.
		trainCaps = []string{"TRAIN_CONTROL", "LAYOUT_STATE", "BLOCK_OCCUPANCY", "SIGNAL_STATE", "DCC_EX"}
		if s.cfg.Mqtt.BrokerURL != "" {
			trainCaps = append(trainCaps, "MQTT_SENSORS")
		}
	}
	if len(s.cfg.Cameras) > 0 {
		trainCaps = append(trainCaps, "MULTI_CAMERA")
	}

	trains := make([]trainEntry, 0, len(s.cfg.Trains))
	for _, t := range s.cfg.Trains {
		trains = append(trains, trainEntry{
			TrainID: t.TrainID, TrainSlug: t.TrainSlug,
			DisplayName: t.DisplayName, DccAddress: t.DccAddress,
			Online: s.cs.IsConnected(), Capabilities: trainCaps,
		})
	}
	cams := make([]camEntry, 0, len(s.cfg.Cameras))
	for _, c := range s.cfg.Cameras {
		cams = append(cams, camEntry{
			CameraSlug: c.CameraID, Label: c.Label,
			CameraType: c.Type, TrainSlug: c.TrainSlug, Online: true,
		})
	}

	body, _ := json.Marshal(map[string]any{
		"layoutId":  s.cfg.LayoutID,
		"trains":    trains,
		"cameras":   cams,
		"timestamp": time.Now().UnixMilli(),
	})
	if err := s.publish(dest, body); err != nil {
		s.logger.Printf("[STOMP] heartbeat send error: %v", err)
	}
}

// ── Subscription drains ───────────────────────────────────────────────────────

func (s *Session) drainTrainControl(ctx context.Context, sub *stomp.Subscription, dccAddr int, trainID int64) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-sub.C:
			if !ok {
				return
			}
			var cmd ControlMessage
			if err := json.Unmarshal(msg.Body, &cmd); err != nil {
				s.logger.Printf("[STOMP] bad train control msg: %v", err)
				continue
			}
			if cmd.Type == "SET_SPEED" {
				s.SetRequestedSpeed(dccAddr, cmd.Speed)
			}
			s.handlers.OnTrainControl(trainID, cmd)
		}
	}
}

func (s *Session) drainStreamCommands(ctx context.Context, sub *stomp.Subscription, cameraSlug string) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-sub.C:
			if !ok {
				return
			}
			var cmd StreamCommand
			if err := json.Unmarshal(msg.Body, &cmd); err != nil {
				s.logger.Printf("[STOMP] bad stream cmd: %v", err)
				continue
			}
			cmd.CameraSlug = cameraSlug
			if isZeroICEConfig(cmd.ICE) {
				cmd.ICE = s.CurrentICE()
			}
			s.handlers.OnStreamCommand(cmd)
		}
	}
}

func (s *Session) drainNodeCommands(ctx context.Context, sub *stomp.Subscription) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-sub.C:
			if !ok {
				return
			}
			var cmd NodeCommand
			if err := json.Unmarshal(msg.Body, &cmd); err != nil {
				s.logger.Printf("[STOMP] bad node command: %v", err)
				continue
			}
			s.logger.Printf("[STOMP] node command: node=%s cmd=%s", cmd.NodeID, cmd.Command)
			if s.handlers.OnNodeCommand != nil {
				s.handlers.OnNodeCommand(cmd)
			}
		}
	}
}

func (s *Session) drainLayoutState(ctx context.Context, sub *stomp.Subscription) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-sub.C:
			if !ok {
				return
			}
			var payload map[string]any
			if err := json.Unmarshal(msg.Body, &payload); err != nil {
				continue
			}

			msgType, _ := payload["type"].(string)

			if msgType == "SIGNAL" {
				if signalsRaw, ok := payload["signals"].(map[string]any); ok {
					aspects := make(map[string]string, len(signalsRaw))
					for k, v := range signalsRaw {
						if str, ok := v.(string); ok {
							aspects[k] = str
						}
					}
					if len(aspects) > 0 && s.handlers.OnSignalUpdate != nil {
						s.handlers.OnSignalUpdate(aspects)
					}
				}
			}

			if msgType == "ROUTE" {
				s.handleRouteEvent(payload)
			}
		}
	}
}

func (s *Session) handleRouteEvent(payload map[string]any) {
	routeKey, _ := payload["routeKey"].(string)
	status, _ := payload["status"].(string)
	if routeKey == "" || status == "" {
		return
	}

	var trainID *int64
	if v, ok := payload["trainId"]; ok && v != nil {
		switch n := v.(type) {
		case float64:
			id := int64(n)
			trainID = &id
		case int64:
			trainID = &n
		}
	}

	var blockSeq []string
	if raw, ok := payload["blockSequence"].([]any); ok {
		for _, v := range raw {
			if s, ok := v.(string); ok {
				blockSeq = append(blockSeq, s)
			}
		}
	}

	ts, _ := payload["timestamp"].(float64)

	s.routesMu.Lock()
	defer s.routesMu.Unlock()

	switch status {
	case "LOCKED", "ACTIVE", "RELEASING":
		s.routes[routeKey] = &ActiveRoute{
			RouteKey:      routeKey,
			TrainID:       trainID,
			Status:        status,
			BlockSequence: blockSeq,
			Timestamp:     int64(ts),
		}
	case "COMPLETE", "CANCELLED", "DENIED":
		delete(s.routes, routeKey)
	}
}

// ── Public publish methods ────────────────────────────────────────────────────

func (s *Session) PublishTrainState(layoutID string, trainID int64, state TrainState) {
	dest := fmt.Sprintf("/app/layout/%s/train/%d/telemetry", layoutID, trainID)
	state.LastUpdateTime = time.Now().UTC().Format(time.RFC3339)
	body, _ := json.Marshal(map[string]any{
		"layoutId":  layoutID,
		"trainId":   trainID,
		"state":     state,
		"timestamp": time.Now().UnixMilli(),
	})
	if err := s.publish(dest, body); err != nil {
		s.logger.Printf("[STOMP] train state send error: %v", err)
	}
}

func (s *Session) PublishLayoutState(layoutID string, state map[string]any) {
	dest := fmt.Sprintf("/app/layout/%s/state", layoutID)
	state["layoutId"] = layoutID
	state["timestamp"] = time.Now().UnixMilli()
	body, _ := json.Marshal(state)
	if err := s.publish(dest, body); err != nil {
		s.logger.Printf("[STOMP] state send error: %v", err)
	}
}

func (s *Session) PublishCameraHB(layoutID, cameraSlug string, roomID int64, publishing bool) {
	dest := fmt.Sprintf("/app/layout/%s/camera/%s/publisherHeartbeat", layoutID, cameraSlug)
	body, _ := json.Marshal(map[string]any{
		"layoutId": layoutID, "cameraSlug": cameraSlug,
		"roomId": roomID, "publishing": publishing,
		"ts": time.Now().UnixMilli(),
	})
	if err := s.publish(dest, body); err != nil {
		s.logger.Printf("[STOMP] cam HB send error: %v", err)
	}
}

func (s *Session) PublishNodeState(layoutID string, nodes any) {
	dest := fmt.Sprintf("/app/layout/%s/nodes/state", layoutID)
	body, _ := json.Marshal(map[string]any{
		"layoutId":  layoutID,
		"nodes":     nodes,
		"timestamp": time.Now().UnixMilli(),
	})
	if err := s.publish(dest, body); err != nil {
		s.logger.Printf("[STOMP] node state send error: %v", err)
	}
}

func (s *Session) PublishNodeReply(layoutID string, nodeID string, replyType string, payload map[string]any) {
	dest := fmt.Sprintf("/app/layout/%s/nodes/reply", layoutID)
	body, _ := json.Marshal(map[string]any{
		"layoutId":  layoutID,
		"nodeId":    nodeID,
		"replyType": replyType,
		"payload":   payload,
		"timestamp": time.Now().UnixMilli(),
	})
	if err := s.publish(dest, body); err != nil {
		s.logger.Printf("[STOMP] node reply send error: %v", err)
	}
}

func isZeroICEConfig(ice agentmedia.ICEConfig) bool {
	return ice.StunURL == "" &&
		ice.TurnURL == "" &&
		len(ice.TurnURLs) == 0 &&
		ice.Username == "" &&
		ice.Password == "" &&
		!ice.ForceRelay &&
		ice.TTLSeconds == 0
}
