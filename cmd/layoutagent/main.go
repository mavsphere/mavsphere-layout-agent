// mavsphere-layout-agent — local edge bridge between MavSphere backend,
// DCC-EX command station, and ESP32 sensor nodes via MQTT.
//
//	Backend  ←→ (STOMP/WS)  ←→  layout-agent  ←→ (serial/TCP) ←→ DCC-EX
//	                                  ↕
//	                           (MQTT local broker)
//	                                  ↕
//	                        ESP32 sensor nodes
//
// Startup sequence:
//  1. Login to backend → get JWT
//  2. Fetch train list from backend (source of truth)
//  3. Connect DCC-EX (serial or TCP)
//  4. Connect MQTT broker → subscribe to sensor topics
//  5. Open STOMP/WS session → subscribe to train control queues
//  6. Forward sensor events → backend STOMP as LayoutState
//  7. Serve local config UI at :8091
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/mavsphere/mavsphere-layout-agent/internal/backend"
	cammgr "github.com/mavsphere/mavsphere-layout-agent/internal/camera"
	"github.com/mavsphere/mavsphere-layout-agent/internal/commandstation"
	"github.com/mavsphere/mavsphere-layout-agent/internal/dccex"
	"github.com/mavsphere/mavsphere-layout-agent/internal/jmri"
	jmritunnel "github.com/mavsphere/mavsphere-layout-agent/internal/jmritunnel"
	"github.com/mavsphere/mavsphere-layout-agent/internal/mqtt"
	"github.com/mavsphere/mavsphere-layout-agent/internal/nodes"
	"github.com/mavsphere/mavsphere-layout-agent/internal/webui"
	"github.com/mavsphere/mavsphere-layout-agent/pkg/auth"
	"github.com/mavsphere/mavsphere-layout-agent/pkg/config"
	agentmedia "github.com/mavsphere/mavsphere-layout-agent/pkg/media"
)

// speedCap returns the maximum permitted speed fraction (0..1) given a signal aspect.
// An empty string means the aspect is not yet known — treated as permissive (1.0)
// because the backend ThrottleGuard is the authoritative enforcer. The agent only
// intervenes when it has received an explicit RED from the backend.
func speedCap(aspect string) float64 {
	switch aspect {
	case "GREEN":
		return 1.0
	case "YELLOW":
		return 0.3
	case "RED":
		return 0.0
	default:
		// DARK, UNKNOWN, or empty (not yet received) → pass through
		return 1.0
	}
}

// AgentRuntimeState tracks observable startup/runtime state for the status endpoint.
type AgentRuntimeState struct {
	Phase             string    `json:"phase"`
	Ready             bool      `json:"ready"`
	Degraded          bool      `json:"degraded"`
	Reasons           []string  `json:"reasons,omitempty"`
	LastAuth          time.Time `json:"lastAuth,omitempty"`
	LastTrainFetch    time.Time `json:"lastTrainFetch,omitempty"`
	LastTopologyFetch time.Time `json:"lastTopologyFetch,omitempty"`
}

// safeLoop runs fn and recovers from panics, logging them.
func safeLoop(name string, fn func()) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[%s] panic recovered: %v", name, r)
		}
	}()
	fn()
}

func main() {
	cfgPath := flag.String("config", "config.json", "path to agent config file")
	uiAddr := flag.String("ui-addr", ":8091", "address for the web UI (empty to disable)")
	flag.Parse()

	logger := log.New(os.Stdout, "[layoutagent] ", log.LstdFlags|log.Lmicroseconds)

	runState := &AgentRuntimeState{Phase: "init"}

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		logger.Fatalf("load config: %v", err)
	}
	logger.Printf("Starting layout agent for layoutId=%s", cfg.LayoutID)
	logger.Printf("DCC-EX: %s | MQTT: %s | Backend: %s", cfg.DccEx.Port, cfg.Mqtt.BrokerURL, cfg.BackendWsURL)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// ── Web UI ─────────────────────────────────────────────────────────────────
	var uiServer *webui.Server
	if *uiAddr != "" {
		uiServer = webui.Start(*uiAddr, *cfgPath)
	}

	// ── Command station ───────────────────────────────────────────────────────
	// Select DCC-EX or JMRI based on config. Both satisfy commandstation.CommandStation.
	var (
		cs         commandstation.CommandStation
		csFailsafe *commandstation.Failsafe
		dccClient  *dccex.Client // non-nil only in DCC-EX mode
	)

	if cfg.Jmri.Enabled && cfg.Jmri.URL != "" {
		logger.Printf("Command station: JMRI JSON throttle at %s", cfg.Jmri.URL)
		jmriClient := jmri.NewThrottleClient(cfg.Jmri.URL, logger)
		if err := jmriClient.Connect(); err != nil {
			logger.Printf("WARNING: initial JMRI connect failed: %v — background retry enabled", err)
		}
		go jmriClient.ReconnectLoop(ctx)
		cs = jmriClient
	} else {
		logger.Printf("Command station: DCC-EX at %s", cfg.DccEx.Port)
		dccClient = dccex.NewClient(cfg.DccEx.Port, cfg.DccEx.BaudRate, cfg.DccEx.CommandTimeoutMs, logger)
		if err := dccClient.Connect(); err != nil {
			logger.Printf("WARNING: initial DCC-EX connect failed: %v — background retry enabled", err)
		}
		go func() {
			ticker := time.NewTicker(2 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					if cs.IsConnected() {
						continue
					}
					if err := dccClient.Connect(); err != nil {
						logger.Printf("[dccex] reconnect failed: %v", err)
						continue
					}
					logger.Printf("[dccex] reconnect succeeded")
				}
			}
		}()
		go func() {
			for resp := range dccClient.Responses() {
				switch resp.Tag {
				case "iDCCEX":
					logger.Printf("[dccex] version: %s", resp.Raw)
				case "l":
					logger.Printf("[dccex] loco feedback: %v", resp.Data)
				case "H":
					logger.Printf("[dccex] turnout: %v", resp.Data)
				case "r":
					logger.Printf("[dccex] CV read result: %v", resp.Data)
				default:
					logger.Printf("[dccex] raw: %s", resp.Raw)
				}
			}
		}()
		cs = commandstation.NewDccExAdapter(dccClient)
	}
	csFailsafe = commandstation.NewFailsafe(cfg.Failsafe.ControlTimeoutMs, cfg.Failsafe.ReissueStopMs, cs, logger)

	// ── JMRI remote panel tunnel ──────────────────────────────────────────────
	// Started later (after tokenMu, doLogin, currentToken are declared).
	// See "STOMP session" goroutines section below.

	// ── Camera manager ────────────────────────────────────────────────────────
	camMgr := cammgr.NewCameraManager(cfg, logger)

	// ── Session reference (set after STOMP handlers are wired) ────────────────
	var sessMu sync.RWMutex
	var sess *backend.Session

	getSession := func() *backend.Session {
		sessMu.RLock()
		defer sessMu.RUnlock()
		return sess
	}

	// ── Active sessions tracking (for /api/sessions + simulator) ─────────────
	var activeSessionsMu sync.RWMutex
	activeSessions := make(map[int]bool)

	setSessionActive := func(dccAddr int, active bool) {
		activeSessionsMu.Lock()
		if active {
			activeSessions[dccAddr] = true
		} else {
			delete(activeSessions, dccAddr)
		}
		activeSessionsMu.Unlock()
	}

	// Build lookup maps from config — guarded so the periodic train-sync goroutine
	// can update them without a data race.
	var trainsMu sync.RWMutex
	addressToTrainID := make(map[int]int64, len(cfg.Trains))
	addressToTrainCfg := make(map[int]config.TrainConfig, len(cfg.Trains))
	allAddresses := make([]int, 0, len(cfg.Trains))
	for _, t := range cfg.Trains {
		if t.TrainID != 0 {
			addressToTrainID[t.DccAddress] = t.TrainID
		}
		addressToTrainCfg[t.DccAddress] = t
		allAddresses = append(allAddresses, t.DccAddress)
	}

	// rebuildTrainMaps replaces cfg.Trains and the lookup maps atomically.
	// Returns the set of trainIDs that are new (not previously known).
	rebuildTrainMaps := func(fetched []auth.TrainConfig) []config.TrainConfig {
		trainsMu.Lock()
		defer trainsMu.Unlock()

		// Record which trainIDs we already know so we can detect additions.
		knownIDs := make(map[int64]bool, len(cfg.Trains))
		for _, t := range cfg.Trains {
			if t.TrainID != 0 {
				knownIDs[t.TrainID] = true
			}
		}

		cfg.Trains = make([]config.TrainConfig, 0, len(fetched))
		addressToTrainID = make(map[int]int64, len(fetched))
		addressToTrainCfg = make(map[int]config.TrainConfig, len(fetched))
		allAddresses = make([]int, 0, len(fetched))

		var added []config.TrainConfig
		for _, bt := range fetched {
			tc := config.TrainConfig{
				TrainID: bt.TrainID, TrainSlug: bt.TrainSlug,
				DccAddress: bt.DccAddress, DisplayName: bt.DisplayName,
				GuardingSignal: bt.GuardingSignal, StartBlock: bt.StartBlock,
			}
			cfg.Trains = append(cfg.Trains, tc)
			if tc.TrainID != 0 {
				addressToTrainID[tc.DccAddress] = tc.TrainID
			}
			addressToTrainCfg[tc.DccAddress] = tc
			allAddresses = append(allAddresses, tc.DccAddress)
			if !knownIDs[tc.TrainID] {
				added = append(added, tc)
			}
		}
		return added
	}

	// Signal aspect cache — updated from backend STOMP signal state pushes.
	// Used by the redundant speedCap() enforcer on SET_SPEED.
	var signalAspectsMu sync.RWMutex
	signalAspects := make(map[string]string) // signalId → "RED" | "YELLOW" | "GREEN"

	// Train → guarding signal mapping. Built from config GuardingSignal field.
	trainGuardingSignal := make(map[int]string) // dccAddr → signalId
	for _, t := range cfg.Trains {
		if t.GuardingSignal != "" {
			trainGuardingSignal[t.DccAddress] = t.GuardingSignal
		}
	}

	getSignalAspect := func(signalId string) string {
		signalAspectsMu.RLock()
		defer signalAspectsMu.RUnlock()
		if a, ok := signalAspects[signalId]; ok {
			return a
		}
		// Return empty string when not yet known — the backend guard is authoritative.
		// Only cap speed when we have an explicit RED from the backend, not by default.
		return ""
	}

	updateSignalAspects := func(aspects map[string]string) {
		signalAspectsMu.Lock()
		defer signalAspectsMu.Unlock()
		for k, v := range aspects {
			signalAspects[k] = v
		}
	}
	_ = updateSignalAspects // used in OnPolicy handler

	activeSessionSnapshot := func() []map[string]any {
		activeSessionsMu.RLock()
		defer activeSessionsMu.RUnlock()
		out := make([]map[string]any, 0, len(activeSessions))
		for addr := range activeSessions {
			if trainID, ok := addressToTrainID[addr]; ok {
				slug := ""
				if tc, ok2 := addressToTrainCfg[addr]; ok2 {
					slug = tc.TrainSlug
				}
				out = append(out, map[string]any{
					"trainId":    trainID,
					"trainSlug":  slug,
					"dccAddress": addr,
				})
			}
		}
		return out
	}

	// ── Auth ───────────────────────────────────────────────────────────────────
	// Retry logic:
	//   - ErrBadCredentials (401/403): stop retrying entirely. Wrong credentials
	//     will never succeed and hammering the endpoint locks the account for
	//     legitimate users. Log clearly and wait for a config change + restart.
	//   - ErrRateLimited (429): back off for the duration the backend specifies.
	//   - Transient errors (network, 5xx): exponential backoff 5s → 10s → 20s → 60s cap.
	runState.Phase = "auth"
	doLogin := func() (string, error) { return auth.Login() }
	var currentToken string
	authBackoff := 5 * time.Second
	for {
		t, err := doLogin()
		if err == nil {
			currentToken = t
			authBackoff = 5 * time.Second // reset backoff on success
			break
		}

		if errors.Is(err, auth.ErrBadCredentials) {
			// Wrong credentials — retrying will not help and will lock the account.
			// Mark as degraded and park here until the container is restarted with
			// corrected config.
			runState.Degraded = true
			runState.Reasons = append(runState.Reasons, "bad credentials — check username and password in config, then restart agent")
			logger.Printf("AUTH FAILED — bad credentials. Fix username/password in config (http://<pi>:8091) and restart the agent. Not retrying.")
			for {
				time.Sleep(60 * time.Second)
				// Re-check in case config was hot-updated via the web UI.
				// config.Set() is called by the web UI on save, so config.Get()
				// will reflect any change without a restart. If credentials change,
				// exit so Docker restarts with fresh state.
				newCfg := config.Get()
				if newCfg.Username != cfg.Username || newCfg.Password != cfg.Password {
					logger.Printf("[auth] credentials changed in config — restarting to apply")
					os.Exit(0)
				}
			}
		}

		if errors.Is(err, auth.ErrRateLimited) {
			wait := 15 * time.Minute // conservative default
			blockedBy := "unknown"
			if rle, ok := auth.AsRateLimit(err); ok {
				wait = rle.RetryAfter
				blockedBy = rle.BlockedBy
			}
			retryAt := time.Now().Add(wait)
			msg := fmt.Sprintf(
				"login rate limited (blocked_by=%s) — retry at %s (in ~%s)",
				blockedBy, retryAt.Format("15:04:05"), wait.Round(time.Second),
			)
			runState.Degraded = true
			runState.Reasons = append(runState.Reasons, msg)
			logger.Printf("[auth] %s", msg)
			time.Sleep(wait)
			runState.Degraded = false
			runState.Reasons = nil
			continue
		}

		// Transient error — exponential backoff capped at 60s.
		logger.Printf("Auth error: %v — retry in %s", err, authBackoff)
		time.Sleep(authBackoff)
		if authBackoff < 60*time.Second {
			authBackoff *= 2
			if authBackoff > 60*time.Second {
				authBackoff = 60 * time.Second
			}
		}
	}
	auth.SetToken(currentToken)
	runState.LastAuth = time.Now()
	logger.Printf("[auth] logged in as %s", cfg.Username)

	// ── Fetch train list from backend (source of truth) ───────────────────────
	runState.Phase = "fetch-trains"
	backendTrains, fetchErr := auth.FetchTrains(cfg.BackendURL, cfg.LayoutID, currentToken)
	if fetchErr != nil {
		logger.Printf("[trains] fetch from backend failed: %v — using config.json trains", fetchErr)
		runState.Degraded = true
		runState.Reasons = append(runState.Reasons, "train fetch failed: "+fetchErr.Error())
	} else {
		runState.LastTrainFetch = time.Now()
		logger.Printf("[trains] loaded %d trains from backend", len(backendTrains))
		rebuildTrainMaps(backendTrains)
		for _, bt := range backendTrains {
			logger.Printf("[trains] trainId=%d slug=%s dcc=%d", bt.TrainID, bt.TrainSlug, bt.DccAddress)
		}
	}

	// ── Fetch topology for local serving (simulator / debug tools) ─────────────
	runState.Phase = "fetch-topology"
	var cachedTopology map[string]any
	if topo, err := auth.FetchTopology(cfg.BackendURL, cfg.LayoutID, currentToken); err != nil {
		logger.Printf("[topology] fetch failed (non-fatal): %v", err)
		runState.Degraded = true
		runState.Reasons = append(runState.Reasons, "topology fetch failed: "+err.Error())
	} else {
		runState.LastTopologyFetch = time.Now()
		cachedTopology = map[string]any{
			"blocks":      topo.Blocks,
			"connections": topo.Connections,
			"signals":     topo.Signals,
			"sensors":     topo.Sensors,
			"layoutId":    cfg.LayoutID,
		}
		logger.Printf("[topology] fetched %d blocks, %d connections, %d signals, %d sensors",
			len(topo.Blocks), len(topo.Connections), len(topo.Signals), len(topo.Sensors))
	}

	// ── MQTT bridge (sensor events → backend STOMP) ───────────────────────────
	nodeRegistry := nodes.NewRegistry()

	// prevSensorStates tracks the last-seen state per sensor key (nodeId/sensorId)
	// so we only log actual state transitions, not retained-message replays.
	var prevSensorStateMu sync.Mutex
	prevSensorStates := make(map[string]string)

	publishFullSensorState := func(reason string) {
		s := getSession()
		if s == nil || !s.IsConnected() {
			return
		}

		blockStates := nodeRegistry.BuildBlockStateSnapshot()
		if len(blockStates) == 0 {
			// Only log the skip on non-periodic calls to avoid log spam when no
			// topology-bound sensors are present (e.g. JMRI mode with retained sim-node messages).
			if reason != "periodic-resync" {
				logger.Printf("[state-sync] skipped (%s): no sensor state cached", reason)
			}
			return
		}

		payload := map[string]any{
			"type":        "STATE",
			"blockStates": blockStates,
			"timestamp":   time.Now().UnixMilli(),
		}

		s.PublishLayoutState(cfg.LayoutID, payload)
		// Only log periodic resyncs at debug level (they fire every 5s).
		if reason != "periodic-resync" {
			logger.Printf("[state-sync] full snapshot sent (%s, %d sensors)", reason, len(blockStates))
		}
	}

	var mqttBridge *mqtt.Bridge
	stateSyncMu := sync.Mutex{}
	lastStateSyncRequest := map[string]time.Time{}
	requestNodeStateSync := func(nodeID, reason string) {
		if nodeID == "" || mqttBridge == nil || !mqttBridge.IsConnected() {
			return
		}
		stateSyncMu.Lock()
		last := lastStateSyncRequest[nodeID]
		if time.Since(last) < 30*time.Second {
			stateSyncMu.Unlock()
			return
		}
		lastStateSyncRequest[nodeID] = time.Now()
		stateSyncMu.Unlock()

		mqttBridge.PublishStateSync(nodeID)
		logger.Printf("[state-sync] requested node full sensor state node=%s reason=%s", nodeID, reason)
	}

	mqttBridge = mqtt.NewBridge(
		cfg.Mqtt.BrokerURL,
		cfg.Mqtt.Username,
		cfg.Mqtt.Password,
		cfg.Mqtt.TopicPrefix,
		"layout-agent-"+cfg.LayoutID,

		func(event mqtt.SensorEvent) {
			// Suppress log noise from MQTT retained-message replay on connect.
			// The broker delivers all retained sensor messages immediately, which
			// floods the log on every restart (especially with a sim-node present).
			// Only log when the state actually changes.
			sensorKey := event.NodeID + "/" + event.SensorID
			prevSensorStateMu.Lock()
			changed := prevSensorStates[sensorKey] != event.State
			if changed {
				prevSensorStates[sensorKey] = event.State
			}
			prevSensorStateMu.Unlock()
			if changed {
				logger.Printf("[sensor] node=%s sensor=%s state=%s", event.NodeID, event.SensorID, event.State)
			}

			nodeRegistry.UpdateSensor(event.NodeID, event.SensorID, event.Payload)

			s := getSession()
			if s == nil {
				return
			}

			s.PublishLayoutState(cfg.LayoutID, map[string]any{
				"type": "SENSOR",
				"blockStates": map[string]any{
					event.SensorID: event.State == "OCCUPIED" || event.State == "BROKEN",
				},
				"sensorEvents": []map[string]any{{
					"nodeId":   event.NodeID,
					"sensorId": event.SensorID,
					"state":    event.State,
					"active":   event.Active,
					"payload":  event.Payload,
				}},
			})
		},

		func(event mqtt.GenericEvent) {
			switch event.Type {
			case "heartbeat":
				nodeRegistry.UpdateHeartbeat(event.NodeID, event.Payload)
				logger.Printf("[node] heartbeat received node=%s ip=%v", event.NodeID, event.Payload["ipAddress"])
				requestNodeStateSync(event.NodeID, "node-heartbeat")

			case "status":
				nodeRegistry.UpdateStatus(event.NodeID, event.Payload)
				requestNodeStateSync(event.NodeID, "node-status")
				logger.Printf("[node] status received node=%s ip=%v online=%v", event.NodeID, event.Payload["ipAddress"], event.Payload["mqttConnected"])

			case "rfid":
				nodeRegistry.UpdateRfid(event.NodeID, event.Payload)
				logger.Printf("[rfid] node=%s payload=%v", event.NodeID, event.Payload)

				s := getSession()
				if s != nil {
					s.PublishLayoutState(cfg.LayoutID, map[string]any{
						"type":       "RFID",
						"rfidEvents": []map[string]any{event.Payload},
					})
				}

			case "reply":
				nodeRegistry.UpdateReply(event.NodeID, event.SubType, event.Payload)
				logger.Printf("[reply] node=%s type=%s payload=%v", event.NodeID, event.SubType, event.Payload)

				s := getSession()
				if s != nil {
					s.PublishNodeReply(cfg.LayoutID, event.NodeID, event.SubType, event.Payload)
				}
			}
		},

		logger,
	)

	logger.Printf("[mqtt] connecting to %s", cfg.Mqtt.BrokerURL)
	if err := mqttBridge.Connect(); err != nil {
		logger.Printf("WARNING: MQTT connect failed: %v — sensors inactive", err)
	} else {
		logger.Printf("[mqtt] connected")
	}

	// ── STOMP handlers ────────────────────────────────────────────────────────
	var tokenMu sync.RWMutex
	handlers := backend.Handlers{
		OnTrainControl: func(trainID int64, msg backend.ControlMessage) {
			// SET_TURNOUT is layout-wide, not per-train — handle before DCC address lookup
			if msg.Type == "SET_TURNOUT" {
				turnoutAddr := msg.TurnoutDccAddress
				if turnoutAddr == 0 {
					logger.Printf("[turnout] SET_TURNOUT missing DCC address (turnoutId=%s)", msg.TurnoutID)
					return
				}
				thrown := msg.Position == "REVERSE" || msg.Position == "THROWN"
				if err := cs.SetTurnout(turnoutAddr, thrown); err != nil {
					logger.Printf("[dcc] SetTurnout error id=%s addr=%d: %v", msg.TurnoutID, turnoutAddr, err)
				} else {
					logger.Printf("[dcc] SET_TURNOUT id=%s addr=%d position=%s", msg.TurnoutID, turnoutAddr, msg.Position)
				}
				return
			}

			// For all other commands, look up the train's DCC address.
			dccAddr := 0
			for _, t := range cfg.Trains {
				if t.TrainID == trainID {
					dccAddr = t.DccAddress
					break
				}
			}
			if dccAddr == 0 {
				logger.Printf("[train] no DCC address for trainId=%d", trainID)
				return
			}

			switch msg.Type {
			case "CONTROL_PING":
				csFailsafe.Refresh(dccAddr)
				setSessionActive(dccAddr, true)

			case "SET_SPEED":
				setSessionActive(dccAddr, true)
				speed := msg.Speed
				requestedSpeed := speed
				authority := "GRANTED"
				reason := "NONE"

				// Redundant signal-based speed cap — agent independently enforces
				// even if the backend already capped the speed.
				if guardingSig, ok := trainGuardingSignal[dccAddr]; ok {
					aspect := getSignalAspect(guardingSig)
					cap := speedCap(aspect)
					if speed > cap {
						logger.Printf("[safety] capping speed %.2f → %.2f (signal %s = %s)",
							speed, cap, guardingSig, aspect)
						speed = cap
						if cap <= 0 {
							authority = "DENIED"
							reason = "SIGNAL_RED"
						} else {
							authority = "RESTRICTED"
							reason = "SIGNAL_YELLOW"
						}
					}
				}

				speed126 := int(speed * 126)
				if speed126 > 126 {
					speed126 = 126
				}
				dir := 1
				if !msg.Forward {
					dir = 0
				}
				if speed126 > 0 {
					csFailsafe.ArmMoving(dccAddr)
				} else {
					csFailsafe.Release(dccAddr)
				}
				logger.Printf("[dcc] SET_SPEED trainId=%d dcc=%d speed126=%d dir=%d fwd=%v req=%.3f applied=%.3f authority=%s reason=%s",
					trainID, dccAddr, speed126, dir, msg.Forward, requestedSpeed, speed, authority, reason)
				if err := cs.SetLocoSpeed(1, dccAddr, speed126, dir); err != nil {
					logger.Printf("[dcc] SetLocoSpeed error: %v", err)
				}
				// Publish telemetry with both requested and applied speeds
				s := getSession()
				if s != nil {
					direction := "FORWARD"
					if !msg.Forward {
						direction = "REVERSE"
					}
					s.PublishTrainState(cfg.LayoutID, trainID, backend.TrainState{
						Online: true, RequestedSpeed: requestedSpeed, AppliedSpeed: speed,
						Direction: direction, MovementAuthority: authority,
						MovementReason: reason, DccThrottleActive: true,
					})
				}

			case "SET_DIRECTION":
				// Direction-only changes send speed 0, so do not arm failsafe.
				csFailsafe.Release(dccAddr)
				dir := 1
				if !msg.Forward {
					dir = 0
				}
				logger.Printf("[dcc] SET_DIRECTION trainId=%d dcc=%d dir=%d fwd=%v",
					trainID, dccAddr, dir, msg.Forward)
				if err := cs.SetLocoSpeed(1, dccAddr, 0, dir); err != nil {
					logger.Printf("[dcc] SetDirection error: %v", err)
				}

			case "SET_FUNCTION":
				fnState := 0
				if msg.FunctionOn {
					fnState = 1
				}
				if err := cs.SetFunction(dccAddr, msg.Function, fnState); err != nil {
					logger.Printf("[dcc] SetFunction error: %v", err)
				}

			case "E_STOP":
				setSessionActive(dccAddr, false)
				csFailsafe.Release(dccAddr)
				if err := cs.StopLoco(dccAddr); err != nil {
					logger.Printf("[dcc] StopLoco error: %v", err)
				}

			case "E_STOP_ALL":
				for _, addr := range allAddresses {
					setSessionActive(addr, false)
					csFailsafe.Release(addr)
				}
				if err := cs.EmergencyStop(); err != nil {
					logger.Printf("[dcc] EmergencyStop error: %v", err)
				}

			default:
				logger.Printf("[train] unknown control type: %s", msg.Type)
			}
		},

		OnStreamCommand: func(cmd backend.StreamCommand) {
			ice := cmd.ICE
			if isZeroICE(ice) {
				if s := getSession(); s != nil {
					ice = s.CurrentICE()
				}
			}
			switch cmd.Type {
			case "START_VIDEO":
				if err := camMgr.StartCamera(ctx, cmd.CameraSlug, cmd.RoomID, ice); err != nil {
					logger.Printf("StartCamera error cam=%s: %v", cmd.CameraSlug, err)
				}
			case "STOP_VIDEO":
				camMgr.StopCamera(cmd.CameraSlug)
			}
		},

		OnPolicy: func(payload map[string]any) {
			logger.Printf("[policy] update received")

			// Extract signal aspects if present (pushed by the signalling engine)
			if signals, ok := payload["signals"].(map[string]any); ok {
				aspects := make(map[string]string, len(signals))
				for k, v := range signals {
					if s, ok := v.(string); ok {
						aspects[k] = s
					}
				}
				if len(aspects) > 0 {
					updateSignalAspects(aspects)
					logger.Printf("[signals] updated %d aspects from backend", len(aspects))
				}
			}
		},

		OnNodeCommand: func(cmd backend.NodeCommand) {
			logger.Printf("[node-cmd] node=%s command=%s", cmd.NodeID, cmd.Command)
			switch cmd.Command {
			case "config/set":
				if err := mqttBridge.PublishConfig(cmd.NodeID, cmd.Payload); err != nil {
					logger.Printf("[node-cmd] config push error node=%s: %v", cmd.NodeID, err)
				}
			case "ping":
				mqttBridge.PublishPing(cmd.NodeID)
			case "reboot":
				mqttBridge.PublishReboot(cmd.NodeID)
			default:
				logger.Printf("[node-cmd] unknown command: %s", cmd.Command)
			}
		},

		OnSignalUpdate: func(signals map[string]string) {
			updateSignalAspects(signals)
			// Publish each signal aspect to MQTT so sensor nodes can drive LEDs
			for signalID, aspect := range signals {
				mqttBridge.PublishSignalAspect(signalID, aspect)
			}
			logger.Printf("[signals] %d aspect(s) updated from backend → MQTT", len(signals))
		},

		OnConnected: func() {
			logger.Printf("[stomp] connected → replaying cached sensor state")
			publishFullSensorState("stomp-connect")
			for _, node := range nodeRegistry.Snapshot() {
				requestNodeStateSync(node.NodeID, "stomp-connect")
			}

			// Re-fetch topology with the current (possibly refreshed) token.
			// The initial fetch at startup may have failed if the token was expired.
			tokenMu.Lock()
			tok := currentToken
			tokenMu.Unlock()
			if topo, err := auth.FetchTopology(cfg.BackendURL, cfg.LayoutID, tok); err != nil {
				logger.Printf("[topology] re-fetch failed: %v", err)
			} else {
				runState.LastTopologyFetch = time.Now()
				runState.Phase = "ready"
				runState.Ready = true
				cachedTopology = map[string]any{
					"blocks":      topo.Blocks,
					"connections": topo.Connections,
					"signals":     topo.Signals,
					"sensors":     topo.Sensors,
					"layoutId":    cfg.LayoutID,
				}
				// Re-wire sensor bindings so incoming MQTT events map to the right blocks
				logger.Printf("[topology] sensor bindings updated: %d sensors active", len(topo.Sensors))
				logger.Printf("[topology] re-fetched on reconnect: %d blocks, %d signals, %d sensors",
					len(topo.Blocks), len(topo.Signals), len(topo.Sensors))
			}
		},

		TokenRefresh: func() (string, error) {
			t, err := doLogin()
			if err == nil {
				auth.SetToken(t)
				tokenMu.Lock()
				currentToken = t
				tokenMu.Unlock()
			}
			return t, err
		},
	}

	newSess := backend.NewSession(cfg, cs, csFailsafe, mqttBridge, handlers, logger)
	sessMu.Lock()
	sess = newSess
	sessMu.Unlock()

	// ── Background goroutines ─────────────────────────────────────────────────

	// STOMP session (reconnects automatically)
	go func() {
		tokenMu.RLock()
		t := currentToken
		tokenMu.RUnlock()

		logger.Printf("[stomp] starting session to %s", cfg.BackendWsURL)
		newSess.Run(ctx, t)
		logger.Println("STOMP disconnected — emergency stopping all trains")
		for _, addr := range allAddresses {
			csFailsafe.Release(addr)
		}
		_ = cs.EmergencyStop()
		camMgr.StopAll()
	}()

	// JMRI remote panel tunnel (reconnects automatically, refreshes token on failure)
	// Placed here so doLogin, tokenMu, and currentToken are all in scope.
	if cfg.Jmri.Enabled && cfg.Jmri.URL != "" {
		tunnelClient := jmritunnel.NewTunnelClient(
			cfg.BackendURL,
			cfg.Jmri.URL,
			cfg.LayoutID,
			auth.CurrentToken,
			func() (string, error) {
				t, err := doLogin()
				if err == nil {
					auth.SetToken(t)
					tokenMu.Lock()
					currentToken = t
					tokenMu.Unlock()
				}
				return t, err
			},
			logger,
		)
		go func() {
			logger.Printf("[jmri-tunnel] starting tunnel to backend")
			tunnelClient.Run(ctx)
		}()
	}

	// Camera heartbeat
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s := getSession()
				if s == nil {
					continue
				}
				for _, cam := range cfg.Cameras {
					s.PublishCameraHB(cfg.LayoutID, cam.CameraID, 0, camMgr.IsStreaming(cam.CameraID))
				}
			}
		}
	}()

	// Node registry push — send node state snapshots to backend every 10s
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s := getSession()
				if s == nil {
					continue
				}
				snapshot := nodeRegistry.Snapshot()
				if len(snapshot) > 0 {
					logger.Printf("[node] pushing %d node(s) to backend: %v", len(snapshot), func() []string {
						ids := make([]string, len(snapshot))
						for i, n := range snapshot {
							ids[i] = n.NodeID
						}
						return ids
					}())
					s.PublishNodeState(cfg.LayoutID, snapshot)
				}
			}
		}
	}()

	// Full sensor-state resync — resend current occupancy/sensor truth every 5s.
	// In JMRI mode this is still useful if MQTT sensor nodes are present, but we
	// suppress the log line when there are no topology-bound sensors to avoid noise.
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				publishFullSensorState("periodic-resync")
			}
		}
	}()

	// Periodic status log
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				logger.Printf("status | dccex=%v mqtt=%v stomp=%v",
					cs.IsConnected(), mqttBridge.IsConnected(),
					func() bool { s := getSession(); return s != nil && s.IsConnected() }())
			}
		}
	}()

	// ── Periodic train sync ──────────────────────────────────────────────────
	// Re-fetches the train list every 60s so trains added via the MavSphere UI
	// become active without a process restart. New trains get STOMP subscriptions
	// wired immediately; the next heartbeat cycle then marks them ACTIVE.
	go func() {
		ticker := time.NewTicker(60 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				tokenMu.RLock()
				tok := currentToken
				tokenMu.RUnlock()

				fetched, err := auth.FetchTrains(cfg.BackendURL, cfg.LayoutID, tok)
				if err != nil {
					logger.Printf("[trains] periodic sync failed: %v", err)
					continue
				}
				added := rebuildTrainMaps(fetched)
				if len(added) == 0 {
					continue
				}
				logger.Printf("[trains] periodic sync: %d new train(s) detected", len(added))
				for _, t := range added {
					logger.Printf("[trains] new train: trainId=%d slug=%s dcc=%d", t.TrainID, t.TrainSlug, t.DccAddress)
				}
				runState.LastTrainFetch = time.Now()
				// Wire STOMP control subscriptions for the new trains immediately.
				if s := getSession(); s != nil {
					s.SubscribeNewTrains(ctx, added)
				}
			}
		}
	}()

	// ── Wire status into web UI ───────────────────────────────────────────────
	if uiServer != nil {
		uiServer.AttachStatus(func() map[string]any {
			s := getSession()
			stompConnected := s != nil && s.IsConnected()
			lastHB := int64(0)
			var connState backend.BackendConnState
			if s != nil {
				lastHB = s.LastHeartbeatMs()
				connState = s.ConnState()
			}

			csType := "DCC-EX"
			if cfg.Jmri.Enabled && cfg.Jmri.URL != "" {
				csType = "JMRI"
			}

			// Only surface the startup phase while the agent is still initialising.
			// Once ready it's always "ready" and the field just adds noise.
			phaseVal := runState.Phase
			if runState.Ready {
				phaseVal = ""
			}

			// ── Live stream status ────────────────────────────────────────────
			// Collect per-camera stream snapshots. Build a top-level summary
			// from the first running camera (or an idle sentinel if none running).
			camInfos := camMgr.StreamInfos()

			// Serialise to []map[string]any so the JSON shape is explicit and
			// independent of the StreamInfo struct tags.
			camStreams := make([]map[string]any, 0, len(camInfos))
			for _, c := range camInfos {
				camStreams = append(camStreams, map[string]any{
					"cameraId": c.CameraID,
					"running":  c.Running,
					"healthy":  c.Healthy,
					"codec":    c.Codec,
					"encoder":  c.Encoder,
					"profile":  c.Profile,
					"width":    c.Width,
					"height":   c.Height,
					"fps":      c.Fps,
					"pixFmt":   c.PixFmt,
					"iceState": c.IceState,
					"roomId":   c.RoomId,
				})
			}

			// Top-level stream summary: first running camera, or idle.
			var topStream map[string]any
			for _, c := range camInfos {
				if c.Running {
					topStream = map[string]any{
						"running":      true,
						"healthy":      c.Healthy,
						"codec":        c.Codec,
						"encoder":      c.Encoder,
						"profile":      c.Profile,
						"width":        c.Width,
						"height":       c.Height,
						"fps":          c.Fps,
						"pixFmt":       c.PixFmt,
						"bitrateBps":   c.BitrateBps,
						"webrtcMaxBps": c.WebrtcMaxBps,
						"iceState":     c.IceState,
						"roomId":       c.RoomId,
						"pipeline":     c.Pipeline,
						"cameras":      camStreams,
					}
					break
				}
			}
			if topStream == nil {
				topStream = map[string]any{"running": false}
			}

			return map[string]any{
				"phase":                   phaseVal,
				"ready":                   runState.Ready,
				"degraded":                runState.Degraded,
				"reasons":                 runState.Reasons,
				"stompConnected":          stompConnected,
				"commandStationConnected": cs.IsConnected(),
				"commandStationType":      csType,
				"mqttConnected":           mqttBridge.IsConnected(),
				"lastHeartbeatMs":         lastHB,
				"reconnectCount":          connState.ReconnectCount,
				"lastConnect":             connState.LastConnect,
				"lastDisconnect":          connState.LastDisconnect,
				"lastAuth":                runState.LastAuth,
				"lastTrainFetch":          runState.LastTrainFetch,
				"lastTopoFetch":           runState.LastTopologyFetch,
				"layoutId":                cfg.LayoutID,
				"dccPort":                 cfg.DccEx.Port,
				"mqttBroker":              cfg.Mqtt.BrokerURL,
				"activeSessions":          activeSessionSnapshot(),
				"nodes":                   nodeRegistry.Snapshot(),
				"stream":                  topStream,
			}
		})
		uiServer.AttachSessions(activeSessionSnapshot)

		// Serve topology to local tools (simulator, debug clients) via /api/topology.
		// cachedTopology is nil if the fetch failed at startup; the handler returns 503.
		uiServer.AttachTopology(func() map[string]any {
			if cachedTopology == nil {
				return nil
			}
			return cachedTopology
		})

		// Serve train list to local tools (simulator) via /api/trains.
		// cfg.Trains is populated from the backend at startup and on reconnect.
		uiServer.AttachTrains(func() []map[string]any {
			out := make([]map[string]any, 0, len(cfg.Trains))
			for _, t := range cfg.Trains {
				out = append(out, map[string]any{
					"trainId":     t.TrainID,
					"trainSlug":   t.TrainSlug,
					"dccAddress":  t.DccAddress,
					"displayName": t.DisplayName,
					"startBlock":  t.StartBlock,
				})
			}
			return out
		})

		// Serve active route state to local tools via /api/routes.
		// Reads from the session's in-memory route cache (updated from STOMP ROUTE events).
		uiServer.AttachRoutes(func() []map[string]any {
			s := getSession()
			if s == nil {
				return []map[string]any{}
			}
			routes := s.GetActiveRoutes()
			out := make([]map[string]any, 0, len(routes))
			for _, r := range routes {
				entry := map[string]any{
					"routeKey":      r.RouteKey,
					"status":        r.Status,
					"blockSequence": r.BlockSequence,
					"timestamp":     r.Timestamp,
				}
				if r.TrainID != nil {
					entry["trainId"] = *r.TrainID
				}
				out = append(out, entry)
			}
			return out
		})

		uiServer.SetPreRestart(func() {
			_ = cs.EmergencyStop()
			camMgr.StopAll()
		})
	}

	// ── Signal handling ───────────────────────────────────────────────────────
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	logger.Println("Shutdown signal received")
	cancel()
	_ = cs.EmergencyStop()
	time.Sleep(100 * time.Millisecond)
	_ = cs.Close()
	mqttBridge.Disconnect()
	camMgr.StopAll()
	logger.Println("Shutdown complete")
}

func isZeroICE(ice agentmedia.ICEConfig) bool {
	return ice.StunURL == "" && ice.TurnURL == "" && len(ice.TurnURLs) == 0 &&
		ice.Username == "" && ice.Password == "" && !ice.ForceRelay && ice.TTLSeconds == 0
}
