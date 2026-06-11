// Package jmri provides a CommandStation implementation that drives locos via
// JMRI's JSON WebSocket API (port 12080 by default).
//
// # JMRI JSON throttle protocol
//
// JMRI exposes a JSON WebSocket at ws://<host>:<port>/json/.
// On connect JMRI immediately sends a hello message.
// Each throttle is a named virtual throttle — we use the DCC address as the
// name (e.g. "loco-3") so one WebSocket connection can multiplex all locos.
//
//	Acquire:   {"type":"throttle","data":{"name":"loco-3","address":3,"longAddress":false}}
//	Speed:     {"type":"throttle","data":{"name":"loco-3","speed":<0.0-1.0>,"forward":<bool>}}
//	Function:  {"type":"throttle","data":{"name":"loco-3","F0":true}}
//	Release:   {"type":"throttle","data":{"name":"loco-3","release":""}}
//
// Turnouts use a separate message type:
//
//	Set:       {"type":"turnout","data":{"name":"<systemName>","state":<4=THROWN|2=CLOSED>}}
//
// Emergency stop: set speed=-1 on all acquired throttles, or use the power API:
//
//	Power off: {"type":"power","data":{"state":4}}  (UNKNOWN/off in JMRI)
//	Power on:  {"type":"power","data":{"state":2}}
//
// The agent uses DCC addresses as throttle names. JMRI maps these to roster
// entries if they exist, or creates a transient throttle otherwise.
//
// # Connection lifecycle
//
// A single persistent WebSocket is shared across all throttles. On connect we
// send a hello response and then throttle acquire messages for any addresses we
// know about (from config). On disconnect the agent reconnects automatically
// (same pattern as DCC-EX). JMRI's dead-man failsafe is the agent-side one —
// the agent calls StopLoco when it detects a missed heartbeat, just as with
// DCC-EX.
package jmri

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	// jmriTurnoutThrown is JMRI's state value for a thrown/reverse turnout.
	jmriTurnoutThrown = 4
	// jmriTurnoutClosed is JMRI's state value for a closed/normal turnout.
	jmriTurnoutClosed = 2

	// jmriPowerOff is JMRI's power state for track power off.
	jmriPowerOff = 4
)

// ThrottleClient implements commandstation.CommandStation via JMRI's JSON WS API.
type ThrottleClient struct {
	url    string // e.g. "ws://localhost:12080/json/"
	logger *log.Logger

	mu        sync.Mutex
	conn      *websocket.Conn
	acquired  map[int]bool // dccAddr → throttle acquired in JMRI
	connected bool
	closeOnce sync.Once
	closeCh   chan struct{}
	writeMu   sync.Mutex // serialises writes; websocket.Conn is not concurrent-write-safe
}

// NewThrottleClient creates a JMRI throttle client.
// jmriURL should be the HTTP base URL of the JMRI web server,
// e.g. "http://localhost:12080". The client derives the WS URL automatically.
func NewThrottleClient(jmriURL string, logger *log.Logger) *ThrottleClient {
	wsURL := httpToWS(jmriURL) + "/json/"
	return &ThrottleClient{
		url:      wsURL,
		logger:   logger,
		acquired: make(map[int]bool),
		closeCh:  make(chan struct{}),
	}
}

// Connect opens the WebSocket connection to JMRI.
// Safe to call when already connected — returns nil immediately.
func (c *ThrottleClient) Connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.connected && c.conn != nil {
		return nil
	}

	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
	}
	conn, resp, err := dialer.Dial(c.url, http.Header{})
	if err != nil {
		if resp != nil {
			return fmt.Errorf("jmri ws dial %s: %w (HTTP %d)", c.url, err, resp.StatusCode)
		}
		return fmt.Errorf("jmri ws dial %s: %w", c.url, err)
	}

	c.conn = conn
	c.connected = true
	c.acquired = make(map[int]bool)
	c.logger.Printf("[jmri] connected to %s", c.url)

	go c.readLoop(conn)
	return nil
}

// IsConnected reports whether the WebSocket is currently live.
func (c *ThrottleClient) IsConnected() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.connected && c.conn != nil
}

// Close shuts the connection cleanly.
func (c *ThrottleClient) Close() error {
	c.closeOnce.Do(func() { close(c.closeCh) })
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		err := c.conn.Close()
		c.conn = nil
		c.connected = false
		return err
	}
	return nil
}

// SetLocoSpeed sends a speed+direction command via JMRI JSON throttle.
// speed: 0–126 (DCC 128-step); the client normalises to 0.0–1.0 for JMRI.
// regID is ignored (DCC-EX concept not needed in JMRI mode).
func (c *ThrottleClient) SetLocoSpeed(_, dccAddr, speed126, direction int) error {
	if err := c.ensureAcquired(dccAddr); err != nil {
		return err
	}

	// Normalise DCC 128-step to JMRI 0.0–1.0 float.
	// DCC step 0 = stop; 126 = full speed; -1 = emergency stop in JMRI.
	speedF := float64(speed126) / 126.0
	if speedF > 1.0 {
		speedF = 1.0
	}
	forward := direction == 1

	return c.sendThrottle(dccAddr, map[string]any{
		"name":    throttleName(dccAddr),
		"speed":   speedF,
		"forward": forward,
	})
}

// StopLoco sends speed=0 for one address. JMRI will decelerate using any
// momentum configured in the roster entry.
func (c *ThrottleClient) StopLoco(dccAddr int) error {
	if err := c.ensureAcquired(dccAddr); err != nil {
		return err
	}
	return c.sendThrottle(dccAddr, map[string]any{
		"name":  throttleName(dccAddr),
		"speed": 0.0,
	})
}

// EmergencyStop cuts track power via JMRI's power API. This is the equivalent
// of DCC-EX's <0> — all motion stops immediately regardless of momentum.
func (c *ThrottleClient) EmergencyStop() error {
	msg := map[string]any{
		"type": "power",
		"data": map[string]any{
			"state": jmriPowerOff,
		},
	}
	return c.send(msg)
}

// SetTurnout sets a turnout by its JMRI system name, derived from the DCC
// accessory address. JMRI system names follow the pattern "MT<address>" for
// the default DCC system (MyDCC). The operator can override this by configuring
// turnout system names explicitly in the layout config (future work).
func (c *ThrottleClient) SetTurnout(turnoutID int, thrown bool) error {
	state := jmriTurnoutClosed
	if thrown {
		state = jmriTurnoutThrown
	}
	// Derive JMRI system name from DCC accessory address.
	// JMRI default: "MT<addr>" for NMRA DCC.
	systemName := fmt.Sprintf("MT%d", turnoutID)
	msg := map[string]any{
		"type": "turnout",
		"data": map[string]any{
			"name":  systemName,
			"state": state,
		},
	}
	c.logger.Printf("[jmri] SET_TURNOUT %s state=%d", systemName, state)
	return c.send(msg)
}

// SetFunction sets a decoder function F0–F28.
func (c *ThrottleClient) SetFunction(dccAddr, fn, state int) error {
	if err := c.ensureAcquired(dccAddr); err != nil {
		return err
	}
	fnKey := fmt.Sprintf("F%d", fn)
	return c.sendThrottle(dccAddr, map[string]any{
		"name": throttleName(dccAddr),
		fnKey:  state == 1,
	})
}

// ── Internal helpers ──────────────────────────────────────────────────────────

// ensureAcquired sends a throttle acquire message to JMRI if we haven't already
// claimed this address in the current connection session.
func (c *ThrottleClient) ensureAcquired(dccAddr int) error {
	c.mu.Lock()
	already := c.acquired[dccAddr]
	c.mu.Unlock()

	if already {
		return nil
	}

	// Small delay before acquiring to let JMRI finish its hello handshake.
	// Without this, acquiring immediately after connect can cause JMRI to
	// crash with "this.throttle is null" (JMRI internal race on fast reconnect).
	time.Sleep(200 * time.Millisecond)

	if err := c.acquire(dccAddr); err != nil {
		return err
	}
	c.mu.Lock()
	c.acquired[dccAddr] = true
	c.mu.Unlock()
	return nil
}

// acquire sends the JMRI throttle acquire message for a DCC address.
func (c *ThrottleClient) acquire(dccAddr int) error {
	// longAddress: DCC addresses ≥ 128 use long addressing.
	longAddr := dccAddr >= 128
	msg := map[string]any{
		"type": "throttle",
		"data": map[string]any{
			"name":        throttleName(dccAddr),
			"address":     dccAddr,
			"longAddress": longAddr,
		},
	}
	c.logger.Printf("[jmri] acquiring throttle dcc=%d longAddr=%v", dccAddr, longAddr)
	return c.send(msg)
}

// sendThrottle sends a throttle command with the given data fields.
func (c *ThrottleClient) sendThrottle(dccAddr int, data map[string]any) error {
	_ = dccAddr // used by callers to build data; kept for symmetry
	return c.send(map[string]any{
		"type": "throttle",
		"data": data,
	})
}

// send serialises msg to JSON and writes it to the WebSocket.
func (c *ThrottleClient) send(msg any) error {
	b, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("jmri marshal: %w", err)
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	c.mu.Lock()
	conn := c.conn
	c.mu.Unlock()

	if conn == nil {
		return fmt.Errorf("jmri: not connected")
	}

	if err := conn.WriteMessage(websocket.TextMessage, b); err != nil {
		c.markDisconnected()
		return fmt.Errorf("jmri write: %w", err)
	}
	return nil
}

// readLoop drains incoming JMRI messages and logs notable ones.
// It detects disconnection and clears the connection state so
// the reconnect loop in main.go can re-establish.
func (c *ThrottleClient) readLoop(conn *websocket.Conn) {
	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			select {
			case <-c.closeCh:
				// Deliberate close — no noise.
			default:
				c.logger.Printf("[jmri] read loop ended: %v", err)
			}
			c.markDisconnected()
			return
		}

		// Log interesting JMRI responses (errors, throttle feedback).
		c.handleIncoming(msg)
	}
}

// handleIncoming processes a single inbound JMRI JSON message.
// Currently just logs; future work can extract signal aspects, roster data, etc.
func (c *ThrottleClient) handleIncoming(raw []byte) {
	var msg map[string]any
	if err := json.Unmarshal(raw, &msg); err != nil {
		c.logger.Printf("[jmri] unreadable message: %s", raw)
		return
	}

	msgType, _ := msg["type"].(string)
	data, _ := msg["data"].(map[string]any)

	switch msgType {
	case "hello":
		version, _ := data["JMRI"].(string)
		c.logger.Printf("[jmri] hello from JMRI %s", version)

	case "error":
		code, _ := data["code"].(float64)
		message, _ := data["message"].(string)
		c.logger.Printf("[jmri] ERROR code=%.0f: %s", code, message)

	case "throttle":
		// JMRI echoes throttle state back. Useful for diagnostics.
		name, _ := data["name"].(string)
		speed, _ := data["speed"].(float64)
		c.logger.Printf("[jmri] throttle echo name=%s speed=%.2f", name, speed)

	case "power":
		state, _ := data["state"].(float64)
		c.logger.Printf("[jmri] power state=%.0f", state)

	default:
		// Silently drop unknown types (roster, memory, sensor updates, etc.)
	}
}

// markDisconnected clears connected state so reconnect loop triggers.
func (c *ThrottleClient) markDisconnected() {
	c.mu.Lock()
	c.conn = nil
	c.connected = false
	c.acquired = make(map[int]bool) // throttles must be re-acquired after reconnect
	c.mu.Unlock()
}

// throttleName returns the JMRI throttle name for a DCC address.
// Using a consistent naming scheme means the same throttle is reused if the
// agent reconnects mid-session.
func throttleName(dccAddr int) string {
	return fmt.Sprintf("loco-%d", dccAddr)
}

// httpToWS converts an http(s) base URL to ws(s).
func httpToWS(u string) string {
	if len(u) >= 5 && u[:5] == "https" {
		return "wss" + u[5:]
	}
	if len(u) >= 4 && u[:4] == "http" {
		return "ws" + u[4:]
	}
	return u
}

// ReconnectLoop runs a background goroutine that keeps the JMRI connection alive.
// It mirrors the DCC-EX reconnect pattern in main.go — call once at startup.
// Also sends periodic pings to prevent JMRI's idle timeout (~18s).
func (c *ThrottleClient) ReconnectLoop(ctx context.Context) {
	reconnect := time.NewTicker(3 * time.Second)
	ping := time.NewTicker(10 * time.Second) // well under JMRI's ~18s idle timeout
	defer reconnect.Stop()
	defer ping.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-reconnect.C:
			if c.IsConnected() {
				continue
			}
			// Brief pause before reconnecting — prevents hammering JMRI if it
			// closed the connection due to an internal error (e.g. throttle race).
			time.Sleep(500 * time.Millisecond)
			if err := c.Connect(); err != nil {
				c.logger.Printf("[jmri] reconnect failed: %v", err)
				continue
			}
			c.logger.Printf("[jmri] reconnect succeeded")
		case <-ping.C:
			if !c.IsConnected() {
				continue
			}
			// Send a JMRI JSON ping to keep the connection alive
			if err := c.send(map[string]any{"type": "ping"}); err != nil {
				c.logger.Printf("[jmri] ping failed: %v", err)
			}
		}
	}
}
