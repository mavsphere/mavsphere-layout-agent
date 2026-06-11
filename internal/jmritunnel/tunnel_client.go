package jmritunnel

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// TunnelClient runs on the agent. It maintains a persistent outbound WebSocket
// to the backend's /api/ws/jmri-tunnel endpoint and services proxy requests
// from the backend — both HTTP and WebSocket — by forwarding them to the local
// JMRI web server.
type TunnelClient struct {
	backendWsURL string // e.g. "wss://mavsphere.com/api/ws/jmri-tunnel"
	jmriBaseURL  string // e.g. "http://localhost:12080"
	layoutID     string
	token        func() string          // called on each connect to get current JWT
	tokenRefresh func() (string, error) // called when connect fails with 401 to get a fresh JWT
	logger       *log.Logger

	mu      sync.Mutex
	conn    *websocket.Conn
	writeMu sync.Mutex

	// active WS connections agent has opened toward JMRI, keyed by connID
	wsMu    sync.Mutex
	wsConns map[string]*proxiedWSConn

	closeOnce sync.Once
	closeCh   chan struct{}
}

// proxiedWSConn wraps a local JMRI WebSocket connection.
//
// Gorilla WebSocket permits one concurrent reader and one concurrent writer,
// but it does not permit multiple concurrent writers. The tunnel can receive
// multiple WS_FRAME messages from the backend in separate goroutines, so all
// writes to the same JMRI WebSocket must be serialised.
type proxiedWSConn struct {
	conn      *websocket.Conn
	writeMu   sync.Mutex
	closeOnce sync.Once
}

func (w *proxiedWSConn) write(msgType int, data []byte) error {
	if w == nil || w.conn == nil {
		return fmt.Errorf("websocket connection is nil")
	}

	w.writeMu.Lock()
	defer w.writeMu.Unlock()

	return w.conn.WriteMessage(msgType, data)
}

func (w *proxiedWSConn) close() {
	if w == nil || w.conn == nil {
		return
	}

	w.closeOnce.Do(func() {
		w.writeMu.Lock()
		defer w.writeMu.Unlock()
		_ = w.conn.Close()
	})
}

// NewTunnelClient creates a JMRI tunnel client.
// backendBaseURL: e.g. "https://mavsphere.com" — client derives the WS URL.
// jmriBaseURL:    e.g. "http://localhost:12080"
func NewTunnelClient(backendBaseURL, jmriBaseURL, layoutID string, token func() string, tokenRefresh func() (string, error), logger *log.Logger) *TunnelClient {
	wsURL := httpToWS(strings.TrimRight(backendBaseURL, "/")) + "/api/ws/jmri-tunnel"
	return &TunnelClient{
		backendWsURL: wsURL,
		jmriBaseURL:  strings.TrimRight(jmriBaseURL, "/"),
		layoutID:     layoutID,
		token:        token,
		tokenRefresh: tokenRefresh,
		logger:       logger,
		wsConns:      make(map[string]*proxiedWSConn),
		closeCh:      make(chan struct{}),
	}
}

// Run maintains the tunnel connection, reconnecting on failure.
// Blocks until ctx is cancelled.
func (c *TunnelClient) Run(ctx context.Context) {
	backoff := 3 * time.Second
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.closeCh:
			return
		default:
		}

		if err := c.connect(ctx); err != nil {
			c.logger.Printf("[jmri-tunnel] connect failed: %v — retry in %s", err, backoff)
			// Refresh the token on every connect failure — the JWT may have
			// expired, especially after a backend restart or agent in-place restart.
			if c.tokenRefresh != nil {
				if t, rErr := c.tokenRefresh(); rErr == nil {
					c.logger.Printf("[jmri-tunnel] token refreshed")
					_ = t // token is stored globally by tokenRefresh
				} else {
					c.logger.Printf("[jmri-tunnel] token refresh failed: %v", rErr)
				}
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			if backoff < 60*time.Second {
				backoff *= 2
			}
			continue
		}

		backoff = 3 * time.Second
		c.logger.Printf("[jmri-tunnel] connected to backend")
		c.readLoop(ctx)
		c.logger.Printf("[jmri-tunnel] disconnected — will reconnect")
	}
}

func (c *TunnelClient) connect(ctx context.Context) error {
	tok := c.token()
	headers := http.Header{
		"Authorization": {"Bearer " + tok},
		"X-Layout-Id":   {c.layoutID},
	}

	dialer := websocket.Dialer{HandshakeTimeout: 15 * time.Second}
	conn, _, err := dialer.DialContext(ctx, c.backendWsURL, headers)
	if err != nil {
		return fmt.Errorf("dial %s: %w", c.backendWsURL, err)
	}

	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()
	return nil
}

func (c *TunnelClient) readLoop(ctx context.Context) {
	c.mu.Lock()
	conn := c.conn
	c.mu.Unlock()
	if conn == nil {
		return
	}
	defer func() {
		conn.Close()
		c.mu.Lock()
		if c.conn == conn {
			c.conn = nil
		}
		c.mu.Unlock()
		// Close any open JMRI WebSocket connections on tunnel drop
		c.closeAllWSConns()
	}()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		_, msg, err := conn.ReadMessage()
		if err != nil {
			if !isNormalClose(err) {
				c.logger.Printf("[jmri-tunnel] read error: %v", err)
			}
			return
		}

		var frame Frame
		if err := json.Unmarshal(msg, &frame); err != nil {
			c.logger.Printf("[jmri-tunnel] bad frame: %v", err)
			continue
		}

		go c.handleFrame(ctx, frame)
	}
}

func (c *TunnelClient) handleFrame(ctx context.Context, frame Frame) {
	switch frame.Type {
	case MsgHTTPReq:
		c.handleHTTPReq(frame)
	case MsgWSOpen:
		c.handleWSOpen(ctx, frame)
	case MsgWSFrame:
		c.handleWSFrame(frame)
	case MsgWSClose:
		c.handleWSClose(frame)
	case MsgPanelList:
		c.handlePanelList(frame)
	default:
		c.logger.Printf("[jmri-tunnel] unknown frame type: %s", frame.Type)
	}
}

// ── HTTP proxying ──────────────────────────────────────────────────────────────

func (c *TunnelClient) handleHTTPReq(frame Frame) {
	if !isAllowedPath(frame.Path) {
		c.sendError(frame.ConnID, fmt.Sprintf("path not allowed: %s", frame.Path))
		return
	}

	url := c.jmriBaseURL + frame.Path
	if frame.Query != "" {
		url += "?" + frame.Query
	}

	req, err := http.NewRequest(frame.Method, url, bytesReader(frame.Body))
	if err != nil {
		c.sendError(frame.ConnID, err.Error())
		return
	}
	for k, v := range frame.Headers {
		req.Header.Set(k, v)
	}
	// Tell JMRI the real origin is proxied
	req.Header.Set("X-Forwarded-Host", "mavsphere.com")
	req.Header.Del("Host")

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		c.sendError(frame.ConnID, fmt.Sprintf("jmri request failed: %v", err))
		return
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4*1024*1024)) // 4 MB cap

	respHeaders := make(map[string]string)
	for k := range resp.Header {
		// Forward content-type and cache headers; skip connection-specific ones
		kl := strings.ToLower(k)
		if kl == "content-type" || kl == "cache-control" || kl == "last-modified" || kl == "etag" {
			respHeaders[k] = resp.Header.Get(k)
		}
	}

	c.send(Frame{
		Type:        MsgHTTPResp,
		ConnID:      frame.ConnID,
		Status:      resp.StatusCode,
		RespHeaders: respHeaders,
		RespBody:    body,
	})
}

// ── WebSocket proxying ────────────────────────────────────────────────────────

func (c *TunnelClient) handleWSOpen(ctx context.Context, frame Frame) {
	if !isAllowedPath(frame.Path) {
		c.sendError(frame.ConnID, fmt.Sprintf("ws path not allowed: %s", frame.Path))
		return
	}

	wsURL := wsBaseURL(c.jmriBaseURL) + frame.Path
	if frame.Query != "" {
		wsURL += "?" + frame.Query
	}

	dialer := websocket.Dialer{HandshakeTimeout: 10 * time.Second}
	jmriConn, _, err := dialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		c.sendError(frame.ConnID, fmt.Sprintf("jmri ws dial failed: %v", err))
		return
	}

	ws := &proxiedWSConn{conn: jmriConn}

	c.wsMu.Lock()
	c.wsConns[frame.ConnID] = ws
	c.wsMu.Unlock()

	c.logger.Printf("[jmri-tunnel] WS opened connId=%s → %s", frame.ConnID, wsURL)

	// Forward JMRI→backend frames in a goroutine.
	go func() {
		defer func() {
			c.removeWSConn(frame.ConnID, ws)
			ws.close()

			// Notify backend this WS is closed. This uses the backend tunnel
			// write mutex and is independent of the local JMRI WS write mutex.
			c.send(Frame{Type: MsgWSClose, ConnID: frame.ConnID})
			c.logger.Printf("[jmri-tunnel] WS closed connId=%s", frame.ConnID)
		}()
		for {
			msgType, data, err := jmriConn.ReadMessage()
			if err != nil {
				if !isNormalClose(err) {
					c.logger.Printf("[jmri-tunnel] WS read error connId=%s: %v", frame.ConnID, err)
				}
				return
			}
			c.send(Frame{
				Type:     MsgWSFrame,
				ConnID:   frame.ConnID,
				WSData:   data,
				WSBinary: msgType == websocket.BinaryMessage,
			})
		}
	}()
}

func (c *TunnelClient) handleWSFrame(frame Frame) {
	c.wsMu.Lock()
	ws, ok := c.wsConns[frame.ConnID]
	c.wsMu.Unlock()
	if !ok {
		return // connection may have already closed
	}

	msgType := websocket.TextMessage
	if frame.WSBinary {
		msgType = websocket.BinaryMessage
	}

	if err := ws.write(msgType, frame.WSData); err != nil {
		c.logger.Printf("[jmri-tunnel] WS write error connId=%s: %v", frame.ConnID, err)
		c.removeWSConn(frame.ConnID, ws)
		ws.close()
		c.send(Frame{Type: MsgWSClose, ConnID: frame.ConnID})
	}
}

func (c *TunnelClient) handleWSClose(frame Frame) {
	c.wsMu.Lock()
	ws, ok := c.wsConns[frame.ConnID]
	if ok {
		delete(c.wsConns, frame.ConnID)
	}
	c.wsMu.Unlock()

	if ok {
		ws.close()
	}
}

func (c *TunnelClient) removeWSConn(connID string, expected *proxiedWSConn) {
	c.wsMu.Lock()
	defer c.wsMu.Unlock()

	current, ok := c.wsConns[connID]
	if !ok {
		return
	}
	if expected != nil && current != expected {
		return
	}

	delete(c.wsConns, connID)
}

// ── Panel list ────────────────────────────────────────────────────────────────

func (c *TunnelClient) handlePanelList(frame Frame) {
	c.logger.Printf("[jmri-tunnel] PANEL_LIST request connId=%s", frame.ConnID)
	panels, err := fetchJmriPanels(c.jmriBaseURL)
	if err != nil {
		c.logger.Printf("[jmri-tunnel] PANEL_LIST error: %v", err)
		c.sendError(frame.ConnID, err.Error())
		return
	}
	c.logger.Printf("[jmri-tunnel] PANEL_LIST found %d panel(s)", len(panels))
	c.send(Frame{
		Type:   MsgPanelListResp,
		ConnID: frame.ConnID,
		Panels: panels,
	})
}

func fetchJmriPanels(baseURL string) ([]PanelInfo, error) {
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(baseURL + "/json/panels")
	if err != nil {
		return nil, fmt.Errorf("JMRI not reachable: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("JMRI /json/panels returned %d", resp.StatusCode)
	}

	// JMRI wraps each panel in a {type, data} envelope:
	// [{"type":"panel","data":{"name":"Layout/Foo","URL":"/panel/Layout/Foo","userName":"Foo","type":"Layout"}}]
	var raw []struct {
		Type string         `json:"type"`
		Data map[string]any `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return nil, fmt.Errorf("decode panel list: %w", err)
	}

	var panels []PanelInfo
	for _, item := range raw {
		if item.Data == nil {
			continue
		}
		// userName is the human-readable display name e.g. "MavSphere Example Starter"
		userName, _ := item.Data["userName"].(string)
		// type is the panel type e.g. "Layout"
		typ, _ := item.Data["type"].(string)
		// URL is the direct JMRI path e.g. "/panel/Layout/MavSphere%20Example%20Starter?format=xml"
		// Strip query string — we serve the panel HTML not the XML
		jmriURL, _ := item.Data["URL"].(string)

		if userName == "" {
			continue
		}
		typ = normalisePanelType(typ)

		// Build the JMRI path from the URL field, stripping ?format=xml
		jmriPath := jmriURL
		if idx := strings.Index(jmriPath, "?"); idx != -1 {
			jmriPath = jmriPath[:idx]
		}
		// Fallback if URL not present
		if jmriPath == "" {
			jmriPath = fmt.Sprintf("/panel/%s/%s", typ, strings.ReplaceAll(userName, " ", "%20"))
		}

		panels = append(panels, PanelInfo{
			Name:     userName,
			Type:     typ,
			JmriPath: jmriPath,
		})
	}
	return panels, nil
}

// ── Send helpers ──────────────────────────────────────────────────────────────

func (c *TunnelClient) send(frame Frame) {
	b, err := json.Marshal(frame)
	if err != nil {
		c.logger.Printf("[jmri-tunnel] marshal error: %v", err)
		return
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	c.mu.Lock()
	conn := c.conn
	c.mu.Unlock()
	if conn == nil {
		return
	}
	if err := conn.WriteMessage(websocket.TextMessage, b); err != nil {
		c.logger.Printf("[jmri-tunnel] write error: %v", err)
	}
}

func (c *TunnelClient) sendError(connID, msg string) {
	c.send(Frame{Type: MsgError, ConnID: connID, ErrMsg: msg})
	c.logger.Printf("[jmri-tunnel] error connId=%s: %s", connID, msg)
}

func (c *TunnelClient) closeAllWSConns() {
	c.wsMu.Lock()
	conns := make(map[string]*proxiedWSConn, len(c.wsConns))
	for id, conn := range c.wsConns {
		conns[id] = conn
		delete(c.wsConns, id)
	}
	c.wsMu.Unlock()

	for _, conn := range conns {
		conn.close()
	}
}

// ── Path allow-list ───────────────────────────────────────────────────────────

func isAllowedPath(path string) bool {
	for _, blocked := range BlockedPathPrefixes {
		if strings.HasPrefix(path, blocked) {
			return false
		}
	}
	for _, allowed := range AllowedPathPrefixes {
		if strings.HasPrefix(path, allowed) {
			return true
		}
	}
	// Also allow root and static assets by extension
	if path == "/" {
		return true
	}
	lower := strings.ToLower(path)
	for _, ext := range []string{".html", ".css", ".js", ".png", ".gif", ".jpg", ".jpeg", ".svg", ".ico", ".bmp", ".xpm", ".webp", ".woff", ".woff2", ".ttf", ".map"} {
		if strings.HasSuffix(lower, ext) {
			return true
		}
	}
	return false
}

// ── URL helpers ───────────────────────────────────────────────────────────────

func httpToWS(u string) string {
	if strings.HasPrefix(u, "https://") {
		return "wss://" + u[8:]
	}
	if strings.HasPrefix(u, "http://") {
		return "ws://" + u[7:]
	}
	return u
}

func wsBaseURL(httpBase string) string {
	return httpToWS(httpBase)
}

func normalisePanelType(t string) string {
	switch {
	case strings.Contains(t, "Layout"):
		return "Layout"
	case strings.Contains(t, "ControlPanel"):
		return "ControlPanel"
	case strings.Contains(t, "Switchboard"):
		return "Switchboard"
	case strings.Contains(t, "Panel"):
		return "Panel"
	default:
		if t == "" {
			return "Panel"
		}
		return t
	}
}

func bytesReader(b []byte) io.Reader {
	if len(b) == 0 {
		return http.NoBody
	}
	return strings.NewReader(string(b))
}

func isNormalClose(err error) bool {
	if websocket.IsCloseError(err,
		websocket.CloseNormalClosure,
		websocket.CloseGoingAway,
		websocket.CloseNoStatusReceived,
	) {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "use of closed network connection") ||
		strings.Contains(msg, "EOF")
}
