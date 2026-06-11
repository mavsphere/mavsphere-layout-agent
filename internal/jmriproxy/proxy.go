// Package jmriproxy provides a reverse proxy for JMRI's built-in web server.
//
// The proxy is mounted at /jmri/ on the layout agent's local UI port (:8091).
// All HTTP requests are forwarded to the configured JMRI URL. WebSocket
// upgrades (used by JMRI's JSON API and panel live-update feeds) are tunnelled
// via a bidirectional gorilla/websocket pipe.
//
// The proxy rewrites:
//   - Absolute URLs in HTML responses (href/src/action pointing at JMRI origin)
//   - WebSocket upgrade requests (Upgrade: websocket)
//   - Location headers on redirects
//
// JMRI's web throttle (/roster) is explicitly blocked — throttle control must
// stay inside MavSphere's own HUD for session integrity (dead-man failsafe,
// control claim registry, billing timer).
package jmriproxy

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

const (
	// PathPrefix is the URL prefix under which the proxy is mounted on :8091.
	PathPrefix = "/jmri/"

	// blockedPaths lists JMRI paths that must never be proxied.
	// The JMRI web throttle at /roster is excluded because it bypasses
	// MavSphere's control claim registry and dead-man failsafe.
)

var blockedPaths = []string{
	"/roster",
}

// Proxy is the JMRI reverse proxy handler.
type Proxy struct {
	target   *url.URL
	rp       *httputil.ReverseProxy
	logger   *log.Logger
	upgrader websocket.Upgrader
}

// New creates a Proxy targeting the given JMRI URL (e.g. "http://localhost:12080").
func New(jmriURL string, logger *log.Logger) (*Proxy, error) {
	target, err := url.Parse(strings.TrimRight(jmriURL, "/"))
	if err != nil {
		return nil, fmt.Errorf("jmriproxy: parse target URL %q: %w", jmriURL, err)
	}
	if target.Scheme != "http" && target.Scheme != "https" {
		return nil, fmt.Errorf("jmriproxy: unsupported scheme %q (want http or https)", target.Scheme)
	}

	p := &Proxy{
		target: target,
		logger: logger,
		upgrader: websocket.Upgrader{
			CheckOrigin:  func(r *http.Request) bool { return true },
			Subprotocols: []string{"json"},
		},
	}

	rp := httputil.NewSingleHostReverseProxy(target)

	// Modify the outbound request before forwarding
	originalDirector := rp.Director
	rp.Director = func(req *http.Request) {
		originalDirector(req)
		// Strip the /jmri prefix so JMRI sees its own URL space
		req.URL.Path = strings.TrimPrefix(req.URL.Path, "/jmri")
		if req.URL.Path == "" {
			req.URL.Path = "/"
		}
		req.URL.RawPath = ""
		// Set Host to match target (some JMRI builds check this)
		req.Host = target.Host
		req.Header.Set("X-Forwarded-Host", req.Host)
		req.Header.Set("X-Forwarded-Prefix", "/jmri")
	}

	// Rewrite Location headers on redirects so the browser follows /jmri/... not JMRI origin
	rp.ModifyResponse = func(resp *http.Response) error {
		if loc := resp.Header.Get("Location"); loc != "" {
			if rewritten := rewriteLocation(loc, target, PathPrefix); rewritten != "" {
				resp.Header.Set("Location", rewritten)
			}
		}
		return nil
	}

	rp.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		logger.Printf("[jmriproxy] upstream error %s %s: %v", r.Method, r.URL.Path, err)
		http.Error(w, fmt.Sprintf("JMRI proxy error: %v\n\nCheck that JMRI is running and its web server is enabled at %s", err, jmriURL), http.StatusBadGateway)
	}

	p.rp = rp
	return p, nil
}

// Handler returns an http.Handler that should be registered at PathPrefix.
func (p *Proxy) Handler() http.Handler {
	return http.HandlerFunc(p.serveHTTP)
}

func (p *Proxy) serveHTTP(w http.ResponseWriter, r *http.Request) {
	// Strip /jmri prefix to get the JMRI-side path for policy checks
	jmriPath := strings.TrimPrefix(r.URL.Path, "/jmri")
	if jmriPath == "" {
		jmriPath = "/"
	}

	// Block disallowed paths
	for _, blocked := range blockedPaths {
		if strings.HasPrefix(jmriPath, blocked) {
			p.logger.Printf("[jmriproxy] BLOCKED %s %s (disallowed path)", r.Method, jmriPath)
			http.Error(w,
				"This JMRI endpoint is not available through MavSphere.\n"+
					"The JMRI web throttle (/roster) is disabled — use the MavSphere throttle HUD to control trains.",
				http.StatusForbidden)
			return
		}
	}

	// WebSocket upgrade: tunnel with bidirectional pipe
	if isWebSocketUpgrade(r) {
		p.proxyWebSocket(w, r, jmriPath)
		return
	}

	// Standard HTTP: delegate to httputil.ReverseProxy
	p.rp.ServeHTTP(w, r)
}

// isWebSocketUpgrade returns true if the request is a WebSocket upgrade.
func isWebSocketUpgrade(r *http.Request) bool {
	return strings.EqualFold(r.Header.Get("Upgrade"), "websocket")
}

// proxyWebSocket tunnels a WebSocket connection between the browser and JMRI.
// JMRI uses WebSockets for its JSON API (/json) and panel live updates.
func (p *Proxy) proxyWebSocket(w http.ResponseWriter, r *http.Request, jmriPath string) {
	// Build the upstream WS URL
	upstreamScheme := "ws"
	if p.target.Scheme == "https" {
		upstreamScheme = "wss"
	}
	upstreamURL := fmt.Sprintf("%s://%s%s", upstreamScheme, p.target.Host, jmriPath)
	if r.URL.RawQuery != "" {
		upstreamURL += "?" + r.URL.RawQuery
	}

	p.logger.Printf("[jmriproxy] WS tunnel %s → %s", r.URL.Path, upstreamURL)

	// Connect to JMRI WebSocket
	reqHeader := http.Header{}
	if proto := r.Header.Get("Sec-Websocket-Protocol"); proto != "" {
		reqHeader.Set("Sec-Websocket-Protocol", proto)
	}

	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
		Subprotocols:     websocket.Subprotocols(r),
	}
	upstream, resp, err := dialer.Dial(upstreamURL, reqHeader)
	if err != nil {
		body := ""
		if resp != nil {
			b, _ := io.ReadAll(resp.Body)
			body = string(b)
		}
		p.logger.Printf("[jmriproxy] WS dial %s failed: %v %s", upstreamURL, err, body)
		http.Error(w, fmt.Sprintf("JMRI WebSocket proxy error: %v\n\nCheck that JMRI is running at %s", err, p.target), http.StatusBadGateway)
		return
	}
	defer upstream.Close()

	// Upgrade the client connection
	respHeader := http.Header{}
	if proto := resp.Header.Get("Sec-Websocket-Protocol"); proto != "" {
		respHeader.Set("Sec-Websocket-Protocol", proto)
	}
	client, err := p.upgrader.Upgrade(w, r, respHeader)
	if err != nil {
		p.logger.Printf("[jmriproxy] WS upgrade client failed: %v", err)
		return
	}
	defer client.Close()

	errc := make(chan error, 2)

	// Client → JMRI
	go func() {
		for {
			msgType, msg, err := client.ReadMessage()
			if err != nil {
				errc <- err
				return
			}
			if err := upstream.WriteMessage(msgType, msg); err != nil {
				errc <- err
				return
			}
		}
	}()

	// JMRI → Client
	go func() {
		for {
			msgType, msg, err := upstream.ReadMessage()
			if err != nil {
				errc <- err
				return
			}
			if err := client.WriteMessage(msgType, msg); err != nil {
				errc <- err
				return
			}
		}
	}()

	err = <-errc
	if err != nil && !isNormalClose(err) {
		p.logger.Printf("[jmriproxy] WS tunnel closed: %v", err)
	}
}

// rewriteLocation rewrites an absolute Location header from JMRI origin to the proxy prefix.
// e.g. "http://localhost:12080/panel/Layout/Main" → "/jmri/panel/Layout/Main"
func rewriteLocation(loc string, target *url.URL, prefix string) string {
	u, err := url.Parse(loc)
	if err != nil {
		return ""
	}
	// Only rewrite if the redirect points at our JMRI target
	if !strings.EqualFold(u.Host, target.Host) {
		return ""
	}
	// Build the proxy-relative URL
	rewritten := &url.URL{
		Path:     strings.TrimRight(prefix, "/") + u.Path,
		RawQuery: u.RawQuery,
		Fragment: u.Fragment,
	}
	return rewritten.String()
}

// isNormalClose returns true for expected WebSocket close conditions.
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
		strings.Contains(msg, "connection reset by peer") ||
		strings.Contains(msg, "EOF")
}
