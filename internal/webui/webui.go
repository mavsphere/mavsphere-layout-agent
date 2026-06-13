// Package webui serves the layout agent configuration UI and local API on :8091.
package webui

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
	"unicode"

	gorillaws "github.com/gorilla/websocket"

	"github.com/mavsphere/mavsphere-layout-agent/internal/jmriproxy"
	"github.com/mavsphere/mavsphere-layout-agent/pkg/config"
	"github.com/mavsphere/mavsphere-layout-agent/pkg/device"
)

//go:embed static/*
var uiFS embed.FS

// StatusProvider is called by the /api/status endpoint.
type StatusProvider func() map[string]any

// SessionsProvider returns trains with active user control sessions.
type SessionsProvider func() []map[string]any

// TopologyProvider returns the layout topology for local consumers (e.g. simulator).
type TopologyProvider func() map[string]any

// RoutesProvider returns the currently cached active routes.
type RoutesProvider func() []map[string]any

// TrainsProvider returns the current train list fetched from the backend.
type TrainsProvider func() []map[string]any

// Server is the configuration web UI server.
type Server struct {
	addr    string
	cfgPath string
	srv     *http.Server

	mu          sync.Mutex
	getStatus   StatusProvider
	getSessions SessionsProvider
	getTopology TopologyProvider
	getRoutes   RoutesProvider
	getTrains   TrainsProvider
	preRestart  func()
}

// AttachStatus wires the live status callback.
func (s *Server) AttachStatus(fn StatusProvider) {
	s.mu.Lock()
	s.getStatus = fn
	s.mu.Unlock()
}

// AttachSessions wires the active sessions provider.
func (s *Server) AttachSessions(fn SessionsProvider) {
	s.mu.Lock()
	s.getSessions = fn
	s.mu.Unlock()
}

// AttachTopology wires the topology provider (fetched from backend at startup).
func (s *Server) AttachTopology(fn TopologyProvider) {
	s.mu.Lock()
	s.getTopology = fn
	s.mu.Unlock()
}

// AttachRoutes wires the active route state provider.
func (s *Server) AttachRoutes(fn RoutesProvider) {
	s.mu.Lock()
	s.getRoutes = fn
	s.mu.Unlock()
}

// AttachTrains wires the train list provider.
func (s *Server) AttachTrains(fn TrainsProvider) {
	s.mu.Lock()
	s.getTrains = fn
	s.mu.Unlock()
}

// SetPreRestart registers a cleanup hook called just before restart.
func (s *Server) SetPreRestart(fn func()) {
	s.mu.Lock()
	s.preRestart = fn
	s.mu.Unlock()
}

func configResponse(cfg *config.AgentConfig, revision string) map[string]any {
	b, err := json.Marshal(cfg)
	if err != nil {
		return map[string]any{"_revision": revision}
	}

	var out map[string]any
	if err := json.Unmarshal(b, &out); err != nil {
		return map[string]any{"_revision": revision}
	}
	out["_revision"] = revision
	return out
}

// Start launches the HTTP server and returns immediately.
func Start(addr, cfgPath string) *Server {
	s := &Server{addr: addr, cfgPath: cfgPath}

	mux := http.NewServeMux()

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/ui/") {
			http.StripPrefix("/ui/", http.FileServer(http.FS(uiFS))).ServeHTTP(w, r)
			return
		}
		f, err := uiFS.Open("static/index.html")
		if err != nil {
			http.Error(w, "index not found", http.StatusInternalServerError)
			return
		}
		defer f.Close()
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = io.Copy(w, f)
	})
	mux.Handle("/ui/", http.StripPrefix("/ui/", http.FileServer(http.FS(uiFS))))

	// ── /jmri/ — JMRI reverse proxy ──────────────────────────────────────────
	// Registered unconditionally; requests are rejected with a clear error if
	// JMRI is not configured or not reachable. This keeps the mux static —
	// the proxy is only active when cfg.Jmri.Enabled && cfg.Jmri.URL != "".
	mux.Handle(jmriproxy.PathPrefix, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cfg := config.Get()
		if cfg == nil || !cfg.Jmri.Enabled || cfg.Jmri.URL == "" {
			http.Error(w,
				"JMRI proxy is not enabled. Set jmri.enabled=true and jmri.url in the agent config.",
				http.StatusServiceUnavailable)
			return
		}
		p, err := jmriproxy.New(cfg.Jmri.URL, log.Default())
		if err != nil {
			http.Error(w, fmt.Sprintf("JMRI proxy config error: %v", err), http.StatusInternalServerError)
			return
		}
		p.Handler().ServeHTTP(w, r)
	}))

	// ── Bare JMRI path passthrough ────────────────────────────────────────────
	//
	// JMRI panel HTML/JS often uses root-relative URLs such as:
	//   /json/
	//   /panel/...
	//   /xml/...
	//   /resources/...
	//   /roster/...
	//
	// When viewing a panel through the local agent UI, those URLs resolve against
	// the agent host (:8091), not the JMRI host (:12080). These handlers forward
	// those bare paths directly to JMRI without the /jmri/ prefix.
	//
	// This is deliberately broader than the first version because JMRI signal
	// mast icons and panel support assets can live under /xml, /resources,
	// /webjars, /dist, /fonts, /prefs, /program, etc.
	jmriBareHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cfg := config.Get()
		if cfg == nil || !cfg.Jmri.Enabled || cfg.Jmri.URL == "" {
			http.NotFound(w, r)
			return
		}

		jmriBase := strings.TrimRight(cfg.Jmri.URL, "/")

		if isWebSocketUpgrade(r) {
			// WebSocket: dial JMRI directly and pipe bidirectionally.
			proxyJmriWebSocket(w, r, jmriBase, log.Default())
			return
		}

		// HTTP: reverse proxy directly to JMRI, no path prefix manipulation.
		target, err := url.Parse(jmriBase)
		if err != nil {
			http.Error(w, "bad JMRI URL", http.StatusInternalServerError)
			return
		}

		rp := httputil.NewSingleHostReverseProxy(target)
		origDirector := rp.Director
		rp.Director = func(req *http.Request) {
			origDirector(req)
			req.Host = target.Host
		}
		rp.ServeHTTP(w, r)
	})

	// Exact paths without trailing slash. Required because JMRI menus sometimes
	// navigate to /panel instead of /panel/.
	for _, p := range []string{
		"/json",
		"/web",
		"/tables",
		"/panel",
		"/help",
		"/js",
		"/css",
		"/resources",
		"/icons",
		"/images",
		"/xml",
		"/webjars",
		"/dist",
		"/fonts",
		"/font",
		"/prefs",
		"/program",
		"/roster",
		"/operations",
		"/about",
		"/permission",
		"/config",
	} {
		mux.Handle(p, jmriBareHandler)
	}

	// Prefix paths with trailing slash. Required for assets and dynamic JMRI
	// endpoints such as /xml/signals/..., /resources/..., /roster/entry/...
	for _, p := range []string{
		"/json/",
		"/web/",
		"/tables/",
		"/panel/",
		"/help/",
		"/js/",
		"/css/",
		"/resources/",
		"/icons/",
		"/images/",
		"/xml/",
		"/webjars/",
		"/dist/",
		"/fonts/",
		"/font/",
		"/prefs/",
		"/program/",
		"/roster/",
		"/operations/",
		"/about/",
		"/permission/",
		"/config/",
	} {
		mux.Handle(p, jmriBareHandler)
	}

	mux.HandleFunc("/api/config", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			cfg := config.Get()
			if cfg == nil {
				http.Error(w, "config not loaded", http.StatusServiceUnavailable)
				return
			}

			revision, err := config.FileRevision(s.cfgPath)
			if err != nil {
				http.Error(w, "config revision failed: "+err.Error(), http.StatusInternalServerError)
				return
			}

			w.Header().Set("X-Config-Revision", revision)
			writeJSON(w, configResponse(cfg, revision))

		case http.MethodPut:
			// X-Config-Revision is an optimistic-lock token. If omitted the
			// staleness check is skipped — acceptable for the web UI (which
			// always sends it) but any future CLI tool calling this endpoint
			// must also supply the header to get conflict protection.
			clientRevision := strings.TrimSpace(r.Header.Get("X-Config-Revision"))
			if clientRevision != "" {
				currentRevision, err := config.FileRevision(s.cfgPath)
				if err != nil {
					http.Error(w, "config revision failed: "+err.Error(), http.StatusInternalServerError)
					return
				}
				if currentRevision != clientRevision {
					w.Header().Set("X-Config-Revision", currentRevision)
					http.Error(w, "config changed since this page loaded; reload the config before saving", http.StatusConflict)
					return
				}
			}

			var incoming config.AgentConfig
			if err := json.NewDecoder(r.Body).Decode(&incoming); err != nil {
				http.Error(w, "invalid json: "+err.Error(), http.StatusBadRequest)
				return
			}

			incoming.LayoutID = strings.TrimSpace(incoming.LayoutID)
			incoming.BackendURL = strings.TrimSpace(incoming.BackendURL)
			incoming.BackendWsURL = strings.TrimSpace(incoming.BackendWsURL)
			incoming.Username = strings.TrimSpace(incoming.Username)
			incoming.JanusURL = strings.TrimSpace(incoming.JanusURL)
			incoming.VideoCodec = strings.TrimSpace(incoming.VideoCodec)
			incoming.H264Encoder = strings.TrimSpace(incoming.H264Encoder)

			if err := normalizeCameraConfigs(&incoming); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			if j, err := normalizeJanusURL(incoming.JanusURL, incoming.BackendURL); err != nil {
				http.Error(w, "janusUrl invalid: "+err.Error(), http.StatusBadRequest)
				return
			} else {
				incoming.JanusURL = j
			}

			if incoming.BackendWsURL == "" {
				incoming.BackendWsURL = deriveWsURL(incoming.BackendURL, "/api/ws/agent")
			}

			if err := config.Save(s.cfgPath, &incoming); err != nil {
				http.Error(w, "save failed: "+err.Error(), http.StatusBadRequest)
				return
			}

			newRevision, err := config.FileRevision(s.cfgPath)
			if err != nil {
				http.Error(w, "config revision failed after save: "+err.Error(), http.StatusInternalServerError)
				return
			}
			w.Header().Set("X-Config-Revision", newRevision)

			restart := r.URL.Query().Get("restart")
			if restart == "1" || strings.EqualFold(restart, "true") {
				writeJSON(w, map[string]any{"ok": true, "restarting": true, "revision": newRevision})
				s.restartSelf(300 * time.Millisecond)
				return
			}
			writeJSON(w, map[string]any{"ok": true, "revision": newRevision})

		default:
			w.Header().Set("Allow", "GET, PUT")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	// ── /api/status ───────────────────────────────────────────────────────────
	// Returns richer state including phase, ready, degraded flags.
	mux.HandleFunc("/api/status", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", "GET")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		s.mu.Lock()
		fn := s.getStatus
		s.mu.Unlock()
		if fn == nil {
			writeJSON(w, map[string]any{"attached": false})
			return
		}
		writeJSON(w, fn())
	})

	// ── /api/sessions ─────────────────────────────────────────────────────────
	mux.HandleFunc("/api/sessions", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", "GET")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		s.mu.Lock()
		fn := s.getSessions
		s.mu.Unlock()
		if fn == nil {
			writeJSON(w, map[string]any{"activeSessions": []map[string]any{}})
			return
		}
		writeJSON(w, map[string]any{"activeSessions": fn()})
	})

	// ── /api/topology ─────────────────────────────────────────────────────────
	// Serves cached topology to local tools (simulator, debug clients).
	// No authentication required — this server only binds to localhost.
	mux.HandleFunc("/api/topology", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", "GET")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		s.mu.Lock()
		fn := s.getTopology
		s.mu.Unlock()
		if fn == nil {
			http.Error(w, "topology not available yet", http.StatusServiceUnavailable)
			return
		}
		topo := fn()
		if topo == nil {
			http.Error(w, "topology not available yet", http.StatusServiceUnavailable)
			return
		}
		writeJSON(w, topo)
	})

	// ── /api/routes ───────────────────────────────────────────────────────────
	// Serves active route cache to local tools (simulator).
	mux.HandleFunc("/api/routes", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", "GET")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		s.mu.Lock()
		fn := s.getRoutes
		s.mu.Unlock()
		if fn == nil {
			writeJSON(w, map[string]any{"activeRoutes": []any{}})
			return
		}
		writeJSON(w, map[string]any{"activeRoutes": fn()})
	})

	// ── /api/trains ───────────────────────────────────────────────────────────
	// Serves current train list to local tools (simulator).
	mux.HandleFunc("/api/trains", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", "GET")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		s.mu.Lock()
		fn := s.getTrains
		s.mu.Unlock()
		if fn == nil {
			writeJSON(w, map[string]any{"trains": []any{}})
			return
		}
		writeJSON(w, map[string]any{"trains": fn()})
	})

	// ── /api/jmri/panels ─────────────────────────────────────────────────────
	// Fetches the list of open panels from JMRI's JSON API and returns them.
	// Used by the config UI to auto-populate the panel picker.
	// Returns: {"panels": [{"name": "...", "type": "Layout|ControlPanel|Panel|Switchboard", "url": "/jmri/panel/..."}]}
	mux.HandleFunc("/api/jmri/panels", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", "GET")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		cfg := config.Get()
		if cfg == nil || !cfg.Jmri.Enabled || cfg.Jmri.URL == "" {
			writeJSON(w, map[string]any{"panels": []any{}, "error": "JMRI not configured"})
			return
		}
		panels, err := fetchJmriPanels(cfg.Jmri.URL)
		if err != nil {
			log.Printf("[jmri] panel fetch error: %v", err)
			writeJSON(w, map[string]any{"panels": []any{}, "error": err.Error()})
			return
		}
		writeJSON(w, map[string]any{"panels": panels})
	})

	// ── /api/cameras/scan ─────────────────────────────────────────────────────
	// Enumerates all /dev/video* devices and returns their capabilities.
	// Used by the UI "Scan for cameras" feature.
	mux.HandleFunc("/api/cameras/scan", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", "GET")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		results := scanAllVideoDevices()
		writeJSON(w, map[string]any{"devices": results})
	})

	// ── /api/cameras/check ────────────────────────────────────────────────────
	// Checks each configured camera device and returns its status + caps.
	// For USB cameras: runs v4l2-ctl to enumerate modes.
	// For RTSP/HTTP-MJPEG cameras: does a TCP dial to confirm host reachability.
	// Used by the UI "Check cameras" feature on startup.
	mux.HandleFunc("/api/cameras/check", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", "GET")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		cfg := config.Get()
		if cfg == nil {
			writeJSON(w, map[string]any{"cameras": []any{}})
			return
		}
		type camResult struct {
			CameraID string            `json:"cameraId"`
			Label    string            `json:"label"`
			Source   string            `json:"source"`
			Device   string            `json:"device,omitempty"`
			RTSPURL  string            `json:"rtspUrl,omitempty"`
			OK       bool              `json:"ok"`
			Error    string            `json:"error,omitempty"`
			Caps     *device.VideoCaps `json:"caps,omitempty"`
		}
		var out []camResult
		for _, c := range cfg.Cameras {
			src := c.Source
			if src == "" {
				src = "usb"
			}
			switch src {
			case "rtsp", "http_mjpeg":
				// TCP connectivity probe: extract host:port from URL
				u := c.RTSPURL
				host, dialErr := probeIPCameraURL(u)
				if dialErr != nil {
					out = append(out, camResult{
						CameraID: c.CameraID,
						Label:    c.Label,
						Source:   src,
						RTSPURL:  u,
						OK:       false,
						Error:    fmt.Sprintf("unreachable (%s): %v", host, dialErr),
					})
				} else {
					out = append(out, camResult{
						CameraID: c.CameraID,
						Label:    c.Label,
						Source:   src,
						RTSPURL:  u,
						OK:       true,
					})
				}
			default:
				caps, err := device.GetVideoCaps(c.Device)
				if err != nil {
					out = append(out, camResult{
						CameraID: c.CameraID,
						Label:    c.Label,
						Source:   src,
						Device:   c.Device,
						OK:       false,
						Error:    err.Error(),
					})
				} else {
					out = append(out, camResult{
						CameraID: c.CameraID,
						Label:    c.Label,
						Source:   src,
						Device:   c.Device,
						OK:       true,
						Caps:     caps,
					})
				}
			}
		}
		writeJSON(w, map[string]any{"cameras": out})
	})

	// ── /api/restart ──────────────────────────────────────────────────────────
	mux.HandleFunc("/api/restart", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", "POST")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		writeJSON(w, map[string]any{"ok": true, "restarting": true})
		s.restartSelf(300 * time.Millisecond)
	})

	s.srv = &http.Server{
		Addr:    addr,
		Handler: logRequests(mux),
	}

	go func() {
		log.Printf("[UI] Layout agent config UI at http://%s/", addr)
		if err := s.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("[UI] server error: %v", err)
		}
	}()

	return s
}

// Stop gracefully shuts down the HTTP server.
func (s *Server) Stop() error {
	if s.srv == nil {
		return nil
	}
	return s.srv.Close()
}

func (s *Server) restartSelf(delay time.Duration) {
	go func() {
		time.Sleep(delay)

		// Run pre-restart cleanup hook.
		s.mu.Lock()
		fn := s.preRestart
		s.mu.Unlock()
		if fn != nil {
			func() {
				defer func() {
					if r := recover(); r != nil {
						log.Printf("[UI] pre-restart hook panicked: %v", r)
					}
				}()
				fn()
			}()
		}

		// Shut down the HTTP server cleanly before exiting.
		if s.srv != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
			_ = s.srv.Shutdown(ctx)
			cancel()
		}

		log.Printf("[UI] restarting — exiting process (Docker will restart container)")
		os.Exit(0)
	}()
}

func normalizeCameraConfigs(cfg *config.AgentConfig) error {
	seen := map[string]int{}
	for i := range cfg.Cameras {
		c := &cfg.Cameras[i]
		c.CameraID = strings.TrimSpace(c.CameraID)
		c.Label = strings.TrimSpace(c.Label)
		c.Type = strings.TrimSpace(c.Type)
		c.Device = strings.TrimSpace(c.Device)
		c.TrainSlug = strings.TrimSpace(c.TrainSlug)
		c.Source = strings.ToLower(strings.TrimSpace(c.Source))
		c.RTSPURL = strings.TrimSpace(c.RTSPURL)
		c.RTSPTransport = strings.ToLower(strings.TrimSpace(c.RTSPTransport))

		// Default source to "usb" for backward compat
		if c.Source == "" {
			c.Source = config.CameraSourceUSB
		}

		if c.Type == "" {
			c.Type = "OVERVIEW"
		}
		if !strings.EqualFold(c.Type, "ONBOARD") {
			c.TrainSlug = ""
		}

		if c.CameraID == "" {
			c.CameraID = slugCameraID(c.Label)
		}
		if c.CameraID == "" {
			switch c.Source {
			case config.CameraSourceRTSP, config.CameraSourceHTTPMJPEG:
				// derive id from URL host part
				if c.RTSPURL != "" {
					// strip scheme, use host
					u := c.RTSPURL
					if idx := strings.Index(u, "://"); idx >= 0 {
						u = u[idx+3:]
					}
					if idx := strings.IndexAny(u, "/:"); idx > 0 {
						u = u[:idx]
					}
					c.CameraID = "cam-" + slugCameraID(strings.ReplaceAll(u, ".", "-"))
				}
			default:
				if c.Device != "" {
					c.CameraID = "cam-" + slugCameraID(filepath.Base(c.Device))
				}
			}
		}
		if c.CameraID == "" {
			return fmt.Errorf("camera %d is missing a camera name", i+1)
		}

		// Normalise even manually supplied IDs so the backend command topic remains URL/topic-safe.
		c.CameraID = slugCameraID(c.CameraID)
		if c.CameraID == "" {
			return fmt.Errorf("camera %d has an invalid camera name", i+1)
		}
		if c.Label == "" {
			c.Label = c.CameraID
		}

		// Source-specific required field validation
		switch c.Source {
		case config.CameraSourceRTSP, config.CameraSourceHTTPMJPEG:
			if c.RTSPURL == "" {
				return fmt.Errorf("camera %q: source=%s requires rtspUrl", c.Label, c.Source)
			}
		default:
			// USB
			if c.Device == "" {
				return fmt.Errorf("camera %q is missing a device", c.Label)
			}
		}

		seen[c.CameraID]++
		if seen[c.CameraID] > 1 {
			return fmt.Errorf("camera name %q generates duplicate cameraId %q; camera names must be unique", c.Label, c.CameraID)
		}
	}
	return nil
}

func slugCameraID(value string) string {
	value = strings.TrimSpace(strings.ToLower(value))
	var b strings.Builder
	lastDash := false
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
			lastDash = false
		case r == '-' || r == '_' || unicode.IsSpace(r) || r == '/' || r == '.':
			if b.Len() > 0 && !lastDash {
				b.WriteByte('-')
				lastDash = true
			}
		default:
			if unicode.IsLetter(r) || unicode.IsDigit(r) {
				b.WriteRune(r)
				lastDash = false
			} else if b.Len() > 0 && !lastDash {
				b.WriteByte('-')
				lastDash = true
			}
		}
	}
	return strings.Trim(b.String(), "-")
}

func normalizeJanusURL(input, backendURL string) (string, error) {
	in := strings.TrimSpace(input)
	if in == "" {
		return "", fmt.Errorf("empty")
	}
	defaultScheme := "ws"
	if u, err := url.Parse(backendURL); err == nil && strings.EqualFold(u.Scheme, "https") {
		defaultScheme = "wss"
	}
	if !strings.Contains(in, "://") {
		in = defaultScheme + "://" + in
	}
	u, err := url.Parse(in)
	if err != nil {
		return "", fmt.Errorf("parse: %w", err)
	}
	if u.Host == "" {
		return "", fmt.Errorf("missing host")
	}
	switch strings.ToLower(u.Scheme) {
	case "ws", "wss":
	case "http":
		u.Scheme = "ws"
	case "https":
		u.Scheme = "wss"
	default:
		return "", fmt.Errorf("unsupported scheme: %s", u.Scheme)
	}
	if u.Path == "" || u.Path == "/" {
		u.Path = "/janus"
	} else if !strings.HasSuffix(u.Path, "/janus") {
		u.Path = strings.TrimRight(u.Path, "/") + "/janus"
	}
	u.RawQuery = ""
	u.Fragment = ""
	return u.String(), nil
}

func deriveWsURL(base, path string) string {
	u, err := url.Parse(base)
	if err != nil || u.Host == "" {
		return ""
	}
	scheme := "ws"
	if strings.EqualFold(u.Scheme, "https") {
		scheme = "wss"
	}
	return fmt.Sprintf("%s://%s%s", scheme, u.Host, path)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.RawQuery != "" {
			log.Printf("[UI] %s %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)
		} else {
			log.Printf("[UI] %s %s", r.Method, r.URL.Path)
		}
		next.ServeHTTP(w, r)
	})
}

// JmriPanel describes a single JMRI panel available for proxying.
type JmriPanel struct {
	Name     string `json:"name"`
	Type     string `json:"type"`     // "Layout", "ControlPanel", "Panel", "Switchboard"
	URL      string `json:"url"`      // proxy-relative URL, e.g. "/jmri/panel/..."
	JmriPath string `json:"jmriPath"` // JMRI-side path, e.g. "/panel/Layout/Main"
}

// fetchJmriPanels queries JMRI's JSON API to get the list of open panels.
func fetchJmriPanels(jmriURL string) ([]JmriPanel, error) {
	base := strings.TrimRight(jmriURL, "/")
	client := &http.Client{Timeout: 5 * time.Second}

	resp, err := client.Get(base + "/json/panels")
	if err != nil {
		return nil, fmt.Errorf("JMRI not reachable at %s: %w", jmriURL, err)
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
		return nil, fmt.Errorf("decode JMRI panel list: %w", err)
	}

	var panels []JmriPanel
	for _, item := range raw {
		if item.Data == nil {
			continue
		}
		userName, _ := item.Data["userName"].(string)
		panelType, _ := item.Data["type"].(string)
		jmriURLField, _ := item.Data["URL"].(string)

		if userName == "" {
			continue
		}
		panelType = normaliseJmriPanelType(panelType)

		// Strip ?format=xml from JMRI's URL field — we want the HTML panel.
		jmriPath := jmriURLField
		if idx := strings.Index(jmriPath, "?"); idx != -1 {
			jmriPath = jmriPath[:idx]
		}
		if jmriPath == "" {
			jmriPath = fmt.Sprintf("/panel/%s/%s", panelType, url.PathEscape(userName))
		}

		panels = append(panels, JmriPanel{
			Name:     userName,
			Type:     panelType,
			URL:      jmriproxy.PathPrefix + strings.TrimPrefix(jmriPath, "/"),
			JmriPath: jmriPath,
		})
	}
	return panels, nil
}

// normaliseJmriPanelType maps JMRI's verbose editor type names to short forms.
func normaliseJmriPanelType(t string) string {
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

// probeIPCameraURL extracts host:port from a camera URL and tries a TCP dial.
// Returns the host string and any error. Used by /api/cameras/check for IP sources.
func probeIPCameraURL(rawURL string) (string, error) {
	if rawURL == "" {
		return "", fmt.Errorf("empty URL")
	}
	// Parse as URL; fall back to treating it as host:port directly.
	u, err := url.Parse(rawURL)
	if err != nil {
		return rawURL, fmt.Errorf("invalid URL: %w", err)
	}
	host := u.Hostname()
	port := u.Port()
	if port == "" {
		switch strings.ToLower(u.Scheme) {
		case "rtsp", "rtsps":
			port = "554"
		case "http":
			port = "80"
		case "https":
			port = "443"
		default:
			port = "554"
		}
	}
	addr := net.JoinHostPort(host, port)
	conn, dialErr := net.DialTimeout("tcp", addr, 3*time.Second)
	if dialErr != nil {
		return addr, dialErr
	}
	conn.Close()
	return addr, nil
}

// scanAllVideoDevices enumerates /dev/video* and returns caps for each capture-capable device.
func scanAllVideoDevices() []map[string]any {
	matches, _ := filepath.Glob("/dev/video*")
	var results []map[string]any
	for _, dev := range matches {
		info, err := os.Stat(dev)
		if err != nil || info.IsDir() {
			continue
		}
		caps, err := device.GetVideoCaps(dev)
		if err != nil {
			// Not a capture device (e.g. metadata node) — skip silently
			continue
		}
		if len(caps.Modes) == 0 {
			continue
		}
		results = append(results, map[string]any{
			"device": dev,
			"caps":   caps,
		})
	}
	return results
}

// isWebSocketUpgrade returns true if the request is a WebSocket upgrade.
func isWebSocketUpgrade(r *http.Request) bool {
	return strings.EqualFold(r.Header.Get("Upgrade"), "websocket")
}

// proxyJmriWebSocket dials JMRI's WebSocket directly and pipes frames
// bidirectionally between the browser and JMRI. Used for bare /json/
// connections from JMRI panel page JavaScript.
func proxyJmriWebSocket(w http.ResponseWriter, r *http.Request, jmriBase string, logger *log.Logger) {
	upgrader := gorillaws.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}

	// Build upstream WS URL.
	wsBase := jmriBase
	if strings.HasPrefix(wsBase, "https://") {
		wsBase = "wss://" + wsBase[8:]
	} else if strings.HasPrefix(wsBase, "http://") {
		wsBase = "ws://" + wsBase[7:]
	}
	upstreamURL := wsBase + r.URL.RequestURI()

	// Connect to JMRI.
	dialer := gorillaws.Dialer{HandshakeTimeout: 10 * time.Second}
	upstream, _, err := dialer.Dial(upstreamURL, nil)
	if err != nil {
		logger.Printf("[webui-jmri-ws] dial %s failed: %v", upstreamURL, err)
		http.Error(w, "JMRI WebSocket unavailable: "+err.Error(), http.StatusBadGateway)
		return
	}
	defer upstream.Close()

	// Upgrade browser connection.
	client, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		logger.Printf("[webui-jmri-ws] upgrade failed: %v", err)
		return
	}
	defer client.Close()

	errc := make(chan error, 2)

	go func() {
		for {
			mt, msg, err := client.ReadMessage()
			if err != nil {
				errc <- err
				return
			}
			if err := upstream.WriteMessage(mt, msg); err != nil {
				errc <- err
				return
			}
		}
	}()

	go func() {
		for {
			mt, msg, err := upstream.ReadMessage()
			if err != nil {
				errc <- err
				return
			}
			if err := client.WriteMessage(mt, msg); err != nil {
				errc <- err
				return
			}
		}
	}()

	<-errc
}
