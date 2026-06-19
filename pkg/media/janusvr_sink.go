package media

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"math"
	"net/url"
	"os"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	cfg "github.com/mavsphere/mavsphere-layout-agent/pkg/config"
	"github.com/mavsphere/mavsphere-layout-agent/pkg/device"
	"github.com/mavsphere/mavsphere-layout-agent/pkg/iceutil"
)

// ICEConfig is the local ICE configuration that must be applied to the
// local WebRTC stack (inside janusvrwebrtcsink) so candidate gathering works
// in restrictive networks (TURN/relay required).
type ICEConfig struct {
	StunURL string

	// Backward-compatible single TURN URL (may be empty if TurnURLs is used)
	TurnURL string

	// NEW: full ordered list of TURN URLs from backend
	TurnURLs []string

	Username   string
	Password   string
	ForceRelay bool
	TTLSeconds int
}

// Manager owns the janusvrwebrtcsink publisher process and keeps it alive
// for a given VideoRoom (roomID). On error it will auto-restart the pipeline
// with exponential backoff until Stop or context cancellation.
type Manager struct {
	mu sync.Mutex

	conf     *cfg.AgentConfig
	videoDev string
	audioDev string

	// IP camera source (populated when source != USB).
	// ipSource is one of cfg.CameraSourceRTSP / CameraSourceHTTPMJPEG.
	// ipURL is the full stream URL.
	// ipTransport is the rtspsrc protocols override (e.g. "tcp").
	// ipBufferMs is the latency buffer in ms (0 = GStreamer default).
	ipSource    string
	ipURL       string
	ipTransport string
	ipBufferMs  int

	cmd    *exec.Cmd
	roomID int64
	stopCh chan struct{}
	logger *log.Logger

	// closed when the currently tracked cmd exits (used to wait for clean shutdown).
	procDone chan struct{}

	// Health tracking.
	healthy      bool
	lastICEState string
	lastHealthy  time.Time

	// ICE config for janusvrwebrtcsink (must be set in NULL/READY)
	ice ICEConfig

	// lastAttempt records the videoAttempt that produced the currently-running
	// gst-launch pipeline. Set at the moment cmd.Start() succeeds so that
	// StreamInfo() can report what is actually running rather than what the
	// config requested (the two can differ when auto-fallback logic fires).
	lastAttempt videoAttempt
	// lastPixFmt is the capture pixel format selected for the current pipeline
	// by selectCameraMode (e.g. "MJPG", "YUYV", "H264").
	lastPixFmt string
	// lastPipeline is the full gst-launch pipeline string for the current run.
	lastPipeline string
}

// NewManager creates a new Manager bound to the given config and devices.
// logger may be nil; in that case log.Printf is used.
func NewManager(conf *cfg.AgentConfig, videoDev, audioDev string, logger *log.Logger) *Manager {
	return &Manager{
		conf:     conf,
		videoDev: videoDev,
		audioDev: audioDev,
		logger:   logger,
	}
}

// NewIPSourceManager creates a Manager that pulls video from an IP stream
// (RTSP or HTTP-MJPEG) rather than a local V4L2 device.
//
//	source    – cfg.CameraSourceRTSP or cfg.CameraSourceHTTPMJPEG
//	url       – full stream URL
//	transport – rtspsrc protocols value ("tcp", "udp", "auto" …); ignored for HTTP-MJPEG
//	bufferMs  – latency buffer in ms (0 = built-in default)
func NewIPSourceManager(conf *cfg.AgentConfig, source, url, transport string, bufferMs int, logger *log.Logger) *Manager {
	return &Manager{
		conf:        conf,
		ipSource:    source,
		ipURL:       url,
		ipTransport: transport,
		ipBufferMs:  bufferMs,
		logger:      logger,
	}
}

// SetICE updates the ICE configuration used when building the pipeline.
// If a pipeline is currently running, it is restarted so the new ICE settings
// can take effect (janusvrwebrtcsink ICE properties are NULL/READY only).
func (m *Manager) SetICE(ctx context.Context, ice ICEConfig) {
	// Apply local override if present
	backendForceRelay := ice.ForceRelay

	var (
		overridePresent bool
		overrideVal     bool
	)
	if m.conf != nil && m.conf.ForceRelayOverride != nil {
		overridePresent = true
		overrideVal = *m.conf.ForceRelayOverride
		ice.ForceRelay = overrideVal
	}

	if overridePresent {
		m.logf("[stream][ice] backendForceRelay=%v overridePresent=true overrideVal=%v effective=%v",
			backendForceRelay, overrideVal, ice.ForceRelay)
	} else {
		m.logf("[stream][ice] backendForceRelay=%v overridePresent=false effective=%v",
			backendForceRelay, ice.ForceRelay)
	}

	turnList := normalizeTurnList(ice)
	m.logf("[stream][ice] received TURN list (%d): %v", len(turnList), turnList)

	if ice.ForceRelay {
		chosen := chooseTurnURLForForceRelay(turnList)
		if chosen != "" && chosen != ice.TurnURL {
			m.logf("[stream][ice] forceRelay=true selecting TURN URL: %s (was %s)",
				iceutil.PrettyTurnVariant(chosen), iceutil.PrettyTurnVariant(ice.TurnURL))
			ice.TurnURL = chosen
		}
	}

	m.mu.Lock()
	m.ice = ice

	// If running, restart to apply new NULL/READY-only properties.
	running := m.cmd != nil
	roomID := m.roomID
	m.mu.Unlock()

	if running && roomID > 0 {
		m.logf("[stream][ice] ICE updated (STUN=%q TURN=%s user=%q forceRelay=%v ttl=%ds); restarting pipeline to apply",
			ice.StunURL, iceutil.PrettyTurnVariant(ice.TurnURL), ice.Username, ice.ForceRelay, ice.TTLSeconds)
		_ = m.ForceRestart(ctx, roomID)
	} else {
		m.logf("[stream][ice] ICE updated (STUN=%q TURN=%s user=%q forceRelay=%v ttl=%ds); will apply on next start",
			ice.StunURL, iceutil.PrettyTurnVariant(ice.TurnURL), ice.Username, ice.ForceRelay, ice.TTLSeconds)
	}
}

func (m *Manager) logf(format string, args ...interface{}) {
	if m.logger != nil {
		m.logger.Printf(format, args...)
		return
	}
	log.Printf(format, args...)
}

// IsHealthy reports whether the currently tracked gst-launch pipeline is
// considered healthy (ICE connected/completed at least once and not failed).
//
// Note: With main.go ignoring duplicate START_VIDEO unconditionally,
// "healthy" is no longer used to decide whether to restart on viewer spam.
// It is only useful for diagnostics / watchdog logic.
func (m *Manager) IsHealthy() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.cmd == nil {
		return false
	}

	if m.healthy {
		return true
	}

	// If we were healthy very recently, treat as healthy (covers brief log gaps).
	const recentHealthyWindow = 20 * time.Second
	if !m.lastHealthy.IsZero() && time.Since(m.lastHealthy) < recentHealthyWindow {
		return true
	}

	return false
}

func (m *Manager) markICE(state string) {
	state = strings.ToLower(strings.TrimSpace(state))
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastICEState = state
	switch state {
	case "connected", "completed":
		m.healthy = true
		m.lastHealthy = time.Now()
	case "failed":
		// Do not immediately mark unhealthy if we were healthy very recently;
		// avoids transient churn causing “unhealthy” signals.
		const recentHealthyWindow = 20 * time.Second
		if !m.lastHealthy.IsZero() && time.Since(m.lastHealthy) < recentHealthyWindow {
			return
		}
		m.healthy = false
	}
}

func (m *Manager) markHealthy(reason string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.healthy = true
	m.lastHealthy = time.Now()
	if reason != "" {
		m.lastICEState = reason
	}
}

// IsRunning reports whether there is a gst-launch child currently tracked.
func (m *Manager) IsRunning() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.cmd != nil
}

// Start ensures there is a running publisher pipeline for roomID.
// If a different room is active it is stopped and replaced.
// The pipeline is supervised and will auto-restart on error.
func (m *Manager) Start(ctx context.Context, roomID int64) error {
	if roomID <= 0 {
		return fmt.Errorf("stream.Manager.Start: invalid roomID %d", roomID)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Idempotent for same-room starts: never restart because viewers spam START_VIDEO.
	if m.cmd != nil && m.roomID == roomID {
		m.logf("[stream] START_VIDEO ignored; already publishing roomId=%d (healthy=%v ice=%s)",
			roomID, m.healthy, m.lastICEState)
		return nil
	}

	// Stop any existing pipeline/supervisor (room switch or previously stopped).
	if m.stopCh != nil {
		close(m.stopCh)
		m.stopCh = nil
	}
	if m.cmd != nil {
		m.gracefulTerminate(m.cmd, "switch_room", 5*time.Second)
		// Wait for hardware devices (V4L2, ALSA) to be released by the dying
		// process. gracefulTerminate already waits for process exit via procDone,
		// but the kernel can take a moment after process exit to release V4L2/ALSA.
		// 1.5s is enough on tested hardware; 300ms was not — it caused every
		// subsequent attempt in the new stream's runLoop to see V4L2 busy.
		time.Sleep(1500 * time.Millisecond)
		m.cmd = nil
		m.procDone = nil
	}

	m.roomID = roomID
	m.healthy = false
	m.lastICEState = ""
	m.lastHealthy = time.Time{}
	m.stopCh = make(chan struct{})

	go m.runLoop(ctx, roomID, m.stopCh)
	return nil
}

// ForceRestart tears down any existing pipeline and starts a new one.
func (m *Manager) ForceRestart(ctx context.Context, roomID int64) error {
	if roomID <= 0 {
		return fmt.Errorf("stream.Manager.ForceRestart: invalid roomID %d", roomID)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.stopCh != nil {
		close(m.stopCh)
		m.stopCh = nil
	}
	if m.cmd != nil {
		m.gracefulTerminate(m.cmd, "restart_request", 5*time.Second)
		time.Sleep(300 * time.Millisecond)
		m.cmd = nil
		m.procDone = nil
	}

	m.roomID = roomID
	m.healthy = false
	m.lastICEState = ""
	m.lastHealthy = time.Time{}
	m.stopCh = make(chan struct{})

	go m.runLoop(ctx, roomID, m.stopCh)
	return nil
}

// Stop terminates any running pipeline and stops the supervisor loop.
func (m *Manager) Stop() {
	m.mu.Lock()
	if m.stopCh != nil {
		close(m.stopCh)
		m.stopCh = nil
	}
	// Snapshot the cmd under the lock, then release before calling
	// gracefulTerminate — which also acquires m.mu to read m.procDone.
	// Holding the lock across gracefulTerminate causes a self-deadlock that
	// makes it fall through to the fire-and-forget time.AfterFunc path,
	// returning before the gst-launch process has actually exited and
	// released the V4L2 device.
	cmd := m.cmd
	m.mu.Unlock()

	if cmd != nil {
		m.gracefulTerminate(cmd, "stop", 5*time.Second)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.cmd = nil
	m.procDone = nil
	m.roomID = 0
	m.healthy = false
	m.lastICEState = ""
	m.lastHealthy = time.Time{}
}

// videoAttempt describes a single ordered pipeline attempt.
type videoAttempt struct {
	codec   string // "h264" or "vp8"
	enc     string // for h264 only: "v4l2h264enc" or "x264enc"
	useMJPG bool   // prefer camera MJPG (image/jpeg) + jpegdec
	w, h    int
	fps     int
}

// runLoop supervises a gst-launch janusvrwebrtcsink pipeline.
// It restarts on exit with exponential backoff unless ctx or stopCh closes.
func (m *Manager) runLoop(ctx context.Context, roomID int64, stopCh chan struct{}) {
	backoff := 5 * time.Second
	maxBackoff := 60 * time.Second

	// Tuning:
	const (
		iceConnectDeadline    = 30 * time.Second
		iceFailGrace          = 1 * time.Second
		quickFailWindow       = 8 * time.Second
		terminateGraceTimeout = 5 * time.Second
		startFailDelay        = 1500 * time.Millisecond

		alsaBusyDelayOnStartFail = 5 * time.Second
		alsaBusyDelayAfterExit   = 5 * time.Second

		v4l2BusyDelayAfterExit = 2 * time.Second
	)

	for {
		select {
		case <-ctx.Done():
			m.logf("[stream] context canceled, stopping publisher loop for room %d", roomID)
			return
		case <-stopCh:
			m.logf("[stream] stop requested, stopping publisher loop for room %d", roomID)
			return
		default:
		}

		videoDev := defaultIfEmpty(m.videoDev, "/dev/video0")
		// For IP camera sources (RTSP / HTTP-MJPEG) there is no V4L2 device to
		// enumerate; build a single direct attempt using the current config.
		var attempts []videoAttempt
		if m.ipSource == cfg.CameraSourceRTSP || m.ipSource == cfg.CameraSourceHTTPMJPEG {
			attempts = []videoAttempt{
				{
					codec:   m.conf.VideoCodec,
					enc:     m.conf.H264Encoder,
					w:       m.conf.VideoWidth,
					h:       m.conf.VideoHeight,
					fps:     m.conf.VideoFps,
					useMJPG: false,
				},
			}
		} else {
			attempts = buildVideoAttempts(m.conf, videoDev)
		}

		// If ALSA is busy, we can fall back to video-only to keep the stream alive.
		var disableAudio atomic.Bool

		var lastErr error
		var startedOK bool
		v4l2BusyStreak := 0 // consecutive attempts that failed solely due to V4L2 busy

		for i, a := range attempts {
			pipeline := m.buildPipelineForAttempt(roomID, a, !disableAudio.Load())

			args := []string{"-e"}
			args = append(args, strings.Fields(pipeline)...)

			cmd := exec.CommandContext(ctx, "gst-launch-1.0", args...)

			// Set per-child env so ICE scanner always sees what it needs, without mutating
			// the agent process env (os.Setenv affects everything globally).
			cmd.Env = os.Environ()

			// Always ensure no-color output for scanning.
			cmd.Env = append(cmd.Env, "GST_DEBUG_NO_COLOR=1")

			// Only force minimum debug if the operator didn't already set it.
			// This makes ICE log parsing deterministic.
			hostGstDebug := strings.TrimSpace(os.Getenv("GST_DEBUG"))
			if hostGstDebug == "" {
				cmd.Env = append(cmd.Env,
					"GST_DEBUG=2,"+
						// suppress camera spam (WARNs)
						"v4l2*:1,v4l2src:1,v4l2bufferpool:1,"+
						// keep the ICE pieces readable
						"webrtcbin:4,webrtcnice:4,nice:4,libnice:4,"+
						// keep crypto slightly informative
						"dtls:3,srtp:3,"+
						// avoid periodic stats spam
						"webrtcstats:1",
				)
			}

			hostGMsg := strings.TrimSpace(os.Getenv("G_MESSAGES_DEBUG"))
			if hostGMsg == "" {
				cmd.Env = append(cmd.Env, "G_MESSAGES_DEBUG=libnice,libnice-stun,libnice-turn")
			}

			stdout, _ := cmd.StdoutPipe()
			stderr, _ := cmd.StderrPipe()

			iceFailed := make(chan struct{}, 1)

			// Busy detectors for this attempt
			var alsaBusySeen atomic.Bool
			var v4l2BusySeen atomic.Bool

			scan := func(r io.Reader, w io.Writer) {
				s := bufio.NewScanner(r)
				s.Buffer(make([]byte, 0, 64*1024), 1024*1024)
				for s.Scan() {
					line := s.Text()
					fmt.Fprintln(w, line)

					lower := strings.ToLower(line)

					// ALSA busy / audio open errors (specific)
					if isALSABusyLine(lower) {
						if !alsaBusySeen.Swap(true) {
							m.logf("[audio] ALSA appears busy; will fall back to video-only: %s", line)
							disableAudio.Store(true)
						}
					}

					// V4L2 busy / camera contention (separate)
					if isV4L2BusyLine(lower) {
						_ = v4l2BusySeen.Swap(true)
					}

					// ICE state parsing
					if strings.Contains(lower, "ice connection state change") && strings.Contains(lower, "->") {
						parts := strings.Split(lower, "->")
						if len(parts) >= 2 {
							st := strings.TrimSpace(parts[len(parts)-1])
							if j := strings.Index(st, "("); j >= 0 {
								st = strings.TrimSpace(st[:j])
							}
							m.markICE(st)
							if st == "failed" {
								select {
								case iceFailed <- struct{}{}:
								default:
								}
							}
						}
					}

					if strings.Contains(lower, "hangup: ice failed") {
						m.markICE("failed")
						select {
						case iceFailed <- struct{}{}:
						default:
						}
					}

					if strings.Contains(lower, "gathering error") && strings.Contains(lower, "hangup:") {
						m.markICE("failed")
						select {
						case iceFailed <- struct{}{}:
						default:
						}
					}

					if strings.Contains(lower, "ice connection state change") && strings.Contains(lower, " to failed") {
						m.markICE("failed")
						select {
						case iceFailed <- struct{}{}:
						default:
						}
					}

					if strings.Contains(lower, "peer connection state change") && strings.Contains(lower, " to failed") {
						m.markICE("failed")
						select {
						case iceFailed <- struct{}{}:
						default:
						}
					}

					if strings.Contains(lower, "ice connection state change") && strings.Contains(lower, " to ") {
						parts := strings.Split(lower, " to ")
						st := strings.TrimSpace(parts[len(parts)-1])
						if j := strings.Index(st, "("); j >= 0 {
							st = strings.TrimSpace(st[:j])
						}
						if st != "" {
							m.markICE(st)
							if st == "failed" {
								select {
								case iceFailed <- struct{}{}:
								default:
								}
							}
						}
					}

					if strings.Contains(lower, "webrtcnicetransport") && strings.Contains(lower, " connected") {
						m.markICE("connected")
					}

					// DTLS connected is a strong indicator media is flowing
					if strings.Contains(lower, "dtls") && strings.Contains(lower, "connected") {
						m.markHealthy("dtls-connected")
					}

					if strings.Contains(lower, "peer connection state change") && strings.Contains(lower, " to connected") {
						m.markHealthy("pc-connected")
					}

					if strings.Contains(lower, "selected pair") || strings.Contains(lower, "nominated") || strings.Contains(lower, "using pair") {
						m.markHealthy("pair-selected")
					}

					// Candidate selection / TURN relay detection (debug only)
					isCandidateLine := strings.Contains(lower, "candidate") ||
						strings.Contains(lower, "cand") ||
						strings.Contains(lower, "nominated") ||
						strings.Contains(lower, "selected") ||
						strings.Contains(lower, "candidate pair") ||
						strings.Contains(lower, "selected pair") ||
						strings.Contains(lower, "using pair")

					if isCandidateLine {
						relay := strings.Contains(lower, "typ relay") || strings.Contains(lower, " relay ")
						if relay ||
							strings.Contains(lower, "selected") ||
							strings.Contains(lower, "nominated") ||
							strings.Contains(lower, "using pair") ||
							strings.Contains(lower, "candidate pair") {
							m.logf("[ICE][selected] relay=%v | %s", relay, line)
						}
					}
				}
			}

			done := make(chan struct{})

			func() {
				m.mu.Lock()
				defer m.mu.Unlock()
				m.cmd = cmd
				m.procDone = done
				// Reset health markers for this new process.
				m.healthy = false
				m.lastICEState = ""
				m.lastHealthy = time.Time{}
			}()

			// Structured attempt log
			if strings.EqualFold(a.codec, "h264") {
				m.logf("[stream] starting gst-launch (attempt %d/%d, codec=%s, enc=%s, mjpg=%v, %dx%d@%d): gst-launch-1.0 %s",
					i+1, len(attempts), a.codec, a.enc, a.useMJPG, a.w, a.h, a.fps, pipeline)
			} else {
				m.logf("[stream] starting gst-launch (attempt %d/%d, codec=%s, mjpg=%v, %dx%d@%d): gst-launch-1.0 %s",
					i+1, len(attempts), a.codec, a.useMJPG, a.w, a.h, a.fps, pipeline)
			}

			startedAt := time.Now()

			if err := cmd.Start(); err != nil {
				// Failed to start at all: try next attempt
				func() {
					m.mu.Lock()
					defer m.mu.Unlock()
					if m.cmd == cmd {
						m.cmd = nil
					}
					if m.procDone == done {
						m.procDone = nil
					}
				}()
				close(done)

				lastErr = err
				m.logf("[stream] gst failed to start: %v", err)

				if isALSABusyLine(strings.ToLower(err.Error())) {
					m.logf("[audio] start failure looks like ALSA busy; sleeping %v before retry", alsaBusyDelayOnStartFail)
					time.Sleep(alsaBusyDelayOnStartFail)
				} else {
					time.Sleep(startFailDelay)
				}

				if i < len(attempts)-1 {
					continue
				}
			} else {
				// Record the attempt that is now actually running so StreamInfo
				// can report what is live rather than what was merely requested.
				func() {
					pf := ""
					if m.ipSource == "" {
						pf, _, _, _, _ = selectCameraMode(m.videoDev, m.conf, a.w, a.h, a.fps)
					} else {
						pf = strings.ToUpper(m.ipSource) // "RTSP" or "HTTP_MJPEG"
					}
					m.mu.Lock()
					m.lastAttempt = a
					m.lastPixFmt = pf
					m.lastPipeline = pipeline
					m.mu.Unlock()
				}()

				// Started: begin scanning
				go scan(stdout, os.Stdout)
				go scan(stderr, os.Stderr)

				// ICE-fail detection with a grace period.
				go func() {
					select {
					case <-iceFailed:
						time.Sleep(iceFailGrace)

						m.mu.Lock()
						st := m.lastICEState
						m.mu.Unlock()

						if st == "failed" {
							m.logf("[stream] ICE failed persists after %v; terminating gst for roomId=%d to restart",
								iceFailGrace, roomID)
							m.gracefulTerminate(cmd, "ice_failed", terminateGraceTimeout)
						}
					case <-done:
						return
					case <-ctx.Done():
						return
					case <-stopCh:
						return
					}
				}()

				// If ICE never reaches connected/completed within this window, restart.
				go func() {
					ticker := time.NewTicker(500 * time.Millisecond)
					defer ticker.Stop()

					for {
						select {
						case <-done:
							return
						case <-ctx.Done():
							return
						case <-stopCh:
							return
						case <-ticker.C:
							m.mu.Lock()
							st := m.lastICEState
							healthy := m.healthy
							m.mu.Unlock()

							if healthy || st == "connected" || st == "completed" {
								return
							}
							if time.Since(startedAt) > iceConnectDeadline {
								m.logf("[stream] ICE did not connect within %v (healthy=%v last=%q); terminating gst for roomId=%d",
									iceConnectDeadline, healthy, st, roomID)
								m.gracefulTerminate(cmd, "ice_deadline", terminateGraceTimeout)
								return
							}
						}
					}
				}()

				err := cmd.Wait()
				close(done)

				// Clear tracked cmd
				func() {
					m.mu.Lock()
					defer m.mu.Unlock()
					if m.cmd == cmd {
						m.cmd = nil
					}
					if m.procDone == done {
						m.procDone = nil
					}
				}()

				elapsed := time.Since(startedAt)

				if err != nil {
					lastErr = err
					m.logf("[stream] gst exited for roomId=%d after %v: %v", roomID, elapsed, err)
				} else {
					lastErr = nil
					m.logf("[stream] gst exited cleanly for roomId=%d after %v", roomID, elapsed)
				}

				// Always apply minimum delays before retrying to prevent restart storms.
				delayBeforeRetry(elapsed)

				if alsaBusySeen.Load() {
					m.logf("[audio] ALSA busy was observed; sleeping extra %v before retry", alsaBusyDelayAfterExit)
					time.Sleep(alsaBusyDelayAfterExit)
				}
				if v4l2BusySeen.Load() {
					m.logf("[video] V4L2 busy was observed; sleeping extra %v before retry", v4l2BusyDelayAfterExit)
					time.Sleep(v4l2BusyDelayAfterExit)
					v4l2BusyStreak++
				} else {
					v4l2BusyStreak = 0
				}

				// If V4L2 has been busy on every attempt so far (≥4 consecutive),
				// the device is genuinely still held by a dying pipeline — stop
				// grinding through all 24 permutations and let the outer backoff
				// handle the wait. Without this, switching cameras could waste
				// ~3 minutes cycling through every codec/resolution combination.
				if v4l2BusyStreak >= 4 {
					m.logf("[video] V4L2 busy on %d consecutive attempts — aborting inner loop, will retry after outer backoff", v4l2BusyStreak)
					lastErr = fmt.Errorf("V4L2 device busy after %d consecutive attempts", v4l2BusyStreak)
					break
				}

				if elapsed < quickFailWindow && i < len(attempts)-1 {
					m.logf("[stream] gst exited quickly (%v); retrying next attempt after delay", elapsed)
					time.Sleep(2 * time.Second)
					continue
				}

				startedOK = true
				break
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-stopCh:
			return
		default:
		}

		if lastErr != nil && !startedOK {
			m.logf("[stream] all attempts failed; will retry after %v (last error: %v)", backoff, lastErr)
		} else {
			m.logf("[stream] will retry gst for roomId=%d after %v", roomID, backoff)
		}

		time.Sleep(backoff)
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

// buildPipelineForAttempt builds a pipeline using a specific ordered attempt.
func (m *Manager) buildPipelineForAttempt(roomID int64, a videoAttempt, includeAudio bool) string {
	c := *m.conf
	c.VideoCodec = strings.ToLower(strings.TrimSpace(a.codec))
	c.VideoWidth = a.w
	c.VideoHeight = a.h
	c.VideoFps = a.fps
	if strings.EqualFold(c.VideoCodec, "h264") {
		c.H264Encoder = strings.ToLower(strings.TrimSpace(a.enc))
	}
	v := a.useMJPG
	c.PreferMJPG = &v

	return m.buildPipelineFromConfig(roomID, &c, includeAudio)
}

func (m *Manager) buildPipelineFromConfig(roomID int64, conf *cfg.AgentConfig, includeAudio bool) string {
	// ── IP camera source (RTSP / HTTP-MJPEG) ─────────────────────────────────
	if m.ipSource == cfg.CameraSourceRTSP || m.ipSource == cfg.CameraSourceHTTPMJPEG {
		return m.buildIPSourcePipeline(roomID, conf, includeAudio)
	}

	// ── USB / V4L2 (original path) ────────────────────────────────────────────
	videoDev := defaultIfEmpty(m.videoDev, "/dev/video0")

	width, height, fpsNum, fpsDen := pickVideoParamsFromConfig(conf)
	preferMJPG := isPreferMJPG(conf)

	// Snap requested config to a mode that the camera actually supports.
	pixFmt, selW, selH, selFps, selNote := selectCameraMode(videoDev, conf, width, height, fpsNum)
	if selW > 0 && selH > 0 && selFps > 0 {
		width, height, fpsNum, fpsDen = selW, selH, selFps, 1
	}
	if strings.TrimSpace(conf.VideoPixFmt) == "" {
		// Back-compat: if not explicitly set, derive from PreferMJPG
		if preferMJPG {
			pixFmt = "MJPG"
		}
	}
	m.logf("[video] selected camera mode: pixfmt=%s %dx%d@%d (%s)", pixFmt, width, height, fpsNum, selNote)

	if strings.EqualFold(strings.TrimSpace(conf.VideoCodec), "h264") {
		m.logf("[stream] video params: codec=%q h264Profile=%q h264Encoder=%q h264Bitrate=%d %dx%d@%d/%d preferMJPG=%v",
			conf.VideoCodec, conf.H264Profile, conf.H264Encoder, conf.H264BitrateBps, width, height, fpsNum, fpsDen, preferMJPG)
	} else {
		m.logf("[stream] video params: codec=%q (vp8 path) %dx%d@%d/%d preferMJPG=%v",
			conf.VideoCodec, width, height, fpsNum, fpsDen, preferMJPG)
	}

	janusURL := conf.JanusURL
	displayName := sanitizeDisplay(conf.MavID)
	if displayName == "" {
		displayName = "vehicle"
	}

	ju := gstQuote(janusURL)
	dn := gstQuote(displayName)
	vd := gstQuote(videoDev)

	m.mu.Lock()
	ice := m.ice
	m.mu.Unlock()
	iceProps := buildJanusSinkICEProps(ice)
	m.logf("[ICE] configured: stun=%q turn=%q forceRelay=%v", ice.StunURL, ice.TurnURL, ice.ForceRelay)

	videoCapsProp := `video-caps="video/x-vp8"`
	if strings.EqualFold(conf.VideoCodec, "h264") {
		videoCapsProp = `video-caps="video/x-h264"`
	}

	videoChain := buildVideoChain(conf, vd, pixFmt, width, height, fpsNum, fpsDen)

	ccProp := "congestion-control=gcc"
	if strings.EqualFold(conf.VideoCodec, "h264") && strings.EqualFold(conf.H264Encoder, "v4l2h264enc") {
		ccProp = "congestion-control=disabled"
	}

	brProps := fmt.Sprintf("start-bitrate=%d min-bitrate=%d max-bitrate=%d",
		conf.WebRTCStartBitrateBps, conf.WebRTCMinBitrateBps, conf.WebRTCMaxBitrateBps)

	if strings.EqualFold(conf.VideoCodec, "h264") && strings.EqualFold(conf.H264Encoder, "v4l2h264enc") {
		brProps = ""
	}

	base := fmt.Sprintf(
		"janusvrwebrtcsink name=sink %s %s %s "+
			"signaller::janus-endpoint=%s signaller::room-id=%d signaller::display-name=%s "+
			"%s %s",
		videoCapsProp,
		ccProp,
		brProps,
		ju, roomID, dn,
		iceProps,
		videoChain,
	)

	if !includeAudio {
		return base
	}

	audioDev := strings.TrimSpace(m.audioDev)
	if audioDev == "" {
		return base
	}
	ad := gstQuote(audioDev)
	return base + fmt.Sprintf(
		" alsasrc device=%s ! audioconvert ! audioresample ! "+
			"audio/x-raw,channels=1,rate=48000 ! "+
			"queue max-size-buffers=8 leaky=downstream ! sink.",
		ad,
	)
}

func buildJanusSinkICEProps(ice ICEConfig) string {
	var parts []string

	if strings.TrimSpace(ice.StunURL) != "" {
		parts = append(parts, fmt.Sprintf("stun-server=%s", gstQuote(ice.StunURL)))
	}

	turnURLs := normalizeTurnList(ice)
	var entries []string
	for _, tu := range turnURLs {
		e := buildTurnServersEntry(tu, ice.Username, ice.Password)
		if strings.TrimSpace(e) != "" {
			entries = append(entries, e)
		}
	}
	if len(entries) > 0 {
		parts = append(parts, fmt.Sprintf("turn-servers=%s", gstStrv(entries...)))
	}

	if ice.ForceRelay {
		parts = append(parts, "ice-transport-policy=relay")
	} else {
		parts = append(parts, "ice-transport-policy=all")
	}

	return strings.Join(parts, " ")
}

func normalizeTurnList(ice ICEConfig) []string {
	out := make([]string, 0, len(ice.TurnURLs)+1)
	seen := map[string]struct{}{}

	for _, u := range ice.TurnURLs {
		u = strings.TrimSpace(u)
		if u == "" {
			continue
		}
		if _, ok := seen[u]; ok {
			continue
		}
		seen[u] = struct{}{}
		out = append(out, u)
	}

	if strings.TrimSpace(ice.TurnURL) != "" {
		u := strings.TrimSpace(ice.TurnURL)
		if _, ok := seen[u]; !ok {
			out = append(out, u)
		}
	}
	return out
}

func chooseTurnURLForForceRelay(urls []string) string {
	for _, u := range urls {
		lu := strings.ToLower(u)
		if strings.HasPrefix(lu, "turn://") && strings.Contains(lu, "transport=udp") && strings.Contains(lu, ":3478") {
			return u
		}
	}
	for _, u := range urls {
		lu := strings.ToLower(u)
		if strings.HasPrefix(lu, "turn://") && strings.Contains(lu, "transport=udp") && strings.Contains(lu, ":443") {
			return u
		}
	}
	for _, u := range urls {
		lu := strings.ToLower(u)
		if strings.HasPrefix(lu, "turns://") && strings.Contains(lu, "transport=tcp") && strings.Contains(lu, ":443") {
			return u
		}
	}
	if len(urls) > 0 {
		return urls[0]
	}
	return ""
}

func buildTurnServersEntry(turnURL, user, pass string) string {
	turnURL = strings.TrimSpace(turnURL)
	if turnURL == "" {
		return ""
	}

	u, err := url.Parse(turnURL)
	if err != nil {
		return turnURL
	}

	if u.Scheme == "" || u.Host == "" {
		return turnURL
	}

	if user != "" {
		u.User = url.UserPassword(user, pass)
	}
	return u.String()
}

// ALSA-busy detection (ALSA-specific; do NOT match generic "device busy")
func isALSABusyLine(lower string) bool {
	l := strings.ToLower(lower)

	// Strong ALSA-specific signals:
	if strings.Contains(l, "gstalsasrc") || strings.Contains(l, "alsasrc") || strings.Contains(l, "alsa") {
		if strings.Contains(l, "could not open audio device") {
			return true
		}
		if strings.Contains(l, "is busy") || strings.Contains(l, "device or resource busy") || strings.Contains(l, "ebusy") {
			return true
		}
	}

	// Extra common phrasing:
	if strings.Contains(l, "could not open audio device for recording") {
		return true
	}
	if strings.Contains(l, "audio open error") {
		return true
	}

	return false
}

// V4L2/camera busy detection (separate from ALSA)
func isV4L2BusyLine(lower string) bool {
	l := strings.ToLower(lower)

	isV4l2Context :=
		strings.Contains(l, "gstv4l2") ||
			strings.Contains(l, "v4l2src") ||
			strings.Contains(l, "/dev/video")

	if !isV4l2Context {
		return false
	}

	if strings.Contains(l, "device '/dev/video") && strings.Contains(l, "is busy") {
		return true
	}
	if strings.Contains(l, "device or resource busy") {
		return true
	}
	if strings.Contains(l, "s_fmt failed") {
		return true
	}

	return false
}

// keyIntForFps returns a keyframe distance in frames.
// Target ~0.5s GOP to reduce worst-case join latency for late subscribers.
func keyIntForFps(fpsNum, fpsDen int) int {
	// If fps is unknown, assume 30fps -> 15 frames (~0.5s)
	if fpsNum <= 0 {
		return 15
	}
	if fpsDen <= 0 {
		fpsDen = 1
	}

	// frames ≈ (fps/2) => fpsNum / (2*fpsDen)
	ki := fpsNum / (2 * fpsDen)

	// keep sane bounds
	if ki < 5 {
		ki = 5
	}
	if ki > 120 {
		ki = 120
	}
	return ki
}

// buildVideoSrc builds the camera capture portion.

// probeRTSPCodec uses gst-discoverer-1.0 to detect the video codec of an RTSP
// stream without fully decoding it. Returns the encoding name in uppercase,
// e.g. "H264", "H265", "VP8", "JPEG", or "" if detection fails or times out.
// Runs with a 6-second timeout — fast enough for a local LAN camera.
func probeRTSPCodec(url string, transport string) string {
	path, err := exec.LookPath("gst-discoverer-1.0")
	if err != nil {
		return ""
	}

	// Pass transport hint via GST_RTSP_PROTOCOLS env if needed
	cmd := exec.Command(path, "-v", "--timeout=5", url)
	cmd.Env = append(os.Environ(), "GST_DEBUG_NO_COLOR=1", "GST_DEBUG=0")
	if transport != "" && !strings.EqualFold(transport, "auto") {
		// mediamtx/RTSP transport hint: set rtspsrc protocols via env isn't
		// possible; we rely on gst-discoverer auto-negotiating. TCP is tried
		// first by gst-discoverer on most GStreamer builds.
		_ = transport // best-effort; discoverer picks its own transport
	}

	out, err := cmd.Output()
	if err != nil {
		// Timeout or error — can't determine codec
		return ""
	}

	// Parse output for "video:" or "codec:" lines
	for _, line := range strings.Split(string(out), "\n") {
		l := strings.ToLower(strings.TrimSpace(line))
		// gst-discoverer outputs lines like:
		//   video: H.264 (Constrained Baseline Profile)
		//   codec: H.264 (Constrained Baseline Profile)
		//   video codec: H.264
		if strings.Contains(l, "video") || strings.Contains(l, "codec") {
			switch {
			case strings.Contains(l, "h.264") || strings.Contains(l, "h264") || strings.Contains(l, "avc"):
				return "H264"
			case strings.Contains(l, "h.265") || strings.Contains(l, "h265") || strings.Contains(l, "hevc"):
				return "H265"
			case strings.Contains(l, "vp8"):
				return "VP8"
			case strings.Contains(l, "vp9"):
				return "VP9"
			case strings.Contains(l, "jpeg") || strings.Contains(l, "mjpeg"):
				return "JPEG"
			}
		}
	}
	return ""
}

// buildIPSourcePipeline builds a gst-launch pipeline string for RTSP or
// HTTP-MJPEG IP camera sources (ESP32-CAM, Pi cameras via mediamtx, etc.).
//
// For RTSP sources, the codec is probed first using gst-discoverer:
//   - H.264 stream + H.264 WebRTC target → true passthrough using caps-selected
//     pad from rtspsrc (application/x-rtp,encoding-name=H264), no decode/re-encode.
//   - Anything else → decodebin + re-encode to target codec.
//
// For HTTP-MJPEG:
//
//	souphttpsrc → multipartdemux → jpegdec → videoconvert/scale/rate → encode → janusvrwebrtcsink
func (m *Manager) buildIPSourcePipeline(roomID int64, conf *cfg.AgentConfig, includeAudio bool) string {
	width, height, fpsNum, _ := pickVideoParamsFromConfig(conf)

	janusURL := conf.JanusURL
	displayName := sanitizeDisplay(conf.MavID)
	if displayName == "" {
		displayName = "vehicle"
	}

	ju := gstQuote(janusURL)
	dn := gstQuote(displayName)

	m.mu.Lock()
	ice := m.ice
	m.mu.Unlock()
	iceProps := buildJanusSinkICEProps(ice)

	videoCapsProp := `video-caps="video/x-vp8"`
	if strings.EqualFold(conf.VideoCodec, "h264") {
		videoCapsProp = `video-caps="video/x-h264"`
	}

	ccProp := "congestion-control=gcc"
	if strings.EqualFold(conf.VideoCodec, "h264") && strings.EqualFold(conf.H264Encoder, "v4l2h264enc") {
		ccProp = "congestion-control=disabled"
	}

	brProps := fmt.Sprintf("start-bitrate=%d min-bitrate=%d max-bitrate=%d",
		conf.WebRTCStartBitrateBps, conf.WebRTCMinBitrateBps, conf.WebRTCMaxBitrateBps)
	if strings.EqualFold(conf.VideoCodec, "h264") && strings.EqualFold(conf.H264Encoder, "v4l2h264enc") {
		brProps = ""
	}

	su := gstQuote(m.ipURL)

	var videoSrc string
	var encChain string

	switch m.ipSource {

	// ── HTTP MJPEG ────────────────────────────────────────────────────────────
	case cfg.CameraSourceHTTPMJPEG:
		videoSrc = fmt.Sprintf(
			"souphttpsrc location=%s is-live=true ! "+
				"multipartdemux ! image/jpeg ! "+
				"jpegdec ! videoconvert ! videoscale ! videorate ! "+
				"video/x-raw,width=%d,height=%d,framerate=%d/1",
			su, width, height, fpsNum,
		)
		encChain = m.buildEncChain(conf)
		m.logf("[stream][ip] HTTP-MJPEG source url=%s %dx%d@%d → encode:%s",
			m.ipURL, width, height, fpsNum, encChain)

	// ── RTSP ─────────────────────────────────────────────────────────────────
	default: // cfg.CameraSourceRTSP
		transport := strings.TrimSpace(m.ipTransport)
		if transport == "" || strings.EqualFold(transport, "auto") {
			transport = "tcp+udp+udp-mcast+http+tls"
		}
		bufMs := m.ipBufferMs
		if bufMs <= 0 {
			bufMs = 200
		}
		latencyProp := fmt.Sprintf("latency=%d", bufMs)
		transportQ := gstQuote(transport)

		// Probe the stream codec so we can decide passthrough vs decode+re-encode.
		streamCodec := probeRTSPCodec(m.ipURL, transport)
		wantH264 := strings.EqualFold(conf.VideoCodec, "h264")

		if streamCodec == "H264" && wantH264 {
			// Stream is H.264 and target is H.264.
			// Ideal would be passthrough but rtspsrc dynamic pads cannot be
			// statically linked in gst-launch regardless of caps filtering.
			// Use decodebin + re-encode. avdec_h264 in the base image handles
			// the decode; x264enc/openh264enc re-encodes. Quality loss is minimal
			// at the configured bitrate. True passthrough requires programmatic
			// GStreamer (go-gst) which is a future improvement.
			videoSrc = fmt.Sprintf(
				"rtspsrc location=%s protocols=%s %s ! "+
					"decodebin ! videoconvert ! videoscale ! videorate ! "+
					"video/x-raw,width=%d,height=%d,framerate=%d/1",
				su, transportQ, latencyProp,
				width, height, fpsNum,
			)
			encChain = m.buildEncChain(conf)
			m.logf("[stream][ip] RTSP H.264 (decode+re-encode) url=%s transport=%s buf=%dms %dx%d@%d",
				m.ipURL, transport, bufMs, width, height, fpsNum)
		} else {
			// ── Decode + re-encode ────────────────────────────────────────────
			// Used when:
			//   - Stream is not H.264 (H.265, VP8, MJPEG, unknown)
			//   - WebRTC target codec is VP8 (even if stream is H.264)
			//   - gst-discoverer timed out / unavailable (streamCodec == "")
			if streamCodec == "" {
				m.logf("[stream][ip] RTSP codec probe timed out or unavailable; using decodebin url=%s",
					m.ipURL)
			} else {
				m.logf("[stream][ip] RTSP stream codec=%s wantH264=%v → decode+re-encode url=%s",
					streamCodec, wantH264, m.ipURL)
			}
			videoSrc = fmt.Sprintf(
				"rtspsrc location=%s protocols=%s %s ! "+
					"decodebin ! videoconvert ! videoscale ! videorate ! "+
					"video/x-raw,width=%d,height=%d,framerate=%d/1",
				su, transportQ, latencyProp,
				width, height, fpsNum,
			)
			encChain = m.buildEncChain(conf)
		}
	}

	// Join videoSrc and encChain with ! only when there is an encode step.
	// The chain must terminate with "! sink." — an explicit named-pad reference
	// into janusvrwebrtcsink name=sink. Without this, gst-launch tries to
	// auto-link the final element to the sink and fails because
	// janusvrwebrtcsink only exposes request pads, not an "always" sink pad.
	var videoPart string
	if encChain == "" {
		videoPart = videoSrc + " ! sink."
	} else {
		videoPart = videoSrc + " ! " + encChain + " ! sink."
	}

	pipeline := fmt.Sprintf(
		"janusvrwebrtcsink name=sink %s %s %s "+
			"signaller::janus-endpoint=%s signaller::room-id=%d signaller::display-name=%s "+
			"%s %s",
		videoCapsProp, ccProp, brProps,
		ju, roomID, dn,
		iceProps,
		videoPart,
	)
	return strings.TrimSpace(pipeline)
}

// buildEncChain returns the GStreamer encode chain string for the configured
// target codec. The returned string has no trailing " ! ".
func (m *Manager) buildEncChain(conf *cfg.AgentConfig) string {
	switch strings.ToLower(strings.TrimSpace(conf.VideoCodec)) {
	case "h264":
		enc := strings.ToLower(strings.TrimSpace(conf.H264Encoder))
		if enc == "auto" || enc == "" {
			if gstElementExists("openh264enc") {
				enc = "openh264enc"
			} else if gstElementExists("x264enc") {
				enc = "x264enc"
			} else {
				enc = "openh264enc"
			}
		}
		switch {
		case strings.EqualFold(enc, "v4l2h264enc"):
			return `v4l2h264enc extra-controls="controls,repeat_sequence_header=1" ! video/x-h264,level=(string)4 ! h264parse`
		case strings.EqualFold(enc, "openh264enc"):
			return fmt.Sprintf("openh264enc bitrate=%d ! h264parse", conf.H264BitrateBps)
		default: // x264enc
			return fmt.Sprintf("x264enc tune=zerolatency bitrate=%d ! h264parse", conf.H264BitrateBps/1000)
		}
	default: // vp8
		return fmt.Sprintf("vp8enc deadline=1 target-bitrate=%d", conf.H264BitrateBps)
	}
}

func buildVideoSrc(videoDev string, pixFmt string, width, height, fpsNum, fpsDen int, preferMJPG bool) string {
	pix := strings.ToUpper(strings.TrimSpace(pixFmt))
	if pix == "" || pix == "AUTO" {
		if preferMJPG {
			pix = "MJPG"
		} else {
			pix = "YUYV"
		}
	}

	switch pix {
	case "MJPG", "MJPEG":
		// Only attempt MJPG decode if jpegdec exists, otherwise fall back to raw path.
		if gstElementExists("jpegdec") {
			return fmt.Sprintf(
				"v4l2src device=%s ! "+
					"image/jpeg,width=%d,height=%d,framerate=%d/%d ! "+
					"jpegdec ! videoconvert ! videoscale ! videorate ! "+
					"video/x-raw,width=%d,height=%d,framerate=%d/%d ! ",
				videoDev, width, height, fpsNum, fpsDen,
				width, height, fpsNum, fpsDen,
			)
		}
	case "H264":
		// Direct H.264 output from camera; handled in buildVideoChain (passthrough).
		return fmt.Sprintf(
			"v4l2src device=%s ! video/x-h264,width=%d,height=%d,framerate=%d/%d ! ",
			videoDev, width, height, fpsNum, fpsDen,
		)
	}

	// Default raw path (YUYV or anything else): let v4l2src negotiate raw, then convert/scale/rate.
	return fmt.Sprintf(
		"v4l2src device=%s ! videoconvert ! videoscale ! videorate ! "+
			"video/x-raw,width=%d,height=%d,framerate=%d/%d ! ",
		videoDev, width, height, fpsNum, fpsDen,
	)
}

func buildVideoChain(conf *cfg.AgentConfig, videoDev string, pixFmt string, width, height, fpsNum, fpsDen int) string {
	codec := strings.ToLower(strings.TrimSpace(conf.VideoCodec))

	pix := strings.ToUpper(strings.TrimSpace(pixFmt))
	// If camera can output H.264 directly and we're in the H.264 path, use passthrough to minimize startup latency.
	if pix == "H264" && codec != "" && codec != "vp8" {
		src := buildVideoSrc(videoDev, pixFmt, width, height, fpsNum, fpsDen, isPreferMJPG(conf))
		parseChain := ""
		if gstElementExists("h264parse") {
			parseChain = "h264parse config-interval=1 ! "
		}
		return fmt.Sprintf(
			"%s %svideo/x-h264,stream-format=byte-stream,alignment=au ! "+
				"rtph264pay pt=96 config-interval=1 ! sink.",
			src, parseChain,
		)
	}

	if codec == "" || codec == "vp8" {
		// Ensure vp8enc always receives a supported raw format (I420 is the safe default).
		src := buildVideoSrc(videoDev, pixFmt, width, height, fpsNum, fpsDen, isPreferMJPG(conf))
		keyInt := keyIntForFps(fpsNum, fpsDen)
		return fmt.Sprintf(
			"%s"+
				"videoconvert ! video/x-raw,format=I420 ! "+
				"vp8enc deadline=1 keyframe-max-dist=%d lag-in-frames=0 cpu-used=6 threads=4 error-resilient=1 ! "+
				"queue max-size-buffers=2 leaky=downstream ! sink.",
			src, keyInt,
		)
	}

	if codec == "h264" {
		enc := strings.ToLower(strings.TrimSpace(conf.H264Encoder))
		if enc == "" {
			enc = "auto"
		}

		// Resolve "auto" to a real encoder that actually exists on this machine/container.
		if enc == "auto" {
			if gstElementExists("v4l2h264enc") {
				enc = "v4l2h264enc"
			} else if gstElementExists("x264enc") {
				enc = "x264enc"
			} else {
				// No known encoder found; keep as v4l2h264enc so the pipeline fails loudly/early.
				enc = "v4l2h264enc"
			}
		}

		useHW := (enc == "v4l2h264enc")
		if useHW {
			if fpsNum > 30 {
				fpsNum = 30
			}
			if width > 1920 || height > 1080 {
				width, height = 1920, 1080
			}
			// NOTE: keyInt is based on the *requested* fpsNum/fpsDen from pickVideoParamsFromConfig.
			// If you ever allow HW to force a different fps, you may want to recompute keyInt here.
		}

		keyInt := keyIntForFps(fpsNum, fpsDen)

		profile := strings.ToLower(strings.TrimSpace(conf.H264Profile))
		if profile == "" {
			profile = "baseline"
		}

		sdpProfile := "baseline"
		switch profile {
		case "baseline":
			sdpProfile = "baseline"
		case "constrained-baseline":
			sdpProfile = "constrained-baseline"
		case "main":
			sdpProfile = "main"
		case "high":
			sdpProfile = "high"
		default:
			sdpProfile = "baseline"
		}

		h264LevelCaps := "3.1"
		if width*height > 1280*720 {
			h264LevelCaps = "4"
		}

		var encElem string
		switch enc {
		case "v4l2h264enc":
			// Match the known-good gst-launch pipeline:
			// - force mmap io-mode
			// - set bitrate + repeat headers + GOP
			encElem = fmt.Sprintf(
				`v4l2h264enc output-io-mode=mmap capture-io-mode=mmap `+
					`extra-controls="controls,video_bitrate=%d,repeat_sequence_header=1,video_gop_size=%d,h264_i_frame_period=%d"`,
				conf.H264BitrateBps,
				keyInt,
				keyInt,
			)
		case "x264enc":
			// key-int-max governs GOP length (keyframe interval). Smaller => faster first frame for late joiners.
			// bframes=0 avoids reordering latency.
			encElem = fmt.Sprintf(
				"x264enc tune=zerolatency bitrate=%d speed-preset=ultrafast key-int-max=%d bframes=0",
				conf.H264BitrateBps/1000, keyInt,
			)
		default:
			encElem = fmt.Sprintf(
				`v4l2h264enc extra-controls="controls,video_bitrate=%d,repeat_sequence_header=1"`,
				conf.H264BitrateBps,
			)
		}

		src := buildVideoSrc(videoDev, pixFmt, width, height, fpsNum, fpsDen, isPreferMJPG(conf))

		formatCaps := "NV12"

		// x264enc commonly only accepts I420; forcing NV12 can yield NOT_NEGOTIATED on fallback attempts.
		if enc == "x264enc" {
			formatCaps = "I420"
		}

		// v4l2h264enc on Pi is often happiest with I420 coming in (matches your working gst-launch).
		if enc == "v4l2h264enc" {
			formatCaps = "I420"
		}

		parseChain := ""
		if gstElementExists("h264parse") {
			parseChain = "h264parse config-interval=1 ! "
		}

		return fmt.Sprintf(
			"%s"+
				"queue max-size-buffers=2 leaky=downstream ! "+
				"videoconvert ! video/x-raw,format=%s ! "+
				"%s ! "+
				// Put the “known-good” H264 caps immediately after the encoder (like your gst-launch)
				"video/x-h264,profile=(string)%s,level=(string)%s,stream-format=(string)byte-stream,alignment=(string)au ! "+
				"%s"+
				"queue max-size-buffers=2 leaky=downstream ! sink.",
			src, formatCaps, encElem, sdpProfile, h264LevelCaps, parseChain,
		)
	}

	src := buildVideoSrc(videoDev, pixFmt, width, height, fpsNum, fpsDen, isPreferMJPG(conf))
	return fmt.Sprintf("%squeue max-size-buffers=2 leaky=downstream ! sink.", src)
}

func gstQuote(s string) string {
	return fmt.Sprintf("%q", strings.TrimSpace(s))
}

func gstStrv(values ...string) string {
	q := make([]string, 0, len(values))
	for _, v := range values {
		v = strings.TrimSpace(v)
		if v == "" {
			continue
		}
		q = append(q, gstQuote(v))
	}
	return "<" + strings.Join(q, ",") + ">"
}

func defaultIfEmpty(v, fallback string) string {
	if strings.TrimSpace(v) == "" {
		return fallback
	}
	return v
}

func sanitizeDisplay(v string) string {
	v = strings.TrimSpace(v)
	if v == "" {
		return "vehicle"
	}
	b := make([]rune, 0, len(v))
	for _, r := range v {
		switch r {
		case ' ', '\t', '\n', '\r':
			b = append(b, '_')
		default:
			b = append(b, r)
		}
	}
	if len(b) == 0 {
		return "vehicle"
	}
	return string(b)
}

func pickVideoParamsFromConfig(conf *cfg.AgentConfig) (int, int, int, int) {
	width := conf.VideoWidth
	height := conf.VideoHeight
	fps := conf.VideoFps

	if width <= 0 {
		width = 640
	}
	if height <= 0 {
		height = 360
	}
	if fps <= 0 {
		fps = 15
	}

	// Keep den=1
	return width, height, fps, 1
}

func buildVideoAttempts(conf *cfg.AgentConfig, videoDev string) []videoAttempt {

	preferH264 := strings.EqualFold(strings.TrimSpace(conf.VideoCodec), "h264")

	// Fallback tiers only used if camera caps cannot be read.
	fallbackTiers := []struct{ w, h int }{
		{1920, 1080},
		{1280, 720},
		{960, 540},
		{854, 480},
		{640, 360},
		{320, 180},
	}

	startW, startH, reqFps, _ := pickVideoParamsFromConfig(conf)
	if reqFps <= 0 {
		reqFps = 30
	}
	if reqFps > 30 {
		reqFps = 30
	}

	codec := strings.ToLower(strings.TrimSpace(conf.VideoCodec))
	var codecOrder []string
	switch codec {
	case "", "auto":
		codecOrder = []string{"h264", "vp8"}
	case "h264":
		codecOrder = []string{"h264", "vp8"}
	case "vp8":
		codecOrder = []string{"vp8"}
	default:
		codecOrder = []string{"h264", "vp8"}
	}

	preferMJPGEnabled := isPreferMJPG(conf)
	if preferMJPGEnabled && !gstElementExists("jpegdec") {
		preferMJPGEnabled = false
	}

	// --- Build tiers from camera caps (preferred) ---
	type tier struct {
		w, h int
	}
	var tiers []tier
	caps, capsErr := device.GetVideoCaps(videoDev)
	capsOK := (capsErr == nil && caps != nil && len(caps.Modes) > 0)

	if capsOK {
		// Prefer MJPG tiers first (best FPS on your camera), then YUYV.
		// Only include modes that have at least one FPS entry.
		seen := map[[2]int]bool{}

		addModes := func(pix string) {
			for _, m := range caps.Modes {
				if strings.ToUpper(strings.TrimSpace(m.PixFmt)) != pix {
					continue
				}
				if m.Width <= 0 || m.Height <= 0 || len(m.FPS) == 0 {
					continue
				}
				key := [2]int{m.Width, m.Height}
				if seen[key] {
					continue
				}
				seen[key] = true
				tiers = append(tiers, tier{w: m.Width, h: m.Height})
			}
		}

		// This matches your camera reality: MJPG has 30fps at all listed sizes.
		if preferH264 {
			addModes("H264")
			addModes("MJPG")
			addModes("YUYV")
		} else {
			addModes("MJPG")
			addModes("YUYV")
		}

		// Sort tiers: largest area first
		sort.Slice(tiers, func(i, j int) bool {
			ai := tiers[i].w * tiers[i].h
			aj := tiers[j].w * tiers[j].h
			if ai != aj {
				return ai > aj
			}
			// tie-break: wider first
			return tiers[i].w > tiers[j].w
		})
	}

	// If no caps-derived tiers, use fallback tiers
	if len(tiers) == 0 {
		for _, t := range fallbackTiers {
			tiers = append(tiers, tier{w: t.w, h: t.h})
		}
	}

	// Find starting tier index based on requested resolution (closest match)
	startIdx := 0
	{
		bestIdx := 0
		bestScore := int(^uint(0) >> 1)
		for i, t := range tiers {
			dw := startW - t.w
			if dw < 0 {
				dw = -dw
			}
			dh := startH - t.h
			if dh < 0 {
				dh = -dh
			}
			score := dw*dw + dh*dh
			if score < bestScore {
				bestScore = score
				bestIdx = i
			}
		}
		startIdx = bestIdx
	}

	// Helper: choose a good FPS for a given tier by looking up caps (if available)
	chooseTierFPS := func(w, h int) int {
		// If caps are not available, just use reqFps (already clamped)
		if !capsOK {
			return reqFps
		}

		// Prefer MJPG fps list if PreferMJPG is enabled, else try requested pixfmt, else any.
		prefs := []string{}
		if preferMJPGEnabled {
			prefs = append(prefs, "MJPG")
		}
		reqPix := strings.ToUpper(strings.TrimSpace(conf.VideoPixFmt))
		if reqPix != "" && reqPix != "AUTO" && reqPix != "MJPG" {
			prefs = append(prefs, reqPix)
		}
		// fallback
		prefs = append(prefs, "MJPG", "YUYV")

		for _, pix := range prefs {
			for _, m := range caps.Modes {
				if m.Width != w || m.Height != h {
					continue
				}
				if strings.ToUpper(strings.TrimSpace(m.PixFmt)) != pix {
					continue
				}
				if len(m.FPS) == 0 {
					return reqFps
				}
				return closestFPS(m.FPS, reqFps)
			}
		}

		// Last resort: any matching mode
		for _, m := range caps.Modes {
			if m.Width == w && m.Height == h && len(m.FPS) > 0 {
				return closestFPS(m.FPS, reqFps)
			}
		}
		return reqFps
	}

	var out []videoAttempt

	for _, c := range codecOrder {
		switch c {
		case "h264":
			encs := []string{}
			if gstElementExists("v4l2h264enc") {
				encs = append(encs, "v4l2h264enc")
			}
			if gstElementExists("x264enc") {
				encs = append(encs, "x264enc")
			}
			if len(encs) == 0 {
				break
			}

			for i := startIdx; i < len(tiers); i++ {
				t := tiers[i]
				fps := chooseTierFPS(t.w, t.h)

				if preferMJPGEnabled {
					for _, e := range encs {
						out = append(out, videoAttempt{codec: "h264", enc: e, useMJPG: true, w: t.w, h: t.h, fps: fps})
					}
				}
				for _, e := range encs {
					out = append(out, videoAttempt{codec: "h264", enc: e, useMJPG: false, w: t.w, h: t.h, fps: fps})
				}
			}

		case "vp8":
			if !gstElementExists("vp8enc") {
				break
			}
			for i := startIdx; i < len(tiers); i++ {
				t := tiers[i]
				fps := chooseTierFPS(t.w, t.h)

				if preferMJPGEnabled {
					out = append(out, videoAttempt{codec: "vp8", useMJPG: true, w: t.w, h: t.h, fps: fps})
				}
				out = append(out, videoAttempt{codec: "vp8", useMJPG: false, w: t.w, h: t.h, fps: fps})
			}
		}
	}

	return out
}

// StreamInfo is a point-in-time snapshot of what the running GStreamer pipeline
// is actually using. Populated from the lastAttempt that successfully started,
// so it reflects auto-fallback codec/encoder/resolution choices rather than
// the values merely requested in config.
type StreamInfo struct {
	Running      bool   `json:"running"`
	Healthy      bool   `json:"healthy"`
	Codec        string `json:"codec"`
	Encoder      string `json:"encoder"` // e.g. "x264enc", "v4l2h264enc", "vp8enc"
	Profile      string `json:"profile"` // H.264 only; empty for VP8
	Width        int    `json:"width"`
	Height       int    `json:"height"`
	Fps          int    `json:"fps"`
	PixFmt       string `json:"pixFmt"`       // capture pixel format, e.g. "MJPG", "YUYV"
	BitrateBps   int    `json:"bitrateBps"`   // encode bitrate from config
	WebrtcMaxBps int    `json:"webrtcMaxBps"` // GCC ceiling from config
	IceState     string `json:"iceState"`
	RoomId       int64  `json:"roomId"`
	Pipeline     string `json:"pipeline"` // full gst-launch args for the running process
}

// StreamInfo returns a snapshot of the currently-running pipeline.
// All fields are zero/empty when no pipeline is active.
func (m *Manager) StreamInfo() StreamInfo {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.cmd == nil {
		return StreamInfo{Running: false}
	}

	a := m.lastAttempt

	// Resolve encoder name: if config says "auto", report what was actually selected.
	enc := a.enc
	if enc == "" {
		// VP8 path: encoder element is always vp8enc
		if strings.EqualFold(a.codec, "vp8") {
			enc = "vp8enc"
		}
	}

	profile := ""
	if strings.EqualFold(a.codec, "h264") && m.conf != nil {
		profile = m.conf.H264Profile
	}

	bitrate := 0
	webrtcMax := 0
	if m.conf != nil {
		bitrate = m.conf.H264BitrateBps
		webrtcMax = m.conf.WebRTCMaxBitrateBps
	}

	return StreamInfo{
		Running:      true,
		Healthy:      m.healthy,
		Codec:        a.codec,
		Encoder:      enc,
		Profile:      profile,
		Width:        a.w,
		Height:       a.h,
		Fps:          a.fps,
		PixFmt:       m.lastPixFmt,
		BitrateBps:   bitrate,
		WebrtcMaxBps: webrtcMax,
		IceState:     m.lastICEState,
		RoomId:       m.roomID,
		Pipeline:     m.lastPipeline,
	}
}

// gracefulTerminate tries SIGINT first (lets GStreamer/ALSA clean up), then SIGKILL after a timeout.
// waits for process exit (prevents ALSA/video device busy restart loops).
func (m *Manager) gracefulTerminate(cmd *exec.Cmd, reason string, timeout time.Duration) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	pid := cmd.Process.Pid
	m.logf("[stream] terminating gst (%s): pid=%d", reason, pid)

	// Capture the done channel (if this cmd is the tracked one).
	m.mu.Lock()
	done := m.procDone
	isTracked := (m.cmd == cmd)
	m.mu.Unlock()

	_ = cmd.Process.Signal(os.Interrupt)

	// If we have a done channel for this cmd, wait for clean exit.
	if isTracked && done != nil {
		select {
		case <-done:
			return
		case <-time.After(timeout):
			m.logf("[stream] gst did not exit after %v; killing pid=%d", timeout, pid)
			_ = cmd.Process.Kill()
			time.Sleep(300 * time.Millisecond)
			return
		}
	}

	// Fallback: no done channel available; kill after timeout.
	time.AfterFunc(timeout, func() {
		_ = cmd.Process.Kill()
	})
}

func delayBeforeRetry(elapsed time.Duration) {
	switch {
	case elapsed < 2*time.Second:
		time.Sleep(2 * time.Second)
	case elapsed < 10*time.Second:
		time.Sleep(3 * time.Second)
	}
}

var gstElementExistsCache sync.Map // map[string]bool

func gstElementExists(name string) bool {
	name = strings.TrimSpace(name)
	if name == "" {
		return false
	}
	if v, ok := gstElementExistsCache.Load(name); ok {
		return v.(bool)
	}
	// gst-inspect-1.0 is the most reliable way to check plugin availability at runtime.
	path, err := exec.LookPath("gst-inspect-1.0")
	if err != nil {
		gstElementExistsCache.Store(name, false)
		return false
	}
	cmd := exec.Command(path, name)
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	err = cmd.Run()
	exists := (err == nil)
	gstElementExistsCache.Store(name, exists)
	return exists
}

func isPreferMJPG(conf *cfg.AgentConfig) bool {
	if conf == nil || conf.PreferMJPG == nil {
		return true
	}
	return *conf.PreferMJPG
}

// selectCameraMode chooses the best supported camera mode from caps, given a requested mode.
//   - If reqPixFmt == "AUTO" or empty, it will accept any pixfmt, and will prefer H264 first
//     when VideoCodec==h264; otherwise prefers MJPG then YUYV then H264.
//   - Prefers exact matches; otherwise chooses the closest by resolution then FPS.
func selectCameraMode(videoDev string, conf *cfg.AgentConfig, reqW, reqH, reqFps int) (pixFmt string, w int, h int, fps int, note string) {
	pixFmt = strings.ToUpper(strings.TrimSpace(conf.VideoPixFmt))
	if pixFmt == "" {
		pixFmt = "AUTO"
	}
	if reqW <= 0 || reqH <= 0 {
		reqW, reqH = 1280, 720
	}
	if reqFps <= 0 {
		reqFps = 30
	}

	caps, err := device.GetVideoCaps(videoDev)
	if err != nil || caps == nil || len(caps.Modes) == 0 {
		// No caps available; just use requested values.
		return pixFmt, reqW, reqH, reqFps, "caps-unavailable"
	}

	// Prefer camera H264 output when we're aiming to stream H264.
	preferCamH264 := strings.EqualFold(strings.TrimSpace(conf.VideoCodec), "h264")

	// Choose the best matching mode
	mode, bestFps, ok := pickBestMode(caps, pixFmt, reqW, reqH, reqFps, preferCamH264)
	if !ok {
		return pixFmt, reqW, reqH, reqFps, "no-matching-mode"
	}

	outPix := strings.ToUpper(strings.TrimSpace(mode.PixFmt))
	if outPix == "" {
		outPix = pixFmt
	}
	return outPix, mode.Width, mode.Height, bestFps, "snapped-to-supported"
}

func pickBestMode(caps *device.VideoCaps, reqPixFmt string, reqW, reqH, reqFps int, preferCamH264 bool) (device.VideoMode, int, bool) {
	reqPixFmt = strings.ToUpper(strings.TrimSpace(reqPixFmt))
	if reqPixFmt == "" {
		reqPixFmt = "AUTO"
	}

	candidates := make([]device.VideoMode, 0, len(caps.Modes))
	for _, m := range caps.Modes {
		mp := strings.ToUpper(strings.TrimSpace(m.PixFmt))
		if reqPixFmt != "AUTO" && mp != reqPixFmt {
			continue
		}
		candidates = append(candidates, m)
	}
	if len(candidates) == 0 {
		// If explicit pixfmt had no matches, allow all modes (better to run than fail).
		candidates = append(candidates, caps.Modes...)
	}

	// Exact match (w,h,fps) first
	for _, m := range candidates {
		if m.Width == reqW && m.Height == reqH {
			if len(m.FPS) == 0 {
				return m, reqFps, true
			}
			for _, f := range m.FPS {
				if f == reqFps {
					return m, reqFps, true
				}
			}
			// exact res, closest fps
			return m, closestFPS(m.FPS, reqFps), true
		}
	}

	// Otherwise closest by res then fps (plus tie-break pixfmt when AUTO)
	type scored struct {
		mode  device.VideoMode
		fps   int
		score float64
	}
	list := make([]scored, 0, len(candidates))
	for _, m := range candidates {
		bestF := reqFps
		if len(m.FPS) > 0 {
			bestF = closestFPS(m.FPS, reqFps)
		}

		dw := float64(m.Width - reqW)
		dh := float64(m.Height - reqH)
		resDist := math.Sqrt(dw*dw + dh*dh)
		fpsDist := float64(absInt(bestF - reqFps))

		pixPenalty := 0.0
		if reqPixFmt == "AUTO" {
			pixPenalty = float64(pixPref(m.PixFmt, preferCamH264)) * 5.0
		}

		score := resDist*1.0 + fpsDist*2.0 + pixPenalty
		list = append(list, scored{mode: m, fps: bestF, score: score})
	}

	sort.Slice(list, func(i, j int) bool {
		if list[i].score != list[j].score {
			return list[i].score < list[j].score
		}
		if list[i].fps != list[j].fps {
			return list[i].fps > list[j].fps // prefer higher FPS on tie
		}
		ai := list[i].mode.Width * list[i].mode.Height
		aj := list[i].mode.Width * list[i].mode.Height
		return ai > aj
	})

	return list[0].mode, list[0].fps, true
}

func closestFPS(list []int, target int) int {
	if len(list) == 0 {
		return target
	}
	best := list[0]
	bestD := absInt(best - target)
	for _, f := range list[1:] {
		d := absInt(f - target)
		if d < bestD || (d == bestD && f > best) {
			best, bestD = f, d
		}
	}
	return best
}

// pixPref returns a preference rank (lower = better).
// When preferCamH264=true, H264 is preferred first, then MJPG, then YUYV.
func pixPref(p string, preferCamH264 bool) int {
	p = strings.ToUpper(strings.TrimSpace(p))

	if preferCamH264 {
		switch p {
		case "H264":
			return 0
		case "MJPG", "MJPEG":
			return 1
		case "YUYV", "YUY2":
			return 2
		default:
			return 3
		}
	}

	switch p {
	case "MJPG", "MJPEG":
		return 0
	case "YUYV", "YUY2":
		return 1
	case "H264":
		return 2
	default:
		return 3
	}
}

func absInt(x int) int {
	if x < 0 {
		return -x
	}
	return x
}
