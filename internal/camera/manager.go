// Package camera manages GStreamer pipelines for layout cameras.
// Each camera gets its own media.Manager from pkg/media.
package camera

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/mavsphere/mavsphere-layout-agent/pkg/config"
	agentmedia "github.com/mavsphere/mavsphere-layout-agent/pkg/media"
)

// deviceRestartDelay is the minimum time to wait between stopping and restarting
// a pipeline on the same V4L2 device. The Linux kernel can take a moment to
// fully release a V4L2 node after the GStreamer process exits — even though
// gracefulTerminate blocks until the process has actually exited, the inode
// lock can linger. 2s matches the V4L2-busy extra sleep in janusvr_sink.go.
const deviceRestartDelay = 2 * time.Second

// CameraManager owns one stream.Manager per camera, started on demand.
type CameraManager struct {
	mu       sync.Mutex
	managers map[string]*agentmedia.Manager // key: cameraSlug
	logger   *log.Logger

	// audioDevice is the single ALSA capture device (if any) shared across
	// all cameras, resolved once at agent startup (auto-detected or set via
	// config.AudioDevice). ALSA is exclusive-open — only one GStreamer pipeline
	// can hold it at a time. audioOwner tracks which camera currently holds it;
	// all others get video-only pipelines to prevent ALSA contention causing
	// V4L2 device locks when pipelines crash on audio-open failure.
	audioDevice string
	audioOwner  string // cameraSlug that currently holds the ALSA device, or ""

	// deviceLastStopped tracks the last time each V4L2 device path was freed.
	// StartCamera checks this and waits if the device was stopped too recently.
	// Key: device path (e.g. "/dev/video0"). RTSP/IP cameras are never keyed here.
	deviceLastStopped map[string]time.Time
}

func NewCameraManager(_ *config.AgentConfig, audioDevice string, logger *log.Logger) *CameraManager {
	// The config pointer argument is intentionally unused: we always call
	// config.Get() at the point of use so that resolution/codec changes saved
	// via the web UI are reflected in new pipelines without a full restart.
	return &CameraManager{
		managers:          make(map[string]*agentmedia.Manager),
		logger:            logger,
		audioDevice:       audioDevice,
		deviceLastStopped: make(map[string]time.Time),
	}
}

// StartCamera brings up a GStreamer publisher pipeline for the named camera.
// roomID is the Janus VideoRoom already provisioned by the backend.
func (m *CameraManager) StartCamera(ctx context.Context, cameraSlug string, roomID int64, ice agentmedia.ICEConfig) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.managers[cameraSlug]; ok {
		m.logger.Printf("[CamMgr] %s already streaming — ignoring START", cameraSlug)
		return nil
	}

	// Use the live config so any resolution/codec change saved via the web UI
	// is picked up immediately for the next StartCamera call.
	cfg := config.Get()

	cam := findCamera(cfg, cameraSlug)
	if cam == nil {
		return fmt.Errorf("unknown camera: %s", cameraSlug)
	}

	// Build a config copy that applies per-camera overrides for resolution/fps.
	// Zero means "not set" — fall through to the global values already in cfg.
	camCfg := *cfg
	if cam.Width > 0 {
		camCfg.VideoWidth = cam.Width
	}
	if cam.Height > 0 {
		camCfg.VideoHeight = cam.Height
	}
	if cam.Fps > 0 {
		camCfg.VideoFps = cam.Fps
	}

	var mgr *agentmedia.Manager
	switch cam.Source {
	case config.CameraSourceRTSP, config.CameraSourceHTTPMJPEG:
		if cam.RTSPURL == "" {
			return fmt.Errorf("camera %q: source=%s but rtspUrl is empty", cameraSlug, cam.Source)
		}
		mgr = agentmedia.NewIPSourceManager(&camCfg, cam.Source, cam.RTSPURL, cam.RTSPTransport, cam.BufferMs, m.logger)
	default:
		// USB / V4L2. Enforce minimum restart delay per device to prevent V4L2-busy
		// errors caused by the kernel inode lock lingering after process exit.
		device := cam.Device
		if lastStop, ok := m.deviceLastStopped[device]; ok {
			sinceStop := time.Since(lastStop)
			if sinceStop < deviceRestartDelay {
				wait := deviceRestartDelay - sinceStop
				m.logger.Printf("[CamMgr] camera=%s device=%s stopped %v ago — waiting %v before restart",
					cameraSlug, device, sinceStop.Round(time.Millisecond), wait.Round(time.Millisecond))
				m.mu.Unlock()
				time.Sleep(wait)
				m.mu.Lock()
				// Re-check: another goroutine may have started this camera while we waited.
				if _, ok := m.managers[cameraSlug]; ok {
					m.logger.Printf("[CamMgr] %s started by another goroutine while waiting — ignoring START", cameraSlug)
					return nil
				}
			}
		}

		// Only pass the audio device to this camera if:
		//   1. An audio device is configured globally, and
		//   2. Per-camera audioDisable is not set, and
		//   3. No other camera is already holding ALSA (audioOwner is empty).
		audioDev := ""
		if m.audioDevice != "" && !cam.AudioDisable && m.audioOwner == "" {
			audioDev = m.audioDevice
			m.audioOwner = cameraSlug
			m.logger.Printf("[CamMgr] camera=%s assigned audio device=%s", cameraSlug, audioDev)
		} else if m.audioDevice != "" && !cam.AudioDisable && m.audioOwner != "" {
			m.logger.Printf("[CamMgr] camera=%s starting video-only (audio held by camera=%s)", cameraSlug, m.audioOwner)
		}
		mgr = agentmedia.NewManager(&camCfg, device, audioDev, m.logger)
	}
	mgr.SetICE(ctx, ice)

	if err := mgr.Start(ctx, roomID); err != nil {
		return fmt.Errorf("stream start for %s: %w", cameraSlug, err)
	}

	m.managers[cameraSlug] = mgr
	m.logger.Printf("[CamMgr] started stream for camera=%s roomID=%d", cameraSlug, roomID)
	return nil
}

// StopCamera tears down the stream for the named camera.
func (m *CameraManager) StopCamera(cameraSlug string) {
	m.mu.Lock()
	mgr, ok := m.managers[cameraSlug]
	if !ok {
		m.mu.Unlock()
		return
	}
	delete(m.managers, cameraSlug)

	// Record which device this camera was using and whether it held audio,
	// so we can enforce a restart delay and release audio ownership correctly.
	cfg := config.Get()
	cam := findCamera(cfg, cameraSlug)
	var devicePath string
	var wasAudioOwner bool
	if cam != nil {
		switch cam.Source {
		case config.CameraSourceRTSP, config.CameraSourceHTTPMJPEG:
			// IP cameras don't use V4L2 — no device restart delay needed.
		default:
			devicePath = cam.Device
		}
	}
	wasAudioOwner = (m.audioOwner == cameraSlug)
	m.mu.Unlock()

	// Stop outside the lock: Manager.Stop() acquires its own internal mutex
	// and blocks until the GStreamer process has fully exited.
	mgr.Stop()
	m.logger.Printf("[CamMgr] stopped stream for camera=%s", cameraSlug)

	// Record the stop time for this V4L2 device. StartCamera will wait if the
	// device was stopped less than deviceRestartDelay ago.
	if devicePath != "" {
		m.mu.Lock()
		m.deviceLastStopped[devicePath] = time.Now()
		m.mu.Unlock()
	}

	// Release audio ownership after a delay so the ALSA device has time to
	// fully release before the next camera claims it. We do NOT release
	// immediately because although the GStreamer process has exited, the ALSA
	// device can have a brief kernel-side lock. The delay matches deviceRestartDelay.
	if wasAudioOwner {
		go func() {
			time.Sleep(deviceRestartDelay)
			m.mu.Lock()
			if m.audioOwner == cameraSlug {
				m.audioOwner = ""
				m.logger.Printf("[CamMgr] camera=%s released audio ownership", cameraSlug)
			}
			m.mu.Unlock()
		}()
	}
}

// StopAll tears down every active stream (called on STOMP disconnect or shutdown).
// Blocks until all gst-launch child processes have exited so the V4L2 device is
// fully released before the caller proceeds (e.g. os.Exit on a web-UI restart).
func (m *CameraManager) StopAll() {
	m.mu.Lock()
	toStop := make(map[string]*agentmedia.Manager, len(m.managers))
	for slug, mgr := range m.managers {
		toStop[slug] = mgr
	}
	m.managers = make(map[string]*agentmedia.Manager)
	m.audioOwner = ""
	m.deviceLastStopped = make(map[string]time.Time)
	m.mu.Unlock()

	// Stop outside the lock so Manager.Stop() can acquire its own mutex freely.
	for slug, mgr := range toStop {
		m.logger.Printf("[CamMgr] stopping stream for camera=%s (StopAll)", slug)
		mgr.Stop()
		m.logger.Printf("[CamMgr] stream stopped for camera=%s", slug)
	}
}

// IsStreaming returns true if there is an active pipeline for this camera.
func (m *CameraManager) IsStreaming(cameraSlug string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.managers[cameraSlug]
	return ok
}

// ForEach calls fn for each camera that has an active stream.Manager entry,
// holding the CameraManager lock for the duration of each call.
// Use this to collect StreamInfo snapshots without exposing internal state.
func (m *CameraManager) ForEach(fn func(slug string, mgr *agentmedia.Manager)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for slug, mgr := range m.managers {
		fn(slug, mgr)
	}
}

// CameraStreamInfo is a per-camera stream snapshot for the status API.
type CameraStreamInfo struct {
	CameraID string `json:"cameraId"`
	agentmedia.StreamInfo
}

// StreamInfos returns a snapshot of all active camera streams.
// Cameras with no active pipeline are omitted.
func (m *CameraManager) StreamInfos() []CameraStreamInfo {
	m.mu.Lock()
	slugs := make([]string, 0, len(m.managers))
	mgrs := make([]*agentmedia.Manager, 0, len(m.managers))
	for slug, mgr := range m.managers {
		slugs = append(slugs, slug)
		mgrs = append(mgrs, mgr)
	}
	m.mu.Unlock()

	out := make([]CameraStreamInfo, 0, len(slugs))
	for i, mgr := range mgrs {
		out = append(out, CameraStreamInfo{
			CameraID:   slugs[i],
			StreamInfo: mgr.StreamInfo(),
		})
	}
	return out
}

func findCamera(cfg *config.AgentConfig, slug string) *config.CameraConfig {
	if cfg == nil {
		return nil
	}
	for i := range cfg.Cameras {
		if cfg.Cameras[i].CameraID == slug {
			return &cfg.Cameras[i]
		}
	}
	return nil
}
