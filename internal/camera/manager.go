// Package camera manages GStreamer pipelines for layout cameras.
// Each camera gets its own media.Manager from pkg/media.
package camera

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/mavsphere/mavsphere-layout-agent/pkg/config"
	agentmedia "github.com/mavsphere/mavsphere-layout-agent/pkg/media"
)

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
}

func NewCameraManager(_ *config.AgentConfig, audioDevice string, logger *log.Logger) *CameraManager {
	// The config pointer argument is intentionally unused: we always call
	// config.Get() at the point of use so that resolution/codec changes saved
	// via the web UI are reflected in new pipelines without a full restart.
	return &CameraManager{
		managers:    make(map[string]*agentmedia.Manager),
		logger:      logger,
		audioDevice: audioDevice,
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
		// USB / V4L2. Only pass the audio device to this camera if:
		//   1. An audio device is configured globally, and
		//   2. Per-camera audioDisable is not set, and
		//   3. No other camera is already holding ALSA (audioOwner is empty).
		// This ensures all cameras can publish video simultaneously — ALSA is
		// exclusive-open and giving it to every pipeline causes the second
		// camera's GStreamer process to crash on audio open, which in turn
		// corrupts the V4L2 device state and blocks all subsequent attempts.
		audioDev := ""
		if m.audioDevice != "" && !cam.AudioDisable && m.audioOwner == "" {
			audioDev = m.audioDevice
			m.audioOwner = cameraSlug
			m.logger.Printf("[CamMgr] camera=%s assigned audio device=%s", cameraSlug, audioDev)
		} else if m.audioDevice != "" && !cam.AudioDisable && m.audioOwner != "" {
			m.logger.Printf("[CamMgr] camera=%s starting video-only (audio held by camera=%s)", cameraSlug, m.audioOwner)
		}
		mgr = agentmedia.NewManager(&camCfg, cam.Device, audioDev, m.logger)
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
	// Deliberately do NOT release audioOwner here. Audio ownership is fixed for
	// the lifetime of the agent session (until StopAll). Releasing it on individual
	// camera stops would allow a subsequent StartCamera to claim ALSA while the
	// stopping camera's GStreamer process is still tearing down, causing the same
	// V4L2-busy race condition the audioOwner scheme was introduced to prevent.
	// The audio room architecture (see AUDIO_ROOM_ARCHITECTURE.md) will supersede
	// this entirely — at that point audioOwner is removed.
	m.mu.Unlock()

	// Stop outside the lock: Manager.Stop() acquires its own internal mutex.
	mgr.Stop()
	m.logger.Printf("[CamMgr] stopped stream for camera=%s", cameraSlug)
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
