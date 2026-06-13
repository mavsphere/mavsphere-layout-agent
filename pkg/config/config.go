// Package config is the single source of truth for layout-agent configuration.
package config

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

// CameraSource describes where the video comes from.
// "usb"        – V4L2 device at Device path (default; backward-compatible)
// "rtsp"       – RTSP stream at RTSPURL (e.g. ESP32-CAM, IP camera, Pi via mediamtx)
// "http_mjpeg" – HTTP MJPEG stream at RTSPURL (e.g. ESP32-CAM /stream endpoint)
type CameraSource = string

const (
	CameraSourceUSB       CameraSource = "usb"
	CameraSourceRTSP      CameraSource = "rtsp"
	CameraSourceHTTPMJPEG CameraSource = "http_mjpeg"
)

type CameraConfig struct {
	CameraID  string `json:"cameraId"`
	Label     string `json:"label"`
	Type      string `json:"type"`
	Device    string `json:"device"`
	TrainSlug string `json:"trainSlug"`

	// Source selects the capture mechanism.  Empty / absent means "usb" so
	// existing configs require no changes.
	Source string `json:"source,omitempty"`

	// RTSPURL is used when Source is "rtsp" or "http_mjpeg".
	// Examples:
	//   rtsp://192.168.1.42:8554/stream   (ESP32-CAM via mediamtx)
	//   rtsp://192.168.1.42/stream1        (generic IP cam)
	//   http://192.168.1.42:81/stream      (ESP32-CAM Arduino sketch MJPEG)
	RTSPURL string `json:"rtspUrl,omitempty"`

	// RTSPTransport overrides the GStreamer rtspsrc protocols property.
	// Values: "tcp" | "udp" | "udp-mcast" | "http" | "tls" | "auto" (default).
	// Use "tcp" for ESP32-CAM which struggles with UDP reassembly over Wi-Fi.
	RTSPTransport string `json:"rtspTransport,omitempty"`

	// BufferMs is the rtspsrc/souphttpsrc latency buffer in milliseconds.
	// 0 = use built-in default (200ms for RTSP, 0 for HTTP-MJPEG).
	// Lower values reduce latency but increase risk of decode artefacts on
	// poor Wi-Fi.  100–300 ms is a reasonable range for local ESP32-CAM use.
	BufferMs int `json:"bufferMs,omitempty"`

	// Per-camera resolution/fps overrides.
	// When non-zero, these take precedence over the global VideoWidth/VideoHeight/VideoFps.
	// Zero means "use global default", so existing configs are unaffected.
	// For RTSP/HTTP-MJPEG sources these are passed as capsfilter hints only;
	// the stream resolution is ultimately whatever the remote end sends.
	Width  int `json:"width,omitempty"`
	Height int `json:"height,omitempty"`
	Fps    int `json:"fps,omitempty"`
}

type TrainConfig struct {
	TrainID        int64  `json:"trainId"`
	TrainSlug      string `json:"trainSlug"`
	DccAddress     int    `json:"dccAddress"`
	DisplayName    string `json:"displayName"`
	GuardingSignal string `json:"guardingSignal,omitempty"` // signal protecting this train's home block
	StartBlock     string `json:"startBlock,omitempty"`     // block where this train homes
}

type FailsafeConfig struct {
	ControlTimeoutMs int `json:"controlTimeoutMs"`
	ReissueStopMs    int `json:"reissueStopMs"`
}

// JmriConfig describes the optional local JMRI web server to reverse-proxy.
// When Enabled is true, the layout agent proxies JMRI HTTP pages and WebSocket
// traffic under the /jmri/ path prefix of its own web UI port (:8091).
// URL must point to the JMRI web server, e.g. "http://localhost:12080".
type JmriConfig struct {
	Enabled bool   `json:"enabled"`
	URL     string `json:"url"` // e.g. "http://localhost:12080"
}

// DccExConfig describes the DCC-EX command station connection.
// Port: serial path ("/dev/ttyUSB0", "COM3") or TCP host:port ("192.168.1.100:2560").
type DccExConfig struct {
	Port             string `json:"port"`
	BaudRate         int    `json:"baudRate"`
	CommandTimeoutMs int    `json:"commandTimeoutMs"`
}

// MqttConfig describes the local MQTT broker used by sensor nodes.
// Topics follow: {topicPrefix}/node/{nodeId}/sensor/{sensorId}/state
type MqttConfig struct {
	BrokerURL   string `json:"brokerUrl"`
	Username    string `json:"username,omitempty"`
	Password    string `json:"password,omitempty"`
	TopicPrefix string `json:"topicPrefix"`
}

type AgentConfig struct {
	// Identity + backend connection
	LayoutID     string `json:"layoutId"`
	BackendWsURL string `json:"backendWsUrl"`
	BackendURL   string `json:"backendUrl"`
	Username     string `json:"username"`
	Password     string `json:"password"`

	// Hardware
	DccEx DccExConfig `json:"dccEx"`
	Mqtt  MqttConfig  `json:"mqtt"`
	Jmri  JmriConfig  `json:"jmri"`

	// Janus / WebRTC
	JanusURL              string `json:"janusUrl"`
	ForceRelayOverride    *bool  `json:"forceRelay,omitempty"`
	VideoCodec            string `json:"videoCodec"`
	H264Encoder           string `json:"h264Encoder"`
	H264Profile           string `json:"h264Profile"`
	H264BitrateBps        int    `json:"h264BitrateBps"`
	VideoPixFmt           string `json:"videoPixFmt,omitempty"`
	PreferMJPG            *bool  `json:"preferMjpg,omitempty"`
	VideoWidth            int    `json:"videoWidth"`
	VideoHeight           int    `json:"videoHeight"`
	VideoFps              int    `json:"videoFps"`
	WebRTCStartBitrateBps int    `json:"webrtcStartBitrateBps"`
	WebRTCMaxBitrateBps   int    `json:"webrtcMaxBitrateBps"`
	WebRTCMinBitrateBps   int    `json:"webrtcMinBitrateBps"`

	MavID string `json:"-"`

	Cameras      []CameraConfig `json:"cameras"`
	Trains       []TrainConfig  `json:"trains"`
	AllowControl bool           `json:"allowControl"`
	Failsafe     FailsafeConfig `json:"failsafe"`
}

var (
	mu     sync.RWMutex
	global *AgentConfig
)

func Load(path string) (*AgentConfig, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var cfg AgentConfig
	if err := json.Unmarshal(b, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	applyDefaults(&cfg)
	mu.Lock()
	global = &cfg
	mu.Unlock()
	return &cfg, nil
}

func Get() *AgentConfig { mu.RLock(); defer mu.RUnlock(); return global }

func Set(cfg *AgentConfig) { mu.Lock(); global = cfg; mu.Unlock() }

// FileRevision returns a stable hash of the on-disk config file.
//
// The web UI uses this as an optimistic-lock token: the browser loads a
// revision, sends it back on save, and the server rejects the save if the
// config file has changed underneath that browser tab.
func FileRevision(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read config revision: %w", err)
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

func Save(path string, cfg *AgentConfig) error {
	applyDefaults(cfg)
	b, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return fmt.Errorf("encode: %w", err)
	}
	tmp := path + ".tmp"
	// Config contains backend credentials, so do not make it world-readable.
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	mu.Lock()
	global = cfg
	mu.Unlock()
	return nil
}

func applyDefaults(cfg *AgentConfig) {
	if cfg.BackendWsURL == "" {
		cfg.BackendWsURL = "ws://localhost:8080/api/ws/agent"
	}
	if cfg.BackendURL == "" {
		cfg.BackendURL = "http://localhost:8080"
	}
	if cfg.DccEx.Port == "" {
		cfg.DccEx.Port = "/dev/ttyUSB0"
	}
	if cfg.DccEx.BaudRate == 0 {
		cfg.DccEx.BaudRate = 115200
	}
	if cfg.DccEx.CommandTimeoutMs == 0 {
		cfg.DccEx.CommandTimeoutMs = 3000
	}
	if cfg.Mqtt.BrokerURL == "" {
		cfg.Mqtt.BrokerURL = "tcp://localhost:1883"
	}
	if cfg.Mqtt.TopicPrefix == "" {
		cfg.Mqtt.TopicPrefix = "layout"
	}
	if cfg.JanusURL == "" {
		cfg.JanusURL = "ws://localhost:8188/janus"
	}
	if cfg.VideoCodec == "" {
		cfg.VideoCodec = "vp8"
	}
	if cfg.H264Encoder == "" {
		cfg.H264Encoder = "auto"
	}
	if cfg.H264Profile == "" {
		cfg.H264Profile = "baseline"
	}
	if cfg.H264BitrateBps <= 0 {
		cfg.H264BitrateBps = 1_500_000
	}
	if cfg.VideoWidth == 0 {
		cfg.VideoWidth = 1280
	}
	if cfg.VideoHeight == 0 {
		cfg.VideoHeight = 720
	}
	if cfg.VideoFps == 0 {
		cfg.VideoFps = 25
	}
	if cfg.PreferMJPG == nil {
		v := true
		cfg.PreferMJPG = &v
	}
	if cfg.WebRTCMaxBitrateBps <= 0 {
		cfg.WebRTCMaxBitrateBps = cfg.H264BitrateBps
	}
	if cfg.WebRTCStartBitrateBps <= 0 {
		cfg.WebRTCStartBitrateBps = min(800_000, cfg.WebRTCMaxBitrateBps)
	}
	if cfg.WebRTCMinBitrateBps <= 0 {
		cfg.WebRTCMinBitrateBps = 150_000
	}
	if cfg.Failsafe.ControlTimeoutMs <= 0 {
		cfg.Failsafe.ControlTimeoutMs = 2000
	}
	if cfg.Failsafe.ReissueStopMs <= 0 {
		cfg.Failsafe.ReissueStopMs = 1000
	}
	if cfg.MavID == "" {
		cfg.MavID = cfg.LayoutID
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (t *TrainConfig) UnmarshalJSON(data []byte) error {
	type Alias TrainConfig
	aux := struct {
		Alias
		LegacyJmriAddress int `json:"jmriAddress"`
	}{}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	*t = TrainConfig(aux.Alias)
	if t.DccAddress == 0 && aux.LegacyJmriAddress != 0 {
		t.DccAddress = aux.LegacyJmriAddress
	}
	return nil
}
