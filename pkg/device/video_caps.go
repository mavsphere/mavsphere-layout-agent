package device

import (
	"bufio"
	"bytes"
	"fmt"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

var (
	gstOnce sync.Once
	gstBin  string
)

// VideoMode describes a single V4L2 mode (pixel format + resolution + fps list).
// PixFmt is the 4CC as printed by v4l2-ctl (e.g. "MJPG", "YUYV", "H264").
type VideoMode struct {
	PixFmt string `json:"pixFmt"`
	Width  int    `json:"width"`
	Height int    `json:"height"`
	FPS    []int  `json:"fps"`
}

// VideoCaps is a structured representation of `v4l2-ctl --list-formats-ext`.
type VideoCaps struct {
	Device string      `json:"device"`
	Modes  []VideoMode `json:"modes"`
}

func gstInspectBin() string {
	gstOnce.Do(func() {
		// Prefer /opt/gst build if present, fall back to PATH
		if _, err := exec.LookPath("/opt/gst/bin/gst-inspect-1.0"); err == nil {
			gstBin = "/opt/gst/bin/gst-inspect-1.0"
			return
		}
		if p, err := exec.LookPath("gst-inspect-1.0"); err == nil {
			gstBin = p
			return
		}
		gstBin = "gst-inspect-1.0"
	})
	return gstBin
}

// HasGstElement returns true if gst-inspect can find the element/plugin.
func HasGstElement(name string) bool {
	cmd := exec.Command(gstInspectBin(), name)
	// We only care about exit code.
	if err := cmd.Run(); err != nil {
		return false
	}
	return true
}

// GetVideoCaps returns parsed v4l2 modes for a device. Requires `v4l2-ctl` to be installed.
func GetVideoCaps(device string) (*VideoCaps, error) {
	device = strings.TrimSpace(device)
	if device == "" {
		return nil, fmt.Errorf("empty device")
	}

	cmd := exec.Command("v4l2-ctl", "--list-formats-ext", "-d", device)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("v4l2-ctl failed: %w: %s", err, strings.TrimSpace(out.String()))
	}

	caps := &VideoCaps{Device: device}
	modes, err := parseV4L2FormatsExt(out.Bytes())
	if err != nil {
		return nil, err
	}
	caps.Modes = modes
	return caps, nil
}

// parseV4L2FormatsExt parses output of v4l2-ctl --list-formats-ext.
func parseV4L2FormatsExt(b []byte) ([]VideoMode, error) {
	scanner := bufio.NewScanner(bytes.NewReader(b))

	// Example lines:
	// 	[0]: 'MJPG' (Motion-JPEG, compressed)
	// 		Size: Discrete 1920x1080
	// 			Interval: Discrete 0.033s (30.000 fps)
	fmtRe := regexp.MustCompile(`^\s*\[\d+\]:\s*'([A-Za-z0-9]+)'`)
	sizeRe := regexp.MustCompile(`^\s*Size:\s*Discrete\s*(\d+)x(\d+)`)
	fpsRe := regexp.MustCompile(`\((\d+(?:\.\d+)?)\s*fps\)`)

	curFmt := ""
	curW, curH := 0, 0

	// key: fmt|w|h
	type key struct {
		f string
		w int
		h int
	}
	m := map[key]map[int]struct{}{}

	for scanner.Scan() {
		line := scanner.Text()

		if mm := fmtRe.FindStringSubmatch(line); mm != nil {
			curFmt = strings.ToUpper(mm[1])
			curW, curH = 0, 0
			continue
		}
		if mm := sizeRe.FindStringSubmatch(line); mm != nil {
			w, _ := strconv.Atoi(mm[1])
			h, _ := strconv.Atoi(mm[2])
			curW, curH = w, h
			continue
		}
		if curFmt != "" && curW > 0 && curH > 0 && strings.Contains(line, "fps") {
			mm := fpsRe.FindStringSubmatch(line)
			if mm != nil {
				f, _ := strconv.ParseFloat(mm[1], 64)
				fps := int(f + 0.5)
				k := key{f: curFmt, w: curW, h: curH}
				set := m[k]
				if set == nil {
					set = map[int]struct{}{}
					m[k] = set
				}
				if fps > 0 {
					set[fps] = struct{}{}
				}
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	// flatten
	out := make([]VideoMode, 0, len(m))
	for k, set := range m {
		fps := make([]int, 0, len(set))
		for f := range set {
			fps = append(fps, f)
		}
		// stable-ish order: numeric ascending
		for i := 0; i < len(fps); i++ {
			for j := i + 1; j < len(fps); j++ {
				if fps[j] < fps[i] {
					fps[i], fps[j] = fps[j], fps[i]
				}
			}
		}
		out = append(out, VideoMode{PixFmt: k.f, Width: k.w, Height: k.h, FPS: fps})
	}

	return out, nil
}

// BestMatch picks the closest supported mode to (wantW,wantH,wantFps) for a preferred pixel format.
// preferredPixFmt can be "MJPG" or "YUYV" etc. If empty, any format is allowed.
func (c *VideoCaps) BestMatch(preferredPixFmt string, wantW, wantH, wantFps int) (VideoMode, bool) {
	if c == nil {
		return VideoMode{}, false
	}
	preferredPixFmt = strings.ToUpper(strings.TrimSpace(preferredPixFmt))

	bestScore := int(^uint(0) >> 1)
	best := VideoMode{}
	found := false

	scoreMode := func(m VideoMode) int {
		dw := wantW - m.Width
		if dw < 0 {
			dw = -dw
		}
		dh := wantH - m.Height
		if dh < 0 {
			dh = -dh
		}
		// fps diff: use nearest
		bestFpsDiff := 0
		if wantFps > 0 && len(m.FPS) > 0 {
			bestFpsDiff = 1 << 30
			for _, f := range m.FPS {
				d := wantFps - f
				if d < 0 {
					d = -d
				}
				if d < bestFpsDiff {
					bestFpsDiff = d
				}
			}
		}
		return dw*dw + dh*dh + bestFpsDiff*bestFpsDiff
	}

	for _, m := range c.Modes {
		if preferredPixFmt != "" && strings.ToUpper(m.PixFmt) != preferredPixFmt {
			continue
		}
		s := scoreMode(m)
		if s < bestScore {
			bestScore = s
			best = m
			found = true
		}
	}
	if found {
		return best, true
	}
	// If preferred format not found, fall back to any.
	if preferredPixFmt != "" {
		for _, m := range c.Modes {
			s := scoreMode(m)
			if s < bestScore {
				bestScore = s
				best = m
				found = true
			}
		}
	}
	return best, found
}
