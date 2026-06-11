package device

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// DetectVideoDevice tries to find a compatible video device supporting MJPG or H264.
func DetectVideoDevice() (string, error) {
	matches, err := filepath.Glob("/dev/video*")
	if err != nil {
		return "", err
	}

	for _, device := range matches {
		info, err := os.Stat(device)
		if err != nil || info.IsDir() {
			continue
		}

		cmd := exec.Command("v4l2-ctl", "--list-formats", "-d", device)
		var out bytes.Buffer
		cmd.Stdout = &out
		cmd.Stderr = nil

		err = cmd.Run()
		if err != nil {
			continue // skip unresponsive devices
		}

		output := out.String()
		if strings.Contains(output, "H264") || strings.Contains(output, "MJPG") || strings.Contains(output, "YUYV") {
			fmt.Printf("[Video] ✅ Found compatible device: %s\n", device)
			return device, nil
		}
	}

	return "", fmt.Errorf("[Video] ❌ No compatible video device found")
}

func DetectAudioDevice() (string, error) {
	cmd := exec.Command("arecord", "-l")
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = nil

	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("arecord -l failed: %w", err)
	}

	lines := strings.Split(out.String(), "\n")
	var card, dev string

	for _, line := range lines {
		line = strings.TrimSpace(line)

		// Example:
		// card 1: USB [USB Audio Device], device 0: USB Audio [USB Audio]
		if strings.HasPrefix(line, "card ") {
			// find "card N:" and "device M:"
			fields := strings.Fields(line)
			// fields: ["card", "1:", "USB", "[USB", "Audio", "Device],", "device", "0:", ...]
			for i := 0; i < len(fields); i++ {
				// card index
				if fields[i] == "card" && i+1 < len(fields) {
					cardField := strings.TrimSuffix(fields[i+1], ":")
					card = cardField
				}
				// device index
				if fields[i] == "device" && i+1 < len(fields) {
					devField := strings.TrimSuffix(fields[i+1], ":")
					dev = devField
				}
			}
			if card != "" && dev != "" {
				break
			}
		}
	}

	if card == "" || dev == "" {
		return "", fmt.Errorf("no ALSA capture card/device found in arecord -l output")
	}

	device := fmt.Sprintf("hw:%s,%s", card, dev)
	return device, nil
}
