package auth

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

// TopologyBundle holds the raw topology data fetched from the backend REST API.
// Served verbatim by the agent's local /api/topology endpoint.
type TopologyBundle struct {
	Blocks      []json.RawMessage `json:"blocks"`
	Connections []json.RawMessage `json:"connections"`
	Signals     []json.RawMessage `json:"signals"`
	Sensors     []json.RawMessage `json:"sensors"`
}

var (
	topoCacheMu    sync.RWMutex
	cachedTopology *TopologyBundle
)

// FetchTopology fetches blocks, connections, signals, and sensors from the backend.
//
// On failure it returns the last successfully fetched bundle (last-known-good).
// An error is always returned alongside cached data so the caller can log it.
func FetchTopology(backendURL, layoutID, token string) (*TopologyBundle, error) {
	client := &http.Client{Timeout: 15 * time.Second}

	fetch := func(resource string) ([]json.RawMessage, error) {
		url := fmt.Sprintf("%s/api/layouts/%s/%s", backendURL, layoutID, resource)
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("GET %s: %w", url, err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("GET %s HTTP %d: %s", url, resp.StatusCode, string(body))
		}
		var items []json.RawMessage
		if err := json.Unmarshal(body, &items); err != nil {
			return nil, fmt.Errorf("decode %s: %w", resource, err)
		}
		return items, nil
	}

	blocks, err := fetch("blocks")
	if err != nil {
		return cachedTopoSnapshot(), fmt.Errorf("topology blocks: %w", err)
	}
	connections, err := fetch("connections")
	if err != nil {
		return cachedTopoSnapshot(), fmt.Errorf("topology connections: %w", err)
	}
	signals, err := fetch("signals")
	if err != nil {
		return cachedTopoSnapshot(), fmt.Errorf("topology signals: %w", err)
	}
	sensors, err := fetch("sensors")
	if err != nil {
		return cachedTopoSnapshot(), fmt.Errorf("topology sensors: %w", err)
	}

	bundle := &TopologyBundle{
		Blocks:      blocks,
		Connections: connections,
		Signals:     signals,
		Sensors:     sensors,
	}

	topoCacheMu.Lock()
	cachedTopology = bundle
	topoCacheMu.Unlock()

	return bundle, nil
}

func cachedTopoSnapshot() *TopologyBundle {
	topoCacheMu.RLock()
	defer topoCacheMu.RUnlock()
	return cachedTopology // nil if never fetched successfully
}
