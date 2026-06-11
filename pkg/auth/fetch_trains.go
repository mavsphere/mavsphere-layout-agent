package auth

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

// TrainConfig is the agent-ready train config returned by the backend.
type TrainConfig struct {
	TrainID        int64  `json:"trainId"`
	TrainSlug      string `json:"trainSlug"`
	DccAddress     int    `json:"dccAddress"`
	DisplayName    string `json:"displayName"`
	GuardingSignal string `json:"guardingSignal"`
	StartBlock     string `json:"startBlock"`
}

var (
	trainCacheMu sync.RWMutex
	cachedTrains []TrainConfig
)

// FetchTrains calls GET /api/layouts/{layoutId}/trains/config and returns
// the authoritative train list from the backend.
//
// On failure it returns the last successfully fetched list (last-known-good).
// An error is always returned alongside the cached data so the caller can log it.
// If no cached data exists and the fetch fails, returns nil and the error.
func FetchTrains(backendURL, layoutID, token string) ([]TrainConfig, error) {
	url := fmt.Sprintf("%s/api/layouts/%s/trains/config", backendURL, layoutID)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return cachedSnapshot(), err
	}
	req.Header.Set("Authorization", "Bearer "+token)

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return cachedSnapshot(), fmt.Errorf("fetch trains HTTP: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return cachedSnapshot(), fmt.Errorf("fetch trains HTTP %d: %s", resp.StatusCode, string(body))
	}

	var trains []TrainConfig
	if err := json.Unmarshal(body, &trains); err != nil {
		return cachedSnapshot(), fmt.Errorf("fetch trains decode: %w", err)
	}

	// Update cache on success
	trainCacheMu.Lock()
	cachedTrains = trains
	trainCacheMu.Unlock()

	return trains, nil
}

func cachedSnapshot() []TrainConfig {
	trainCacheMu.RLock()
	defer trainCacheMu.RUnlock()
	if len(cachedTrains) == 0 {
		return nil
	}
	cp := make([]TrainConfig, len(cachedTrains))
	copy(cp, cachedTrains)
	return cp
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
