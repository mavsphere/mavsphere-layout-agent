// Package auth — pairing.go
//
// Implements the agent side of the pairing flow:
//
//  1. Generate a 6-char alphanumeric code.
//  2. Expose it via GetPairingCode() for the web UI to display.
//  3. Poll GET /api/agent/pair/{code} every 3 seconds.
//  4. When the backend returns a result (operator entered the code in the UI),
//     write the token + resource ID into config.json and cancel the pair loop.
package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/mavsphere/mavsphere-layout-agent/pkg/config"
)

const (
	pairPollInterval = 3 * time.Second
	pairCodeChars    = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789" // no O/0/I/1 to avoid confusion
	pairCodeLen      = 6
)

// pairResult mirrors AgentPairingService.PairResult on the backend.
type pairResult struct {
	AgentToken   string `json:"agentToken"`
	ResourceType string `json:"resourceType"`
	ResourceID   int64  `json:"resourceId"`
}

var (
	pairMu   sync.RWMutex
	pairCode string
)

// GetPairingCode returns the current pairing code, or "" if not in pairing mode.
func GetPairingCode() string {
	pairMu.RLock()
	defer pairMu.RUnlock()
	return pairCode
}

func setPairingCode(code string) {
	pairMu.Lock()
	pairCode = code
	pairMu.Unlock()
}

// generateCode returns a random 6-char code from pairCodeChars.
func generateCode() string {
	b := make([]byte, pairCodeLen)
	for i := range b {
		b[i] = pairCodeChars[rand.Intn(len(pairCodeChars))]
	}
	return string(b)
}

// RunPairingLoop generates a pairing code, polls the backend until the operator
// confirms, writes the result into config.json, and then returns so main can
// proceed with normal startup.
//
// cfgPath is the path to the on-disk config file (needed for Save).
// The function blocks until pairing completes or ctx is cancelled.
func RunPairingLoop(ctx context.Context, cfgPath string) error {
	cfg := config.Get()
	if cfg == nil {
		return fmt.Errorf("config not loaded")
	}

	code := generateCode()
	setPairingCode(code)
	defer setPairingCode("") // clear when done

	pollURL := fmt.Sprintf("%s/api/agent/pair/%s", cfg.BackendURL, code)

	log.Printf("════════════════════════════════════════════════════════")
	log.Printf(" AGENT PAIRING REQUIRED")
	log.Printf(" ")
	log.Printf(" Go to the MavSphere UI → Manage Layouts → Agent Tokens")
	log.Printf(" and enter this pairing code:")
	log.Printf(" ")
	log.Printf("         %s", code)
	log.Printf(" ")
	log.Printf(" Or open the agent web UI (http://<this-device>:8091)")
	log.Printf(" for the same code with a QR-friendly display.")
	log.Printf(" ")
	log.Printf(" Waiting for operator to confirm pairing…")
	log.Printf("════════════════════════════════════════════════════════")

	client := &http.Client{Timeout: 10 * time.Second}
	ticker := time.NewTicker(pairPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			result, err := pollOnce(client, pollURL)
			if err != nil {
				log.Printf("[pair] poll error: %v", err)
				continue
			}
			if result == nil {
				// Not yet paired — keep waiting.
				continue
			}

			// Pairing confirmed — write token and resource ID to config.
			log.Printf("[pair] pairing confirmed! type=%s id=%d", result.ResourceType, result.ResourceID)

			cfg := config.Get()
			cfg.AgentToken = result.AgentToken
			cfg.LayoutID = fmt.Sprintf("%d", result.ResourceID)

			if err := config.Save(cfgPath, cfg); err != nil {
				return fmt.Errorf("pair: failed to save config: %w", err)
			}

			log.Printf("[pair] config saved — agent is now paired to layoutId=%s", cfg.LayoutID)
			return nil
		}
	}
}

func pollOnce(client *http.Client, url string) (*pairResult, error) {
	resp, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusNoContent:
		// 204 — not yet paired.
		return nil, nil
	case http.StatusOK:
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("read body: %w", err)
		}
		var result pairResult
		if err := json.Unmarshal(body, &result); err != nil {
			return nil, fmt.Errorf("parse pair result: %w", err)
		}
		return &result, nil
	default:
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(body))
	}
}
