// Package auth handles backend authentication for the layout-agent.
//
// The agent authenticates using a pre-issued agent token (set via the
// pairing flow into the agentToken field in config.json) by calling
// POST /api/agent/token-login. No password is ever stored on the device.
//
// Error classification for 401 responses:
//
//	ErrTokenRevoked  — backend explicitly says token is invalid or revoked.
//	                   Caller should clear the token and enter pairing mode.
//	transient error  — 401 without explicit revocation body (e.g. clock skew,
//	                   temporary auth service issue). Caller should retry with
//	                   normal exponential backoff, not clear the token.
package auth

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/mavsphere/mavsphere-layout-agent/pkg/config"
)

// ── Request/response types ────────────────────────────────────────────────────

type TokenLoginRequest struct {
	AgentToken string `json:"agentToken"`
	LayoutID   string `json:"layoutId"`
}

type LoginResponse struct {
	Token string `json:"token"`
}

// tokenRejectResponse is the shape of a rejection body from /api/agent/token-login.
type tokenRejectResponse struct {
	// The backend returns a plain string message on rejection.
	// We read raw text rather than a structured body.
}

// rateLimitResponse mirrors the backend's LoginRateLimitResponse DTO.
type rateLimitResponse struct {
	Error             string `json:"error"`
	Message           string `json:"message"`
	RetryAfterSeconds int64  `json:"retryAfterSeconds"`
	BlockedBy         string `json:"blockedBy"` // "username" | "ip" | "both"
}

// ── Sentinel errors ───────────────────────────────────────────────────────────

// ErrTokenRevoked is returned when the backend explicitly rejects an agent token
// as invalid or revoked (401 with a body containing "invalid" or "revoked").
// The caller should clear the token from config and restart into pairing mode.
var ErrTokenRevoked = errors.New("agent token revoked or invalid")

// ErrRateLimited is returned when the server returns 429 (Too Many Requests).
var ErrRateLimited = errors.New("rate limited")

// RateLimitError carries the parsed backend rate-limit details.
type RateLimitError struct {
	RetryAfter time.Duration
	BlockedBy  string
	Message    string
}

func (e *RateLimitError) Error() string {
	return fmt.Sprintf("rate limited (blocked_by=%s, retry_after=%v): %s",
		e.BlockedBy, e.RetryAfter.Round(time.Second), e.Message)
}

func (e *RateLimitError) Is(target error) bool { return target == ErrRateLimited }

// AsRateLimit extracts a *RateLimitError from err if one is present.
func AsRateLimit(err error) (*RateLimitError, bool) {
	var rle *RateLimitError
	if errors.As(err, &rle) {
		return rle, true
	}
	return nil, false
}

// ── Login ─────────────────────────────────────────────────────────────────────

// Login authenticates the layout-agent against the backend using its agent
// token and returns a JWT.
func Login() (string, error) {
	cfg := config.Get()
	return loginWithToken(cfg)
}

func loginWithToken(cfg *config.AgentConfig) (string, error) {
	url := fmt.Sprintf("%s/api/agent/token-login", cfg.BackendURL)

	reqBody := TokenLoginRequest{
		AgentToken: cfg.AgentToken,
		LayoutID:   cfg.LayoutID,
	}

	data, err := json.Marshal(reqBody)
	if err != nil {
		return "", err
	}

	log.Printf("[auth] attempting token login for layout %s", cfg.LayoutID)

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Post(url, "application/json", bytes.NewBuffer(data))
	if err != nil {
		log.Printf("[auth] token login request failed: %v", err)
		return "", err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	switch resp.StatusCode {
	case http.StatusOK:
		// fall through to parse JWT

	case http.StatusUnauthorized:
		// 401 — could be:
		//   a) token explicitly revoked/invalid  → ErrTokenRevoked (clear + re-pair)
		//   b) transient auth issue (clock skew) → return transient error (retry)
		//
		// Distinguish by inspecting the response body. The backend returns a plain
		// string message like "Invalid or revoked agent token" on (a).
		bodyStr := strings.ToLower(string(body))
		if strings.Contains(bodyStr, "invalid") || strings.Contains(bodyStr, "revoked") {
			log.Printf("[auth] token rejected as invalid/revoked by backend — token should be cleared")
			return "", ErrTokenRevoked
		}
		// Transient — do not clear token, let the caller retry.
		log.Printf("[auth] token login got 401 without revocation body (transient?) — will retry")
		return "", fmt.Errorf("token login 401 (transient): %s", string(body))

	case http.StatusForbidden:
		// 403 — access denied (e.g. owner lost rail_host role). Treat as permanent.
		log.Printf("[auth] token login 403 — access denied")
		return "", ErrTokenRevoked

	case http.StatusTooManyRequests:
		rle := parseRateLimitBody(body, resp.Header.Get("Retry-After"))
		log.Printf("[auth] token login rate limited: %v", rle)
		return "", rle

	default:
		log.Printf("[auth] token login failed: HTTP %d — %s", resp.StatusCode, string(body))
		return "", fmt.Errorf("token login failed HTTP %d: %s", resp.StatusCode, string(body))
	}

	var loginResp LoginResponse
	if err := json.Unmarshal(body, &loginResp); err != nil {
		log.Printf("[auth] token login response parse error: %v", err)
		return "", err
	}
	if loginResp.Token == "" {
		log.Printf("[auth] token login succeeded but JWT was empty")
		return "", errors.New("token login returned empty JWT")
	}

	log.Printf("[auth] token login succeeded for layout %s", cfg.LayoutID)
	return loginResp.Token, nil
}

func parseRateLimitBody(body []byte, retryAfterHeader string) *RateLimitError {
	const fallbackWait = 15 * time.Minute

	var parsed rateLimitResponse
	if err := json.Unmarshal(body, &parsed); err == nil && parsed.RetryAfterSeconds > 0 {
		return &RateLimitError{
			RetryAfter: time.Duration(parsed.RetryAfterSeconds) * time.Second,
			BlockedBy:  parsed.BlockedBy,
			Message:    parsed.Message,
		}
	}

	if retryAfterHeader != "" {
		var secs int64
		if _, err := fmt.Sscanf(retryAfterHeader, "%d", &secs); err == nil && secs > 0 {
			return &RateLimitError{
				RetryAfter: time.Duration(secs) * time.Second,
				BlockedBy:  "unknown",
				Message:    "rate limited",
			}
		}
	}

	return &RateLimitError{
		RetryAfter: fallbackWait,
		BlockedBy:  "unknown",
		Message:    "rate limited",
	}
}
