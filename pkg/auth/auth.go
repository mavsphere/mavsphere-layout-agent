package auth

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/mavsphere/mavsphere-layout-agent/pkg/config"
)

type LoginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type LoginResponse struct {
	Token string `json:"token"`
}

// rateLimitResponse mirrors the backend's LoginRateLimitResponse DTO.
type rateLimitResponse struct {
	Error             string `json:"error"`
	Message           string `json:"message"`
	RetryAfterSeconds int64  `json:"retryAfterSeconds"`
	BlockedBy         string `json:"blockedBy"` // "username" | "ip" | "both"
}

// ErrBadCredentials is returned when the server explicitly rejects the credentials
// (HTTP 401 or 403). The agent should NOT retry automatically — this requires
// the user to fix the config and restart. Retrying would hammer the login rate
// limiter and lock the account for other users.
var ErrBadCredentials = errors.New("bad credentials")

// ErrRateLimited is returned when the server returns 429 (Too Many Requests).
// Use AsRateLimit to extract the wait duration and reason.
var ErrRateLimited = errors.New("rate limited")

// RateLimitError carries the parsed backend rate-limit details so callers can
// display an accurate countdown and decide whether to retry.
type RateLimitError struct {
	RetryAfter time.Duration
	BlockedBy  string // "username" | "ip" | "both"
	Message    string
}

func (e *RateLimitError) Error() string {
	return fmt.Sprintf("rate limited (blocked_by=%s, retry_after=%v): %s",
		e.BlockedBy, e.RetryAfter.Round(time.Second), e.Message)
}

func (e *RateLimitError) Is(target error) bool {
	return target == ErrRateLimited
}

// AsRateLimit extracts a *RateLimitError from err if one is present.
func AsRateLimit(err error) (*RateLimitError, bool) {
	var rle *RateLimitError
	if errors.As(err, &rle) {
		return rle, true
	}
	return nil, false
}

func Login() (string, error) {
	cfg := config.Get()
	loginURL := fmt.Sprintf("%s/api/auth/login", cfg.BackendURL)

	log.Printf("[auth] attempting login as '%s' at %s", cfg.Username, loginURL)

	reqBody := LoginRequest{
		Username: cfg.Username,
		Password: cfg.Password,
	}

	data, err := json.Marshal(reqBody)
	if err != nil {
		return "", err
	}

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Post(loginURL, "application/json", bytes.NewBuffer(data))
	if err != nil {
		log.Printf("[auth] login request failed: %v", err)
		return "", err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	switch resp.StatusCode {
	case http.StatusOK:
		// success — fall through

	case http.StatusUnauthorized, http.StatusForbidden:
		// Wrong credentials — don't retry, don't hammer the rate limiter.
		log.Printf("[auth] login rejected (HTTP %d) — check username and password in config", resp.StatusCode)
		return "", ErrBadCredentials

	case http.StatusTooManyRequests:
		rle := parseRateLimitBody(body, resp.Header.Get("Retry-After"))
		log.Printf("[auth] %s", rle.Error())
		return "", rle

	default:
		log.Printf("[auth] login failed: HTTP %d — %s", resp.StatusCode, string(body))
		return "", fmt.Errorf("login failed HTTP %d: %s", resp.StatusCode, string(body))
	}

	var loginResp LoginResponse
	if err := json.Unmarshal(body, &loginResp); err != nil {
		log.Printf("[auth] login response parse error: %v", err)
		return "", err
	}

	if loginResp.Token == "" {
		log.Printf("[auth] login succeeded but token was empty")
		return "", errors.New("login returned empty token")
	}

	log.Printf("[auth] login succeeded for '%s'", cfg.Username)
	return loginResp.Token, nil
}

// parseRateLimitBody tries to extract a structured RateLimitError from the 429 body.
// Falls back to a conservative 15-minute wait if the body cannot be parsed.
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

	// Try Retry-After header as fallback (RFC 7231 — integer seconds)
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
