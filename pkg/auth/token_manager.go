package auth

import (
	"context"
	"sync"
)

var (
	mu    sync.RWMutex
	token string
)

// CurrentToken returns the in-memory token (may be empty).
func CurrentToken() string {
	mu.RLock()
	defer mu.RUnlock()
	return token
}

// SetToken stores the token in memory.
func SetToken(t string) {
	mu.Lock()
	token = t
	mu.Unlock()
}

// EnsureToken returns a token, logging in if none is set.
func EnsureToken(ctx context.Context) (string, error) {
	t := CurrentToken()
	if t != "" {
		return t, nil
	}
	nt, err := Login()
	if err != nil {
		return "", err
	}
	SetToken(nt)
	return nt, nil
}

// RefreshToken forces a re-login and returns the new token.
func RefreshToken(ctx context.Context) (string, error) {
	nt, err := Login()
	if err != nil {
		return "", err
	}
	SetToken(nt)
	return nt, nil
}
