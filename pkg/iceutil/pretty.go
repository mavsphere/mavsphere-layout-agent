package iceutil

import (
	"net/url"
	"strings"
)

// PrettyTurnVariant returns a safe, human-friendly label for the TURN URL.
// It strips creds and normalizes transport hints so logs don't leak secrets.
func PrettyTurnVariant(raw string) string {
	if strings.TrimSpace(raw) == "" {
		return "none"
	}
	// Ensure parseable
	s := raw
	if !strings.Contains(s, "://") {
		if strings.HasPrefix(s, "turns:") {
			s = "turns://" + strings.TrimPrefix(s, "turns:")
		} else if strings.HasPrefix(s, "turn:") {
			s = "turn://" + strings.TrimPrefix(s, "turn:")
		}
	}
	u, err := url.Parse(s)
	if err != nil {
		return "invalid"
	}
	// Strip userinfo (credentials)
	u.User = nil
	transport := u.Query().Get("transport")
	if transport == "" && u.Scheme == "turns" {
		// Default to TCP for turns://
		transport = "tcp"
	}
	host := u.Host
	if transport != "" {
		return u.Scheme + "://" + host + "?transport=" + transport
	}
	return u.Scheme + "://" + host
}

func maskSecret(s string, keep int) string {
	if s == "" {
		return ""
	}
	if len(s) <= keep {
		return strings.Repeat("*", len(s))
	}
	return s[:keep] + "…" + strings.Repeat("*", len(s)-keep)
}

func maskTurnUser(u string) string {
	if u == "" {
		return ""
	}
	parts := strings.Split(u, ":")
	if len(parts) >= 2 {
		return parts[0] + ":" + parts[1]
	}
	return u
}
