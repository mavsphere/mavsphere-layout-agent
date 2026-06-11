// Package wsconn adapts a gorilla/websocket connection to the io.ReadWriteCloser
// interface expected by go-stomp, and provides a dialer with JWT auth.
package wsconn

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/go-stomp/stomp"
	"github.com/gorilla/websocket"
)

// HandshakeError carries the HTTP status for failed WebSocket upgrades.
type HandshakeError struct {
	Status int
	Err    error
}

func (e *HandshakeError) Error() string {
	if e.Status > 0 {
		return fmt.Sprintf("websocket handshake failed: status=%d err=%v", e.Status, e.Err)
	}
	return fmt.Sprintf("websocket handshake failed: %v", e.Err)
}

func (e *HandshakeError) Unwrap() error {
	return e.Err
}

// WSConn adapts a Gorilla WebSocket connection to io.ReadWriteCloser with
// streaming semantics for STOMP.
type WSConn struct {
	*websocket.Conn
	pending bytes.Buffer

	// Gorilla WebSocket requires a single writer at a time.
	writeMu      sync.Mutex
	writeTimeout time.Duration

	doneOnce sync.Once
	doneCh   chan struct{}
}

// NewWSConn opens a WebSocket connection using Bearer auth and layoutId header.
func NewWSConn(wsURL, token, layoutID string) (*WSConn, error) {
	u, err := url.Parse(wsURL)
	if err != nil {
		return nil, fmt.Errorf("parse ws url %q: %w", wsURL, err)
	}

	headers := http.Header{}
	if token != "" {
		headers.Set("Authorization", "Bearer "+token)
	}
	if layoutID != "" {
		headers.Set("layoutId", layoutID)
	}

	d := *websocket.DefaultDialer
	d.HandshakeTimeout = 15 * time.Second
	d.EnableCompression = true
	// Do not request a specific WebSocket subprotocol — Spring's WebSocket
	// endpoint does not advertise "v12.stomp" so gorilla rejects the mismatch.
	// STOMP version is negotiated at the STOMP protocol layer instead.

	wsConn, resp, err := d.Dial(u.String(), headers)
	if err != nil {
		status := 0
		if resp != nil {
			status = resp.StatusCode
		}
		return nil, &HandshakeError{Status: status, Err: err}
	}

	w := &WSConn{
		Conn:         wsConn,
		writeTimeout: 15 * time.Second,
		doneCh:       make(chan struct{}),
	}

	// Keep the connection alive with ping/pong.
	_ = wsConn.SetReadDeadline(time.Now().Add(60 * time.Second))
	wsConn.SetPongHandler(func(string) error {
		return wsConn.SetReadDeadline(time.Now().Add(60 * time.Second))
	})

	wsConn.SetCloseHandler(func(code int, text string) error {
		w.signalDone()
		return nil
	})

	go w.pingLoop(25*time.Second, 5*time.Second)

	return w, nil
}

func (w *WSConn) pingLoop(every time.Duration, writeBudget time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()

	for {
		select {
		case <-w.doneCh:
			return
		case <-t.C:
			w.writeMu.Lock()
			_ = w.SetWriteDeadline(time.Now().Add(writeBudget))
			_ = w.WriteControl(websocket.PingMessage, []byte("ping"), time.Now().Add(writeBudget))
			w.writeMu.Unlock()
		}
	}
}

func (w *WSConn) signalDone() {
	w.doneOnce.Do(func() {
		close(w.doneCh)
	})
}

// Read implements streaming semantics on top of message-framed WebSockets.
func (w *WSConn) Read(b []byte) (int, error) {
	for w.pending.Len() == 0 {
		mt, msg, err := w.ReadMessage()
		if err != nil {
			return 0, err
		}

		// STOMP frames may arrive as text or binary. Accept both.
		if mt == websocket.TextMessage || mt == websocket.BinaryMessage {
			_, _ = w.pending.Write(msg)
			break
		}
		// Ignore non-data frames.
	}

	return w.pending.Read(b)
}

// Write sends STOMP frames as text WebSocket messages.
func (w *WSConn) Write(b []byte) (int, error) {
	w.writeMu.Lock()
	defer w.writeMu.Unlock()

	if w.writeTimeout > 0 {
		_ = w.SetWriteDeadline(time.Now().Add(w.writeTimeout))
	}

	if err := w.WriteMessage(websocket.TextMessage, b); err != nil {
		return 0, err
	}
	return len(b), nil
}

func (w *WSConn) Close() error {
	w.signalDone()
	return w.Conn.Close()
}

// DialStompOverWebSocket dials WebSocket, then performs the STOMP CONNECT handshake.
// The entire operation (WebSocket dial + STOMP CONNECT frame exchange) is bounded
// by a 20-second timeout so a silent server-side rejection never hangs the agent.
func DialStompOverWebSocket(wsURL, token, layoutID string) (*stomp.Conn, error) {
	ws, err := NewWSConn(wsURL, token, layoutID)
	if err != nil {
		return nil, err
	}

	opts := []func(*stomp.Conn) error{
		stomp.ConnOpt.AcceptVersion(stomp.V12),
		stomp.ConnOpt.Host("/"),
		stomp.ConnOpt.HeartBeat(10*time.Second, 10*time.Second),
		stomp.ConnOpt.HeartBeatGracePeriodMultiplier(3.0),
		stomp.ConnOpt.HeartBeatError(35 * time.Second),
		stomp.ConnOpt.MsgSendTimeout(15 * time.Second),
		stomp.ConnOpt.ReadChannelCapacity(32),
		stomp.ConnOpt.WriteChannelCapacity(32),
		stomp.ConnOpt.ReadBufferSize(64 * 1024),
		stomp.ConnOpt.WriteBufferSize(64 * 1024),
	}

	if token != "" {
		opts = append(opts, stomp.ConnOpt.Header("Authorization", "Bearer "+token))
	}
	if layoutID != "" {
		opts = append(opts, stomp.ConnOpt.Header("layoutId", layoutID))
	}

	// stomp.Connect blocks waiting for the CONNECTED frame. If the backend
	// rejects the CONNECT silently (no frame sent back), this hangs forever.
	// Wrap it in a goroutine with a timeout to surface the failure cleanly.
	type result struct {
		conn *stomp.Conn
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		conn, err := stomp.Connect(ws, opts...)
		ch <- result{conn, err}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	select {
	case r := <-ch:
		if r.err != nil {
			_ = ws.Close()
			return nil, fmt.Errorf("stomp handshake: %w", r.err)
		}
		return r.conn, nil
	case <-ctx.Done():
		_ = ws.Close()
		return nil, fmt.Errorf("stomp handshake timeout (20s): backend did not send CONNECTED frame — check StompAuthChannelInterceptor logs")
	}
}
