// Package dccex provides a client for the DCC-EX command station.
//
// DCC-EX accepts text commands over serial or TCP/WiFi (same protocol):
//
//	<t 1 DCC_ADDR SPEED DIRECTION>  — throttle
//	<T TURNOUT_ID THROWN>           — set turnout
//	<1 MAIN>                        — track power on
//	<0>                             — emergency stop / power off
//	<R CV CALLBACK CALLBACKSUB>     — read CV (programming track only)
//	<W CV VALUE CALLBACK CALLBACKSUB> — write CV (programming track)
//	<w DCC_ADDR CV VALUE>           — write CV on main (ops mode)
//
// Responses come back as <tag data...> on the same connection.
// This client handles both serial and TCP connections transparently.
package dccex

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"runtime"
	"strings"
	"sync"
	"time"

	"go.bug.st/serial"
)

// Command represents a raw DCC-EX command string (without angle brackets).
type Command string

// Response is one parsed response from DCC-EX.
type Response struct {
	Raw  string
	Tag  string
	Data []string
}

// Client manages the connection to a DCC-EX command station.
type Client struct {
	port    string
	baud    int
	timeout time.Duration
	logger  *log.Logger

	mu        sync.Mutex
	conn      io.ReadWriteCloser
	scanner   *bufio.Scanner
	respCh    chan Response
	stopCh    chan struct{}
	closeOnce sync.Once
	isTCP     bool
}

// NewClient creates a new DCC-EX client.
// port: "/dev/ttyUSB0" (serial) or "192.168.1.100:2560" (TCP/WiFi).
func NewClient(port string, baud int, timeoutMs int, logger *log.Logger) *Client {
	return &Client{
		port:    port,
		baud:    baud,
		timeout: time.Duration(timeoutMs) * time.Millisecond,
		logger:  logger,
		respCh:  make(chan Response, 64),
		stopCh:  make(chan struct{}),
		isTCP:   strings.Contains(port, ":"),
	}
}

// Connect opens the connection to DCC-EX.
func (c *Client) Connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		return nil
	}

	var conn io.ReadWriteCloser
	var err error

	if c.isTCP {
		conn, err = net.DialTimeout("tcp", c.port, 5*time.Second)
	} else {
		conn, err = openSerial(c.port, c.baud)
	}
	if err != nil {
		return fmt.Errorf("dccex connect %s: %w", c.port, err)
	}

	c.conn = conn
	c.scanner = bufio.NewScanner(conn)
	c.logger.Printf("[dccex] connected to %s", c.port)

	go c.readLoop(conn, c.scanner)
	return nil
}

func openSerial(port string, baud int) (io.ReadWriteCloser, error) {
	if baud <= 0 {
		baud = 115200
	}
	mode := &serial.Mode{
		BaudRate: baud,
		DataBits: 8,
		Parity:   serial.NoParity,
		StopBits: serial.OneStopBit,
	}
	s, err := serial.Open(port, mode)
	if err != nil {
		if runtime.GOOS == "linux" {
			return nil, fmt.Errorf("open serial %s at %d baud: %w (check device mapping/permissions; Docker must see the device)", port, baud, err)
		}
		return nil, fmt.Errorf("open serial %s at %d baud: %w", port, baud, err)
	}
	return s, nil
}

func (c *Client) readLoop(conn io.ReadWriteCloser, scanner *bufio.Scanner) {
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || !strings.HasPrefix(line, "<") {
			continue
		}
		resp := parseResponse(line)
		select {
		case c.respCh <- resp:
		default:
			c.logger.Printf("[dccex] response buffer full, dropping: %s", line)
		}
	}

	c.mu.Lock()
	if c.conn == conn {
		c.conn = nil
		c.scanner = nil
	}
	c.mu.Unlock()

	_ = conn.Close()
	if err := scanner.Err(); err != nil {
		c.logger.Printf("[dccex] read loop ended: %v", err)
	} else {
		c.logger.Printf("[dccex] read loop ended")
	}
}

func parseResponse(raw string) Response {
	raw = strings.TrimPrefix(raw, "<")
	raw = strings.TrimSuffix(raw, ">")
	parts := strings.Fields(raw)
	if len(parts) == 0 {
		return Response{Raw: raw}
	}
	return Response{Raw: raw, Tag: parts[0], Data: parts[1:]}
}

// Send writes a command to DCC-EX. cmd should be the inner content without < >.
func (c *Client) Send(cmd string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		return fmt.Errorf("not connected")
	}
	_, err := fmt.Fprintf(c.conn, "<%s>\r\n", cmd)
	return err
}

// Responses returns the channel of parsed responses from DCC-EX.
func (c *Client) Responses() <-chan Response { return c.respCh }

// ── High-level commands ───────────────────────────────────────────────────────

// SetLocoSpeed sets throttle. speed 0..126, direction 1=fwd 0=rev.
func (c *Client) SetLocoSpeed(regID, dccAddr, speed, direction int) error {
	return c.Send(fmt.Sprintf("t %d %d %d %d", regID, dccAddr, speed, direction))
}

// StopLoco sends emergency stop for one address.
func (c *Client) StopLoco(dccAddr int) error {
	return c.Send(fmt.Sprintf("t 1 %d 0 1", dccAddr))
}

// EmergencyStop cuts all track power.
func (c *Client) EmergencyStop() error { return c.Send("0") }

// SetTrackPower sets power on MAIN ("1 MAIN") or off ("0").
func (c *Client) SetTrackPower(on bool, district string) error {
	if on {
		return c.Send(fmt.Sprintf("1 %s", district))
	}
	return c.Send("0")
}

// SetTurnout sets a DCC turnout. state: "T" thrown, "C" closed.
func (c *Client) SetTurnout(turnoutID int, thrown bool) error {
	state := "C"
	if thrown {
		state = "T"
	}
	return c.Send(fmt.Sprintf("T %d %s", turnoutID, state))
}

// SetFunction sets a decoder function. state: 1=on, 0=off.
func (c *Client) SetFunction(dccAddr, fn, state int) error {
	return c.Send(fmt.Sprintf("F %d %d %d", dccAddr, fn, state))
}

// ReadCV reads a CV from the programming track.
// callbackNum and callbackSub are used to correlate the response.
func (c *Client) ReadCV(cv, callbackNum, callbackSub int) error {
	return c.Send(fmt.Sprintf("R %d %d %d", cv, callbackNum, callbackSub))
}

// WriteCVProgrammingTrack writes a CV on the programming track.
func (c *Client) WriteCVProgrammingTrack(cv, value, callbackNum, callbackSub int) error {
	return c.Send(fmt.Sprintf("W %d %d %d %d", cv, value, callbackNum, callbackSub))
}

// WriteCVOnMain writes a CV in ops mode (no readback).
func (c *Client) WriteCVOnMain(dccAddr, cv, value int) error {
	return c.Send(fmt.Sprintf("w %d %d %d", dccAddr, cv, value))
}

// Close shuts down the connection.
func (c *Client) Close() error {
	c.closeOnce.Do(func() {
		close(c.stopCh)
	})
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		err := c.conn.Close()
		c.conn = nil
		c.scanner = nil
		return err
	}
	return nil
}

func (c *Client) IsConnected() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn != nil
}
