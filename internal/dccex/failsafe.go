// Package dccex — failsafe dead-man timer.
//
// The failsafe monitors only moving locos. A non-zero speed command arms or
// refreshes the timer; an explicit zero-speed command or emergency stop releases
// the loco from monitoring. This prevents a stopped train from repeatedly
// generating stop commands while still protecting against lost browser/backend
// control streams during movement.
package dccex

import (
	"log"
	"sync"
	"time"
)

type Failsafe struct {
	controlTimeoutMs int
	reissueStopMs    int
	client           *Client
	logger           *log.Logger

	mu       sync.Mutex
	active   map[int]time.Time // dccAddr → last moving-control heartbeat
	stopSent map[int]bool
	lastStop map[int]time.Time
}

func NewFailsafe(controlTimeoutMs, reissueStopMs int, client *Client, logger *log.Logger) *Failsafe {
	f := &Failsafe{
		controlTimeoutMs: controlTimeoutMs,
		reissueStopMs:    reissueStopMs,
		client:           client,
		logger:           logger,
		active:           make(map[int]time.Time),
		stopSent:         make(map[int]bool),
		lastStop:         make(map[int]time.Time),
	}
	go f.loop()
	return f
}

// Heartbeat records that a moving control command was received for this DCC address.
// Prefer ArmMoving/Release for new call sites; Heartbeat is retained as an alias
// for existing callers that already mean "moving command received".
func (f *Failsafe) Heartbeat(dccAddr int) {
	f.ArmMoving(dccAddr)
}

// ArmMoving arms or refreshes failsafe monitoring for a moving loco.
func (f *Failsafe) ArmMoving(dccAddr int) {
	f.mu.Lock()
	f.active[dccAddr] = time.Now()
	f.stopSent[dccAddr] = false
	delete(f.lastStop, dccAddr)
	f.mu.Unlock()
}

// Refresh refreshes an already-active failsafe entry without creating a new one.
// This lets CONTROL_PING keep a moving loco alive, while avoiding phantom active
// entries for stopped trains.
func (f *Failsafe) Refresh(dccAddr int) {
	f.mu.Lock()
	if _, ok := f.active[dccAddr]; ok {
		f.active[dccAddr] = time.Now()
		f.stopSent[dccAddr] = false
		delete(f.lastStop, dccAddr)
	}
	f.mu.Unlock()
}

// Release removes a DCC address from active monitoring.
func (f *Failsafe) Release(dccAddr int) {
	f.mu.Lock()
	delete(f.active, dccAddr)
	delete(f.stopSent, dccAddr)
	delete(f.lastStop, dccAddr)
	f.mu.Unlock()
}

func (f *Failsafe) loop() {
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for range tick.C {
		f.mu.Lock()
		now := time.Now()
		timeout := time.Duration(f.controlTimeoutMs) * time.Millisecond
		reissue := time.Duration(f.reissueStopMs) * time.Millisecond
		for addr, last := range f.active {
			if now.Sub(last) <= timeout {
				continue
			}

			lastStop := f.lastStop[addr]
			shouldSend := !f.stopSent[addr] || lastStop.IsZero() || now.Sub(lastStop) >= reissue
			if !shouldSend {
				continue
			}

			if !f.stopSent[addr] {
				f.logger.Printf("[failsafe] timeout on DCC %d — sending stop", addr)
			}
			if err := f.client.StopLoco(addr); err != nil {
				f.logger.Printf("[failsafe] stop error: %v", err)
			}
			f.stopSent[addr] = true
			f.lastStop[addr] = now
		}
		f.mu.Unlock()
	}
}
