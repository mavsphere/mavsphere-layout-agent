// Package commandstation — generic dead-man failsafe.
//
// This replaces the dccex-specific Failsafe with an identical implementation
// that accepts any CommandStation. The dccex.Failsafe still exists for
// backwards compatibility but main.go should use this one going forward.
package commandstation

import (
	"log"
	"sync"
	"time"
)

// Failsafe monitors moving locos and issues stop commands if heartbeats stop.
// Only addresses that have been armed (ArmMoving) are monitored — stopped
// trains are not watched.
type Failsafe struct {
	controlTimeoutMs int
	reissueStopMs    int
	cs               CommandStation
	logger           *log.Logger

	mu       sync.Mutex
	active   map[int]time.Time // dccAddr → last moving-control heartbeat
	stopSent map[int]bool
	lastStop map[int]time.Time
}

// NewFailsafe creates a Failsafe against the given CommandStation.
func NewFailsafe(controlTimeoutMs, reissueStopMs int, cs CommandStation, logger *log.Logger) *Failsafe {
	f := &Failsafe{
		controlTimeoutMs: controlTimeoutMs,
		reissueStopMs:    reissueStopMs,
		cs:               cs,
		logger:           logger,
		active:           make(map[int]time.Time),
		stopSent:         make(map[int]bool),
		lastStop:         make(map[int]time.Time),
	}
	go f.loop()
	return f
}

// ArmMoving arms or refreshes failsafe monitoring for a moving loco.
func (f *Failsafe) ArmMoving(dccAddr int) {
	f.mu.Lock()
	f.active[dccAddr] = time.Now()
	f.stopSent[dccAddr] = false
	delete(f.lastStop, dccAddr)
	f.mu.Unlock()
}

// Refresh refreshes an already-active entry without creating a new one.
// Used by CONTROL_PING to keep a moving loco alive.
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
			if err := f.cs.StopLoco(addr); err != nil {
				f.logger.Printf("[failsafe] stop error: %v", err)
			}
			f.stopSent[addr] = true
			f.lastStop[addr] = now
		}
		f.mu.Unlock()
	}
}
